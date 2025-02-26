package com.vidyasource


import dev.langchain4j.data.document.BlankDocumentException
import dev.langchain4j.data.document.DocumentParser
import dev.langchain4j.data.document.loader.FileSystemDocumentLoader
import dev.langchain4j.data.document.parser.apache.tika.ApacheTikaDocumentParser
import dev.langchain4j.model.ollama.OllamaChatModel
import jakarta.annotation.PreDestroy
import org.apache.commons.net.ftp.FTPClient
import org.apache.tika.config.TikaConfig
import org.apache.tika.metadata.Metadata
import org.apache.tika.parser.AutoDetectParser
import org.apache.tika.parser.ParseContext
import org.apache.tika.sax.BodyContentHandler
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.bind.ConstructorBinding
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Lazy
import org.springframework.expression.common.LiteralExpression
import org.springframework.http.HttpMethod
import org.springframework.http.MediaType
import org.springframework.integration.annotation.ServiceActivator
import org.springframework.integration.channel.DirectChannel
import org.springframework.integration.channel.ExecutorChannel
import org.springframework.integration.dsl.IntegrationFlow
import org.springframework.integration.dsl.integrationFlow
import org.springframework.integration.file.remote.gateway.AbstractRemoteFileOutboundGateway
import org.springframework.integration.file.support.FileExistsMode
import org.springframework.integration.ftp.filters.FtpRegexPatternFileListFilter
import org.springframework.integration.ftp.gateway.FtpOutboundGateway
import org.springframework.integration.ftp.session.DefaultFtpSessionFactory
import org.springframework.integration.ftp.session.FtpFileInfo
import org.springframework.integration.http.outbound.HttpRequestExecutingMessageHandler
import org.springframework.integration.support.MessageBuilder
import org.springframework.messaging.Message
import org.springframework.messaging.MessageHeaders
import org.testcontainers.DockerClientFactory
import org.testcontainers.ollama.OllamaContainer
import org.testcontainers.utility.DockerImageName
import java.io.File
import java.io.IOException
import java.util.concurrent.Executors
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi

val OLLAMA_IMAGE = "ollama/ollama:latest"
val DOCUMENT_MODEL = "llama3.2:1b"
val IMAGE_MODEL = "llama3.2-vision:11b"
val NEW_DOC_IMAGE = "vidyasource/ollama-test-docs"
val NEW_IMAGE_IMAGE = "vidyasource/ollama-test-images"

@ConfigurationProperties(prefix = "ftp")
data class FtpProperties @ConstructorBinding constructor(
    val host: String,
    val port: Int,
    val username: String,
    val password: String
)

data class LibraryFile(val fullPathName: String, val timestamp: Long)


@Configuration
class FtpConnectionConfiguration(val ftpProperties: FtpProperties) {
    @Autowired
    private lateinit var ollamaDocAIContainer: OllamaContainer
    @Autowired
    private lateinit var ollamaImageAIContainer: OllamaContainer

    @Bean
    fun chatModel(ollamaDocAIContainer: OllamaContainer): OllamaChatModel {
        return OllamaChatModel.builder()
            .baseUrl(ollamaDocAIContainer.getEndpoint())
            .temperature(0.0)
            .logRequests(true)
            .logResponses(true)
            .modelName(DOCUMENT_MODEL)
            .build()
    }

    fun ftpSessionFactory() = DefaultFtpSessionFactory().apply {
        setHost(ftpProperties.host)
        setPort(ftpProperties.port)
        setUsername(ftpProperties.username)
        setPassword(ftpProperties.password)
        setClientMode(FTPClient.PASSIVE_LOCAL_DATA_CONNECTION_MODE)
    }


    @Bean
    fun inputChannel() = DirectChannel()

    @Bean
    fun startupRunner(@Qualifier("inputChannel") inputChannel: DirectChannel) = ApplicationRunner {
        inputChannel.send(MessageBuilder.withPayload("").build()) // Empty payload to trigger the flow
    }

    @Bean
    fun listOutboundGateway() = FtpOutboundGateway(ftpSessionFactory(), "ls", "'RFI No. LVA-AI-25-009'").apply {
        setOption(
            AbstractRemoteFileOutboundGateway.Option.RECURSIVE,
            AbstractRemoteFileOutboundGateway.Option.PRESERVE_TIMESTAMP
        )
        setFilter(FtpRegexPatternFileListFilter("""^(?!.*\.zip$)(?!\.DS_Store$).*"""))
    }

    @Bean
    fun getFileOutboundGateway() = FtpOutboundGateway(ftpSessionFactory(), "get").apply {
        setLocalDirectoryExpression(LiteralExpression("local/"))
        setFileExistsMode(FileExistsMode.APPEND)
        setLocalFilenameGeneratorExpressionString("#remoteFileName.replace(' ', '-')")
    }


    @Bean
    fun downloadFlow(
        inputChannel: DirectChannel,
        listOutboundGateway: FtpOutboundGateway,
        getFileOutboundGateway: FtpOutboundGateway
    ): IntegrationFlow {
        return integrationFlow(inputChannel) {
           log("starting download flow")
            handle(listOutboundGateway)
            split<Message<*>> { it.payload }
            transform<Message<FtpFileInfo>> { "${it.payload.remoteDirectory}${it.payload.filename}" }
            channel { queue(50) }
            handle(getFileOutboundGateway)
            route<Message<File>> { m ->
                if (m.headers["file_remoteFile"].toString().lowercase().endsWith(".jpg")) "imageChannel" else "textChannel"
            }
        }
    }


    @Bean
    fun textChannel() = ExecutorChannel(Executors.newCachedThreadPool())

    @Bean
    fun imageChannel() = ExecutorChannel(Executors.newCachedThreadPool())


    @Bean
    @Lazy
    fun documentParser(): ApacheTikaDocumentParser {
        val tikaConfig = TikaConfig(ClassLoader.getSystemResourceAsStream("tika-config.xml"))

        return ApacheTikaDocumentParser(
            { AutoDetectParser(tikaConfig) },
            { BodyContentHandler(-1) },
            { Metadata() },
            { ParseContext() },
            true
        )
    }


    @Bean
    @ServiceActivator(inputChannel = "imageChannel", autoStartup = "false")
    fun httpHandler(): HttpRequestExecutingMessageHandler {
        return HttpRequestExecutingMessageHandler("${ollamaImageAIContainer.getEndpoint()}/api/chat").apply {
            setHttpMethod(HttpMethod.POST)
            setExpectedResponseType(String::class.java)

            // Custom message converters
            setMessageConverters(
                listOf(
                    org.springframework.http.converter.json.MappingJackson2HttpMessageConverter(),
                    org.springframework.http.converter.StringHttpMessageConverter()
                )
            )

        }
    }


    data class VisionApiRequest(
        val model: String,
        val messages: List<ApiMessage>
    )

    data class ApiMessage(
        val role: String,
        val content: String,
        val images: List<String>
    )


    @OptIn(ExperimentalEncodingApi::class)
    @Bean
    fun imageProcessingFlow(httpHandler: HttpRequestExecutingMessageHandler): IntegrationFlow {
        return integrationFlow("imageChannel") {
            log("starting imageProcessing flow")
            channel { queue() }
            transform<Message<File>> { m ->
                println("Image processor flow")
                val base64 = Base64.encode(m.payload.readBytes())
                val prompt = """
                    Describe the image in as much detail as possible and parse the following info in your write up to discover dates, locations, organizations, occasions, and other entities for context: {image_name: %s}
                """.trimIndent()
                val path = "${m.headers["file_remoteDirectory"]}${m.headers["file_remoteFile"]}"
                VisionApiRequest(
                    model = IMAGE_MODEL,
                    messages = listOf(
                        ApiMessage(
                            role = "user",
                            content = String.format(prompt, path),
                            images = listOf(base64)
                        )
                    )
                )
            }
            enrichHeaders { header<String>(MessageHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE) }
            handle(httpHandler)
            handle { m: Message<*> ->
                println("Image processor result for ${m.headers["file_remoteDirectory"]}${m.headers["file_remoteFile"]}: ${m.payload}")
            }
        }
    }

    @Bean
    fun documentProcessingFlow(parser: DocumentParser, chatModel: OllamaChatModel): IntegrationFlow {
        val prompt = """
            This is the file path of a document which may contain hyphenated dates and the names of organizations and events related to the document: %s

            Below is the full content of the document:
            %s

            Summarize the document. Ignore the top-level directory in the file path if present, but use other information in the file path to add context to the summary.
        """.trimIndent()
        return integrationFlow("textChannel") {
            log("starting documentProcessingFlow")
            channel { queue() }
            transform<Message<File>> { m ->
                println("Doc processor headers: ${m.headers}")
                val path = "${m.headers["file_remoteDirectory"]}${m.headers["file_remoteFile"]}"
                println("Doc processor path: $path")
                try {
                    val document = FileSystemDocumentLoader.loadDocument(m.payload.absolutePath, parser)
                    val summary = chatModel.chat(String.format(prompt, path, document.text()))
                    "Summary of ${m.headers["file_remoteFile"]}: $summary"
                } catch (e: BlankDocumentException) {
                    "Document ${m.headers["file_remoteFile"]} was blank: probably a PDF that was a scan."
                }
            }
            handle { m: Message<*> ->
                if (!m.payload.toString().contains("blank", ignoreCase = true)) {
                    println("Doc processor result for ${m.headers["file_remoteDirectory"]}${m.headers["file_remoteFile"]}: ${m.payload}")
                }
            }
        }
    }


    @PreDestroy
    fun cleanup() {
        ollamaDocAIContainer.stop()
        ollamaImageAIContainer.stop()
    }
}

@Configuration
class OllamaConfiguration {
    @Bean
    fun ollamaDocAIContainer(): OllamaContainer {
        println("Starting ollama doc container init ...")
        val dockerImageName = DockerImageName.parse(OLLAMA_IMAGE)
        val dockerClient = DockerClientFactory.instance().client()
        val images = dockerClient.listImagesCmd().withReferenceFilter(NEW_DOC_IMAGE).exec()
        val ollama = if (images.isEmpty()) {
            OllamaContainer(dockerImageName)
        } else {
            OllamaContainer(DockerImageName.parse(NEW_DOC_IMAGE).asCompatibleSubstituteFor(OLLAMA_IMAGE))
        }
        ollama.start()
        println("Start pulling the doc model ... would take several minutes ...")
        try {
            ollama.execInContainer("ollama", "pull", DOCUMENT_MODEL)
        } catch (e: IOException) {
            throw RuntimeException("Error pulling doc model", e);
        } catch (e: InterruptedException) {
            throw RuntimeException("Error pulling doc model", e);
        }
        ollama.commitToImage(NEW_DOC_IMAGE)
        println("Doc image endpoint ${ollama.getEndpoint()}")
        return ollama
    }

    @Bean
    fun ollamaImageAIContainer(): OllamaContainer {
        println("Starting ollama image container init ...")
        val dockerImageName = DockerImageName.parse(OLLAMA_IMAGE)
        val dockerClient = DockerClientFactory.instance().client()
        val images = dockerClient.listImagesCmd().withReferenceFilter(NEW_IMAGE_IMAGE).exec()
        val ollama = if (images.isEmpty()) {
            OllamaContainer(dockerImageName)
        } else {
            OllamaContainer(DockerImageName.parse(NEW_IMAGE_IMAGE).asCompatibleSubstituteFor(OLLAMA_IMAGE))
        }
        ollama.start()
        println("Start pulling the image model ... would take several minutes ...")
        try {
            ollama.execInContainer("ollama", "pull", IMAGE_MODEL)
        } catch (e: IOException) {
            throw RuntimeException("Error pulling image model", e);
        } catch (e: InterruptedException) {
            throw RuntimeException("Error pulling image model", e);
        }
        ollama.commitToImage(NEW_IMAGE_IMAGE)
        println("Image image endpoint ${ollama.getEndpoint()}")
        return ollama
    }
}

