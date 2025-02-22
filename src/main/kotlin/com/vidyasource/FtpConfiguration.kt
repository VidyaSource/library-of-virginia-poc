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
import org.springframework.integration.channel.QueueChannel
import org.springframework.integration.dsl.IntegrationFlow
import org.springframework.integration.dsl.integrationFlow
import org.springframework.integration.file.FileHeaders
import org.springframework.integration.file.FileReadingMessageSource
import org.springframework.integration.file.filters.RegexPatternFileListFilter
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
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi

val OLLAMA_IMAGE = "ollama/ollama:latest"
val DOCUMENT_MODEL = "llama3.2:1b"
val IMAGE_MODEL = "llama3.2-vision:11b"
val NEW_IMAGE_NAME = "vidyasource/ollama-test"

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
    @Lazy
    private lateinit var ollama: OllamaContainer

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
    fun listOutboundGateway() = FtpOutboundGateway(ftpSessionFactory(), "ls").apply {
        setOption(AbstractRemoteFileOutboundGateway.Option.RECURSIVE)
        setFilter(FtpRegexPatternFileListFilter("""^(?!.*\.zip$).*"""))
    }

    @Bean
    fun getFileOutboundGateway() = FtpOutboundGateway(ftpSessionFactory(), "get").apply {
        setLocalDirectoryExpression(LiteralExpression("local/"))
        //setOption(AbstractRemoteFileOutboundGateway.Option.STREAM)
        setFileExistsMode(FileExistsMode.APPEND)
        setSendTimeout(Long.MAX_VALUE)
        setLocalFilenameGeneratorExpressionString("#remoteFileName.replace(' ', '-')")
    }


    fun fileTransformer(ftpFileInfo: FtpFileInfo): LibraryFile {
        return LibraryFile(
            fullPathName = ftpFileInfo.filename,
            timestamp = ftpFileInfo.modified
        )
    }


    @Bean
    fun fileReadingMessageSource(): FileReadingMessageSource = FileReadingMessageSource(10).apply {
        setDirectory(File("/Users/neilchaudhuri/Vidya/applications/library-of-virginia-poc/local"))
        setAutoCreateDirectory(true)

        setFilter(RegexPatternFileListFilter("""^(?!.*\.writing$)(?!\.DS_Store$).*"""))
        setScanEachPoll(true)
        isUseWatchService = true
        setWatchMaxDepth(2)

    }


    @Bean
    fun downloadFlow(
        inputChannel: DirectChannel,
        listOutboundGateway: FtpOutboundGateway,
        getFileOutboundGateway: FtpOutboundGateway
    ): IntegrationFlow {
        return integrationFlow(inputChannel) {
            log("starting integration flow")
            handle(listOutboundGateway)
            split<Message<*>> { it.payload }
            transform<FtpFileInfo> { fileTransformer(it) }
            channel { queue(10) }
            transform<LibraryFile> { "${it.fullPathName}" }
            handle(getFileOutboundGateway)

            handle { file: Message<*> ->
                println("Flow 1: $file")
            }
        }
    }

    @Bean
    fun textChannel() = QueueChannel(10)

    @Bean
    fun imageChannel() = QueueChannel(10)


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
        return HttpRequestExecutingMessageHandler("http://localhost:11434/api/chat").apply {
            setHttpMethod(HttpMethod.POST)
            setExpectedResponseType(String::class.java)

            setMessageConverters(
                listOf(
                    org.springframework.http.converter.json.MappingJackson2HttpMessageConverter(),
                    org.springframework.http.converter.StringHttpMessageConverter()
                )
            )

        }
    }

    @Bean
    @Lazy
    fun chatModel(ollama: OllamaContainer): OllamaChatModel {
        return OllamaChatModel.builder()
            .baseUrl(ollama.getEndpoint())
            .temperature(0.0)
            .logRequests(true)
            .logResponses(true)
            .modelName(DOCUMENT_MODEL)
            .build()
    }


    @Bean
    fun fileReaderFlow(fileReadingMessageSource: FileReadingMessageSource): IntegrationFlow {
        return integrationFlow(fileReadingMessageSource) {
            log("starting file reader flow")
            route<Message<*>> { m ->
                if (m.headers[FileHeaders.FILENAME].toString().endsWith(".jpg")) "imageChannel" else "textChannel"
            }
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
            transform<Message<File>> { m ->
                val base64 = Base64.encode(m.payload.readBytes())
                VisionApiRequest(
                    model = IMAGE_MODEL,
                    messages = listOf(
                        ApiMessage(
                            role = "user",
                            content = "Describe this image in detail. Note any prominent individuals in Virginia politics or other positions of influence.",
                            images = listOf(base64)
                        )
                    )
                )
            }
            enrichHeaders { header<String>(MessageHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE) }
            handle(httpHandler)
            handle { m: Message<*> ->
                println("Image processor result: $m")
            }
        }
    }

    @Bean
    fun documentProcessingFlow(parser: DocumentParser, chatModel: OllamaChatModel, imageChannel: QueueChannel): IntegrationFlow {
        return integrationFlow("textChannel") {
            transform<Message<File>> { m ->
                try {
                    val document = FileSystemDocumentLoader.loadDocument(m.payload.absolutePath, parser)
                    val summary = chatModel.chat("Summarize this document: ${document.text()}")
                    "Summary of ${m.headers[FileHeaders.FILENAME]}: $summary"
                } catch (e: BlankDocumentException) {
                    "Document ${m.headers[FileHeaders.FILENAME]} was blank: probably a PDF that was a scan."
                }
            }
            handle { m: Message<*> ->
                println("Doc processor: $m")
            }
        }
    }

    @PreDestroy
    fun cleanup() {
        ollama.stop()
    }
}

@Configuration
class OllamaConfiguration {
    @Bean
    @Lazy
    fun ollama(): OllamaContainer {
        val dockerImageName = DockerImageName.parse(OLLAMA_IMAGE)
        val dockerClient = DockerClientFactory.instance().client()
        val images = dockerClient.listImagesCmd().withReferenceFilter(NEW_IMAGE_NAME).exec()
        val ollama = if (images.isEmpty()) {
            OllamaContainer(dockerImageName)
        } else {
            OllamaContainer(DockerImageName.parse(NEW_IMAGE_NAME).asCompatibleSubstituteFor(OLLAMA_IMAGE))
        }
        ollama.start()
        try {
            ollama.execInContainer("ollama", "pull", DOCUMENT_MODEL)
            ollama.execInContainer("ollama", "pull", IMAGE_MODEL)
        } catch (e: IOException) {
            throw RuntimeException("Error pulling model", e);
        } catch (e: InterruptedException) {
            throw RuntimeException("Error pulling model", e);
        }
        ollama.commitToImage(NEW_IMAGE_NAME)
        return ollama
    }
}