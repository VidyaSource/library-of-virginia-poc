package com.vidyasource


import dev.langchain4j.data.document.parser.apache.tika.ApacheTikaDocumentParser
import org.apache.commons.net.ftp.FTPClient
import org.apache.tika.config.TikaConfig
import org.apache.tika.parser.AutoDetectParser
import org.apache.tika.parser.ParseContext
import org.apache.tika.sax.BodyContentHandler
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.bind.ConstructorBinding
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.expression.common.LiteralExpression
import org.springframework.integration.channel.DirectChannel
import org.springframework.integration.dsl.IntegrationFlow
import org.springframework.integration.dsl.integrationFlow
import org.springframework.integration.file.FileReadingMessageSource
import org.springframework.integration.file.filters.RegexPatternFileListFilter
import org.springframework.integration.file.remote.gateway.AbstractRemoteFileOutboundGateway
import org.springframework.integration.file.support.FileExistsMode
import org.springframework.integration.ftp.filters.FtpRegexPatternFileListFilter
import org.springframework.integration.ftp.gateway.FtpOutboundGateway
import org.springframework.integration.ftp.session.DefaultFtpSessionFactory
import org.springframework.integration.ftp.session.FtpFileInfo
import org.springframework.integration.support.MessageBuilder
import org.springframework.messaging.Message
import java.io.File


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
    private val OLLAMA_IMAGE = "ollama/ollama:latest"
    private val DOCUMENT_MODEL = "llama3.2:1b"
    private val IMAGE_MODEL = "llama3.2-vision:11b"
    private val NEW_IMAGE_NAME = "vidyasource/ollama-test"

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
        //setLocalDirectoryExpression(LiteralExpression("local/"))
        setOption(AbstractRemoteFileOutboundGateway.Option.RECURSIVE)
        //setOption(AbstractRemoteFileOutboundGateway.Option.NAME_ONLY)
        setFilter(FtpRegexPatternFileListFilter("""^(?!.*\.zip$).*"""))
    }

    @Bean
    fun getFileOutboundGateway() = FtpOutboundGateway(ftpSessionFactory(), "get").apply {
        setLocalDirectoryExpression(LiteralExpression("local/"))
        //setOption(AbstractRemoteFileOutboundGateway.Option.STREAM)
        setFileExistsMode(FileExistsMode.APPEND)
        setSendTimeout(Long.MAX_VALUE)
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
    fun integrationFlow(
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
            transform<LibraryFile> { "/RFI No. LVA-AI-25-009/${it.fullPathName}" }
            handle(getFileOutboundGateway)

            handle { file: Message<*> ->
                println("Flow 1: $file")
            }
        }
    }


    @Bean
    fun fileReaderFlow(fileReadingMessageSource: FileReadingMessageSource): IntegrationFlow {
        return integrationFlow(fileReadingMessageSource) {
            log("starting file reader flow")
            handle { file: Message<*> ->
                println("File processor: $file")
            }
            //route<Message<*>> { m -> if (m.headers[FileHeaders.FILENAME].toString().endsWith(".jpg")) "imageChannel" else "textChannel" }
        }
    }




}

