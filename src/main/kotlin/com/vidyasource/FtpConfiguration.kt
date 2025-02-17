package com.vidyasource


import dev.langchain4j.data.document.loader.UrlDocumentLoader
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
import org.springframework.integration.file.remote.gateway.AbstractRemoteFileOutboundGateway
import org.springframework.integration.file.support.FileExistsMode
import org.springframework.integration.ftp.filters.FtpRegexPatternFileListFilter
import org.springframework.integration.ftp.gateway.FtpOutboundGateway
import org.springframework.integration.ftp.session.DefaultFtpSessionFactory
import org.springframework.integration.ftp.session.FtpFileInfo
import org.springframework.integration.metadata.PropertiesPersistingMetadataStore
import org.springframework.integration.support.MessageBuilder
import org.springframework.messaging.Message
import java.util.function.Supplier
import org.apache.tika.metadata.Metadata

@ConfigurationProperties(prefix = "ftp")
data class FtpProperties @ConstructorBinding constructor(
    val host: String,
    val port: Int,
    val username: String,
    val password: String
)

data class LibraryFile(val fullPathName: String, val timestamp: Long)

class TextDocumentParser {
    private val tikaConfig by lazy {
        TikaConfig(ClassLoader.getSystemResourceAsStream("tika-config.xml"))
    }

    val parser by lazy {
        ApacheTikaDocumentParser(
            { AutoDetectParser(tikaConfig) },
            { BodyContentHandler(-1) },
            { Metadata() },
            { ParseContext() },
            true
        )
    }
}

@Configuration
class FtpConnectionConfiguration(val ftpProperties: FtpProperties) {
    val textDocumentParser = TextDocumentParser()

    fun ftpSessionFactory() = DefaultFtpSessionFactory().apply {
        setHost(ftpProperties.host)
        setPort(ftpProperties.port)
        setUsername(ftpProperties.username)
        setPassword(ftpProperties.password)
        setClientMode(FTPClient.PASSIVE_LOCAL_DATA_CONNECTION_MODE)
    }


    @Bean
    fun metadataStore(): PropertiesPersistingMetadataStore {
        return PropertiesPersistingMetadataStore().apply {
            setBaseDirectory("./metadata")
            afterPropertiesSet()
        }
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


        //outputChannel
        //setFileExistsMode(FileExistsMode.APPEND)
    }

    @Bean
    fun getFileOutboundGateway() = FtpOutboundGateway(ftpSessionFactory(), "get").apply {
        setLocalDirectoryExpression(LiteralExpression("local/"))
        //setOption(AbstractRemoteFileOutboundGateway.Option.STREAM)
        setFileExistsMode(FileExistsMode.APPEND)
        setSendTimeout(Long.MAX_VALUE)
    }


    fun fileTransformer(ftpFileInfo: FtpFileInfo): LibraryFile {
        println("ftpFileInfo: $ftpFileInfo")
        return LibraryFile(
            fullPathName = ftpFileInfo.filename,
            timestamp = ftpFileInfo.modified
        )
    }

    fun parseDocument(path: String): String {
        val url =
            "ftp://${ftpProperties.username}:${ftpProperties.password}@${ftpProperties.host}:${ftpProperties.port}/${path}"

        val document = UrlDocumentLoader.load(url, textDocumentParser.parser)
        return document.text()
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
            //transform<LibraryFile> { parseDocument(it.fullPathName) }
            handle { file: Message<*> ->
                println("LibraryFile: $file")
            }
        }
    }
}

