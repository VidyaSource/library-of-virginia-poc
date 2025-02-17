package com.vidyasource


import org.apache.commons.net.ftp.FTPClient
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.bind.ConstructorBinding
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.integration.channel.DirectChannel
import org.springframework.integration.dsl.IntegrationFlow
import org.springframework.integration.dsl.integrationFlow
import org.springframework.integration.file.remote.gateway.AbstractRemoteFileOutboundGateway
import org.springframework.integration.ftp.filters.FtpRegexPatternFileListFilter
import org.springframework.integration.ftp.gateway.FtpOutboundGateway
import org.springframework.integration.ftp.session.DefaultFtpSessionFactory
import org.springframework.integration.ftp.session.FtpFileInfo
import org.springframework.integration.metadata.PropertiesPersistingMetadataStore
import org.springframework.integration.support.MessageBuilder
import org.springframework.messaging.Message

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
    fun outboundGateway() = FtpOutboundGateway(ftpSessionFactory(), "ls").apply {
        //setLocalDirectoryExpression(LiteralExpression("local/"))
        setOption(AbstractRemoteFileOutboundGateway.Option.RECURSIVE)
        //setOption(AbstractRemoteFileOutboundGateway.Option.NAME_ONLY)
        setFilter(FtpRegexPatternFileListFilter("""^(?!.*\.zip$).*"""))
        //outputChannel
        //setFileExistsMode(FileExistsMode.APPEND)
    }


    fun fileTransformer(ftpFileInfo: FtpFileInfo): LibraryFile {
        return LibraryFile(
            fullPathName = ftpFileInfo.filename,
            timestamp = ftpFileInfo.modified
        )
    }


    @Bean
    fun integrationFlow(inputChannel: DirectChannel,
                        outboundGateway: FtpOutboundGateway): IntegrationFlow {
        return integrationFlow(inputChannel) {
            log("starting integration flow")
            handle(outboundGateway)
            split<Message<*>> { it.payload }
            transform<FtpFileInfo> { fileTransformer(it) }
            channel { queue(10) }
            handle { file: Message<*> ->
                val libraryFile = file.payload as? LibraryFile
                println("LibraryFile: $libraryFile")
            }
        }
    }
}

