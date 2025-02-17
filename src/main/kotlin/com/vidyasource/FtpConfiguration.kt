package com.vidyasource

import org.apache.commons.net.ftp.FTPClient
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
    fun startupRunner(inputChannel: DirectChannel) = ApplicationRunner {
        println("Triggering FTP mget command...")
        inputChannel.send(MessageBuilder.withPayload("").build()) // Empty payload to trigger the flow
    }

    @Bean
    fun outboundGateway() = FtpOutboundGateway(ftpSessionFactory(), "mget").apply {
        setLocalDirectoryExpression(LiteralExpression("local/"))
        setOption(AbstractRemoteFileOutboundGateway.Option.RECURSIVE)
        setFilter(FtpRegexPatternFileListFilter("""^(?!.*\.zip$).*"""))
        setFileExistsMode(FileExistsMode.APPEND)
    }

    @Bean
    fun integrationFlow(inputChannel: DirectChannel, outboundGateway: FtpOutboundGateway): IntegrationFlow {
        return integrationFlow(inputChannel) {
            handle(outboundGateway)
            handle { message: Message<*> ->
                println("Headers: ${message.headers}")
            }
        }
    }
}

