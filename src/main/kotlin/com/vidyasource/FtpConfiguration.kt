package com.vidyasource

import org.apache.commons.net.ftp.FTPClient
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.bind.ConstructorBinding
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.integration.annotation.InboundChannelAdapter
import org.springframework.integration.channel.QueueChannel
import org.springframework.integration.dsl.IntegrationFlow
import org.springframework.integration.dsl.integrationFlow
import org.springframework.integration.file.FileHeaders.REMOTE_FILE
import org.springframework.integration.ftp.inbound.FtpStreamingMessageSource
import org.springframework.integration.ftp.session.DefaultFtpSessionFactory
import org.springframework.integration.ftp.session.FtpRemoteFileTemplate
import org.springframework.integration.metadata.PropertiesPersistingMetadataStore
import org.springframework.messaging.Message
import java.io.InputStream

@ConfigurationProperties(prefix = "ftp")
data class FtpProperties @ConstructorBinding constructor(val host: String,
                         val port: Int,
                         val username: String,
                         val password: String)


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
    @InboundChannelAdapter(channel = "inputChannel")
    fun ftpStreamingMessageSource() = FtpStreamingMessageSource(FtpRemoteFileTemplate(ftpSessionFactory())).apply {
        maxFetchSize = 20
        setRemoteDirectory(".")

    }

    @Bean
    fun inputChannel() = QueueChannel()

    @Bean
    fun integrationFlow(ftpStreamingMessageSource: FtpStreamingMessageSource): IntegrationFlow {
        return integrationFlow(ftpStreamingMessageSource, { poller { it.fixedDelay(3).maxMessagesPerPoll(10) } }) {
            filter<Message<InputStream>> { m ->
                println("header: ${m.headers[REMOTE_FILE]}")
                !m.headers[REMOTE_FILE].toString().endsWith(".zip", ignoreCase = true)
            }
            handle { message ->
                println("Found file: ${message}")
            }
        }
    }
}

