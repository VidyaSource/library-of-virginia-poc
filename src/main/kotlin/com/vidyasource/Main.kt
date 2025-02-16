package com.vidyasource

import org.springframework.boot.WebApplicationType
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication

@SpringBootApplication
@EnableConfigurationProperties(FtpProperties::class)
class LibraryPOC

fun main(args: Array<String>) {
    runApplication<LibraryPOC>(*args) {
        webApplicationType = WebApplicationType.NONE
    }
}
