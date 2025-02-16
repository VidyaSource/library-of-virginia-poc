plugins {
    kotlin("jvm") version "2.1.10"
    id("org.jetbrains.kotlin.plugin.spring") version "2.1.10"
    id("org.springframework.boot") version "3.4.2"
    id("io.spring.dependency-management") version "1.1.7"}

group = "org.example"
version = "1.0-SNAPSHOT"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

configurations {
    compileOnly {
        extendsFrom(configurations.annotationProcessor.get())
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("dev.langchain4j:langchain4j:1.0.0-beta1")
    implementation("dev.langchain4j:langchain4j-document-parser-apache-tika:1.0.0-beta1")
    implementation("dev.langchain4j:langchain4j-ollama:1.0.0-beta1")
    implementation("org.springframework.integration:spring-integration-core:6.4.1")
    implementation("org.springframework.integration:spring-integration-ftp:6.4.1")

    annotationProcessor("org.springframework.boot:spring-boot-configuration-processor")

    testImplementation("org.testcontainers:ollama:1.0.0-beta1:1.19.1")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(21)
}