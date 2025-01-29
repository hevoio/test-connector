plugins {
    `java`
    id("application")
    id("com.github.johnrengelman.shadow") version "7.1.2"
    // Apply Spotless plugin for the top-level project
    id("com.diffplug.spotless") version "6.20.0"
    jacoco
}

// Repositories configuration at the root level
repositories {
    mavenCentral() // Add Maven Central for other dependencies
}

// Dependencies required in the main project
dependencies {
    implementation("us.fatehi:schemacrawler:16.21.2") {
        exclude(group = "com.slf4j", module = "slf4j-jdk14")
    }

    implementation("io.hevo:hevo-sdk:1.4.0")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.16.0")
    // Test Dependencies
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.0")
//    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.mockito:mockito-junit-jupiter:5.9.0")
    testImplementation("org.mockito:mockito-core:5.4.0")
}

// Spotless plugin configuration at the root level
spotless {
    java {
        target("**/*.java")
        googleJavaFormat("1.18.1")
        removeUnusedImports()
    }

    kotlinGradle {
        // Targets the root build.gradle.kts and all subprojects' build.gradle.kts files
        target("**/*.gradle.kts")

        ktlint("0.49.1") // Use a specific version of ktlint
        trimTrailingWhitespace() // Remove trailing whitespaces
        indentWithSpaces(4) // Use 4 spaces for indentation
        endWithNewline() // Ensure the file ends with a newline
    }
}

tasks.jar {
    manifest {
        attributes["Main-Class"] = "io.hevo.connector.application.Main"
    }

    from(configurations.runtimeClasspath.get().map({ if (it.isDirectory) it else zipTree(it) }))

    // Exclude specific files from being included in the JAR's META-INF
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

tasks.shadowJar {
    // Exclude default configurations
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")

    // Specify the main class for the JAR
    manifest {
        attributes["Main-Class"] = "io.hevo.connector.application.Main"
    }
    // Merging Service Files
    mergeServiceFiles()
}

application {
    mainClass = "io.hevo.connector.application.Main"
}

// Jacoco test configuration
tasks.named<Test>("test") {
    useJUnitPlatform()
    maxHeapSize = "1G"
}
