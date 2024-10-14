plugins {
    // Apply the application plugin to add support for building a CLI application in Java.
    id("application")
    id("com.github.johnrengelman.shadow") version "7.1.2"
}

dependencies {
    implementation("us.fatehi:schemacrawler:16.21.2") {
        exclude(group = "com.slf4j", module = "slf4j-jdk14")
    }

    implementation("io.hevo:hevo-sdk:1.0.0-SNAPSHOT")

    // Test Dependencies
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.mockito:mockito-junit-jupiter")
    testImplementation("org.mockito:mockito-core")
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