
import java.net.URL
import java.util.Base64

plugins {
    `java`
    id("application")
    id("com.github.johnrengelman.shadow") version "7.1.2"
    // Apply Spotless plugin for the top-level project
    id("com.diffplug.spotless") version "6.20.0"
    id("org.sonarqube") version "5.0.0.4638"
    id("maven-publish")
    jacoco
}

fun downloadCredentials(): Map<String, String> {
    val isCI = System.getenv("CIRCLECI") == "true"
    val url: URL = if (isCI) {
        val urlString = System.getenv("HEVO_M2_WAGON_AUTH_URL")
        URL(urlString)
    } else {
        URL("https://aws-key.hevo.me/mvn-read")
    }

    val awsResponseStr = if (isCI) {
        val urlConnection = url.openConnection()
        val userCredentials = "circleci:" + System.getenv("HEVO_M2_WAGON_PASSWORD")
        val basicAuth = "Basic " + Base64.getEncoder().encodeToString(userCredentials.toByteArray())
        urlConnection.setRequestProperty("Authorization", basicAuth)
        urlConnection.inputStream.bufferedReader().use { it.readText() }
    } else {
        url.readText()
    }

    val jsonSlurper = groovy.json.JsonSlurper() // Ensure JsonSlurper is accessible
    return jsonSlurper.parseText(awsResponseStr) as Map<String, String>
}

val awsResponseJson = downloadCredentials()

val baseMavenURL = "s3://hevo-artifacts/package/mvn"
val releaseRepoURL = "$baseMavenURL/release"
val snapshotRepoURL = "$baseMavenURL/snapshot"

// Repositories configuration at the root level
repositories {
    maven {
        name = "hevoMavenRelease"
        url = uri("$releaseRepoURL")
        credentials(AwsCredentials::class) {
            accessKey = awsResponseJson["access_key"].orEmpty()
            secretKey = awsResponseJson["secret_key"].orEmpty()
            sessionToken = awsResponseJson["token"].orEmpty()
        }
    }

    maven {
        name = "hevoMavenSnapshot"
        url = uri("$snapshotRepoURL")
        credentials(AwsCredentials::class) {
            accessKey = awsResponseJson["access_key"].orEmpty()
            secretKey = awsResponseJson["secret_key"].orEmpty()
            sessionToken = awsResponseJson["token"].orEmpty()
        }
    }

    mavenCentral() // Add Maven Central for other dependencies
}

// Dependencies required in the main project
dependencies {
    implementation("us.fatehi:schemacrawler:16.21.2") {
        exclude(group = "com.slf4j", module = "slf4j-jdk14")
    }

//    implementation(files("$projectDir/libs/*.jar"))
    implementation("io.hevo:hevo-sdk:1.0.0-SNAPSHOT")
    implementation("io.hevo:jdbc-connector:1.0.0-SNAPSHOT")

    implementation("com.fasterxml.jackson.core:jackson-databind")
    // Test Dependencies
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.mockito:mockito-junit-jupiter")
    testImplementation("org.mockito:mockito-core")
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

// Sonar configuration
sonar {
    properties {
        property("sonar.host.url", System.getenv("SONARQUBE_SERVER_URL"))
        property("sonar.coverage.jacoco.xmlReportPaths", "$projectDir/code-coverage-report/build/reports/jacoco/testCodeCoverageReport/testCodeCoverageReport.xml")
    }
}

// Maven publishing setup
extensions.configure<PublishingExtension>("publishing") {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
        }
    }
    repositories {
        maven {
            name = "hevoMavenArtifactory"
            val releasesRepoUrl = uri("$releaseRepoURL")
            val snapshotsRepoUrl = uri("$snapshotRepoURL")

            url = if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl

            credentials(AwsCredentials::class) {
                accessKey = awsResponseJson["access_key"].toString()
                secretKey = awsResponseJson["secret_key"].toString()
                sessionToken = awsResponseJson["token"].toString()
            }
        }
    }
}

afterEvaluate {
    val hevoBomVersion = "1.5.0"

    if (project.plugins.hasPlugin("java")) {
        dependencies {
            "implementation"(platform("io.hevo:bom:$hevoBomVersion")) // Add dependency
        }
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

    // Explicitly include JAR & *.lic files from the libs folder
//    from("libs/") {
//        include("*.jar")
//        include("*.lic")
//    }

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
