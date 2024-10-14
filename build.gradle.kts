import java.net.URL
import java.util.Base64

plugins {
    `kotlin-dsl`
    `java`
    // Apply Spotless plugin for the top-level project
    id("com.diffplug.spotless") version "6.20.0"
    id("org.sonarqube") version "5.0.0.4638"
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
val releaseRepoURL= "${baseMavenURL}/release"
val snapshotRepoURL = "${baseMavenURL}/snapshot"


repositories {
    maven {
        url = uri("https://plugins.gradle.org/m2/")
    }
}

dependencies {
    "implementation"("org.gradle.kotlin:gradle-kotlin-dsl-plugins:4.4.0")
    "implementation"("com.diffplug.spotless:spotless-plugin-gradle:6.25.0")
}

sonar {
    properties {
        property("sonar.host.url", System.getenv("SONARQUBE_SERVER_URL"))
        property("sonar.coverage.jacoco.xmlReportPaths", "$projectDir/code-coverage-report/build/reports/jacoco/testCodeCoverageReport/testCodeCoverageReport.xml")
        property("sonar.exclusions", "**/io/hevo/fortress/command/*.java," + "**/io/hevo/fortress/module/*.java," + "**/io/hevo/connector/hubspot/*.java")
    }
}

subprojects {

    apply(plugin = "com.diffplug.spotless")  // Apply Spotless plugin
    apply(plugin = "java")
    apply(plugin = "maven-publish")
    apply(plugin = "jacoco")

    configure<PublishingExtension> {
        publications {
            create<MavenPublication>(project.name) {
                from(components["java"])
            }
        }
        repositories {
            maven {
                name = "hevoMavenArtifactory"
                val releasesRepoUrl = uri("${releaseRepoURL}")
                val snapshotsRepoUrl = uri("${snapshotRepoURL}")

                url= if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl

                credentials(AwsCredentials::class) {
                    accessKey = awsResponseJson["access_key"].toString()
                    secretKey = awsResponseJson["secret_key"].toString()
                    sessionToken = awsResponseJson["token"].toString()
                }
            }
        }
    }


    repositories {

        val credentialsBlock: MavenArtifactRepository.() -> Unit = {
            credentials(AwsCredentials::class) {
                accessKey = awsResponseJson["access_key"].orEmpty()
                secretKey = awsResponseJson["secret_key"].orEmpty()
                sessionToken = awsResponseJson["token"].orEmpty()
            }
        }

        maven {
            name = "hevoMavenRelease"
            url = uri("${releaseRepoURL}")
            credentialsBlock()
        }

        maven {
            name = "hevoMavenSnapshot"
            url = uri("${snapshotRepoURL}")
            credentialsBlock()
        }

        mavenCentral() // Use Maven Central
    }


    extensions.configure<com.diffplug.gradle.spotless.SpotlessExtension> {
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

    afterEvaluate {
        val hevoBomVersion = "1.5.0"

        if (project.plugins.hasPlugin("java")) {
            dependencies {
                "implementation"(platform("io.hevo:bom:$hevoBomVersion")) // Add dependency
            }
        }
    }

    tasks.named<Test>("test") {
        useJUnitPlatform()
        maxHeapSize = "1G"
    }
}