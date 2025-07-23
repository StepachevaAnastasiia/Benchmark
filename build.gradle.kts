plugins {
    kotlin("jvm") version "2.1.0"
    kotlin("plugin.allopen") version "2.0.20"
    id("org.jetbrains.kotlinx.benchmark") version "0.4.13"
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.duckdb:duckdb_jdbc:0.9.2")
    implementation("org.rocksdb:rocksdbjni:9.9.3")
    implementation("org.jetbrains.kotlinx:kotlinx-benchmark-runtime:0.4.13")
    implementation("org.openjdk.jmh:jmh-core:1.37")
    annotationProcessor("org.openjdk.jmh:jmh-generator-annprocess:1.37")
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(21)
}
allOpen {
    annotation("org.openjdk.jmh.annotations.State")
}
benchmark {
    configurations {
        named("main") {
            warmups = 2
            iterations = 15
            mode = "avgt"
            outputTimeUnit = "s"
        }
    }
    targets {
        register("main") {
            this as kotlinx.benchmark.gradle.JvmBenchmarkTarget
            jmhVersion = "1.37"
        }
    }
}

tasks.withType<JavaExec> {
    jvmArgs = listOf("-Xmx1G", "-XX:MaxMetaspaceSize=1G")
}