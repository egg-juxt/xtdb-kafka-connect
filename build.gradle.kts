plugins {
    id("java-library")
    id("dev.clojurephant.clojure")
    kotlin("jvm") version "2.1.20"
    kotlin("plugin.serialization") version "2.1.21"
    id("com.gradleup.shadow") version "8.3.6"
}

// Clojure access to Kotlin classes
// See Clojurephant default conf: https://github.com/clojurephant/clojurephant/blob/23e84177a2c049a541d5ef19a4dbea495dfe7253/src/main/java/dev/clojurephant/plugin/common/internal/ClojureCommonPlugin.java#L73
val kotlinClassesDir = sourceSets.main.get().kotlin.classesDirectory
sourceSets.dev.get().compileClasspath += files(kotlinClassesDir)
sourceSets.dev.get().runtimeClasspath += files(kotlinClassesDir)
tasks.checkClojure.get().classpath.from(kotlinClassesDir)
tasks.clojureRepl.get().classpath.from(kotlinClassesDir)

group = "com.xtdb"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven {
        name = "clojars"
        url = uri("https://repo.clojars.org/")
        maven { name = "confluent"; url = uri("https://packages.confluent.io/maven/") }
    }
}

java {
    toolchain {
        // Java 17 is the minimum version for dev.clojurephant:jovial from maven!
        languageVersion = JavaLanguageVersion.of(17)
    }
}

dependencies {
    implementation("org.clojure","clojure","1.12.0")
    implementation("org.clojure", "tools.logging", "1.3.0")
    implementation("com.github.seancorfield", "next.jdbc", "1.3.1048")
    implementation("cheshire", "cheshire", "5.13.0")

    // xtdb-api compiled for Java 11, and its dependencies
    implementation(files("libs/xtdb-api-2.0.0-SNAPSHOT.jar"))
    implementation("com.cognitect", "transit-clj", "1.0.329")
    implementation("org.apache.arrow", "arrow-vector", "18.3.0")
    implementation("org.jetbrains.kotlinx", "kotlinx-serialization-json", "1.8.1")
    implementation("org.postgresql", "postgresql", "42.7.5")
    implementation("com.github.ben-manes.caffeine", "caffeine", "3.1.8")

    compileOnly("org.apache.kafka",  "connect-api", "3.9.1")
    testImplementation("org.apache.kafka",  "connect-api", "3.9.1")

    testRuntimeOnly("dev.clojurephant", "jovial", "0.4.1")
    testRuntimeOnly("org.apache.logging.log4j", "log4j-slf4j2-impl", "2.21.1")

    testImplementation("org.testcontainers", "testcontainers", "1.21.3")
    testImplementation("metosin", "jsonista", "0.3.3")
    testImplementation("io.confluent", "kafka-json-schema-serializer", "7.6.6")
    testImplementation("io.confluent", "kafka-avro-serializer", "7.6.6")
}

tasks.test {
    useJUnitPlatform()
    include("xtdb/kafka/**")
}
