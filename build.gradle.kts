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
version = "2.0.0-a03"

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
        languageVersion = JavaLanguageVersion.of(11)
    }
}

dependencies {
    implementation("org.clojure","clojure","1.12.0")
    implementation("org.clojure", "tools.logging", "1.3.0")
    implementation("com.github.seancorfield", "next.jdbc", "1.3.1048")
    implementation("cheshire", "cheshire", "5.13.0")
    implementation("com.zaxxer", "HikariCP", "7.0.2")

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
    testImplementation("org.slf4j", "jul-to-slf4j", "2.0.17")

    testImplementation("org.testcontainers", "testcontainers", "1.21.3")
    testImplementation("metosin", "jsonista", "0.3.3")
    testImplementation("io.confluent", "kafka-json-schema-serializer", "7.6.6")
    testImplementation("io.confluent", "kafka-avro-serializer", "7.6.6")
}

tasks.test {
    useJUnitPlatform {
        excludeTags("manual")
    }
    include("xtdb/kafka/**")
}


// Build an Component Hub archive:
// https://docs.confluent.io/platform/current/connect/confluent-hub/component-archive.html
//
// The official Confluent JDBC Connector can serve as an example:
// https://www.confluent.io/hub/confluentinc/kafka-connect-jdbc

val componentOwner = "xtdb"
val componentName = "kafka-connect-xtdb"

tasks.register<Copy>("manifest") {
    group = "component hub"

    from("src/dist/manifest.json")

    inputs.property("componentOwner", componentOwner)
    inputs.property("componentName", componentName)
    inputs.property("componentVersion", project.version.toString())

    inputs.properties["componentOwener"]
    expand(
        "owner" to inputs.properties["componentOwner"],
        "name" to inputs.properties["componentName"],
        "version" to inputs.properties["componentVersion"]
    )  { escapeBackslash = true }

    into(layout.buildDirectory.dir("tmp/manifest"))
}

tasks.register<Zip>("archive") {
    group = "component hub"

    destinationDirectory = layout.buildDirectory.dir("dist")
    val archiveName = "$componentOwner-$componentName-$version"
    archiveFileName = "$archiveName.zip"

    into(archiveName) {
        into("lib") {
            from(tasks.jar)
            from(configurations.runtimeClasspath)
        }

        from(layout.projectDirectory.dir("src/dist")) {
            exclude("manifest.json")
        }
        from(tasks.getByName("manifest"))
    }
}
