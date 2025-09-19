plugins {
    id("java-library")
    id("dev.clojurephant.clojure")
    kotlin("jvm") version "2.1.20"
    kotlin("plugin.serialization") version "2.1.21"
    id("com.gradleup.shadow") version "8.3.6"
    id("com.github.breadmoirai.github-release") version "2.4.1"
}

// Clojure access to Kotlin classes
// See Clojurephant default conf: https://github.com/clojurephant/clojurephant/blob/23e84177a2c049a541d5ef19a4dbea495dfe7253/src/main/java/dev/clojurephant/plugin/common/internal/ClojureCommonPlugin.java#L73
val kotlinClassesDir = sourceSets.main.get().kotlin.classesDirectory
sourceSets.dev.get().compileClasspath += files(kotlinClassesDir)
sourceSets.dev.get().runtimeClasspath += files(kotlinClassesDir)
tasks.checkClojure.get().classpath.from(kotlinClassesDir)
tasks.clojureRepl.get().classpath.from(kotlinClassesDir)

group = "com.xtdb"
version = "2.0.0-a04"

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
        languageVersion = JavaLanguageVersion.of(21)
    }
}

dependencies {
    implementation("org.clojure","clojure","1.12.0")
    implementation("org.clojure", "tools.logging", "1.3.0")
    implementation("com.github.seancorfield", "next.jdbc", "1.3.1048")
    implementation("com.xtdb", "xtdb-api", "2.0.0")
    compileOnly("org.postgresql", "postgresql", "42.7.5")
    implementation("cheshire", "cheshire", "5.13.0")
    implementation("com.xtdb", "xtdb-api", "2.0.0")
    implementation("com.zaxxer", "HikariCP", "7.0.2")

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

githubRelease {
    token(properties["github.token"] as String)
    owner = "egg-juxt"
    repo = "xtdb-kafka-connect"

    tagName = "v${project.version}"
    releaseName = "Uber-JAR v${project.version}"
    body = "Automated release for version ${project.version}."

    releaseAssets = files(tasks.shadowJar.get().archiveFile.get().asFile, tasks["archive"])

    // This task will depend on shadowJar to ensure the file exists before uploading.
    tasks.register("createGithubRelease") {
        dependsOn(tasks.shadowJar)
        dependsOn(tasks["archive"])
    }
}
