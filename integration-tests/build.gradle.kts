plugins {
    id("dev.clojurephant.clojure")
}

repositories {
    mavenCentral()
    maven { name = "clojars"; url = uri("https://repo.clojars.org/") }
    maven { name = "confluent"; url = uri("https://packages.confluent.io/maven/") }
    maven { name = "gradle"; url = uri("https://repo.gradle.org/gradle/libs-releases") }
}

dependencies {
    implementation("org.clojure","clojure","1.12.0")

    testImplementation("integrant", "integrant", "0.13.1")
    testImplementation("com.github.seancorfield", "next.jdbc", "1.3.1048")
    testImplementation("hato", "hato", "1.0.0")
    testImplementation("metosin", "jsonista", "0.3.3")

    testImplementation("com.xtdb", "xtdb-api", "2.0.0")
    testImplementation("org.postgresql", "postgresql", "42.7.5")

    testImplementation("io.confluent", "kafka-json-schema-serializer", "7.6.6")
    testImplementation("io.confluent", "kafka-avro-serializer", "7.6.6")

    testImplementation("org.testcontainers", "testcontainers", "1.21.3")
    testImplementation("org.testcontainers", "kafka", "1.21.3")
    testImplementation("org.testcontainers", "toxiproxy", "1.21.3")

    testImplementation("org.gradle", "gradle-tooling-api", "8.10")

    testRuntimeOnly("dev.clojurephant", "jovial", "0.4.1")
}

tasks.test {
    dependsOn(":shadowJar")

    useJUnitPlatform {
        excludeTags("manual")
    }
    include("xtdb/kafka/**")
}
