plugins {
    id("java")
    id("dev.clojurephant.clojure")
}

group = "com.xtdb"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven {
        name = "clojars"
        url = uri("https://repo.clojars.org/")
    }
}

dependencies {
    implementation("org.clojure","clojure","1.12.0")
}

tasks.test {
    useJUnitPlatform()
}