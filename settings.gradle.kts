rootProject.name = "xtdb-kafka-connect"

pluginManagement {
    plugins {
//        kotlin("jvm") version "2.1.20"
//        kotlin("plugin.serialization") version "2.1.21"
        id("dev.clojurephant.clojure") version "0.8.0"
    }
}

include("integration-tests")
