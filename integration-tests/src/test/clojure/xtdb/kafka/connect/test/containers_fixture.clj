(ns xtdb.kafka.connect.test.containers-fixture
  (:require [clojure.string :as str]
            [hato.client :as http]
            [clojure.java.io :as io]
            [clojure.test :refer :all]
            [mount.core :as mount :refer [defstate]]
            [jsonista.core :as json]
            [next.jdbc :as jdbc])
  (:import (io.confluent.kafka.serializers KafkaAvroSerializer)
           (io.confluent.kafka.serializers.json KafkaJsonSchemaSerializer)
           (java.io File)
           (java.lang AutoCloseable)
           (java.time Duration)
           (java.util Collection Map Properties)
           (java.util.concurrent ExecutionException)
           (org.apache.kafka.clients.admin AdminClient)
           (org.apache.kafka.clients.consumer KafkaConsumer)
           (org.apache.kafka.clients.producer KafkaProducer ProducerRecord)
           (org.apache.kafka.common PartitionInfo TopicPartition)
           (org.apache.kafka.common.errors UnknownTopicOrPartitionException)
           (org.apache.kafka.common.serialization StringDeserializer StringSerializer)
           (org.gradle.tooling GradleConnector)
           (org.testcontainers.containers BindMode Container GenericContainer Network)
           (org.testcontainers.containers.wait.strategy Wait)
           (org.testcontainers.kafka ConfluentKafkaContainer)
           (org.testcontainers.utility DockerImageName)))

; See compatibility between versions Confluent Platform and Apache Kafka here:
; https://docs.confluent.io/platform/current/installation/versions-interoperability.html#cp-and-apache-ak-compatibility
(def cp-kafka-version "8.0.1") ; compatible with Apache Kafka 4.0.x

(defn with-env [^Container c m]
  (-> (fn [c k v]
        (.withEnv c (name k) (str v)))
    (reduce-kv c m)))

(def ^GenericContainer xtdb-container-conf
  (doto (GenericContainer. "ghcr.io/xtdb/xtdb:2.1.0")
    (.withEnv "XTDB_LOGGING_LEVEL" "debug")
    (.withCommand ^"[Ljava.lang.String;" (into-array ^String ["playground"]))
    (.withNetwork Network/SHARED)
    (.withExposedPorts (into-array [(int 5432)]))))
    ;(.withReuse true)))

(defstate ^{:on-reload :noop} xtdb
  :start (doto xtdb-container-conf .start)
  :stop (.close xtdb))

(defstate ^{:on-reload :noop} kafka
  :start (doto (ConfluentKafkaContainer. (DockerImageName/parse (str "confluentinc/cp-kafka:" cp-kafka-version)))
           (.withExposedPorts (into-array [(int 9092)]))
           (.withNetwork Network/SHARED)
           (.start))
  :stop (.close kafka))

(def project-dir
  (-> (File. "") (.getAbsoluteFile) (.getParentFile)))

(defn load-properties-file [f]
  (doto (Properties.) (.load (io/reader f))))

(def project-version
  (-> (load-properties-file (io/file project-dir "gradle.properties"))
    (get "version")))

(defn run-gradle-task! [task-name]
  (with-open [conn (-> (GradleConnector/newConnector) (.forProjectDirectory project-dir) (.connect))]
    (-> conn .newBuild (.forTasks (into-array [task-name])) .run)))

(defstate ^{:on-reload :noop} connector-jar-file
  :start (do
           (run-gradle-task! "shadowJar")
           (let [jar-file (io/file project-dir "build" "libs" (str "xtdb-kafka-connect-" project-version "-all.jar"))]
             (if (.exists jar-file)
               jar-file
               (throw (IllegalStateException. (str "Not found: " jar-file)))))))

(defn kafka-endpoint-for-containers [kafka]
  (str (-> kafka .getNetworkAliases first) ":9093"))

(defstate ^{:on-reload :noop} connect
  :start (let [plugin-path "/usr/local/share/xtdb-plugin"
               container (doto (GenericContainer. (DockerImageName/parse (str "confluentinc/cp-kafka-connect:" cp-kafka-version)))
                           (.dependsOn [kafka])
                           (.withNetwork (.getNetwork kafka))
                           (.withExposedPorts (into-array [(int 8083)]))
                           (.waitingFor (Wait/forHttp "/connectors"))
                           (.withFileSystemBind (.getParent connector-jar-file) plugin-path BindMode/READ_ONLY)
                           (with-env {:CONNECT_LOG4J_LOGGERS "xtdb.kafka=ALL"

                                      :CONNECT_BOOTSTRAP_SERVERS (kafka-endpoint-for-containers kafka)
                                      :CONNECT_REST_PORT 8083
                                      :CONNECT_GROUP_ID "default"

                                      :CONNECT_CONFIG_STORAGE_TOPIC "default.config"
                                      :CONNECT_OFFSET_STORAGE_TOPIC "default.offsets"
                                      :CONNECT_STATUS_STORAGE_TOPIC "default.status"

                                      ; we only have 1 kafka broker, so topic replication factor must be one
                                      :CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR 1
                                      :CONNECT_STATUS_STORAGE_REPLICATION_FACTOR 1
                                      :CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR 1

                                      :CONNECT_PLUGIN_DISCOVERY "service_load"
                                      :CONNECT_PLUGIN_PATH (str plugin-path "/" (.getName connector-jar-file))

                                      :CONNECT_KEY_CONVERTER "org.apache.kafka.connect.storage.StringConverter"
                                      :CONNECT_VALUE_CONVERTER "org.apache.kafka.connect.json.JsonConverter"
                                      :CONNECT_REST_ADVERTISED_HOST_NAME "localhost"})
                           (.start))]
           container)
  :stop (.close connect))

(defstate ^{:on-reload :noop} schema-registry
  :start (doto (GenericContainer. (str "confluentinc/cp-schema-registry:" cp-kafka-version))
           (.dependsOn [kafka])
           (.withNetwork (.getNetwork kafka))
           (.withExposedPorts (into-array [(int 8081)]))
           (with-env {:SCHEMA_REGISTRY_HOST_NAME "schema-registry"
                      :SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS (kafka-endpoint-for-containers kafka)
                      :SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL "none"})
           (.start))
  :stop (.close schema-registry))

(defn with-containers [f]
  (mount/start)
  (f))

(defn stop-xtdb! []
  (mount/stop #'xtdb))

(defn start-xtdb! []
  (mount/start #'xtdb))

(defn reload-connector! []
  (mount/stop #'connector-jar-file #'connect)
  (mount/start #'connector-jar-file #'connect))

(def ^:dynamic *xtdb-db*)
(def ^:dynamic *xtdb-conn*)

(defn xtdb-jdbc-url-on-host []
  (str "jdbc:xtdb://localhost:" (.getMappedPort xtdb 5432) "/" *xtdb-db*))

(defn with-xtdb-conn [f]
  (binding [*xtdb-db* (random-uuid)]
    (with-open [xtdb-conn (jdbc/get-connection (xtdb-jdbc-url-on-host))]
      (binding [*xtdb-conn* xtdb-conn]
        (f)))))


; Test utilities

(defn connect-api-url []
  (str "http://" (.getHost connect) ":" (.getMappedPort connect 8083)))

(defn schema-registry-base-url []
  (let [host (.getHost schema-registry)
        port (.getMappedPort schema-registry 8081)]
    (str "http://" host ":" port)))

(defn schema-registry-base-url-for-containers []
  (str "http://" (-> schema-registry .getNetworkAliases first) ":8081"))

(defn xtdb-host+port []
  (str (-> xtdb .getNetworkAliases first) ":5432"))

(defn xtdb-jdbc-url-for-containers []
  (str "jdbc:xtdb://" (xtdb-host+port) "/" *xtdb-db*))

(defn kafka-endpoint-on-host []
  (str "localhost:" (.getMappedPort kafka 9092)))

(defn delete-topics! [^Collection topics]
  (with-open [admin (AdminClient/create {"bootstrap.servers" (kafka-endpoint-on-host)})]
    (-> admin (.deleteTopics topics) .all .get)))

(defn with-connector [{:keys [topics] :as conf}]
  (http/post (str (connect-api-url) "/connectors")
    {:body (json/write-value-as-string
             {:name topics
              :config (merge {:tasks.max "1"
                              :connector.class "xtdb.kafka.connect.XtdbSinkConnector"
                              :connection.url (xtdb-jdbc-url-for-containers)}
                             conf)})
     :content-type :json
     :accept :json})
  (reify AutoCloseable
    (close [_]
      (http/delete (str (connect-api-url) "/connectors/" topics))
      (try
        (delete-topics! (conj (str/split topics #",") "dlq"))
        (catch ExecutionException e
          (when-not (instance? UnknownTopicOrPartitionException (ex-cause e))
            (throw e)))))))

(defn list-topic [topic]
  (with-open [consumer (KafkaConsumer.
                         ^Map {"bootstrap.servers" (kafka-endpoint-on-host)
                               "key.deserializer" (.getName StringDeserializer)
                               "value.deserializer" (.getName StringDeserializer)})]
    (let [partitions (for [^PartitionInfo p (.partitionsFor consumer topic)]
                       (TopicPartition. (.topic p) (.partition p)))]
      (.assign consumer partitions)
      (.seekToBeginning consumer partitions))
    (seq (.poll consumer (Duration/ofSeconds 3)))))

(defn list-dlq []
  (list-topic "dlq"))

(defn send-record! [topic k v & [{:keys [value-serializer schema-id]}]]
  (with-open [producer (KafkaProducer.
                         ^Map (merge {"bootstrap.servers" (kafka-endpoint-on-host)
                                      "key.serializer" (.getName StringSerializer)}

                                     (if-not value-serializer
                                       {"value.serializer" (.getName StringSerializer)}

                                       {"value.serializer" (case value-serializer
                                                             :json-schema (.getName KafkaJsonSchemaSerializer)
                                                             :avro (.getName KafkaAvroSerializer))
                                        "schema.registry.url" (schema-registry-base-url)
                                        "use.schema.id" (do
                                                          (when-not (int? schema-id)
                                                            (throw (IllegalArgumentException. "schema-id required")))
                                                          (int schema-id))
                                        "id.compatibility.strict" false
                                        "auto.register.schemas" false
                                        "use.latest.version" true})))]
    (-> (.send producer (ProducerRecord. topic k v))
        (.get))))

(defn register-schema! [{:keys [subject schema-type schema]}]
  (let [url (str (schema-registry-base-url) "/subjects/" subject "/versions")
        resp (http/post url
               {:headers {"Content-Type" "application/vnd.schemaregistry.v1+json"}
                :body (json/write-value-as-string {:schemaType (case schema-type
                                                                 :json "JSON"
                                                                 :avro "AVRO")
                                                   :schema (cond
                                                             (string? schema) schema
                                                             (map? schema) (json/write-value-as-string schema))})})]
    (-> resp :body json/read-value (get "id"))))

; For dev
(comment
  (reload-connector!))
