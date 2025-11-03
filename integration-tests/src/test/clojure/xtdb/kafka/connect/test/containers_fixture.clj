(ns xtdb.kafka.connect.test.containers-fixture
  (:require [clojure.string :as str]
            [hato.client :as http]
            [clojure.java.io :as io]
            [clojure.test :refer :all]
            [integrant.core :as ig]
            [jsonista.core :as json]
            [next.jdbc :as jdbc])
  (:import (io.confluent.kafka.serializers KafkaAvroSerializer)
           (io.confluent.kafka.serializers.json KafkaJsonSchemaSerializer)
           (java.io File)
           (java.lang AutoCloseable)
           (java.util Collection Map Properties)
           (java.util.concurrent ExecutionException)
           (org.apache.kafka.clients.admin AdminClient)
           (org.apache.kafka.clients.producer KafkaProducer ProducerRecord)
           (org.apache.kafka.common.errors UnknownTopicOrPartitionException)
           (org.apache.kafka.common.serialization StringSerializer)
           (org.gradle.tooling GradleConnector)
           (org.testcontainers.containers BindMode Container GenericContainer Network)
           (org.testcontainers.containers.wait.strategy Wait)
           (org.testcontainers.kafka ConfluentKafkaContainer)
           (org.testcontainers.utility DockerImageName)))

(def kafka-version "8.0.1")

(defn with-env [^Container c m]
  (-> (fn [c k v]
        (.withEnv c (name k) (str v)))
    (reduce-kv c m)))

(def ^GenericContainer xtdb-container-conf
  (doto (GenericContainer. "ghcr.io/xtdb/xtdb:2.0.0")
    (.withEnv "XTDB_LOGGING_LEVEL" "debug")
    (.withCommand ^"[Ljava.lang.String;" (into-array ^String ["--playground"]))
    (.withNetwork Network/SHARED)
    (.withExposedPorts (into-array [(int 5432)]))))
    ;(.withReuse true)))

(defmethod ig/init-key ::xtdb [_ _]
  (doto xtdb-container-conf .start))

(defmethod ig/halt-key! ::xtdb [_ container]
  (.close container))

(defmethod ig/init-key ::kafka [_ _]
  (doto (ConfluentKafkaContainer. (DockerImageName/parse (str "confluentinc/cp-kafka:" kafka-version)))
    (.withExposedPorts (into-array [(int 9092)]))
    (.withNetwork Network/SHARED)
    (.start)))

(defmethod ig/halt-key! ::kafka [_ container]
  (.close container))

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

(defmethod ig/init-key ::connector-jar-file [_ _]
  (run-gradle-task! "shadowJar")
  (let [jar-file (io/file project-dir "build" "libs" (str "xtdb-kafka-connect-" project-version "-all.jar"))]
    (if (.exists jar-file)
      jar-file
      (throw (IllegalStateException. (str "Not found: " jar-file))))))

(defn kafka-endpoint-for-containers [kafka]
  (str (-> kafka .getNetworkAliases first) ":9093"))

(defmethod ig/init-key ::connect [_ {:keys [kafka ^File connector-jar-file]}]
  (let [plugin-path "/usr/local/share/xtdb-plugin"
        container (doto (GenericContainer. (DockerImageName/parse (str "confluentinc/cp-kafka-connect:" kafka-version)))
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
    container))

(defmethod ig/halt-key! ::connect [_ container]
  (.close container))

(defmethod ig/init-key ::schema-registry [_ {:keys [kafka]}]
  (doto (GenericContainer. (str "confluentinc/cp-schema-registry:" kafka-version))
    (.dependsOn [kafka])
    (.withNetwork (.getNetwork kafka))
    (.withExposedPorts (into-array [(int 8081)]))
    (with-env {:SCHEMA_REGISTRY_HOST_NAME "schema-registry"
               :SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS (kafka-endpoint-for-containers kafka)
               :SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL "none"})
    (.start)))

(defmethod ig/halt-key! ::schema-registry [_ container]
  (.close container))

(def conf {::connector-jar-file {}
           ::xtdb {}
           ::kafka {}
           ::connect {:xtdb (ig/ref ::xtdb)
                      :kafka (ig/ref ::kafka)
                      :connector-jar-file (ig/ref ::connector-jar-file)}
           ::schema-registry {:kafka (ig/ref ::kafka)}})

(defonce ^:dynamic *containers* nil)

(defn with-containers [f]
  (if *containers*
    (f)
    (binding [*containers* (ig/init conf)]
      (try
        (f)
        (finally
          (ig/halt! *containers*))))))

(defn run-permanently! []
  "Manually starts the fixture until manually stopped, for faster testing at dev-time."
  (let [init-exc (atom nil)]
    (alter-var-root #'*containers* (fn [prev]
                                     (or prev
                                         (try
                                           (ig/init conf)
                                           (catch Exception e
                                             (if-let [partial-system (-> e ex-data :system)]
                                               (do
                                                 (reset! init-exc e)
                                                 partial-system)
                                               (throw e)))))))
    (some-> @init-exc throw)))

(defn stop-permanently! []
  "Manually stops the fixture."
  (alter-var-root #'*containers* (fn [prev]
                                   (ig/halt! prev)
                                   nil)))

(defn stop-container! [k]
  (-> *containers*
    ^GenericContainer (get k)
    .stop))

(defn start-container! [k]
  (-> *containers*
    ^GenericContainer (get k)
    .start))

(def ^:dynamic *xtdb-db*)
(def ^:dynamic *xtdb-conn*)

(defn with-xtdb-conn [f]
  (binding [*xtdb-db* (random-uuid)]
    (with-open [xtdb-conn (jdbc/get-connection (str "jdbc:xtdb://localhost:"
                                                    (-> *containers* ::xtdb (.getMappedPort 5432))
                                                    "/" *xtdb-db*))]
      (binding [*xtdb-conn* xtdb-conn]
        (f)))))


; Test utilities

(defn connect-api-url []
  (let [connect (::connect *containers*)]
    (str "http://" (.getHost connect) ":" (.getMappedPort connect 8083))))

(defn schema-registry-base-url []
  (let [schema-registry (::schema-registry *containers*)
        host (.getHost schema-registry)
        port (.getMappedPort schema-registry 8081)]
    (str "http://" host ":" port)))

(defn schema-registry-base-url-for-containers []
  (let [schema-registry (::schema-registry *containers*)]
    (str "http://" (-> schema-registry .getNetworkAliases first) ":8081")))

(defn xtdb-host+port []
  (str (-> *containers* ::xtdb .getNetworkAliases first)
       ":5432"))

(defn xtdb-jdbc-url-for-containers []
  (str "jdbc:xtdb://" (xtdb-host+port) "/" *xtdb-db*))

(defn kafka-endpoint-on-host []
  (str "localhost:" (.getMappedPort (::kafka *containers*) 9092)))

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
        (delete-topics! (str/split topics #","))
        (catch ExecutionException e
          (if (instance? UnknownTopicOrPartitionException (ex-cause e))
            (println "unknown topic to delete" topics)
            (throw e)))))))

(defn send-record! [topic k v & [{:keys [value-serializer schema-id]}]]
  (with-open [producer (KafkaProducer. ^Map
                         (merge {"bootstrap.servers" (kafka-endpoint-on-host)
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
