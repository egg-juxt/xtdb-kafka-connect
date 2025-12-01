(ns xtdb.kafka.connect.ha-test
  (:require [clojure.test :refer :all]
            [jsonista.core :as json]
            [xtdb.kafka.connect.test.containers-fixture :as fixture]))

(use-fixtures :once fixture/with-containers)
(use-fixtures :each fixture/with-xtdb-conn)

(comment
  (fixture/reload-connector!))

(deftest errors_go_to_dead-letter-queue_once_and_in_the_same_order
  (let [table-name "foo"]
    (with-open [_ (fixture/with-connector
                    {:topics table-name
                     :value.converter "org.apache.kafka.connect.json.JsonConverter"
                     :transforms "xtdbEncode"
                     :transforms.xtdbEncode.type "xtdb.kafka.connect.SchemaDrivenXtdbEncoder"
                     :errors.tolerance "all"
                     :errors.log.enable true
                     :errors.log.include.messages true
                     :errors.deadletterqueue.topic.name "dlq"
                     :errors.deadletterqueue.topic.replication.factor 1
                     :errors.deadletterqueue.context.headers.enable true

                     :retry.backoff.ms 2000

                     ; consume the test records below as a batch, not one-by-one
                     :consumer.override.fetch.min.bytes (* 10 1024 1024)})]
      (let [value1-ok {:schema {:type "struct", :fields [{:field "_id", :type "int32", :optional false}]}
                       :payload {:_id 1}}
            value-sql-exception {:schema {:type "struct", :fields [{:field "_id", :type "float", :optional false}]}
                                 :payload {:_id 1.1}}
            value-record-data-exception {:schema {:type "struct", :fields [{:field "a", :type "string", :optional false}]}
                                         :payload {:a "1"}}
            value2-ok {:schema {:type "struct", :fields [{:field "_id", :type "int32", :optional false}]}
                       :payload {:_id 2}}]
        (doseq [v [value1-ok value-sql-exception value-record-data-exception value2-ok]]
          (fixture/send-record! table-name
            (or (:_id v) "none")
            (json/write-value-as-string v)))
        (Thread/sleep 2000)
        (let [dlq-records (fixture/list-dlq)]
          (is (= 2 (count dlq-records)))
          (is (= value-sql-exception (-> dlq-records (first) (.value) (json/read-value json/keyword-keys-object-mapper))))
          (is (= value-record-data-exception (-> dlq-records (second) (.value) (json/read-value json/keyword-keys-object-mapper))))
          (def dlq-records (seq (fixture/list-dlq))))))))

(comment
  (for [r dlq-records]
    {:value (-> r .value (json/read-value json/keyword-keys-object-mapper) (get :payload))
     :msg (-> r .headers (.lastHeader "__connect.errors.exception.message") .value (String.))
     :topic (-> r .headers (.lastHeader "__connect.errors.topic") .value (String.))})

  (->> dlq-records last (#(do {:headers (for [h (.headers %)]
                                          [(.key h) (-> (.value h) (String.))])
                               :value (.value %)})))
  (-> dlq-records last .headers)
  (for [r dlq-records]
    (.value r)))

(deftest ^:manual retries
  (let [builtin-table-name "information_schema.tables"] ; will error when written to
    (with-open [_ (fixture/with-connector
                    {:topics builtin-table-name
                     :value.converter "org.apache.kafka.connect.json.JsonConverter"
                     :transforms "xtdbEncode"
                     :transforms.xtdbEncode.type "xtdb.kafka.connect.SchemaDrivenXtdbEncoder"})]
      (fixture/send-record! builtin-table-name "1"
        (json/write-value-as-string
          {:schema {:type "struct", :fields [{:field :_id, :type "string", :optional false}]}
           :payload {:_id "1"}}))

      ; Must be retried until no remaining retries left
      (Thread/sleep 50000))))

(deftest ^:manual retries2
  (let [table-name "my_table"]
    (with-open [_ (fixture/with-connector
                    {:topics table-name
                     :value.converter "org.apache.kafka.connect.json.JsonConverter"
                     :transforms "xtdbEncode"
                     :transforms.xtdbEncode.type "xtdb.kafka.connect.SchemaDrivenXtdbEncoder"})]
      (fixture/send-record! table-name "1"
        (json/write-value-as-string
          {:schema {:type "struct", :fields [{:field :_id, :type "float", :optional false}]}
           :payload {:_id 1.1}})) ; a float id will error

      (Thread/sleep 50000))))

(deftest ^:manual xtdb_is_down
  (with-open [_ (fixture/with-connector
                  {:topics "my_table"
                   :value.converter "org.apache.kafka.connect.json.JsonConverter"
                   :transforms "xtdbEncode"
                   :transforms.xtdbEncode.type "xtdb.kafka.connect.SchemaDrivenXtdbEncoder"})]
    (fixture/send-record! "my_table" "1"
      (json/write-value-as-string
        {:schema {:type "struct",
                  :fields [{:field :_id, :type "string", :optional false}]}
         :payload {:_id "1"}}))

    (Thread/sleep 5000)

    (fixture/stop-xtdb!)
    (try
      (Thread/sleep 1000)

      (fixture/send-record! "my_table" "2"
        (json/write-value-as-string
          {:schema {:type "struct",
                    :fields [{:field :_id, :type "string", :optional false}]}
           :payload {:_id "2"}}))
      (println "record sent")

      (Thread/sleep 60000)

      (finally
        (fixture/start-xtdb!)))))


