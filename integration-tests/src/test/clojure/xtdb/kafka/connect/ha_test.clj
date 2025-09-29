(ns xtdb.kafka.connect.ha-test
  (:require [clojure.test :refer :all]
            [jsonista.core :as json]
            [xtdb.kafka.connect.test.e2e-fixture :as fixture]))

(use-fixtures :once fixture/with-containers)
(comment ; For dev
  (fixture/run-permanently!)
  (do
    (fixture/stop-permanently!)
    (fixture/run-permanently!))

  (fixture/stop-permanently!))

(use-fixtures :each fixture/with-xtdb-conn)

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

    (fixture/stop-container! ::fixture/xtdb)
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
        (fixture/start-container! ::fixture/xtdb)))))


