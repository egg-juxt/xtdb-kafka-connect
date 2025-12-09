(ns xtdb.kafka.connect.encode-test
  (:require [clojure.test :refer :all]
            [clojure.set :refer [rename-keys]]
            [jsonista.core :as json]
            [xtdb.api :as xt]
            [xtdb.kafka.connect.test.containers-fixture :as fixture :refer [*xtdb-conn*]]
            [xtdb.kafka.connect.test.util :refer [query-col-types ->avro-record]])
  (:import (java.nio ByteBuffer)
           (java.time LocalDate)
           (java.time.temporal ChronoUnit)))

(use-fixtures :once fixture/with-containers)
(comment ; For dev
  (fixture/run-permanently!)
  (do
    (fixture/stop-permanently!)
    (fixture/run-permanently!)))

(use-fixtures :each fixture/with-xtdb-conn)

(deftest ingest_all_types_with_in-band_connect-schema
  (with-open [_ (fixture/with-connector
                  {:topics "my_table"
                   :value.converter "org.apache.kafka.connect.json.JsonConverter"
                   :transforms "xtdbEncode"
                   :transforms.xtdbEncode.type "xtdb.kafka.connect.SchemaDrivenXtdbEncoder"})]
    (fixture/send-record! "my_table" "my_id"
      (json/write-value-as-string
        {:schema {:type "struct",
                  :fields [{:field :_id, :type "string", :optional false}
                           {:field :my_string, :type "string", :optional false}
                           {:field :my_int16, :type "int16", :optional false}
                           {:field :my_timestamptz, :type "string", :optional false, :parameters {:xtdb.type "timestamptz"}}
                           {:field :_valid_from, :type "string", :optional false, :parameters {:xtdb.type "timestamptz"}}]}
         :payload {:_id "my_id"
                   :my_string "my_string_value"
                   :my_int16 42
                   :my_timestamptz "2020-01-01T00:00:00Z"
                   :_valid_from "2020-01-02T00:00:00Z"}}))

    (Thread/sleep 10000)

    (is (= (xt/q *xtdb-conn* "SELECT *, _valid_from FROM my_table FOR VALID_TIME ALL")
           [{:xt/id "my_id"
             :my-string "my_string_value"
             :my-int16 42
             :my-timestamptz #xt/zdt"2020-01-01T00:00Z"
             :xt/valid-from #xt/zdt"2020-01-02T00:00Z[UTC]"}]))

    (is (= (query-col-types *xtdb-conn* "my_table")
           {:_id :utf8
            :my_string :utf8
            :my_int16 :i16
            :my_timestamptz [:timestamp-tz :micro "Z"]})))) ; TODO: should be UTC?

(deftest patch_with_optional_fields
  (xt/execute-tx *xtdb-conn* [[:put-docs :my_table {:xt/id "my_id"
                                                    :my_string_1 "my_old_string_value_1"
                                                    :my_string_2 "my_old_string_value_2"}]])
  (with-open [_ (fixture/with-connector
                  {:topics "my_table"
                   :value.converter "org.apache.kafka.connect.json.JsonConverter"
                   :insert.mode "patch"
                   :transforms "xtdbEncode"
                   :transforms.xtdbEncode.type "xtdb.kafka.connect.SchemaDrivenXtdbEncoder"})]
    (fixture/send-record! "my_table" "my_id"
      (json/write-value-as-string
        {:schema {:type "struct",
                  :fields [{:field :_id, :type "string", :optional false}
                           {:field :my_string_1, :type "string", :optional false}
                           {:field :my_string_2, :type "string", :optional true}]}
         :payload {:_id "my_id"
                   :my_string_1 "my_new_string_value_1"}}))

    (Thread/sleep 10000)

    (is (= (set (xt/q *xtdb-conn* "SELECT * FROM my_table FOR VALID_TIME ALL"))
           (set [{:xt/id "my_id", :my-string-1 "my_old_string_value_1", :my-string-2 "my_old_string_value_2"}
                 {:xt/id "my_id", :my-string-1 "my_new_string_value_1", :my-string-2 "my_old_string_value_2"}])))))

(defn await-readings-result []
  (let [timeout 5000]
    (Thread/sleep timeout)
    (let [results (xt/q *xtdb-conn* "SELECT *, _valid_from FROM readings FOR VALID_TIME ALL")]
      (when (empty? results)
        (println (.getLogs fixture/connect)))
      (->> results
        (map #(rename-keys % {:xt/id :_id
                              :xt/valid-from :_valid_from}))))))

(def avro-test-elements
  (mapv #(zipmap [:avro-type :xtdb-type :col-value :col-name] %)
    [["long" :i64 1 :_id]

     ; Avro primitive types
     ["boolean" :bool true :bool_col]
     ["int" :i32 42 :int_col]
     ["long" :i64 42 :long_col]
     ["float" :f32 42 :float_col]
     ["double" :f64 42 :double_col]
     ["bytes" :varbinary (ByteBuffer/wrap (byte-array [1 2 3])) :bytes_col]
     ["string" :utf8 "hey!" :string_col]

     ; Avro complex types
     [{:type "array", :items "string"} [:list :utf8] ["hi", "bye"] :strings_col]
     [{:type "array", :items "int"} [:list :i32] [1 2 3] :ints_col]
     [{:type "map", :values "string"} [:struct {'k1 :utf8, 'k2 :utf8}] {"k1" "v1", "k2" "v2"} :string_map_col]
     (let [record-schema {:type "record"
                          :name "MyRecord"
                          :fields [{:name "k1", :type "int"}
                                   {:name "k2", :type "string"}]}]
       [record-schema [:struct {'k1 :i32, 'k2 :utf8}] (->avro-record record-schema {"k1" 1, "k2" "v2"}) :record_col])

     ; logical types
     [{:type "long", :logicalType "timestamp-millis"} [:timestamp-tz :micro "UTC"] (System/currentTimeMillis) :timestamp_col]
     (let [local-date (LocalDate/of 2025 10 1)
           day-count (-> ChronoUnit/DAYS (.between (LocalDate/of 1970 1 1) local-date))]
       [{:type "int", :logicalType "date"} [:date :day] day-count :date_col])

     ; explicit xtdb type
     [{:type "string", :connect.parameters {:xtdb.type "interval"}} [:interval :month-day-micro] "P1DT1H" :xtdb_interval_col]

     ,]))

(deftest test_avro_types
  (with-open [_ (fixture/with-connector {:topics "my_table"
                                         :value.converter "io.confluent.connect.avro.AvroConverter"
                                         :value.converter.schemas.enable "true"
                                         :value.converter.connect.meta.data "true"
                                         :value.converter.schema.registry.url (fixture/schema-registry-base-url-for-containers)
                                         :transforms "xtdbEncode"
                                         :transforms.xtdbEncode.type "xtdb.kafka.connect.SchemaDrivenXtdbEncoder"})]
    (let [avro-schema {:type "record"
                       :name "Reading"
                       :fields (vec (for [elem avro-test-elements]
                                      {:name (:col-name elem)
                                       :type (:avro-type elem)}))}
          schema-id (fixture/register-schema! {:subject "my_table-value"
                                               :schema-type :avro
                                               :schema avro-schema})
          data (into {}
                     (for [elem avro-test-elements]
                       {(:col-name elem) (:col-value elem)}))
          resp (fixture/send-record! "my_table" "1"
                 (->avro-record avro-schema data)
                 {:value-serializer :avro
                  :schema-id schema-id})]
      (println "send resp" resp)
      (await-readings-result)
      (is (= (query-col-types *xtdb-conn* "my_table")
             (into {}
                   (for [elem avro-test-elements]
                     {(:col-name elem) (:xtdb-type elem)}))))

      (do
        (def my-results (xt/q *xtdb-conn* "FROM my_table"))
        (println "row is" my-results)))))
