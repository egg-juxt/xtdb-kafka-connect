(ns xtdb.kafka.connect.encode-test
  (:require [clojure.set :refer [rename-keys]]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [jsonista.core :as json]
            [xtdb.api :as xt]
            [xtdb.kafka.connect.test.containers-fixture :as fixture :refer [*xtdb-conn*]]
            [xtdb.kafka.connect.test.util :refer [query-col-types ->avro-record patiently]])
  (:import (java.nio ByteBuffer)
           (java.time LocalDate)
           (java.time.temporal ChronoUnit)))

(use-fixtures :once fixture/with-containers)
(use-fixtures :each fixture/with-xtdb-conn)

(comment
  (fixture/reload-connector!))

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

    (is (= (patiently seq #(xt/q *xtdb-conn* "SELECT *, _valid_from FROM my_table FOR VALID_TIME ALL"))
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

    (is (= (set (patiently #(= (count %) 2)
                  #(xt/q *xtdb-conn* "SELECT * FROM my_table FOR VALID_TIME ALL")))
           (set [{:xt/id "my_id", :my-string-1 "my_old_string_value_1", :my-string-2 "my_old_string_value_2"}
                 {:xt/id "my_id", :my-string-1 "my_new_string_value_1", :my-string-2 "my_old_string_value_2"}])))))

(defn test_json-schema_types [conn-opts]
  (let [test-cases
        (mapv #(zipmap [:json-type :xtdb-type :col-value :col-name :q-value] %)
          [[{:type "integer"} :i64 1 :_id]

           ; Kafka Connect does not support: "null" type, unspecified type, "not" composition
           ; primitive types
           [{:type "boolean"} :bool true :bool_col]
           [{:type "integer"} :i64 42 :int_col]
           [{:type "number"} :f64 42 :num_col 42.0]
           [{:type "string"} :utf8 "hey!" :string_col]

           ; compositions
           [{:oneOf [{:type "integer"} {:type "string"}]} :utf8 "hey!" :oneof_col1]
           [{:oneOf [{:type "integer"} {:type "null"}]} [:? :null] nil :oneof_col2]

           ; complex types
           [{:type "array", :items {:type "string"}} [:list :utf8] ["hi", "bye"] :strings_col]
           [{:type "object", :properties {:k1 {:type "integer"}}} [:struct {"k1" :i64}] {"k1" 1} :obj_col {:k1 1}]

           ; connect types
           [{:type "string", :connect.type "bytes"} :varbinary "Kg==" :bytes_col] ; base64
           [{:type "integer", :title "org.apache.kafka.connect.data.Date"} [:date :day] 20507 :date_col #xt/date"2026-02-23"] ; days since epoch
           [{:type "integer", :title "org.apache.kafka.connect.data.Time"} [:time-local :nano] 60000 :time_col #xt/time"00:01"] ; millis in day
           [{:type "integer", :title "org.apache.kafka.connect.data.Timestamp"} :instant 1771891200000 :timestamp_col #xt/zdt"2026-02-24T00:00Z[UTC]"] ; millis since epoch
           [{:type "integer", :title "org.apache.kafka.connect.data.Decimal"} [:decimal 32 0 128] 10 :decimal_col 10M]])

        schema-id (fixture/register-schema! {:subject "my_table-value"
                                             :schema-type :json
                                             :schema {:type "object"
                                                      :properties (into {}
                                                                    (for [c test-cases]
                                                                      [(:col-name c) (:json-type c)]))
                                                      :required (map :col-name test-cases)}})]
    (with-open [_ (fixture/with-connector (merge {:topics "my_table"
                                                  :value.converter "io.confluent.connect.json.JsonSchemaConverter"
                                                  :value.converter.connect.meta.data "true"
                                                  :value.converter.schemas.enable "true"
                                                  :value.converter.schema.registry.url (fixture/schema-registry-base-url-for-containers)
                                                  ; See https://docs.confluent.io/cloud/current/connectors/reference/connector-configuration.html
                                                  :transforms "xtdbEncode"
                                                  :transforms.xtdbEncode.type "xtdb.kafka.connect.SchemaDrivenXtdbEncoder"}
                                                 conn-opts))]
      (fixture/send-record! "my_table" "1"
        (into {}
          (for [elem test-cases]
            [(name (:col-name elem)) (:col-value elem)]))
        {:value-serializer :json-schema
         :schema-id schema-id})

      (let [q-result (first (patiently seq #(xt/q *xtdb-conn* "SELECT * FROM my_table")))
            bs (:bytes-col q-result)]
        (is (bytes? bs))
        (is (= (count bs) 1))
        (is (= (aget bs 0) 42))

        (is (= (-> q-result
                 (dissoc :bytes-col))
               (-> (into {}
                     (->> (for [c test-cases]
                            [(:col-name c) (or (:q-value c) (:col-value c))])
                       (filter (fn [[_ v]] (some? v)))))
                 (update-keys #(-> % name (str/replace "_" "-") keyword))
                 (rename-keys {:-id :xt/id})
                 (dissoc :bytes-col)))))

      (is (= (query-col-types *xtdb-conn* "my_table")
             (into {}
               (for [c test-cases]
                 {(:col-name c) (:xtdb-type c)})))))))

(deftest test_json-schema_types_with_default_sum_type
  (test_json-schema_types {}))

(deftest test_json-schema_types_with_generalized_sum_type
  (test_json-schema_types {:value.converter.generalized.sum.type.support "true"}))

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
     [{:type "map", :values "string"} [:struct {"k1" :utf8} {"k2" :utf8}] {"k1" "v1", "k2" "v2"} :string_map_col]
     (let [record-schema {:type "record"
                          :name "MyRecord"
                          :fields [{:name "k1", :type "int"}
                                   {:name "k2", :type "string"}]}]
       [record-schema [:struct {"k1" :i32} {"k2" :utf8}] (->avro-record record-schema {"k1" 1, "k2" "v2"}) :record_col])

     ; logical types
     [{:type "long", :logicalType "timestamp-millis"} :instant (System/currentTimeMillis) :timestamp_col]
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

      (patiently seq #(xt/q *xtdb-conn* "SELECT *, _valid_from FROM my_table FOR VALID_TIME ALL"))

      (is (= (query-col-types *xtdb-conn* "my_table")
             (into {}
                   (for [elem avro-test-elements]
                     {(:col-name elem) (:xtdb-type elem)})))))))
