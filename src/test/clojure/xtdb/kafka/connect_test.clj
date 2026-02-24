(ns xtdb.kafka.connect-test
  (:require [clojure.test :refer :all]
            [xtdb.api :as xt]
            [xtdb.kafka.connect.test.util :refer [->sink-record ->struct]]
            [xtdb.test.xtdb-fixture :as xtdb])
  (:import (org.apache.kafka.connect.data Schema SchemaBuilder)
           (org.apache.kafka.connect.sink SinkTaskContext)
           (xtdb.kafka.connect XtdbSinkTask)))

(declare thrown?)

(use-fixtures :once xtdb/with-container)
(use-fixtures :each xtdb/with-conn)

(defn start-sink! [conf]
  (doto (XtdbSinkTask.)
    (.initialize (reify SinkTaskContext))
    (.start (-> {:connection.url xtdb/*jdbc-url*}
              (merge conf)
              (update-keys name)))))

(defn sink! [sink-task record]
  (.put sink-task [(->sink-record (merge {:topic "foo"}
                                         record))]))

(defn query! [& table]
  (xt/q xtdb/*conn* (str "SELECT * FROM " (or table "foo"))))

(deftest insert_mode-option
  (with-open [sink-task (start-sink! {:insert.mode "insert"})]
    (sink! sink-task {:key-value 1, :value-value {:_id 1, :a 1}})
    (sink! sink-task {:key-value 1, :value-value {:_id 1, :b 2}})
    (is (= (first (query!)) {:xt/id 1, :b 2})))

  (with-open [sink-task (start-sink! {:insert.mode "patch"})]
    (sink! sink-task {:key-value 1, :value-value {:_id 1, :a 1}})
    (sink! sink-task {:key-value 1, :value-value {:_id 1, :b 2}})
    (is (= (first (query!)) {:xt/id 1, :a 1, :b 2}))))

(deftest id_mode-option
  (let [sink (fn [conf record]
               (with-open [sink-task (start-sink! conf)]
                 (.put sink-task [(->sink-record (-> record
                                                     (merge {:topic "foo"})))])))
        query-foo #(first (xt/q xtdb/*conn* "SELECT * FROM foo"))]

    (sink {:id.mode "record_key"} {:key-value 1
                                   :key-schema Schema/INT64_SCHEMA
                                   :value-value {:_id 2, :v "v"}})
    (is (= (query-foo) {:xt/id 1, :v "v"}))

    (sink {:id.mode "record_key"} (let [schema (-> (SchemaBuilder/struct)
                                                   (.field "_id" Schema/INT64_SCHEMA))]
                                    {:key-value (->struct schema {:_id 1})
                                     :key-schema schema
                                     :value-value {:_id 2, :v "v"}}))
    (is (= (query-foo) {:xt/id 1, :v "v"}))

    (sink {} {:key-value nil
              :value-value {:_id 1 :v "v"}})
    (is (= (query-foo) {:xt/id 1, :v "v"}))))

(deftest table_name_map-option
  (with-open [sink-task (start-sink! {:table.name.map "topic1:table1,topic2:table2"})]
    (sink! sink-task {:topic "foo" :key-value 1, :value-value {:_id 0}})
    (sink! sink-task {:topic "topic1" :key-value 1, :value-value {:_id 1}})
    (sink! sink-task {:topic "topic2" :key-value 1, :value-value {:_id 2}})
    (is (= (query! "foo") [{:xt/id 0}]))
    (is (= (query! "table1") [{:xt/id 1}]))
    (is (= (query! "table2") [{:xt/id 2}]))))

(deftest ingest_struct_in_map
  (with-open [sink-task (start-sink! {})]
    (sink! sink-task (let [nested-schema (-> (SchemaBuilder/struct)
                                           (.field "nested_string" Schema/STRING_SCHEMA)
                                           (.build))
                           schema (-> (SchemaBuilder/struct)
                                    (.field "_id" Schema/INT64_SCHEMA)
                                    (.field "my_map" (-> (SchemaBuilder/map Schema/STRING_SCHEMA nested-schema)
                                                       (.build)))
                                    (.build))]
                       {:key-schema SchemaBuilder/INT64_SCHEMA
                        :key-value 1
                        :value-schema schema
                        :value-value (->struct schema
                                       {:_id 1
                                        :my_map {"map_key" (->struct nested-schema
                                                             {:nested_string "nested"})}})}))
    (is (= (query!) [{:xt/id 1
                      :my-map {:map-key {:nested-string "nested"}}}]))))
