(ns xtdb.kafka.connect-test
  (:require [clojure.test :refer :all]
            [xtdb.api :as xt]
            [xtdb.kafka.connect :as kconn]
            [xtdb.kafka.connect.test.util :refer [->sink-record ->struct]]
            [xtdb.test.xtdb-fixture :as xtdb])
  (:import (org.apache.kafka.connect.data Schema SchemaBuilder)
           (xtdb.kafka.connect XtdbSinkConfig)))

(use-fixtures :once xtdb/with-container)
(use-fixtures :each xtdb/with-conn)

(deftest id_mode-option
  (let [sink (fn [conf record]
               (kconn/submit-sink-records xtdb/*conn*
                 (XtdbSinkConfig/parse (-> conf
                                           (merge {:connection.url "url"})
                                           (update-keys name)))
                 [(->sink-record (-> record
                                     (merge {:topic "foo"})))]))
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
