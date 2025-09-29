(ns xtdb.kafka.connect-test
  (:require [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.kafka.connect.test.util :refer [->sink-record ->struct]]
            [xtdb.test.xtdb-fixture :as xtdb])
  (:import (com.zaxxer.hikari HikariDataSource)
           (org.apache.kafka.connect.data Schema SchemaBuilder)
           (xtdb.kafka.connect XtdbSinkTask)))

(use-fixtures :once xtdb/with-container)
(use-fixtures :each xtdb/with-conn)

(defn start-sink! [conf]
  (doto (XtdbSinkTask.)
    (.start (-> {:connection.url xtdb/*jdbc-url*}
              (merge conf)
              (update-keys name)))))

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

(deftest ^:manual connection-reuse
  (with-open [sink-task (start-sink! {})]
    (let [sink (fn [record]
                 (.put sink-task [(->sink-record (-> record
                                                   (merge {:topic "foo"})))]))
          schema (-> (SchemaBuilder/struct)
                   (.field "_id" Schema/INT64_SCHEMA))]
      (log/info "--------------- Before first sink...")
      (sink {:key-value nil
             :key-schema schema
             :value-value {:_id 1, :v "v"}})
      (log/info "--------------- Sleeping...")
      ; When configured below 10 seconds this should reuse connection. Check in logs
      (Thread/sleep 60000)
      (log/info "--------------- Before second sink...")
      (sink {:key-value nil
             :key-schema schema
             :value-value {:_id 2, :v "v"}}))))

(deftest ^:manual check-jdbc4-isValid-works-for-XtConnection
  (with-open [conn (jdbc/get-connection xtdb/*jdbc-url*)]
    (jdbc/execute-one! conn ["SELECT 1"])
    (is (.isValid conn 3000))
    (.close conn)
    (is (not (.isValid conn 3000)))))

(deftest ^:manual just-connect
  (with-open [_ (start-sink! {:connection.url "jdbc:xtdb://localhost:51222/wrongport"})]
    (Thread/sleep 10000)
    (log/info "--------------- Closing...")))
      ; When configured below 10 seconds this should reuse connection. Check in logs

(deftest ^:manual check-first-connection
  (with-open [pool (doto (HikariDataSource.)
                     (.setJdbcUrl "jdbc:xtdb://localhost:51222/wrongport"))]
    (println (.getConnection pool))))