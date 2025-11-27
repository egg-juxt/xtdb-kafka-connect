(ns xtdb.kafka.connect-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.kafka.connect.test.util :refer [->sink-record ->struct]]
            [xtdb.test.xtdb-fixture :as xtdb])
  (:import (com.zaxxer.hikari HikariDataSource)
           (org.apache.kafka.connect.data Schema SchemaBuilder)
           (org.apache.kafka.connect.errors RetriableException)
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

(defn sink! [sink-task, record]
  (.put sink-task [(->sink-record (merge record {:topic "foo"}))]))

(defn query! []
  (xt/q xtdb/*conn* "SELECT * FROM foo"))

(deftest insert_mode-option
  (with-open [sink-task (start-sink! {:insert.mode "insert"})]
    (sink! sink-task {:key-value 1, :value-value {:_id 1, :a 1}})
    (sink! sink-task {:key-value 1, :value-value {:_id 1, :b 2}})
    (is (= (first (query!)) {:xt/id 1, :b 2})))

  (with-open [sink-task (start-sink! {:insert.mode "patch"})]
    (sink! sink-task {:key-value 1, :value-value {:_id 1, :a 1}})
    (sink! sink-task {:key-value 1, :value-value {:_id 1, :b 2}})
    (is (= (first (query!)) {:xt/id 1, :a 1, :b 2}))

    (testing "trying to PATCH with a NULL value"
      (let [exc (is (thrown? Exception (sink! sink-task {:key-value 1, :value-value {:_id 1, :c nil}})))]
        (is (and (not (instance? RetriableException exc)) (str/includes? (ex-message exc) "NULL"))))
      (let [exc (is (thrown? Exception (sink! sink-task {:key-value 1, :value-value {:_id 1, :c {:d nil}}})))]
        (is (not (instance? RetriableException exc)) (str/includes? (ex-message exc) "NULL")))
      (sink! sink-task {:key-value 1, :value-value {:_id 1, :c [nil]}}))))

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