(ns xtdb.kafka.connect.perf-test
  (:require [clojure.test :refer :all]
            [jsonista.core :as json]
            [xtdb.api :as xt]
            [xtdb.kafka.connect.test.e2e-fixture :as fixture :refer [*xtdb-conn*]])
  (:import (eu.rekawek.toxiproxy ToxiproxyClient)
           (eu.rekawek.toxiproxy.model ToxicDirection)
           (org.testcontainers.containers Network ToxiproxyContainer)))

(use-fixtures :once fixture/with-containers)
(comment ; For dev
  (fixture/run-permanently)
  (do
    (fixture/stop-permanently)
    (fixture/run-permanently)))

(use-fixtures :each fixture/with-xtdb-conn)

(deftest ^:manual test_throughput
  (let [lag-ms 500
        total-count 3000]
    (with-open [toxi (let [c (doto (ToxiproxyContainer. "ghcr.io/shopify/toxiproxy:2.12.0")
                               (.withNetwork Network/SHARED)
                               (.start))
                           proxy (-> (ToxiproxyClient. (.getHost c) (.getControlPort c))
                                   (.createProxy "xtdb-proxy" "0.0.0.0:5432" (fixture/xtdb-host+port))
                                   (.toxics) (.latency "lag" ToxicDirection/DOWNSTREAM lag-ms))]
                       c)
                _ (fixture/with-connector
                    {:connection.url (str "jdbc:xtdb://" (-> toxi .getNetworkAliases first) "/" fixture/*xtdb-db*)
                     :consumer.override.fetch.min.bytes (* 10 1024 1024)

                     :topics "my_table"
                     :value.converter "org.apache.kafka.connect.json.JsonConverter"
                     :transforms "xtdbEncode"
                     :transforms.xtdbEncode.type "xtdb.kafka.connect.SchemaDrivenXtdbEncoder"})]
      (doseq [id (range total-count)]
        (fixture/send-record! "my_table" (str id)
          (json/write-value-as-string
            {:schema {:type "struct"
                      :fields [{:field :_id, :type "int32", :optional false}
                               {:field :my_string, :type "string", :optional false}]}
             :payload {:_id id
                       :my_string (str "my_string_value" id)}})))

      (while (-> (let [res (xt/q *xtdb-conn* ["SELECT COUNT(*) AS c FROM my_table"])]
                   (println (java.time.Instant/now) res)
                   res)
               first :c (< total-count))
        (println "awaiting...")
        (Thread/sleep ^long (* 3 lag-ms)))

      (println "#txs" (xt/q *xtdb-conn* ["SELECT COUNT(*) AS c FROM xt.txs"])))

      (Thread/sleep ^long (* 5 lag-ms))))

