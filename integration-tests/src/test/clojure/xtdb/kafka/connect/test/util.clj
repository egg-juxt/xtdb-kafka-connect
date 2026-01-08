(ns xtdb.kafka.connect.test.util
  (:require [clojure.test :refer :all]
            [jsonista.core :as json]
            [xtdb.api :as xt]))

(defn query-col-types [node table-name]
  (let [col-types-res (xt/q node ["SELECT column_name, data_type
                                   FROM information_schema.columns
                                   WHERE table_name = ?"
                                  table-name])]
    (-> col-types-res
        (->> (map (fn [{:keys [column-name data-type]}]
                    [(keyword column-name) (read-string data-type)]))
             (into {}))
      (dissoc :_valid_from :_valid_to :_system_from :_system_to))))

(defn avro-record-put-all [^org.apache.avro.generic.GenericRecord r m]
  (reduce
    (fn [^org.apache.avro.generic.GenericRecord r [k v]]
      (doto r
        (.put (name k) v)))
    r
    m))

(defn ->avro-record [schema m]
  (doto (org.apache.avro.generic.GenericData$Record.
          (-> (org.apache.avro.Schema$Parser.)
              (.parse (json/write-value-as-string schema))))
    (avro-record-put-all m)))

(defn patiently [pred f]
  (let [end-time (+ (System/currentTimeMillis) 10000)]
    (loop [result nil]
      (if (and (not (pred result))
               (<= (System/currentTimeMillis) end-time))
        (do
          (Thread/sleep 1000)
          (recur (f)))
        result))))
