(ns xtdb.kafka.connect
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc])
  (:import (clojure.lang ExceptionInfo)
           (java.sql SQLException SQLTransientConnectionException)
           [java.util List Map]
           [org.apache.kafka.connect.data Field Schema Struct]
           org.apache.kafka.connect.sink.SinkRecord
           (org.apache.kafka.connect.errors RetriableException)
           (org.apache.kafka.connect.sink SinkTaskContext)
           (xtdb.kafka.connect XtdbSinkConfig)))

(defn- map->edn [m]
  (->> (for [[k v] m]
         [(keyword k)
          (if (instance? Map v)
            (map->edn v)
            v)])
       (into {})))

(defn- get-struct-contents [val]
  (cond
    (instance? Struct val)
    (let [struct-schema (.schema ^Struct val)
          struct-fields (.fields ^Schema struct-schema)]
      (reduce conj
              (map (fn [^Field field] {(keyword (.name field)) (get-struct-contents (.get ^Struct val field))})
                   struct-fields)))
    (instance? List val) (into [] (map get-struct-contents val))
    (instance? Map val) (zipmap (map keyword (.keySet ^Map val)) (map get-struct-contents (.values ^Map val)))
    :else val))

(defn- struct->edn [^Struct s]
  (let [output-map (get-struct-contents s)]
    (log/trace "map val: " output-map)
    output-map))

(defn- record->edn [^SinkRecord record]
  (let [schema (.valueSchema record)
        value (.value record)]
    (cond
      (and (instance? Struct value) schema)
      (struct->edn value)

      (and (instance? Map value)
           (nil? schema)
           (= #{"payload" "schema"} (set (keys value))))
      (let [payload (.get ^Map value "payload")]
        (cond
          (string? payload)
          (json/parse-string payload true)

          (instance? Map payload)
          (map->edn payload)

          :else
          (throw (IllegalArgumentException. (str "Unknown JSON payload type: " record)))))

      (instance? Map value)
      (map->edn value)

      (string? value)
      (json/parse-string value true)

      :else
      (throw (IllegalArgumentException. (str "Unknown message type: " record))))))

(defn- find-record-key-eid [^XtdbSinkConfig _conf, ^SinkRecord record]
  (let [r-key (.key record)]
    (cond
      (nil? r-key)
      (throw (IllegalArgumentException. "no record key"))

      (nil? (.keySchema record))
      (throw (IllegalArgumentException. "no record key schema"))

      (-> record .keySchema .type .isPrimitive)
      r-key

      (instance? Struct r-key)
      (or (-> r-key struct->edn :_id)
          (throw (IllegalArgumentException. "no 'id' field in record key")))

      (map? r-key)
      (or (-> r-key :_id)
          (throw (IllegalArgumentException. "no 'id' field in record key"))))))

(defn- find-record-value-eid [^XtdbSinkConfig _conf, ^SinkRecord _record, doc]
  (if-some [id (:_id doc)]
    id
    (throw (IllegalArgumentException. "no 'id' field in record value"))))

(defn- find-eid [^XtdbSinkConfig conf ^SinkRecord record doc]
  (case (.getIdMode conf)
    "record_key" (find-record-key-eid conf record)
    "record_value" (find-record-value-eid conf record doc)))

(defn- tombstone? [^SinkRecord record]
  (and (nil? (.value record)) (.key record)))

(defn table-name [^XtdbSinkConfig conf, ^SinkRecord record]
  (let [topic (.topic record)
        table-name-format (.getTableNameFormat conf)]
    (str/replace table-name-format "${topic}" topic)))

(defn transform-sink-record [^XtdbSinkConfig conf, ^SinkRecord record]
  (log/trace "sink record:" record)
  (let [table (table-name conf record)]
    (doto (cond
            (not (tombstone? record))
            (let [doc (record->edn record)
                  id (find-eid conf record doc)]
              {:op :insert
               :table table
               :params [(assoc doc :_id id)]})

            (= "record_key" (.getIdMode conf))
            (let [id (find-record-key-eid conf record)]
              {:op :delete
               :table table
               :params [id]})

            :else (throw (IllegalArgumentException. (str "Unsupported tombstone mode: " record))))

      (->> (log/trace "tx op:")))))

(defn submit-sink-records-impl [connectable props records]
  (log/debug "processing records..." {:count (count records)})
  (when (seq records)
    (with-open [conn (jdbc/get-connection connectable)]
      (let [start (System/nanoTime)]
        (jdbc/with-transaction [txn conn]
          (doseq [batch (->> records
                          (map (partial transform-sink-record props))
                          (partition-by (juxt :op :table)))]
            (let [{:keys [op table]} (first batch)
                  sql (str/join " " (case op
                                      :insert ["INSERT INTO" table "RECORDS ?"]
                                      :delete ["DELETE FROM" table "WHERE _id = ?"]))]
              (with-open [prep-stmt (jdbc/prepare txn [sql])]
                (jdbc/execute-batch! prep-stmt (map :params batch)))
              (log/debug "sent batch" {:op op, :table table, :batch-size (count batch)}))))
        (log/debug "committed records" {:count (count records)
                                        :ellapsed-ms (-> (System/nanoTime) (- start) double (/ 1000000))})))))

(def retry-delay-ms 10000)
(def max-retries 2)
(def remaining-tries (atom (inc max-retries)))

(defn reset-tries! []
  (reset! remaining-tries (inc max-retries)))

(defn handle-psql-exception [^SinkTaskContext context, ^SQLException e]
  (let [transient-connection-error? (or (instance? SQLTransientConnectionException e)
                                        (= "08001" (.getSQLState e)))]
    (if transient-connection-error?
      (reset-tries!)
      (swap! remaining-tries dec))

    (if (pos? @remaining-tries)
      (do
        (log/info "retrying" {:remaining-retries @remaining-tries, :retry-delay-ms retry-delay-ms})
        (.timeout context retry-delay-ms)
        (throw (RetriableException. ^Throwable e)))
      (throw e))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn submit-sink-records [^SinkTaskContext context connectable props records]
  (try
    (submit-sink-records-impl connectable props records)
    (reset-tries!)

    (catch SQLException e
      (handle-psql-exception context e))

    ; Special handling of next.jdbc exception when rolling back a transaction. See next.jdbc.transaction/transact*
    (catch ExceptionInfo e
      (let [{:keys [handling rollback]} (ex-data e)]
        (when (and handling
                   rollback
                   (instance? SQLException handling))
          (.addSuppressed handling rollback)
          (handle-psql-exception context handling))))))
