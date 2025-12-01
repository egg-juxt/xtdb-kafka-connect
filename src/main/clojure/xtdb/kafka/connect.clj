(ns xtdb.kafka.connect
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [xtdb.kafka.connect.util :refer [sinkRecord-original-offset]])
  (:import (clojure.lang Atom ExceptionInfo)
           (java.sql SQLException SQLTransientConnectionException)
           [java.util Collection List Map]
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
          (throw (IllegalArgumentException. "no id field in record key")))

      (map? r-key)
      (or (-> r-key :_id)
          (throw (IllegalArgumentException. "no id field in record key"))))))

(defn- find-record-value-eid [^XtdbSinkConfig _conf, ^SinkRecord _record, doc]
  (if-some [id (:_id doc)]
    id
    (throw (IllegalArgumentException. "no id field in record value"))))

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

(defn record->op [^XtdbSinkConfig conf, ^SinkRecord record]
  (log/trace "sink record:" record)
  (let [table (table-name conf record)]
    (doto (cond
            (not (tombstone? record))
            (let [doc (record->edn record)
                  id (find-eid conf record doc)]
              {:op (case (.getInsertMode conf)
                     "insert" :insert
                     "patch" (do
                               ; Remove the following assert when XTDB's PATCH supports setting NULLs:
                               (when (some nil? (tree-seq map? vals doc))
                                 (throw (IllegalArgumentException. "PATCH doesn't allow setting NULL values")))
                               :patch))
               :table table
               :params [(assoc doc :_id id)]})

            (= "record_key" (.getIdMode conf))
            (let [id (find-record-key-eid conf record)]
              {:op :delete
               :table table
               :params [id]})

            :else (throw (IllegalArgumentException. (str "Unsupported tombstone mode: " record))))

      (->> (log/trace "tx op:")))))

(defn submit-sink-records* [^SinkTaskContext context
                            connectable
                            ^XtdbSinkConfig conf
                            records]
  (when (seq records)
    (let [record-ops (doall ; Find any record issues now
                       (for [record records]
                         (try
                           (merge {:record record}
                                  (record->op conf record))
                           (catch Exception e
                             (throw (ex-info nil {:type ::record-data-error} e))))))]
      (when (seq record-ops)
        (let [start (System/nanoTime)]
          (with-open [conn (jdbc/get-connection connectable)]
            (try
              (jdbc/with-transaction [txn conn]
                (doseq [batch (->> record-ops
                                (partition-by (juxt :op :table)))]
                  (let [{:keys [op table]} (first batch)
                        sql (str/join " " (case op
                                            :insert ["INSERT INTO" table "RECORDS ?"]
                                            :patch ["PATCH INTO" table "RECORDS ?"]
                                            :delete ["DELETE FROM" table "WHERE _id = ?"]))]
                    (with-open [prep-stmt (jdbc/prepare txn [sql])]
                      (jdbc/execute-batch! prep-stmt (map :params batch)))
                    (log/debug "sent batch" {:op op, :table table, :batch-size (count batch)}))))
              ; Unwrap next.jdbc ExceptionInfo when rollback fails (See next.jdbc.transaction/transact*):
              (catch ExceptionInfo e
                (throw (let [{:keys [handling rollback]} (ex-data e)]
                         (if (and handling rollback)
                           (doto handling (.addSuppressed rollback))
                           e))))))
          (log/debug "committed records" {:record-count (count record-ops)
                                          :ellapsed-ms (-> (System/nanoTime) (- start) double (/ 1000000))}))))))

(defn transient-connection-error? [e]
  (or (instance? SQLTransientConnectionException e)
      (and (instance? SQLException e)
           (= "08001" (.getSQLState e)))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn submit-sink-records [^SinkTaskContext context
                           connectable
                           ^XtdbSinkConfig conf,
                           ^Atom remaining-retries
                           ^Collection records]
  (letfn [(throw-retry! [cause & {:keys [log]}]
            (let [retry-backoff-ms (.getRetryBackoffMs conf)
                  exc-data (merge log
                                  {:retry-backoff-ms retry-backoff-ms
                                   :first-offset (some-> records first sinkRecord-original-offset)
                                   :record-count (count records)})]
              (.timeout context retry-backoff-ms)
              (throw (RetriableException. (str exc-data " " cause) cause))))

          (submit-unrolled! [cause]
            (if-let [reporter (.errantRecordReporter context)]
              (do
                (log/error cause "Submitting in batches failed. Trying to submit single records...")
                (doseq [record records]
                  (try
                    (submit-sink-records* context connectable conf [record])
                    (catch Exception e
                      ; Do not retry at this point, as some records may have moved to the dlq already.
                      (cond
                        (= ::record-data-error (-> e ex-data :type))
                        (.report reporter record (ex-cause e))

                        (instance? SQLException e)
                        (.report reporter record e)

                        :else
                        (throw e))))))
              (throw cause)))]

    (log/debug "processing records..." {:count (count records)})

    (let [dec-retries? (atom false)]
      (try
        (submit-sink-records* context connectable conf records)
        (catch Exception e
          (cond
            (= ::record-data-error (-> e ex-data :type))
            (submit-unrolled! (ex-cause e))

            (transient-connection-error? e)
            (throw-retry! e)

            (instance? SQLException e)
            (if (pos? @remaining-retries)
              (do
                (reset! dec-retries? true)
                (throw-retry! e {:log {:remaining-retries @remaining-retries}}))
              (submit-unrolled! e))

            :else
            (throw e)))

        (finally
          (if @dec-retries?
            (swap! remaining-retries dec)
            (reset! remaining-retries (.getMaxRetries conf))))))))
