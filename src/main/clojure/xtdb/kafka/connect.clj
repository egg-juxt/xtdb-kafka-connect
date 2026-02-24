(ns xtdb.kafka.connect
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [xtdb.kafka.connect.util :refer [sinkRecord-original-offset]])
  (:import (clojure.lang Atom ExceptionInfo)
           (java.sql SQLException SQLTransientConnectionException)
           [java.util Collection List Map]
           [org.apache.kafka.connect.data Field Struct]
           org.apache.kafka.connect.sink.SinkRecord
           (org.apache.kafka.connect.errors RetriableException)
           (org.apache.kafka.connect.sink SinkTaskContext)
           (xtdb.kafka.connect XtdbSinkConfig)))

(defn- data->clj [data]
  (cond
    (instance? Struct data)
    (into {}
      (for [^Field field (-> ^Struct data .schema .fields)]
        [(.name field) (data->clj (.get ^Struct data field))]))

    (instance? Map data)
    (into {}
      (for [[k v] data]
        [(data->clj k) (data->clj v)]))

    (or (sequential? data)
        (instance? List data))
    (mapv data->clj data)

    :else data))

(defn- record->clj [^SinkRecord record]
  (let [data (.value record)]
    (when-not (or (instance? Struct data)
                  (instance? Map data))
      (throw (IllegalArgumentException. (str "Unacceptable record value type: " (type record)))))

    (data->clj data)))

(defn get-xt-id [m]
  (or (get m :xt/id)
      (get m :_id)
      (get m "_id")))

(defn- find-record-key-eid [^XtdbSinkConfig _conf, ^SinkRecord record]
  (let [r-key (.key record)]
    (cond
      (or (instance? Struct r-key)
          (instance? Map r-key))
      (if-some [id (-> r-key data->clj get-xt-id)]
        id
        (throw (IllegalArgumentException. "no `_id` field in record key")))

      :else
      r-key)))

(defn- find-record-value-eid [^XtdbSinkConfig _conf, ^SinkRecord _record, doc]
  (if-some [id (get-xt-id doc)]
    id
    (throw (IllegalArgumentException. "no `_id` field in record value"))))

(defn- assoc-xt-id [doc id]
  (cond
    (contains? doc :xt/id) (assoc doc :xt/id id)
    (contains? doc :_id) (assoc doc :_id id)
    :else (assoc doc "_id" id)))

(defn- tombstone? [^SinkRecord record]
  (and (nil? (.value record)) (.key record)))

(defn table-name [^XtdbSinkConfig conf, ^SinkRecord record]
  (let [topic (.topic record)]
    (or (get (.getTableNameMap conf) topic)
        (str/replace (.getTableNameFormat conf) "${topic}" topic))))

(defn record->op [^XtdbSinkConfig conf, ^SinkRecord record]
  (log/trace "sink record:" record)
  (let [table (table-name conf record)
        id-mode (.getIdMode conf)]
    (doto (cond
            (not (tombstone? record))
            (let [doc (record->clj record)
                  id (case id-mode
                       "record_key" (find-record-key-eid conf record)
                       "record_value" (find-record-value-eid conf record doc))]
              {:op (case (.getInsertMode conf)
                     "insert" :insert
                     "patch" :patch)
               :table table
               :params [(case id-mode
                          "record_key" (assoc-xt-id doc id)
                          "record_value" doc)]})

            (= "record_key" (.getIdMode conf))
            (let [id (find-record-key-eid conf record)]
              {:op :delete
               :table table
               :params [id]})

            :else (throw (IllegalArgumentException. (str "Unsupported tombstone mode: " record))))

      (->> (log/trace "tx op:")))))

(defn submit-sink-records* [connectable
                            ^XtdbSinkConfig conf
                            records]
  (when (seq records)
    (let [record-ops (doall ; Find any record data issues now
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
                    (submit-sink-records* connectable conf [record])
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
        (submit-sink-records* connectable conf records)
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
