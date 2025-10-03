(ns xtdb.kafka.connect.encode
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [cognitect.transit :as transit]
            [xtdb.kafka.connect.util :refer [clone-connect-record]])
  (:import (com.cognitect.transit TaggedValue)
           (java.time ZoneId)
           (java.util List)
           (org.apache.kafka.connect.connector ConnectRecord)
           (org.apache.kafka.connect.data Field Schema Schema$Type Struct)))

(defn ?encode-by-xtdb-type [^Schema schema data]
  (assert (some? data))
  (when-some [xtdb-type (some-> schema .parameters (get "xtdb.type"))]
    (case xtdb-type
      "interval" (transit/tagged-value "xtdb/interval" data)
      "timestamptz" (transit/tagged-value "time/zoned-date-time" data)
      (transit/tagged-value xtdb-type data))))

(defn ?encode-by-simple-type [^Schema schema data]
  (assert (some? data))
  (cond
    (instance? Number data)
    (condp = (.type schema)
      Schema$Type/INT32 (transit/tagged-value "i32" data)
      Schema$Type/INT16 (transit/tagged-value "i16" data)
      Schema$Type/INT8 (transit/tagged-value "i8" data)

      Schema$Type/FLOAT64 (transit/tagged-value "f64" data)
      Schema$Type/FLOAT32 (transit/tagged-value "f32" data)

      nil)

    (and (-> schema .name (= org.apache.kafka.connect.data.Date/LOGICAL_NAME))
         (instance? java.util.Date data))
    (-> data .toInstant (.atZone (ZoneId/of "UTC")) .toLocalDate)))

(defn encode-by-schema* [^Schema schema, data, path]
  (try
    (cond
      (nil? schema)
      data

      (-> schema .type (= Schema$Type/STRUCT))
      (if-not (instance? Struct data)
        (throw (IllegalArgumentException. (str "expected Struct, received " (type data))))
        (reduce
          (fn [m ^Field field]
            (assoc m (.name field) (encode-by-schema* (.schema field)
                                                      (.get data field)
                                                      (conj path (.name field)))))
          {}
          (.fields schema)))

      (-> schema .type (= Schema$Type/MAP))
      (if-not (or (instance? java.util.Map data)
                  (map? data))
        (throw (IllegalArgumentException. (str "expected Map, received " (type data))))
        (->> data
             (map (fn [[k v]]
                    (let [subpath (conj path (name k))]
                      [(encode-by-schema* (.keySchema schema) k subpath)
                       (encode-by-schema* (.valueSchema schema) v subpath)])))
             (into {})))

      (-> schema .type (= Schema$Type/ARRAY))
      (if-not (or (sequential? data)
                  (instance? List data))
        (throw (IllegalArgumentException. "expected array"))
        (map-indexed
          (fn [i x]
            (encode-by-schema* (.valueSchema schema) x (conj path i)))
          data))

      (nil? data)
      nil

      :else
      (let [encoded (or (?encode-by-xtdb-type schema data)
                        (?encode-by-simple-type schema data)
                        data)]
        (log/trace "encoding simple data type" {:path path
                                                :value data
                                                :type (type data)
                                                :schema-type (-> schema .type .name)
                                                :schema-name (.name schema)
                                                :schema-parameters (.parameters schema)
                                                :encoded (if (instance? TaggedValue encoded)
                                                           {:tag (.getTag encoded)
                                                            :rep (.getRep encoded)
                                                            :type-rep (type (.getRep encoded))}
                                                           {:type (type encoded)
                                                            :value encoded})})
        encoded))

    (catch Exception e
      (if (-> e ex-data ::path)
        (throw e)
        (throw (ex-info (str "path [/" (str/join '/ path) "]: " (ex-message e))
                 {::path path}
                 e))))))

(defn encode-by-schema [^Schema schema, data]
  (log/trace "encoding data" {:type (type data), :schema schema, :data data})
  (let [result (encode-by-schema* schema data [])]
    (log/trace "encoded data" data)
    result))

(defn ^ConnectRecord encode-record-value-by-schema [^ConnectRecord record]
  (clone-connect-record record {:value-schema nil
                                :value (encode-by-schema (.valueSchema record) (.value record))}))
