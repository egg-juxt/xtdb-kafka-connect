(ns xtdb.kafka.connect.util
  (:import (org.apache.kafka.connect.connector ConnectRecord)))

(defn clone-connect-record [^ConnectRecord record,
                            {:keys [topic kafka-partition
                                    key-schema key
                                    value-schema value
                                    timestamp headers]
                             :as changes}]
  (.newRecord record
              (if (contains? changes :topic) topic (.topic record))
              (if (contains? changes :kafka-partition) kafka-partition (.kafkaPartition record))
              (if (contains? changes :key-schema) key-schema (.keySchema record))
              (if (contains? changes :key) key (.key record))
              (if (contains? changes :value-schema) value-schema (.valueSchema record))
              (if (contains? changes :value) value (.value record))
              (if (contains? changes :timestamp) timestamp (.timestamp record))
              (if (contains? changes :headers) headers (.headers record))))
