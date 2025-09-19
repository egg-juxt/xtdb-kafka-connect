(ns xtdb.test.xtdb-fixture
  (:require [clojure.tools.logging :as log]
            [next.jdbc :as jdbc])
  (:import (org.testcontainers.containers GenericContainer)))

(def ^:dynamic *container*)

(def reuse true)

(defonce ^GenericContainer xtdb-container-conf
  (doto (GenericContainer. "ghcr.io/xtdb/xtdb:2.0.0")
    (.withEnv "XTDB_LOGGING_LEVEL_PGWIRE" "debug")
    (.withEnv "XTDB_LOGGING_LEVEL_SQL" "debug")
    (.withCommand ^"[Ljava.lang.String;" (into-array ^String ["--playground"]))
    (.withExposedPorts (into-array [(int 5432)]))
    ;(.withStartupTimeout ...)
    (.withReuse reuse)))

(defn with-container [f]
  (binding [*container* (doto xtdb-container-conf
                          (.start))]
    (try
      (f)
      (finally
        (when-not reuse
          (.close *container*))))))

(def ^:dynamic *db-name*)
(def ^:dynamic *jdbc-url*)
(def ^:dynamic *conn*)

(defn with-xtdb [f]
  (binding [*db-name* (random-uuid)]
    (binding [*jdbc-url* (str "jdbc:xtdb://localhost:"
                              (.getMappedPort *container* 5432)
                              "/" *db-name*)]
      (log/debug "JDBC URL:" *jdbc-url*)
      (f))))

(defn with-conn [f]
  (with-xtdb
    #(with-open [conn (jdbc/get-connection *jdbc-url*)]
       (binding [*conn* conn]
         (f)))))
