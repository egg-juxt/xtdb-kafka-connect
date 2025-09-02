(ns xtdb.test.xtdb-fixture
  (:require [next.jdbc :as jdbc])
  (:import (org.testcontainers.containers GenericContainer)))

(def ^:dynamic *container*)

(defonce ^GenericContainer xtdb-container-conf
  (doto (GenericContainer. "ghcr.io/xtdb/xtdb:2.0.0")
    (.withEnv "XTDB_LOGGING_LEVEL" "debug")
    (.withCommand ^"[Ljava.lang.String;" (into-array ^String ["--playground"]))
    (.withExposedPorts (into-array [(int 5432)]))
    ;(.withStartupTimeout ...)
    (.withReuse true)))

(defn with-container [f]
  (binding [*container* (doto xtdb-container-conf
                          (.start))]
    (try
      (f)
      (finally
        (.close *container*)))))

(def ^:dynamic *db-name*)
(def ^:dynamic *conn*)

(defn with-conn [f]
  (binding [*db-name* (random-uuid)]
    (with-open [conn (jdbc/get-connection (str "jdbc:xtdb://localhost:"
                                               (.getMappedPort *container* 5432)
                                               "/" *db-name*))]
      (binding [*conn* conn]
        (f)))))
