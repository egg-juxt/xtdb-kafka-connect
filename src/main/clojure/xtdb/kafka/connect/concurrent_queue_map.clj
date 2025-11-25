(ns xtdb.kafka.connect.concurrent-queue-map
  (:import (clojure.lang PersistentQueue)))

(defn create []
  (atom {}))

(defn show [qm]
  (update-vals @qm seq))

(defn add!-was-absent? [qm k x]
  (let [[old] (swap-vals! qm update k (fnil conj PersistentQueue/EMPTY) x)]
    (nil? (get old k))))

(defn pop-or-dissoc! [qm k]
  (let [[old] (swap-vals! qm (fn [qm]
                               (if (empty? (qm k))
                                 (dissoc qm k)
                                 (update qm k pop))))]
    (peek (old k))))

(comment
  (def qm (create))
  (show qm)
  (add!-was-absent? qm 1 :a)
  (add!-was-absent? qm 1 :b)
  (pop-or-dissoc! qm 1)
  (seq (@qm 1)))
