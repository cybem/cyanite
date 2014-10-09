(ns io.cyanite.stats
  "Collecting statistics"
  (:require [clojure.string     :as s :refer [join]]
            [clojure.core.async :as async :refer [<! >! >!! go chan timeout]]))

(def counters (atom {:index.create 0
                     :index.get_error 0
                     :metrics.recieved 0
                     :store.error 0
                     :store.success 0
                     :store.exception 0}))

(defn counter-get
  "Get counter"
  [key]
  (or (get @counters key) 0))

(defn counter-list
  "Get counter list"
  []
  @counters)

(defn counter-inc!
  "Increment counter"
  [key val]
  (swap! counters update-in [key] (fn [n] (if n (+ n val) val))))

(defn counters-reset!
  "Reset counters"
  []
  (reset! counters {:index.create 0
                    :index.get_error 0
                    :metrics.recieved 0
                    :store.error 0
                    :store.success 0
                    :store.exception 0}))

(defn put-counters
  "Put counters into channel"
  [chan hostname]
  (doseq [[k _] (counter-list)]
    (>! chan (join " " [(str hostname ".cyanite." (name k))
                        (counter-get k)
                        (quot (System/currentTimeMillis) 1000)])))
  (counters-reset!))

(defn stats
  "Collect statistics"
  [chan stats]
  (go
    (let [{:keys [hostname interval]} stats
          tinterval (* interval 1000)]
      (while true
        (<! (timeout tinterval))
        (put-counters chan hostname)))))
