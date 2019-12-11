(ns clj-kafka-repl.channel
  (:require [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as async-protocols]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [puget.printer :as puget])
  (:import (java.io PrintWriter)
           (clojure.lang Atom)))

(s/def ::progress-fn fn?)
(s/def ::channel #(satisfies? async-protocols/Channel %))
(s/def ::tracked-channel (s/keys :req-un [::channel ::progress-fn]))

(defn close!
  "Closes the specified core.async channel."
  [{:keys [channel]}]
  (async/poll! channel)
  (async/close! channel))

(s/fdef close!
        :args (s/cat :tracked-channel ::tracked-channel))

(defn poll!
  "Reads off the next value (if any) from the specified channel."
  [{:keys [channel]}]
  (async/poll! channel))

(s/fdef poll!
        :args (s/cat :tracked-channel ::tracked-channel)
        :ret any?)

(defn closed?
  "Returns flag indicating whether the specified channel is closed."
  [{:keys [channel]}]
  (async-protocols/closed? channel))

(s/fdef closed?
        :args (s/cat :tracked-channel ::tracked-channel))

(defn- loop-channel
  [channel fn]
  (loop [next (async/<!! channel)]
    (when next
      (fn next)
      (recur (async/<!! channel)))))

(defmulti to (fn [_ target] (type target)))

(defmethod to PrintWriter [{:keys [channel]} writer]
  (future
    (binding [*out* writer]
      (loop-channel channel #(puget/pprint % {:print-color true})))))

(defmethod to String [{:keys [channel]} file-path]
  (future
    (with-open [w (io/writer (io/file file-path))]
      (loop-channel channel #(clojure.pprint/pprint % w)))))

(defmethod to Atom [{:keys [channel]} a]
  (future
    (loop-channel channel #(swap! a conj %))))

(defn to-stdout
  "Streams the contents of the specified to stdout."
  [{:keys [channel]}]
  (to channel *out*))

(s/fdef to-stdout
        :args (s/cat :tracked-channel ::tracked-channel
                     :options (s/* (s/alt :print (s/cat :opt #(= % :print)
                                                        :value fn?)))))

(defn to-file
  "Streams the contents of the specified channel to the given file path."
  [{:keys [channel]} f]
  (to channel f))

(s/fdef to-file
        :args (s/cat :tracked-channel ::tracked-channel
                     :f string?))

(defn progress
  "Gives an indication of the progress of the given tracked channel on its partitions."
  [{:keys [progress-fn]}]
  (progress-fn))

(s/fdef progress
        :args (s/cat :tracked-channel ::tracked-channel)
        :ret map?)
