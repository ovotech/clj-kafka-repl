(ns clj-kafka-repl.channel
  (:require [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as async-protocols]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [puget.printer :as puget]))

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

(defn to-file
  "Streams the contents of the specified channel to the given file path."
  [{:keys [channel]} f]
  (future
    (with-open [w (io/writer (io/file f))]
      (loop [next (async/<!! channel)]
        (when next
          (clojure.pprint/pprint next w)
          (recur (async/<!! channel)))))))

(s/fdef to-file
        :args (s/cat :tracked-channel ::tracked-channel
                     :f string?))

(defn to-stdout
  "Streams the contents of the specified to stdout."
  [{:keys [channel]} & {:keys [print]
                        :or   {print #(puget/pprint % {:print-color true})}}]
  (future
    (loop [next (async/<!! channel)]
      (when next
        (print next)
        (recur (async/<!! channel))))))

(s/fdef to-stdout
        :args (s/cat :tracked-channel ::tracked-channel
                     :options (s/* (s/alt :print (s/cat :opt #(= % :print)
                                                        :value fn?)))))

(defn dump!
  "Returns the entire contents of the channel. Will refuse if the channel is not closed."
  [{:keys [channel] :as tracked-channel}]
  (if (closed? tracked-channel)
    (->> channel
         (async/into [])
         (async/<!!))
    :channel-not-closed))

(s/fdef dump!
        :args (s/cat :tracked-channel ::tracked-channel)
        :ret (s/or :success (s/coll-of any?)
                   :failure keyword?))

(defn seek-to-end!
  "Polls channel until no more messages are found."
  [{:keys [channel]}]
  (while
    (deref
      (future (async/<!! channel))
      1000 nil))
  :done)

(s/fdef seek-to-end!
        :args (s/cat :tracked-channel ::tracked-channel)
        :ret keyword?)

(defn progress
  "Gives an indication of the progress of the given tracked channel on its partitions."
  [{:keys [progress-fn]}]
  (progress-fn))

(s/fdef progress
        :args (s/cat :tracked-channel ::tracked-channel)
        :ret map?)
