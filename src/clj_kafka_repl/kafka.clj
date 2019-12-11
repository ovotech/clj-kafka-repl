(ns clj-kafka-repl.kafka
  (:require [clojure.spec.alpha :as s]
            [clj-kafka-repl.confirm :refer [with-confirmation]]
            [clj-kafka-repl.channel :as ch]
            [clj-kafka-repl.confirm :refer [with-confirmation] :as confirm]
            [clj-kafka-repl.deserialization :as dser :refer [new-deserializer]]
            [clj-kafka-repl.serialization :as ser :refer [new-serializer]]
            [clj-kafka-repl.core :refer [*config*]]
            [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as async-protocols]
            [clojure.tools.logging :as log]
            [java-time :as jt])
  (:import (org.apache.kafka.clients.consumer KafkaConsumer)
           (org.apache.kafka.common TopicPartition)
           (java.util.concurrent TimeUnit)
           (java.util UUID)
           (org.apache.kafka.clients.producer ProducerRecord KafkaProducer)))

(def max-poll-records 500)

(defn zoned-date-time-string?
  [s]
  (-> (and
        (string? s)
        (= 20 (count s))
        (try
          (jt/zoned-date-time s)
          (catch Exception _ nil)))
      boolean))

(defn ^:no-doc normalize-config
  [config]
  (->> config
       (clojure.walk/stringify-keys)
       (map (fn [[k v]] [k (if (number? v) (int v) v)]))
       (into {})))

(s/def ::zoned-date-time-string zoned-date-time-string?)
(s/def ::non-blank-string (s/and string? (complement clojure.string/blank?)))
(s/def ::topic ::non-blank-string)
(s/def ::partition nat-int?)
(s/def ::offset nat-int?)
(s/def ::offset-specification (s/or :absolute ::offset
                                    :relative neg-int?
                                    :keyword #{:start :end}
                                    :timestamp ::zoned-date-time-string))
(s/def ::partition-offset-specification
  (s/cat :partition nat-int?
         :offset ::offset-specification))

(s/def ::key any?)
(s/def ::value any?)
(s/def ::timestamp ::zoned-date-time-string)
(s/def ::timestamp-type ::non-blank-string)
(s/def ::kafka-message (s/keys :req-un [::key ::partition ::offset ::value ::timestamp ::timestamp-type]))

(s/def ::bootstrap.servers ::non-blank-string)
(s/def ::kafka-config (s/keys :req-un [::bootstrap.servers]))

(def default-key-deserializer (new-deserializer :string))
(def default-value-deserializer (new-deserializer :edn))

(def default-key-serializer (new-serializer :string))
(def default-value-serializer (new-serializer :edn))

(defn get-group-offset
  "Gets the offset of the given consumer group on the given topic/partition."
  [topic group-id partition]
  (let [kafka-config (:kafka-config *config*)
        cc           (-> kafka-config
                         (merge {:group.id group-id})
                         normalize-config)
        new-consumer (KafkaConsumer. cc default-key-deserializer default-value-deserializer)
        tp           (TopicPartition. topic partition)]
    (try
      (some-> (.committed new-consumer tp)
              (.offset))
      (finally
        (.close new-consumer 0 TimeUnit/SECONDS)))))

(s/fdef get-group-offset
        :args (s/cat :topic ::topic
                     :group-id ::non-blank-string
                     :partition nat-int?)
        :ret nat-int?)

(defn get-topic-partitions
  "Gets the vector of partitions available for the given topic."
  [topic]
  (let [kafka-config (:kafka-config *config*)
        cc           (normalize-config kafka-config)
        new-consumer (KafkaConsumer. cc default-key-deserializer default-value-deserializer)]
    (try
      (->> (.partitionsFor new-consumer topic)
           (map #(.partition %))
           sort
           vec)

      (finally
        (.close new-consumer 0 TimeUnit/SECONDS)))))

(s/fdef get-topic-partitions
        :args (s/cat :topic ::topic)
        :ret (s/coll-of nat-int?))

(defn get-latest-offsets
  "Gets a vector of vectors representing the mapping of partition->latest-offset for
  the partitions of the given topic.

  | key                | default | description |
  |:-------------------|:--------|:------------|
  | `:partitions`      | `nil`   | Limit the results to the specified collection of partitions. |"
  [topic & {:keys [partitions]
            :or   {partitions nil}}]
  (let [kafka-config     (:kafka-config *config*)
        cc               (normalize-config kafka-config)
        new-consumer     (KafkaConsumer. cc default-key-deserializer default-value-deserializer)
        topic-partitions (map #(TopicPartition. topic %)
                              (or partitions (get-topic-partitions topic)))]
    (try
      (->> (.endOffsets new-consumer topic-partitions)
           (map (fn [[tp o]]
                  [(.partition tp) o]))
           (sort-by first)
           vec)

      (finally
        (.close new-consumer 0 TimeUnit/SECONDS)))))

(s/fdef get-latest-offsets
        :args (s/cat :topic ::topic
                     :overrides (s/* (s/alt :partitions (s/cat :opt #(= % :partitions)
                                                               :value (s/coll-of nat-int?)))))
        :ret (s/coll-of ::partition-offset))

(defn get-earliest-offsets
  "Gets a vector of vectors representing the mapping of partition->earliest-offset for
  the partitions of the given topic.

  | key                | default | description |
  |:-------------------|:--------|:------------|
  | `:partitions`      | `nil`   | Limit the results to the specified collection of partitions. |"
  [topic & {:keys [partitions]
            :or   {partitions nil}}]
  (let [kafka-config     (:kafka-config *config*)
        cc               (normalize-config kafka-config)
        new-consumer     (KafkaConsumer. cc default-key-deserializer default-value-deserializer)
        topic-partitions (map #(TopicPartition. topic %)
                              (or partitions (get-topic-partitions topic)))]
    (try
      (->> (.beginningOffsets new-consumer topic-partitions)
           (map (fn [[tp o]]
                  [(.partition tp) o]))
           (sort-by first))

      (finally
        (.close new-consumer 0 TimeUnit/SECONDS)))))

(s/fdef get-earliest-offsets
        :args (s/cat :topic ::topic
                     :overrides (s/* (s/alt :partitions (s/cat :opt #(= % :partitions)
                                                               :value (s/coll-of nat-int?)))))
        :ret (s/coll-of ::partition-offset))

(defn get-group-offsets
  "Gets the offsets on all partitions of the given topic for the specified consumer group."
  [topic group-id]
  (->> (get-topic-partitions topic)
       (map (fn [p]
              [p (get-group-offset topic group-id p)]))
       (remove (fn [[_ o]] (nil? o)))))

(s/fdef get-group-offsets
        :args (s/cat :topic ::topic
                     :group-id ::non-blank-string)
        :ret (s/coll-of ::partition-offset))

(defn- set-group-offset!
  "Set the offset for the specified consumer group on the specified partition."
  [topic consumer group-id partition new-offset]
  ; IMPORTANT: For this to work you need make sure that no consumers in the same group are
  ; already running against this partition.
  (let [tp              (TopicPartition. topic partition)
        adjusted-offset (if (neg-int? new-offset)
                          (->> (.endOffsets consumer [tp])
                               (map (fn [[_ o]] (+ o new-offset)))
                               first)
                          new-offset)]
    (prn (format "Setting offset on topic %s:%s and group %s to %s.\n\nIMPORTANT: Have you made sure that all consumers on the group have stopped?"
                 topic partition group-id adjusted-offset))

    (cond
      (= :end adjusted-offset)
      (.seekToEnd consumer [tp])

      (= :start adjusted-offset)
      (.seekToBeginning consumer [tp])

      :else (.seek consumer tp adjusted-offset))

    (.commitSync consumer)))

(defn set-group-offsets!
  "Sets the offsets for the specified group on the specified topic to the offsets given in the passed
  sequence of partition->offset pairs. The offset in each pair can be one of several types:

  * A natural integer - an absolute offset.
  * A negative integer - an offset relative to the current offset (i.e. deduct from the current offset)
  * :start - seek to start.
  * :end - seek to end.
  * date-time string - set offset to that which was current at the given time."
  [topic group-id partition-offsets & {:keys [consumer] :or {consumer nil}}]
  (let [kafka-config     (:kafka-config *config*)
        cc               (-> kafka-config
                             (merge {:group.id group-id})
                             normalize-config)
        create-consumer? (nil? consumer)
        new-consumer     (or consumer (KafkaConsumer. cc default-key-deserializer default-value-deserializer))
        topic-partitions (->> partition-offsets
                              (map first)
                              (map #(TopicPartition. topic %)))]
    (when create-consumer?
      (.assign new-consumer topic-partitions))

    (with-confirmation
      (format "You are about to set the group offsets for group %s on topic %s for %s partitions.
    Make sure that no other consumers on the same group are running before continuing." group-id topic (count partition-offsets))
      (try
        (doseq [[p o] partition-offsets]
          (set-group-offset! topic new-consumer group-id p o))
        (.poll new-consumer 1000)
        (.commitSync new-consumer)
        (finally
          (when create-consumer?
            (.close new-consumer 0 TimeUnit/SECONDS)))))))

(s/fdef set-group-offsets!
        :args (s/cat :topic ::topic
                     :group-id ::non-blank-string
                     :partition-offsets (s/coll-of ::partition-offset-specification)
                     :overrides (s/* (s/alt :consumer (s/cat :opt #(= % :consumer)
                                                             :value #(instance? KafkaConsumer %))))))

(defn- offsets-diff
  [current-offsets latest-offsets]
  (-> (map (fn [[p current-offset] [_ latest-offset]]
             [p (-> (or current-offset 0) (- latest-offset))])
           current-offsets
           latest-offsets)
      doall vec))

(defn- lag-sum
  [by-partition-lags]
  (- (reduce (fn [acc [_ x]] (+ acc x)) 0 by-partition-lags)))

(defn- to-lag-map
  [current-offsets latest-offsets]
  (let [consumer-group-partitions (set (map first current-offsets))
        refined-latest-offsets    (remove (fn [[p _]]
                                            (not (consumer-group-partitions p)))
                                          latest-offsets)
        lags                      (offsets-diff current-offsets refined-latest-offsets)]
    {:total-lag    (lag-sum lags)
     :by-partition lags
     :offsets      {:current current-offsets
                    :latest  refined-latest-offsets}}))

(defn get-lag
  "Gets the topic lag for the given consumer group.

  | key                | default | description |
  |:-------------------|:--------|:------------|
  | `:verbose?`        | `false` | If `true`, will include by-partition breakdown. |"
  [topic group-id & {:keys [verbose?] :or {verbose? true}}]
  (let [current-offsets (get-group-offsets topic group-id)
        latest-offsets  (get-latest-offsets topic)]
    (if verbose?
      (-> (to-lag-map current-offsets latest-offsets)
          (assoc :topic topic))
      (-> (offsets-diff current-offsets latest-offsets)
          (lag-sum)))))

(s/fdef get-lag
        :args (s/cat :topic ::topic
                     :group-id ::non-blank-string
                     :overrides (s/* (s/alt :verbose? (s/cat :opt #(= % :verbose?) :value boolean?))))
        :ret (s/or :map map? :lag int?))

(defn- cr->kafka-message
  [cr]
  {:key            (.key cr)
   :partition      (.partition cr)
   :offset         (.offset cr)
   :timestamp      (-> (.timestamp cr) jt/instant str)
   :timestamp-type (str (.timestampType cr))
   :value          (.value cr)
   :type           (type (.value cr))})

(defn consume
  "Opens a consumer over the specified topic and returns a ::ch/tracked-channel which is a wrapper over a core.async
  channel.

  The channel will stay open indefinitely unless either: a) the channel is explicitly closed using ch/close! or b)
  the specified message limit is reached.

  Examples of pulling data from channels:

  - Pop the next message (if any) from the channel:
    (ch/poll! tc)

  - Stream channel to file:
    (ch/to-file tc \"/workspace/temp/your-file\")

  - Stream channel to stdout:
    (ch/to-stdout tc)

  - Close and dump current contents of channel to stdout:
    (ch/dump! tc)

  And then close the channel with:
  (ch/close! tc)

  | key                  | default | description |
  |:---------------------|:--------|:------------|
  | `:partition`         | `nil`   | Limit consumption to a specific partition. |
  | `:offset`            | `:end`  | Start consuming from the specified offset. Valid values: `:start`, `:end`, numeric offset, timestamp (as date/time string) |
  | `:partition-offsets` | `nil`   | Vector of partition+offset vector pairs that represent a by-partition representation of offsets to start consuming from. |
  | `:key-deserializer`  | `nil`   | Deserializer to use to deserialize the message key. Will use a string deserializer if not specified. |
  | `:value-deserializer`| `nil`   | Deserializer to use to deserialize the message value. Will use an edn deserializer if not specified. |
  | `:limit`             | `nil`   | The maximum number of messages to pull back either into the stream or the results vector (depending on stream mode). |
  | `:filter-fn`         | `nil`   | `filter` function to apply to the incoming :kafka-message(s). Can be a string, in which case a filter on the message value containing that string is implied. |"
  [topic & {:keys [partition offset partition-offsets key-deserializer value-deserializer limit filter-fn]
            :or   {partition          nil
                   offset             :end
                   partition-offsets  nil
                   key-deserializer   default-key-deserializer
                   value-deserializer default-value-deserializer
                   limit              nil
                   filter-fn          (constantly true)}}]
  (let [kafka-config     (:kafka-config *config*)
        group-id         (str "clj-kafka-repl-" (UUID/randomUUID))
        cc               (-> kafka-config
                             (assoc :group.id group-id
                                    :max.poll.records (cond
                                                        (nil? limit) max-poll-records
                                                        (> limit max-poll-records) max-poll-records
                                                        :else limit))
                             normalize-config)
        consumer         (KafkaConsumer. cc key-deserializer value-deserializer)
        partitions       (cond
                           (some? partition-offsets) (map first partition-offsets)
                           (some? partition) [partition]
                           :else (map #(.partition %)
                                      (.partitionsFor consumer topic)))
        topic-partitions (map #(TopicPartition. topic %) partitions)
        final-filter-fn  (if (string? filter-fn)
                           #(clojure.string/includes? % filter-fn)
                           filter-fn)]

    (.assign consumer topic-partitions)

    ;============================================
    ; Set the offsets for each partition
    ;============================================

    (binding [confirm/*no-confirm?* true]
      (cond
        (some? partition-offsets)
        (set-group-offsets! topic group-id partition-offsets :consumer consumer)

        (= :end offset)
        (.seekToEnd consumer topic-partitions)

        (= :start offset)
        (.seekToBeginning consumer topic-partitions)

        (neg-int? offset)
        (let [latest            (into {} (get-latest-offsets topic :partitions partitions))
              partition-offsets (->> partitions
                                     (map #(vector % (+ (get latest %) offset)))
                                     vec)]
          (set-group-offsets! topic group-id partition-offsets :consumer consumer))

        :else
        (let [earliest-offset   (apply min (map second (get-earliest-offsets kafka-config topic :partitions partitions)))
              partition-offsets (vec (map #(vector % offset) partitions))]
          (if (< earliest-offset offset)
            (set-group-offsets! topic group-id partition-offsets :consumer consumer)
            (do
              (log/info "Specified offset is before the earliest offset. Therefore, will seek from beginning.")
              (.seekToBeginning consumer topic-partitions))))))

    ;============================================
    ; Start consuming
    ;============================================

    (let [count-atom
          (atom 0)

          progress
          (atom {:total-received  0
                 :total-remaining nil
                 :offsets         nil})

          ch
          (async/chan limit)

          tracked-channel
          {:channel     ch
           :progress-fn #(let [{:keys [by-partition total-received]}
                               @progress

                               current-offsets
                               (into [] by-partition)

                               latest-offsets
                               (get-latest-offsets topic)

                               {:keys [total-lag offsets]}
                               (to-lag-map current-offsets latest-offsets)]
                           {:total-received  total-received
                            :total-remaining total-lag
                            :offsets         offsets})}]
      (future
        (try
          (loop []
            (let [messages (->> (.poll consumer 2000)
                                (map cr->kafka-message))
                  filtered (->> messages
                                (filter final-filter-fn)
                                (take (- (or limit Long/MAX_VALUE)
                                         @count-atom)))]

              (doseq [{:keys [partition offset]} messages]
                (swap! progress #(-> %
                                     (assoc-in [:by-partition partition] (inc offset))
                                     (update :total-received inc))))

              (doseq [msg filtered]
                (when (not (async-protocols/closed? ch))
                  (swap! count-atom inc)
                  (async/>!! ch msg)))

              (when (and (not (async-protocols/closed? ch))
                         (or (nil? limit)
                             (< @count-atom limit)))
                (recur))))

          (catch Exception e
            (println e)
            (log/error e))

          (finally
            (.close consumer 0 TimeUnit/SECONDS)
            (async/close! ch)
            (println "Consumer closed."))))

      tracked-channel)))

(s/fdef consume
        :args (s/cat :topic ::topic
                     :args (s/* (s/alt :limit (s/cat :opt #(= % :limit) :value pos-int?)
                                       :partition (s/cat :opt #(= % :partition) :value nat-int?)
                                       :partition-offsets (s/coll-of ::partition-offset-specification)
                                       :offset (s/cat :opt #(= % :offset) :value ::offset-specification)
                                       :key-deserializer (s/cat :opt #(= % :key-deserializer) :value ::dser/deserializer)
                                       :value-deserializer (s/cat :opt #(= % :value-deserializer) :value ::dser/deserializer)
                                       :filter-fn (s/cat :opt #(= % :filter-fn) :value (s/or :string string? :fn fn?)))))
        :ret ::ch/tracked-channel)

(defn sample
  "Convenience function around kafka/consume to just sample a message from the topic."
  [kafka-config topic & opts]
  (let [c (apply consume (concat [kafka-config topic :limit 1 :offset :start] opts))]
    (try
      (deref
        (future
          (loop [m (ch/poll! c)]
            (if m
              m
              (do (Thread/sleep 100)
                  (recur (ch/poll! c))))))
        10000 nil)
      (finally
        (ch/close! c)))))

(s/fdef sample
        :args (s/cat :topic ::topic
                     :args (s/* (s/alt :deserializer (s/cat :opt #(= % :deserializer) :value ::dser/deserializer))))
        :ret map?)

(defn get-message
  "Gets the message at the specified offset on the given topic (if any).

  | key                  | default | description |
  |:---------------------|:--------|:------------|
  | `:partition`         | `nil`   | Limit consumption to a specific partition. |
  | `:deserializer`      | `nil`   | Deserializer to use to deserialize the message value. Will create an avro-deserializer if not specified (or nippy-deserializer if topic name contains the word 'internal'). |"
  [topic offset & {:keys [deserializer partition]
                   :or   {deserializer nil
                          partition    nil}}]
  (let [kafka-config (:kafka-config *config*)
        args         (concat [kafka-config topic
                              :offset (dec offset)
                              :limit 1
                              :filter-fn #(= offset (:offset (meta %)))]
                             (when (some? partition) [:partition partition])
                             (when (some? deserializer) [:deserializer deserializer]))
        ch           (apply consume args)
        f            (future
                       (loop [m (ch/poll! ch)]
                         (if m
                           m
                           (recur (ch/poll! ch)))))]
    (try
      (deref f 5000 nil)
      (finally
        (future-cancel f)
        (ch/close! ch)))))

(s/fdef get-message
        :args (s/cat :topic ::topic
                     :offset ::offset-specification
                     :args (s/* (s/alt :deserializer (s/cat :opt #(= % :deserializer) :value ::dser/deserializer)
                                       :partition (s/cat :opt #(= % :partition) :value ::partition))))
        :ret map?)

(defn produce
  "Produce messages to the specified topic.

  | key                | default | description |
  |:-------------------|:--------|:------------|
  | `:key-serializer`  | `nil`   | Serializer to use to serialize the message key. Will use a string deserializer if not specified. |
  | `:value-serializer`| `nil`   | Serializer to use to serialize the message value. Will use an edn serializer if not specified. |"
  ([topic & {:keys [key-serializer value-serializer]
             :or   {key-serializer   default-key-serializer
                    value-serializer default-value-serializer}}]
   (let [kafka-config    (:kafka-config *config*)
         producer-config (normalize-config kafka-config)
         producer        (KafkaProducer. producer-config key-serializer value-serializer)
         ch              (async/chan)]

     (future
       (try
         (loop [next (async/<!! ch)]
           (when next
             (case next
               :flush (.flush producer)

               (let [[k v] next
                     record (ProducerRecord. topic k v)]
                 (.send producer record)))
             (recur (async/<!! ch))))
         (finally
           (.close producer)
           (println "Producer closed."))))
     ch)))

(s/fdef produce
        :args (s/cat :topic ::topic
                     :args (s/* (s/alt :key-serializer (s/cat :opt #(= % :key-serializer) :value ::ser/serializer)
                                       :value-serializer (s/cat :opt #(= % :value-serializer) :value ::ser/serializer))))
        :ret ::ch/channel)