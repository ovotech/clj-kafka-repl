(ns clj-kafka-repl.serialization
  (:require [kafka-avro-confluent.serializers :refer [->avro-serializer]]
            [kafka-avro-confluent.schema-registry-client :refer [->schema-registry-client]]
            [clj-nippy-serde.serialization :refer [nippy-deserializer nippy-serializer]]
            [clojure.spec.alpha :as s]
            [clj-kafka-repl.core :refer [*config*]])
  (:import (org.apache.kafka.common.serialization Serializer StringSerializer)
           (java.nio.charset StandardCharsets)))

(s/def ::serializer #(instance? Serializer %))

(deftype EdnSerializer []
  Serializer
  (configure [_ _ _])
  (serialize [_ _ data]
    (when data
      (binding [*print-length* false
                *print-level*  false]
        (-> data prn-str (.getBytes StandardCharsets/UTF_8)))))
  (close [_]))

(deftype NoopDeserializer []
  Serializer
  (configure [_ _ _])
  (serialize [_ _ _] nil)
  (close [_]))

(defmulti new-serializer
          "Constructs a kafka serializer of the specified type."
          (fn [x & _] x))

(defmethod new-serializer :edn [_]
  (->EdnSerializer))

(defmethod new-serializer :string [_]
  (StringSerializer.))

(defmethod new-serializer :nippy [_]
  (nippy-serializer))

(defmethod new-serializer :avro [_ schema]
  (-> (:schema-registry-config *config*)
      ->schema-registry-client
      (->avro-serializer schema)))

(defmethod new-serializer :noop [_]
  (->NoopDeserializer))

