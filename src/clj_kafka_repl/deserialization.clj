(ns clj-kafka-repl.deserialization
  (:require [clj-kafka-repl.core :refer [*config*]]
            [clj-nippy-serde.serialization :refer [nippy-deserializer]]
            [clojure.edn :as edn]
            [clojure.spec.alpha :as s]
            [kafka-avro-confluent.deserializers :refer [->avro-deserializer]]
            [kafka-avro-confluent.schema-registry-client :refer [->schema-registry-client]])
  (:import (java.nio.charset StandardCharsets)
           (org.apache.kafka.common.serialization Deserializer StringDeserializer)))

(s/def ::deserializer #(instance? Deserializer %))

(deftype EdnDeserializer []
  Deserializer
  (configure [_ _ _])
  (deserialize [_ _ data]
    (-> data (String. StandardCharsets/UTF_8) edn/read-string))
  (close [_]))

(deftype NoopDeserializer []
  Deserializer
  (configure [_ _ _])
  (deserialize [_ _ _] nil)
  (close [_]))

(defmulti new-deserializer
          "Constructs a kafka deserializer of the specified type."
          (fn [x & _] x))

(defmethod new-deserializer :edn [_]
  (->EdnDeserializer))

(defmethod new-deserializer :string [_]
  (StringDeserializer.))

(defmethod new-deserializer :nippy [_]
  (nippy-deserializer))

(defmethod new-deserializer :avro [_]
  (-> (:schema-registry-config *config*)
      ->schema-registry-client
      ->avro-deserializer))

(defmethod new-deserializer :noop [_]
  (->NoopDeserializer))