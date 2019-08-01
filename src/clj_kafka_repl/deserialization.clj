(ns clj-kafka-repl.deserialization
  (:require [clojure.edn :as edn]
            [kafka-avro-confluent.deserializers :refer [->avro-deserializer]]
            [kafka-avro-confluent.schema-registry-client :refer [->schema-registry-client]]
            [clj-nippy-serde.serialization :refer [nippy-deserializer]]
            [clojure.spec.alpha :as s])
  (:import (org.apache.kafka.common.serialization Deserializer StringDeserializer)
           (java.nio.charset StandardCharsets)))

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
          (fn [x] x))

(defmethod new-deserializer :edn [_]
  (->EdnDeserializer))

(defmethod new-deserializer :string [_]
  (StringDeserializer.))

(defmethod new-deserializer :nippy [_]
  (nippy-deserializer))

(defmethod new-deserializer :avro [schema-registry-config]
  (-> schema-registry-config
      ->schema-registry-client
      ->avro-deserializer))

(defmethod new-deserializer :noop []
  (->NoopDeserializer))