(ns clj-kafka-repl.test-utils
  (:require [clojure.edn :as edn]
            [clj-kafka-repl.kafka-utils :refer [with-producer with-consumer]]
            [clj-kafka-repl.test-utils])
  (:import (java.util UUID)
           (org.apache.kafka.common.serialization Deserializer Serializer StringSerializer StringDeserializer)
           org.slf4j.bridge.SLF4JBridgeHandler))

(defn random-id
  []
  (str (UUID/randomUUID)))

(def edn-serializer
  (reify Serializer
    (configure [_ _ _])
    (serialize [_ _ data] (.getBytes (str data) "UTF-8"))
    (close [_])))

(def edn-deserializer
  (reify Deserializer
    (configure [_ _ _])
    (deserialize [_ _ data] (edn/read-string (String. data "UTF-8")))
    (close [_])))

(defn with-edn-producer
  [kafka-config f]
  (with-producer kafka-config edn-serializer f))
\
(defn with-edn-consumer
  [kafka-config topic group-id seek-to f]
  (with-consumer kafka-config edn-deserializer topic group-id nil f :seek-to seek-to))

(defn init-logging! []
  (SLF4JBridgeHandler/removeHandlersForRootLogger)
  (SLF4JBridgeHandler/install))