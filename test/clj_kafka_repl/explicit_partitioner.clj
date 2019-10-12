(ns clj-kafka-repl.explicit-partitioner
  (:import (org.apache.kafka.clients.producer Partitioner)))

(gen-class
  :name energy_contracts_tools.test_utils.ExplicitPartitioner
  :implements [org.apache.kafka.clients.producer.Partitioner]
  :state state
  :init init
  :main false
  :methods [])

(defn -init [] [[] (atom nil)])
(defn -configure [_ _])
(defn -partition [_ _ _ _ {:keys [partition]} _ _] partition)
(defn -close [_])

(defn m->to-explicit-partitionable
  [m p]
  (assoc m :partition p))