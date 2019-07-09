(ns clj-kafka-repl.admin
  (:require [clj-kafka-repl.config :refer [normalize-config]]
            [clj-kafka-repl.confirm :refer [with-confirmation]]
            [clj-kafka-repl.kafka :as kafka]
            [clojure.spec.alpha :as s])
  (:import (org.apache.kafka.clients.admin KafkaAdminClient)))

(defn delete-topics!
  "Delete the specified topics."
  [kafka-config topics]
  (let [admin       (-> (normalize-config kafka-config)
                        (KafkaAdminClient/create))]
    (with-confirmation
      (str "You are about to delete the following topics:\n\n" (clojure.string/join "\n" topics))
      (.deleteTopics admin (vec topics)))))

(s/fdef delete-topics!
        :args (s/cat :topics (s/coll-of ::kafka/topic)))
