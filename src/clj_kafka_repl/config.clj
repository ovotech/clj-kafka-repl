(ns clj-kafka-repl.config)

(defn normalize-config
  "Normalise configuration map into a format required by kafka."
  [config]
  (->> config
       (clojure.walk/stringify-keys)
       (map (fn [[k v]] [k (if (number? v) (int v) v)]))
       (into {})))