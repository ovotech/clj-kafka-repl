(ns user
  (:require [clj-kafka-repl.channel :as ch]
            [clj-kafka-repl.core :refer [load-config with]]
            [clj-kafka-repl.deserialization :as dser]
            [clj-kafka-repl.kafka :as kafka]
            [clj-kafka-repl.serialization :as ser]
            [clojure.repl :refer [dir-fn]]
            [clojure.spec.test.alpha :as stest]
            [clojure.term.colors :refer [cyan green magenta yellow]]
            [kaocha.repl :as kaocha]))

(defn get-namespace-functions
  [ns]
  (->> (dir-fn ns)
       (map name)
       (sort)
       (map #(-> (symbol (name ns) %)
                 resolve
                 meta))))

(defn help
  ([]
   (->> ['kafka 'ch]
        (map help)
        vec)
   nil)
  ([ns]
   (println "----------------------")
   (println (magenta (name ns)))
   (println "----------------------")
   (println)

   (doseq [f (get-namespace-functions ns)]
     (when-not (:no-doc f)
       (doseq [arglist (:arglists f)]
         (println
           (str "(" (cyan (str (name ns) "/" (:name f))) " " (yellow arglist) ")")))
       (println (green (:doc f)))
       (println)))

   (println)))

(defn run-tests
  [& [ns]]
  (if ns
    (kaocha/run ns)
    (kaocha/run-all {:reporter [kaocha.report/dots]})))

(stest/instrument)
(load-config)

