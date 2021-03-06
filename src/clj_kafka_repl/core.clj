(ns clj-kafka-repl.core
  (:require [clojure.edn :as edn]))

(def ^:dynamic *config* nil)
(def ^:dynamic *options* nil)
(def ^:dynamic *profile* nil)

(def all-config (atom nil))

(defn load-config
  []
  (let [path (str (System/getProperty "user.home") "/.clj-kafka-repl/config.edn")]
    (reset! all-config (-> path slurp edn/read-string))))

(defmacro with
  [profile & body]
  `(binding [*options* (-> (deref all-config)
                           (dissoc :profiles))
             *config*  (get-in (deref all-config) [:profiles ~profile])
             *profile* ~profile]
     ~@body))




