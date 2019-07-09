(ns clj-kafka-repl.core
  (:require [clojure.java.io :as io]
            [aero.core :as aero]))

(def ^:dynamic *profile* nil)

(defmacro with-profile
  "Commands enclosed will be executed in the context of the specified profile."
  [profile & body]
  `(binding [*profile* ~profile]
     (let [result# ~@body]
       (if (seq? result#)
         (vec result#)
         result#))))

(def get-config
  (memoize
    (fn [profile]
      (-> (System/getProperty "user.home")
          (str "/.clj-kafka-repl/config.edn")
          io/file
          (aero/read-config {:profile profile})))))

