(ns clj-kafka-repl.confirm)

(def ^:dynamic *no-confirm?* false)

(defn confirmed?
  [prompt]
  (if *no-confirm?*
    true
    (do
      (println prompt)
      (flush)
      (println "Continue? (y/N)")
      (flush)
      (Thread/sleep 500)
      (let [result (read-line)]
        (= "Y" (clojure.string/upper-case result))))))

(defmacro with-confirmation
  [prompt & body]
  `(if (confirmed? ~prompt)
     ~@body
     (throw (Exception. "Operation cancelled."))))
