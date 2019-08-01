(defproject clj-kafka-repl "0.1.0"
  :dependencies [[aero "1.1.3"]
                 [bigsy/clj-nippy-serde "0.1.0"]
                 [cheshire "5.8.1"]
                 [clojure.java-time "0.3.2"]
                 [clojure-term-colors "0.1.0"]
                 [org.apache.kafka/kafka-clients "2.3.0"]
                 [org.clojure/clojure "1.10.1"]
                 [org.clojure/core.async "0.4.500"]
                 [org.clojure/tools.logging "0.5.0"]
                 [org.slf4j/slf4j-api "1.7.26"]
                 [ovotech/kafka-avro-confluent "2.1.0-3"]]

  :plugins [[mvxcvi/whidbey "2.0.0"]
            [lein-codox "0.10.5"]]

  :middleware [whidbey.plugin/repl-pprint]

  :codox {:output-path "docs"
          :metadata    {:doc/format :markdown}
          :project     {:name "clj-kafka-repl", :version nil, :package nil}}

  :repl-options {:welcome (println
                            (str
                              (clojure.term.colors/yellow "Welcome to the Kafka Tooling REPL. Type ")
                              (clojure.term.colors/magenta "(help)")
                              (clojure.term.colors/yellow " or ")
                              (clojure.term.colors/magenta "(help ns)")
                              (clojure.term.colors/yellow " for more information.")))}

  :profiles {:dev  {:dependencies   [[ch.qos.logback/logback-classic "1.2.3"]
                                     [ch.qos.logback/logback-core "1.2.3"]]

                    :eftest         {:multithread? false}
                    :source-paths   ["dev/clj"]
                    :resource-paths ["dev/resources" "test/resources"]
                    :plugins        [[lein-eftest "0.4.2"]]

                    :repl-options   {:init-ns user}}
             :test {:dependencies [[vise890/zookareg "2.1.0-2"]]}})
