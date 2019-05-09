(defproject number-stations "0.1.0-SNAPSHOT"
  :description "AK3W: Number Stations"

  :url "Decode the mysterious broadcast to unearth a hidden message"

  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/tools.logging "0.4.1"]
                 [ch.qos.logback/logback-classic "1.2.3"]
                 [org.clojure/data.json "0.2.6"]
                 [clojure.java-time "0.3.2"]
                 [org.apache.kafka/kafka-streams-test-utils "2.2.0"]
                 [http-kit "2.3.0"]
                 [metosin/reitit-core "0.3.1"]
                 [metosin/reitit-ring "0.3.1"]
                 [hiccup "1.0.5"]]

  :aot [numbers.serdes numbers.system]

  :main numbers.system)