(defproject number-stations "0.1.0-SNAPSHOT"
  :description "AK3W: Number Stations"

  :url "Decode the mysterious broadcast to unearth a hidden message"

  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/data.json "0.2.6"]
                 [clojure.java-time "0.3.2"]
                 [org.apache.kafka/kafka-streams-test-utils "2.2.0"]
                 [integrant "0.7.0"]
                 [http-kit "2.3.0"]
                 [metosin/reitit-core "0.3.1"]
                 [metosin/reitit-ring "0.3.1"]
                 [hiccup "1.0.5"]
                 [aero "1.1.3"]]

  :aot [number-stations.serdes
        number-stations.system]

  :main number-stations.system

  :repl-options {:init-ns number-stations.core})
