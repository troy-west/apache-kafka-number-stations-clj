(ns number-stations.generator-test
  (:require [clojure.test :refer [deftest is]]
            [number-stations.generator
             :as
             generator
             :refer
             [->JsonDeserializer ->JsonSerializer]])
  (:import [java.util Date Properties]
           org.apache.kafka.clients.producer.ProducerRecord
           [org.apache.kafka.common.serialization StringDeserializer StringSerializer]
           [org.apache.kafka.streams StreamsBuilder TopologyTestDriver]
           [org.apache.kafka.streams.kstream Predicate ValueMapper]
           org.apache.kafka.streams.test.ConsumerRecordFactory))

(def config (let [props (Properties.)]
              (.putAll props {"application.id"      "adsasd"
                              "bootstrap.servers"   "dummy:1234"
                              "default.key.serde"   "org.apache.kafka.common.serialization.Serdes$StringSerde"
                              "default.value.serde" "number_stations.generator.JsonSerde"})
              props))

(defn read-output
  [driver topic]
  (when-let [record (.readOutput ^TopologyTestDriver
                                 driver
                                 topic
                                 (StringDeserializer.)
                                 (->JsonDeserializer))]
    (.value ^ProducerRecord record)))

(defn write-inputs
  [driver factory topic messages]
  (doseq [message messages]
    (.pipeInput driver (.create factory topic (:name message) message))))

(deftest translate-numbers-test

  (let [input-topic    "numbers"
        output-topic   "translate-numbers"

        start-time     (.getTime (Date.))
        factory        (ConsumerRecordFactory. input-topic
                                               (StringSerializer.)
                                               (->JsonSerializer))
        builder        (StreamsBuilder.)
        input-messages [{:name "E-test-english" :numbers ["three" "two" "one"]}
                        {:name "G-test-german" :numbers ["eins" "null" "null"]}
                        {:name "X-test-other" :numbers [1 2 3]}]]

    (with-open [driver (TopologyTestDriver. (generator/translate-numbers-topology input-topic output-topic) config)]
      (write-inputs driver factory input-topic input-messages)

      (is (= 321 (:colour-component (read-output ^TopologyTestDriver driver output-topic))))
      (is (= 100 (:colour-component (read-output ^TopologyTestDriver driver output-topic))))
      (is (= nil (:colour-component (read-output ^TopologyTestDriver driver output-topic)))))))

(deftest correlate-rgb-test

  (let [input-topic    "translated-numbers"
        output-topic   "rgb-stream"
        factory        (ConsumerRecordFactory. input-topic
                                               (StringSerializer.)
                                               (->JsonSerializer))
        input-messages [{:time 0 :name "name"}
                        {:time 1000 :name "name"}
                        {:time 2000 :name "name"}
                        {:time 12000 :name "name"}
                        {:time 11000 :name "name"}
                        {:time 13000 :name "name"}]]

    (with-open [driver (TopologyTestDriver. (generator/correlate-rgb-topology input-topic output-topic) config)]
      (write-inputs driver factory input-topic input-messages)

      (is (= [{:time 0, :name "name"}
              {:time 1000, :name "name"}
              {:time 2000, :name "name"}]
             (read-output ^TopologyTestDriver driver output-topic)))
      (is (= [{:time 11000, :name "name"}
              {:time 12000, :name "name"}
              {:time 13000, :name "name"}]
             (read-output ^TopologyTestDriver driver output-topic))))))
