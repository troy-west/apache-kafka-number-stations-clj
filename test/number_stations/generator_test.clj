(ns number-stations.generator-test
  (:require [clojure.test :refer [deftest is]]
            [number-stations.generator
             :as
             generator
             :refer
             [->JsonDeserializer ->JsonSerializer]])
  (:import java.util.concurrent.TimeUnit
           java.util.Properties
           org.apache.kafka.clients.producer.ProducerRecord
           [org.apache.kafka.common.serialization StringDeserializer StringSerializer]
           [org.apache.kafka.streams StreamsBuilder TopologyTestDriver]
           org.apache.kafka.streams.kstream.SessionWindows
           org.apache.kafka.streams.test.ConsumerRecordFactory))

(def config (let [props (Properties.)]
              (.putAll props {"application.id"      "adsasd1234"
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

(defn read-key-value
  [driver topic]
  (when-let [record (.readOutput ^TopologyTestDriver
                                 driver
                                 topic
                                 (StringDeserializer.)
                                 (->JsonDeserializer))]
    [(.key record)
     (.value ^ProducerRecord record)]))

(defn write-inputs
  [driver factory topic messages]
  (doseq [message messages]
    (.pipeInput driver (.create factory topic (:name message) message))))

(deftest translate-numbers-test

  (let [input-topic    "numbers"
        output-topic   "translate-numbers"

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

(deftest group-by-row-test

  (let [input-topic    "rgb-stream"
        output-topic   "rgb-row-stream"
        factory        (ConsumerRecordFactory. input-topic
                                               (StringSerializer.)
                                               (->JsonSerializer))
        input-messages [{:time 0 :name "name" :number-of-messages 3 :latitude 37 :longitude 144}
                        {:time 1 :name "name" :rgb [1 2 3] :latitude 37 :longitude 144}
                        {:time 2 :name "name" :rgb [4 5 6] :latitude 37 :longitude 144}
                        {:time 3 :name "name" :rgb [7 8 9] :latitude 37 :longitude 144}
                        {:time 60010 :name "name" :number-of-messages 2 :latitude 37 :longitude 144}
                        {:time 60020 :name "name" :rgb [10 11 12] :latitude 37 :longitude 144}
                        {:time 60030 :name "name" :rgb [10 11 13] :latitude 37 :longitude 144}
                        {:time 1060000 :name "name" :number-of-messages 1 :latitude 37 :longitude 144}
                        {:time 1060010 :name "name" :rgb [10 11 12] :latitude 37 :longitude 144}
                        {:time 1160000 :name "name" :number-of-messages 1 :latitude 37 :longitude 144}
                        {:time 1160010 :name "name" :rgb [10 11 12] :latitude 37 :longitude 144}]]

    (with-open [driver (TopologyTestDriver. (generator/group-by-row-topology input-topic output-topic) config)]
      (write-inputs driver factory input-topic input-messages)

      (is (= ["name" {:time      1
                      :name      "name"
                      :latitude  37
                      :longitude 144
                      :pixels    [{:time 1 :name "name" :rgb [1 2 3] :latitude 37 :longitude 144}
                                  {:time 2 :name "name" :rgb [4 5 6] :latitude 37 :longitude 144}
                                  {:time 3 :name "name" :rgb [7 8 9] :latitude 37 :longitude 144}]}]
             (read-key-value ^TopologyTestDriver driver output-topic)))
      (is (= {:time      60020
              :name      "name"
              :latitude  37
              :longitude 144
              :pixels    [{:time 60020 :name "name" :rgb [10 11 12] :latitude 37 :longitude 144}
                          {:time 60030 :name "name" :rgb [10 11 13] :latitude 37 :longitude 144}]}
             (read-output ^TopologyTestDriver driver output-topic)))
      (is (= {:time      1060010
              :name      "name"
              :latitude  37
              :longitude 144
              :pixels    [{:time 1060010 :name "name" :rgb [10 11 12] :latitude 37 :longitude 144}]}
             (read-output ^TopologyTestDriver driver output-topic)))
      (is (= {:time      1160010
              :name      "name"
              :latitude  37
              :longitude 144
              :pixels    [{:time 1160010 :name "name" :rgb [10 11 12] :latitude 37 :longitude 144}]}
             (read-output ^TopologyTestDriver driver output-topic)))
      (is (= nil
             (read-output ^TopologyTestDriver driver output-topic))))))

(deftest group-by-rows

  (let [input-topic    "rgb-row-stream"
        output-topic   "rgb-rows-stream"
        factory        (ConsumerRecordFactory. input-topic
                                               (StringSerializer.)
                                               (->JsonSerializer))
        input-messages [{:time      1
                         :name      "name"
                         :latitude  33
                         :longitude 144
                         :pixels    [{:time 1 :name "name" :rgb [1 2 3] :latitude 33 :longitude 144}
                                     {:time 2 :name "name" :rgb [4 5 6] :latitude 33 :longitude 144}
                                     {:time 3 :name "name" :rgb [7 8 9] :latitude 33 :longitude 144}]}

                        {:time      60020
                         :name      "name"
                         :latitude  34
                         :longitude 144
                         :pixels    [{:time 60020 :name "name" :rgb [10 11 12] :latitude 34 :longitude 144}
                                     {:time 60030 :name "name" :rgb [10 11 13] :latitude 34 :longitude 144}]}

                        {:time      1060010
                         :name      "name"
                         :latitude  35
                         :longitude 144
                         :pixels    [{:time 1060010 :name "name" :rgb [10 11 12] :latitude 35 :longitude 144}]}

                        {:time      1160010
                         :name      "name"
                         :latitude  36
                         :longitude 144
                         :pixels    [{:time 1160010 :name "name" :rgb [10 11 12] :latitude 36 :longitude 144}]}]]

    (with-open [driver (TopologyTestDriver. (generator/group-by-rows-topology input-topic output-topic) config)]
      (write-inputs driver factory input-topic input-messages)

      (is (= ["0" [{:time      1
                    :name      "name"
                    :latitude  33
                    :longitude 144
                    :pixels
                    [{:time 1 :name "name" :rgb [1 2 3] :latitude 33 :longitude 144}
                     {:time 2 :name "name" :rgb [4 5 6] :latitude 33 :longitude 144}
                     {:time 3 :name "name" :rgb [7 8 9] :latitude 33 :longitude 144}]}]]
             (read-key-value ^TopologyTestDriver driver output-topic)))

      (is (= ["0" [{:time      1
                    :name      "name"
                    :latitude  33
                    :longitude 144
                    :pixels
                    [{:time 1 :name "name" :rgb [1 2 3] :latitude 33 :longitude 144}
                     {:time 2 :name "name" :rgb [4 5 6] :latitude 33 :longitude 144}
                     {:time 3 :name "name" :rgb [7 8 9] :latitude 33 :longitude 144}]}
                   {:time      60020
                    :name      "name"
                    :latitude  34
                    :longitude 144
                    :pixels
                    [{:time 60020 :name "name" :rgb [10 11 12] :latitude  34 :longitude 144}
                     {:time 60030 :name "name" :rgb [10 11 13] :latitude  34 :longitude 144}]}]]
             (read-key-value ^TopologyTestDriver driver output-topic))))))
