(ns number-stations.topology-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is]]
            [number-stations.images :as images])
  (:import java.util.Properties
           org.apache.kafka.clients.producer.ProducerRecord
           [org.apache.kafka.common.serialization StringDeserializer StringSerializer]
           [org.apache.kafka.streams StreamsBuilder TopologyTestDriver]
           org.apache.kafka.streams.kstream.Consumed
           org.apache.kafka.streams.processor.TimestampExtractor
           org.apache.kafka.streams.test.ConsumerRecordFactory))

(defn config
  []
  (let [props (Properties.)]
    (.putAll props {"application.id"      (str (rand-int 1000000))
                    "bootstrap.servers"   "localhost:9092"
                    "default.key.serde"   "org.apache.kafka.common.serialization.Serdes$StringSerde"
                    "default.value.serde" "number_stations.generator.JsonSerde"})
    props))

(def extractor
  (reify TimestampExtractor
    (extract [_ record _]
      (:time (.value record)))))

(defn read-output
  [driver topic]
  (when-let [record (.readOutput ^TopologyTestDriver
                                 driver
                                 topic
                                 (StringDeserializer.)
                                 (topology/->JsonDeserializer))]
    (.value ^ProducerRecord record)))

(deftest translate-numbers-test
  (let [factory (ConsumerRecordFactory. "input"
                                        (StringSerializer.)
                                        (topology/->JsonSerializer))
        builder (StreamsBuilder.)
        stream  (.stream builder "input" (Consumed/with extractor))]

    (-> stream
        topology/translate-numbers
        (.to "output"))

    (with-open [driver (TopologyTestDriver. (.build builder) (config))]
      (.pipeInput driver (.create factory "input" "name" {:time 10 :name "E-test-english" :numbers ["three" "two" "one"]}))
      (.pipeInput driver (.create factory "input" "name" {:time 20 :name "G-test-german" :numbers ["eins" "null" "null"]}))

      (is (= {:time 10 :name "E-test-english" :colour-component 321}
             (read-output ^TopologyTestDriver driver "output")))
      (is (= {:time 20 :name "G-test-german" :colour-component 100}
             (read-output ^TopologyTestDriver driver "output"))))))

(deftest correlate-rgb-test
  (let [builder      (StreamsBuilder.)
        factory      (ConsumerRecordFactory. "input"
                                             (StringSerializer.)
                                             (topology/->JsonSerializer))
        stream       (.stream builder "input" (Consumed/with extractor))]

    (topology/correlate-rgb stream)

    (with-open [driver (TopologyTestDriver. (.build builder) (config))]
      (.pipeInput driver (.create factory "input" "name" {:time 10 :name "name" :colour-component 0}))
      (.pipeInput driver (.create factory "input" "name" {:time 1000 :name "name" :colour-component 100}))
      (.pipeInput driver (.create factory "input" "name" {:time 2000 :name "name" :colour-component 200}))
      (.pipeInput driver (.create factory "input" "name" {:time 12000 :name "name" :colour-component 50}))
      (.pipeInput driver (.create factory "input" "name" {:time 11000 :name "name" :colour-component 150}))
      (.pipeInput driver (.create factory "input" "name" {:time 13000 :name "name" :colour-component 250}))

      (with-open [iterator (.fetch (.getWindowStore driver topology/pt10s-store) "name" 0 20001)]
        (is (= [[{:time 10 :name "name" :colour-component 0}
                 {:time 1000 :name "name" :colour-component 100}
                 {:time 2000 :name "name" :colour-component 200}]
                [{:time 12000 :name "name" :colour-component 50}
                 {:time 11000 :name "name" :colour-component 150}
                 {:time 13000 :name "name" :colour-component 250}]]
               (mapv #(.value %) (iterator-seq iterator))))))))

(deftest number-stations-to-image-topology-test
  (let [builder        (StreamsBuilder.)
        factory        (ConsumerRecordFactory. "input"
                                               (StringSerializer.)
                                               (topology/->JsonSerializer))
        stream         (.stream builder "input" (Consumed/with extractor))]

    (-> stream
        (topology/translate-numbers)
        (topology/correlate-rgb))

    (with-open [driver (TopologyTestDriver. (.build builder) (config))]
      (doseq [message (generator/generate-messages images/small-image)]
        (.pipeInput driver (.create factory "input" (:name message) message)))

      (images/radio-stations-to-image (.getWindowStore driver topology/pt10s-store)
                                      (vec (for [i (range 1000)]
                                             (str "E-" i)))
                                      0
                                      2000000000
                                      (io/file "resources/output11.png")))))
