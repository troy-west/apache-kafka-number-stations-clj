(ns number-stations.topology-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [testing deftest is]]
;;            [number-stations.images :as images]
            [number-stations.topology :as topology]
            [number-stations.generator :as generator])
  (:import java.util.Properties
           org.apache.kafka.clients.producer.ProducerRecord
           [org.apache.kafka.common.serialization StringDeserializer StringSerializer]
           [org.apache.kafka.streams StreamsBuilder TopologyTestDriver]
           org.apache.kafka.streams.kstream.Consumed
           org.apache.kafka.streams.processor.TimestampExtractor
           org.apache.kafka.streams.test.ConsumerRecordFactory))

(def input-topic
  "input")

(def output-topic
  "output")

(def record-factory
  (ConsumerRecordFactory. input-topic
                          (StringSerializer.)
                          (topology/->JsonSerializer)))

(def config
  (topology/config {:application-id "test"}))

(defn read-output
  [^TopologyTestDriver driver topic]
  (some-> (.readOutput driver
                       topic
                       (StringDeserializer.)
                       (topology/->JsonDeserializer))
          .value))

(deftest translate-test
  (let [builder (StreamsBuilder.)]
    (some-> (.stream builder input-topic (Consumed/with topology/extractor))
            topology/translate
            (.to output-topic))

    (with-open [driver (TopologyTestDriver. (.build builder) config)]
      (.pipeInput driver (.create record-factory input-topic "E-test-english" {:time 10 :name "E-test-english" :numbers ["three" "two" "one"]}))
      (.pipeInput driver (.create record-factory input-topic "X-unknown" {:time 20 :name "X-unknown" :numbers ["unicorn" "camel" "dropbear"]}))
      (.pipeInput driver (.create record-factory input-topic "G-test-german" {:time 30 :name "G-test-german" :numbers ["eins" "null" "null"]}))

      (is (= {:time 10 :name "E-test-english" :number 321}
             (read-output driver output-topic)))
      (is (= {:time 20 :name "X-unknown" :number nil}
             (read-output driver output-topic)))
      (is (= {:time 30 :name "G-test-german" :number 100}
             (read-output driver output-topic)))
      (is (= nil
             (read-output driver output-topic))))))

(deftest denoise-test
  (let [builder (StreamsBuilder.)]
    (some-> (.stream builder input-topic (Consumed/with topology/extractor))
            topology/denoise
            (.to output-topic))

    (with-open [driver (TopologyTestDriver. (.build builder) config)]
      (.pipeInput driver (.create record-factory input-topic "E-test-english" {:time 10 :name "E-test-english" :number 321}))
      (.pipeInput driver (.create record-factory input-topic "X-unknown" {:time 20 :name "X-unknown" :number nil}))
      (.pipeInput driver (.create record-factory input-topic "G-test-german" {:time 30 :name "G-test-german" :number 100}))

      (is (= {:time 10 :name "E-test-english" :number 321}
             (read-output driver output-topic)))
      (is (= {:time 30 :name "G-test-german" :number 100}
             (read-output driver output-topic)))
      (is (= nil
             (read-output driver output-topic))))))

(deftest correlate-test
  (let [builder (StreamsBuilder.)]
    (some-> (.stream builder input-topic (Consumed/with topology/extractor))
            (topology/correlate))

    (with-open [driver (TopologyTestDriver. (.build builder) config)]
      ;; First window
      (.pipeInput driver (.create record-factory input-topic "E-test-english" {:time 10010 :name "E-test-english" :number 0}))
      (.pipeInput driver (.create record-factory input-topic "E-test-english" {:time 11000 :name "E-test-english" :number 100}))
      (.pipeInput driver (.create record-factory input-topic "G-test-german" {:time 12000 :name "G-test-german" :number 200}))
      ;; Second window
      (.pipeInput driver (.create record-factory input-topic "E-test-english" {:time 22000 :name "E-test-english" :number 50}))
      (.pipeInput driver (.create record-factory input-topic "G-test-german" {:time 20000 :name "G-test-german" :number 210}))
      (.pipeInput driver (.create record-factory input-topic "E-test-english" {:time 21000 :name "E-test-english" :number 150}))
      (.pipeInput driver (.create record-factory input-topic "G-test-german" {:time 25000 :name "G-test-german" :number 220}))
      ;; Third window
      (.pipeInput driver (.create record-factory input-topic "E-test-english" {:time 30000 :name "E-test-english" :number 65}))

      (testing "Fetch all keys for all time"
        (with-open [iterator (.fetchAll (.getWindowStore driver topology/pt10s-store) Long/MIN_VALUE Long/MAX_VALUE)]
          (is (= [[{:time 10010 :name "E-test-english" :number 0}
                   {:time 11000 :name "E-test-english" :number 100}]
                  [{:time 22000 :name "E-test-english" :number 50}
                   {:time 21000 :name "E-test-english" :number 150}]
                  [{:time 30000 :name "E-test-english" :number 65}]
                  ;; New keys grouped here
                  [{:time 12000 :name "G-test-german" :number 200}]
                  [{:time 20000 :name "G-test-german" :number 210}
                   {:time 25000 :name "G-test-german" :number 220}]]

                 (mapv #(.value %) (iterator-seq iterator))))))

      (testing "Fetch by a key for all time"
        (with-open [iterator (.fetch (.getWindowStore driver topology/pt10s-store) "E-test-english" Long/MIN_VALUE Long/MAX_VALUE)]
          (is (= [[{:time 10010 :name "E-test-english" :number 0}
                   {:time 11000 :name "E-test-english" :number 100}]
                  [{:time 22000 :name "E-test-english" :number 50}
                   {:time 21000 :name "E-test-english" :number 150}]
                  [{:time 30000 :name "E-test-english" :number 65}]]

                 (mapv #(.value %) (iterator-seq iterator))))))

      (testing "Fetch by another key for all time"
        (with-open [iterator (.fetch (.getWindowStore driver topology/pt10s-store) "G-test-german" Long/MIN_VALUE Long/MAX_VALUE)]
          (is (= [[{:time 12000 :name "G-test-german" :number 200}]
                  [{:time 20000 :name "G-test-german" :number 210}
                   {:time 25000 :name "G-test-german" :number 220}]]

                 (mapv #(.value %) (iterator-seq iterator))))))

      (testing "Fetch by key and single full window"
        (with-open [iterator (.fetch (.getWindowStore driver topology/pt10s-store) "E-test-english" 10000 (dec 20000))]
          (is (= [[{:time 10010 :name "E-test-english" :number 0}
                   {:time 11000 :name "E-test-english" :number 100}]]

                 (mapv #(.value %) (iterator-seq iterator))))))

      (testing "Fetch from empty windows"
        (with-open [iterator (.fetch (.getWindowStore driver topology/pt10s-store) "E-test-english" 0 (dec 10000))]
          (is (= []
                 (mapv #(.value %) (iterator-seq iterator)))))

        (with-open [iterator (.fetch (.getWindowStore driver topology/pt10s-store) "G-test-german" 0 (dec 10000))]
          (is (= []
                 (mapv #(.value %) (iterator-seq iterator)))))

        (with-open [iterator (.fetch (.getWindowStore driver topology/pt10s-store) "G-test-german" 30000 (dec 40000))]
          (is (= []
                 (mapv #(.value %) (iterator-seq iterator)))))

        (with-open [iterator (.fetch (.getWindowStore driver topology/pt10s-store) "E-test-english" 40000 (dec 50000))]
          (is (= []
                 (mapv #(.value %) (iterator-seq iterator)))))))))

(deftest deduplicate-test
  (let [builder (StreamsBuilder.)]
    (some-> (.stream builder input-topic (Consumed/with topology/extractor))
            (topology/deduplicate builder)
            (.to output-topic))

    (with-open [driver (TopologyTestDriver. (.build builder) config)]
      (.pipeInput driver (.create record-factory input-topic "E-test-english" {:time 10 :name "E-test-english" :number 321}))
      (.pipeInput driver (.create record-factory input-topic "G-test-german" {:time 30 :name "G-test-german" :number 100}))
      (.pipeInput driver (.create record-factory input-topic "G-test-german" {:time 30 :name "G-test-german" :number 100}))
      (.pipeInput driver (.create record-factory input-topic "E-test-english" {:time 10 :name "E-test-english" :number 321}))

      (is (= {:time 10 :name "E-test-english" :number 321}
             (read-output driver output-topic)))
      (is (= {:time 30 :name "G-test-german" :number 100}
             (read-output driver output-topic)))
      (is (= nil
             (read-output driver output-topic))))))

(deftest translate-test
  (let [builder (StreamsBuilder.)]
    (some-> (.stream builder input-topic (Consumed/with topology/extractor))
            topology/translate
            topology/denoise
            (topology/deduplicate builder)
            topology/correlate)

    (with-open [driver (TopologyTestDriver. (.build builder) config)]
      (.pipeInput driver (.create record-factory input-topic "E-test-english" {:time 10 :name "E-test-english" :numbers ["three" "two" "one"]}))
      (.pipeInput driver (.create record-factory input-topic "X-unknown" {:time 20 :name "X-unknown" :numbers ["unicorn" "camel" "dropbear"]}))
      (.pipeInput driver (.create record-factory input-topic "G-test-german" {:time 30 :name "G-test-german" :numbers ["eins" "null" "null"]}))
      (.pipeInput driver (.create record-factory input-topic "X-unknown" {:time 20 :name "X-unknown" :numbers ["unicorn" "camel" "dropbear"]}))
      (.pipeInput driver (.create record-factory input-topic "G-test-german" {:time 30 :name "G-test-german" :numbers ["eins" "null" "null"]}))
      (.pipeInput driver (.create record-factory input-topic "E-test-english" {:time 10 :name "E-test-english" :numbers ["three" "two" "one"]}))
      (.pipeInput driver (.create record-factory input-topic "G-test-german" {:time 30 :name "G-test-german" :numbers ["eins" "null" "null"]}))

      (with-open [iterator (.fetchAll (.getWindowStore driver topology/pt10s-store) Long/MIN_VALUE Long/MAX_VALUE)]
        (is (= [[{:time 10 :name "E-test-english" :number 321}]
                [{:time 30 :name "G-test-german" :number 100}]]
               (mapv #(.value %) (iterator-seq iterator))))))))
