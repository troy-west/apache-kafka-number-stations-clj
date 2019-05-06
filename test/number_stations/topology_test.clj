(ns number-stations.topology-test
  (:require [clojure.test :refer [deftest is testing]]
            [numbers.topology :as topology]
            [numbers.serdes :as serdes])
  (:import (org.apache.kafka.streams.test ConsumerRecordFactory)
           (org.apache.kafka.streams TopologyTestDriver)
           (org.apache.kafka.streams StreamsBuilder)
           (org.apache.kafka.common.serialization StringSerializer)
           (org.apache.kafka.common.serialization StringDeserializer)))

(def record-factory
  (ConsumerRecordFactory. "radio-logs" (StringSerializer.) (serdes/->JsonSerializer)))

(defn read-output
  [^TopologyTestDriver driver topic]
  (some-> (.readOutput driver
                       topic
                       (StringDeserializer.)
                       (serdes/->JsonDeserializer))
          .value))

(deftest filter-test
  (let [builder (StreamsBuilder.)]
    (-> (topology/stream builder)
        topology/filter-recognized
        (.to "output"))

    (with-open [driver (TopologyTestDriver. (.build builder) topology/config)]
      (.pipeInput driver (.create record-factory "radio-logs" "E-test-english" {:time 10 :type "ENG" :name "E-test-english" :number 321}))
      (.pipeInput driver (.create record-factory "radio-logs" "X-unknown" {:time 20 :name "X-unknown" :number nil}))
      (.pipeInput driver (.create record-factory "radio-logs" "G-test-german" {:time 30 :type "GER" :name "E-test-german" :number 100}))

      (is (= {:time 10 :type "ENG" :name "E-test-english" :number 321}
             (read-output driver "output")))
      (is (= {:time 30 :type "GER" :name "E-test-german" :number 100}
             (read-output driver "output")))
      (is (= nil
             (read-output driver "output"))))))

(deftest translate-test
  (let [builder (StreamsBuilder.)]
    (some-> (topology/stream builder)
            topology/translate
            (.to "output"))

    (with-open [driver (TopologyTestDriver. (.build builder) topology/config)]
      (.pipeInput driver (.create record-factory "radio-logs" "E-test-english" {:time 10 :type "ENG" :name "E-test-english" :numbers ["three" "two" "one"]}))
      (.pipeInput driver (.create record-factory "radio-logs" "X-unknown" {:time 20 :name "X-unknown" :numbers ["unicorn" "camel" "dropbear"]}))
      (.pipeInput driver (.create record-factory "radio-logs" "G-test-german" {:time 30 :type "GER" :name "E-test-german" :numbers ["eins" "null" "null"]}))

      (is (= {:time 10 :type "ENG" :name "E-test-english" :number 321}
             (read-output driver "output")))
      (is (= {:time 20 :name "X-unknown" :number nil}
             (read-output driver "output")))
      (is (= {:time 30 :type "GER" :name "E-test-german" :number 100}
             (read-output driver "output")))
      (is (= nil
             (read-output driver "output"))))))

(deftest correlate-test
  (let [builder (StreamsBuilder.)]
    (some-> (topology/stream builder)
            (topology/correlate))

    (with-open [driver (TopologyTestDriver. (.build builder) topology/config)]
      ;; First window
      (.pipeInput driver (.create record-factory "radio-logs" "E-test-english" {:time 10010 :type "ENG" :name "E-test-english" :number 0}))
      (.pipeInput driver (.create record-factory "radio-logs" "E-test-english" {:time 11000 :type "ENG" :name "E-test-english" :number 100}))
      (.pipeInput driver (.create record-factory "radio-logs" "G-test-german" {:time 12000 :type "GER" :name "E-test-german" :number 200}))
      ;; Second window
      (.pipeInput driver (.create record-factory "radio-logs" "E-test-english" {:time 22000 :type "ENG" :name "E-test-english" :number 50}))
      (.pipeInput driver (.create record-factory "radio-logs" "G-test-german" {:time 20000 :type "GER" :name "E-test-german" :number 210}))
      (.pipeInput driver (.create record-factory "radio-logs" "E-test-english" {:time 21000 :type "ENG" :name "E-test-english" :number 150}))
      (.pipeInput driver (.create record-factory "radio-logs" "G-test-german" {:time 25000 :type "GER" :name "E-test-german" :number 220}))
      ;; Third window
      (.pipeInput driver (.create record-factory "radio-logs" "E-test-english" {:time 30000 :type "ENG" :name "E-test-english" :number 65}))

      (testing "Fetch all keys for all time"
        (with-open [iterator (.fetchAll (.getWindowStore driver "PT10S-Store") Long/MIN_VALUE Long/MAX_VALUE)]
          (is (= [[{:time 10010 :type "ENG" :name "E-test-english" :number 0}
                   {:time 11000 :type "ENG" :name "E-test-english" :number 100}]
                  [{:time 22000 :type "ENG" :name "E-test-english" :number 50}
                   {:time 21000 :type "ENG" :name "E-test-english" :number 150}]
                  [{:time 30000 :type "ENG" :name "E-test-english" :number 65}]
                  ;; New keys grouped here
                  [{:time 12000 :type "GER" :name "E-test-german" :number 200}]
                  [{:time 20000 :type "GER" :name "E-test-german" :number 210}
                   {:time 25000 :type "GER" :name "E-test-german" :number 220}]]

                 (mapv #(.value %) (iterator-seq iterator))))))

      (testing "Fetch by a key for all time"
        (with-open [iterator (.fetch (.getWindowStore driver "PT10S-Store") "E-test-english" Long/MIN_VALUE Long/MAX_VALUE)]
          (is (= [[{:time 10010 :type "ENG" :name "E-test-english" :number 0}
                   {:time 11000 :type "ENG" :name "E-test-english" :number 100}]
                  [{:time 22000 :type "ENG" :name "E-test-english" :number 50}
                   {:time 21000 :type "ENG" :name "E-test-english" :number 150}]
                  [{:time 30000 :type "ENG" :name "E-test-english" :number 65}]]

                 (mapv #(.value %) (iterator-seq iterator))))))

      (testing "Fetch by another key for all time"
        (with-open [iterator (.fetch (.getWindowStore driver "PT10S-Store") "G-test-german" Long/MIN_VALUE Long/MAX_VALUE)]
          (is (= [[{:time 12000 :type "GER" :name "E-test-german" :number 200}]
                  [{:time 20000 :type "GER" :name "E-test-german" :number 210}
                   {:time 25000 :type "GER" :name "E-test-german" :number 220}]]

                 (mapv #(.value %) (iterator-seq iterator))))))

      (testing "Fetch by key and single full window"
        (with-open [iterator (.fetch (.getWindowStore driver "PT10S-Store") "E-test-english" 10000 (dec 20000))]
          (is (= [[{:time 10010 :type "ENG" :name "E-test-english" :number 0}
                   {:time 11000 :type "ENG" :name "E-test-english" :number 100}]]

                 (mapv #(.value %) (iterator-seq iterator))))))

      (testing "Fetch from empty windows"
        (with-open [iterator (.fetch (.getWindowStore driver "PT10S-Store") "E-test-english" 0 (dec 10000))]
          (is (= []
                 (mapv #(.value %) (iterator-seq iterator)))))

        (with-open [iterator (.fetch (.getWindowStore driver "PT10S-Store") "G-test-german" 0 (dec 10000))]
          (is (= []
                 (mapv #(.value %) (iterator-seq iterator)))))

        (with-open [iterator (.fetch (.getWindowStore driver "PT10S-Store") "G-test-german" 30000 (dec 40000))]
          (is (= []
                 (mapv #(.value %) (iterator-seq iterator)))))

        (with-open [iterator (.fetch (.getWindowStore driver "PT10S-Store") "E-test-english" 40000 (dec 50000))]
          (is (= []
                 (mapv #(.value %) (iterator-seq iterator)))))))))

(deftest deduplicate-test
  (let [builder (StreamsBuilder.)]
    (some-> (topology/stream builder)
            (topology/deduplicate builder)
            (.to "output"))

    (with-open [driver (TopologyTestDriver. (.build builder) topology/config)]
      (.pipeInput driver (.create record-factory "radio-logs" "E-test-english" {:time 10 :type "ENG" :name "E-test-english" :number 321}))
      (.pipeInput driver (.create record-factory "radio-logs" "G-test-german" {:time 30 :type "GER" :name "E-test-german" :number 100}))
      (.pipeInput driver (.create record-factory "radio-logs" "G-test-german" {:time 30 :type "GER" :name "E-test-german" :number 100}))
      (.pipeInput driver (.create record-factory "radio-logs" "E-test-english" {:time 10 :type "ENG" :name "E-test-english" :number 321}))

      (is (= {:time 10 :type "ENG" :name "E-test-english" :number 321}
             (read-output driver "output")))
      (is (= {:time 30 :type "GER" :name "E-test-german" :number 100}
             (read-output driver "output")))
      (is (= nil
             (read-output driver "output"))))))