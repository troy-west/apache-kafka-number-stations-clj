(ns number-stations.topology-test
  (:require [clojure.test :refer [deftest is testing]]
            [numbers.topology :as topology]
            [numbers.serdes :as serdes])
  (:import (org.apache.kafka.streams.test ConsumerRecordFactory)
           (org.apache.kafka.streams StreamsBuilder TopologyTestDriver)
           (org.apache.kafka.common.serialization StringSerializer)
           (org.apache.kafka.common.serialization StringDeserializer Serializer)
           (org.apache.kafka.streams.kstream KStream)))

(def test-messages
  [{:time 1557125670789 :type "GER" :name "G-85" :long -92 :lat -30 :value ["eins" "null" "sechs"]}
   {:time 1557125670790 :type "UXX" :name "X-RAY"}
   {:time 1557125670794 :type "MOR" :name "M-425" :long 77 :lat 25 :value ["....." "----."]}
   {:time 1557125670795 :type "UXX" :name "X-RAY"}
   {:time 1557125670799 :type "ENG" :name "NZ-1" :long -78 :lat 166 :value ["two"]}
   {:time 1557125670807 :type "ENG" :name "E-159" :long -55 :lat -18 :value ["three" "five"]}
   {:time 1557125670812 :type "ENG" :name "E-426" :long 78 :lat 26 :value ["six" "three"]}
   {:time 1557125670814 :type "GER" :name "G-85" :long -92 :lat -30 :value ["drei" "neun"]}
   {:time 1557125670819 :type "MOR" :name "M-425" :long 77 :lat 25 :value [".----"]}
   {:time 1557125670824 :type "ENG" :name "NZ-1" :long -78 :lat 166 :value ["two"]}
   {:time 1557125670827 :type "ENG" :name "E-324" :long 27 :lat 9 :value ["two" "nine"]}
   {:time 1557125670829 :type "GER" :name "G-460" :long 95 :lat 31 :value ["fünf" "sieben"]}
   {:time 1557125670831 :type "GER" :name "G-355" :long 42 :lat 14 :value ["sieben"]}
   {:time 1557125670832 :type "ENG" :name "E-159" :long -55 :lat -18 :value ["three" "five"]}
   {:time 1557125670837 :type "ENG" :name "E-426" :long 78 :lat 26 :value ["one"]}
   {:time 1557125670839 :type "GER" :name "G-85" :long -92 :lat -30 :value ["fünf" "fünf"]}
   {:time 1557125670840 :type "GER" :name "G-505" :long 117 :lat 39 :value ["eins" "null" "vier"]}
   {:time 1557125670841 :type "GER" :name "G-487" :long 108 :lat 36 :value ["eins" "null" "neun"]}
   {:time 1557125670842 :type "MOR" :name "M-20" :long -125 :lat -41 :value ["...--"]}
   {:time 1557125670843 :type "GER" :name "G-199" :long -35 :lat -11 :value ["eins" "vier"]}])

(def record-factory
  (ConsumerRecordFactory. "radio-logs" (StringSerializer.) ^Serializer (serdes/->JsonSerializer)))

(defn send-message
  [driver message]
  (.pipeInput driver (.create record-factory "radio-logs" (:name message) message)))

(defn send-messages
  [driver messages]
  (doseq [message messages]
    (send-message driver message)))

(defn read-output
  ([^TopologyTestDriver driver]
   (some-> (.readOutput driver "output" (StringDeserializer.) (serdes/->JsonDeserializer)) .value))
  ([^TopologyTestDriver driver n]
   (repeatedly n #(read-output driver))))

(deftest filter-test
  (let [builder (StreamsBuilder.)]
    (-> ^KStream (topology/stream builder)
        (topology/filter-known)
        (.to "output"))

    (with-open [driver (TopologyTestDriver. (.build builder) topology/config)]

      (send-messages driver test-messages)

      (is (= [{:time 1557125670789 :type "GER" :name "G-85" :long -92 :lat -30 :value ["eins" "null" "sechs"]}
              {:time 1557125670794 :type "MOR" :name "M-425" :long 77 :lat 25 :value ["....." "----."]}
              {:time 1557125670799 :type "ENG" :name "NZ-1" :long -78 :lat 166 :value ["two"]}
              {:time 1557125670807 :type "ENG" :name "E-159" :long -55 :lat -18 :value ["three" "five"]}
              {:time 1557125670812 :type "ENG" :name "E-426" :long 78 :lat 26 :value ["six" "three"]}]
             (read-output driver 5))))))

(deftest translate-test
  (let [builder (StreamsBuilder.)]
    (some-> (topology/stream builder)
            (topology/filter-known)
            (topology/translate)
            (.to "output"))

    (with-open [driver (TopologyTestDriver. (.build builder) topology/config)]

      (send-messages driver test-messages)

      (is (= [{:time 1557125670789 :type "GER" :name "G-85" :long -92 :lat -30 :value 106}
              {:time 1557125670794 :type "MOR" :name "M-425" :long 77 :lat 25 :value 59}
              {:time 1557125670799 :type "ENG" :name "NZ-1" :long -78 :lat 166 :value 2}
              {:time 1557125670807 :type "ENG" :name "E-159" :long -55 :lat -18 :value 35}
              {:time 1557125670812 :type "ENG" :name "E-426" :long 78 :lat 26 :value 63}]
             (read-output driver 5))))))

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