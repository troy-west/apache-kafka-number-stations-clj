(ns numbers.compute-test
  (:require [clojure.test :refer [deftest is testing]]
            [numbers.compute :as compute]
            [numbers.serdes :as serdes])
  (:import (org.apache.kafka.streams.test ConsumerRecordFactory)
           (org.apache.kafka.streams StreamsBuilder TopologyTestDriver)
           (org.apache.kafka.common.serialization StringSerializer)
           (org.apache.kafka.common.serialization StringDeserializer Serializer)
           (org.apache.kafka.streams.kstream KStream)
           (org.apache.kafka.clients.consumer ConsumerRecord)))

(def test-messages
  [{:time 1557125670789 :type "GER" :name "85" :long -92 :lat -30 :content ["eins" "null" "sechs"]}
   {:time 1557125670790 :type "UXX" :name "XRAY"}
   {:time 1557125670794 :type "MOR" :name "425" :long 77 :lat 25 :content ["....." "----."]}
   {:time 1557125670795 :type "UXX" :name "XRAY"}
   {:time 1557125670799 :type "ENG" :name "NZ1" :long 166 :lat -78 :content ["two"]}
   {:time 1557125670807 :type "ENG" :name "159" :long -55 :lat -18 :content ["three" "five"]}
   {:time 1557125670812 :type "ENG" :name "426" :long 78 :lat 26 :content ["six" "three"]}
   {:time 1557125670814 :type "GER" :name "85" :long -92 :lat -30 :content ["drei" "neun"]}
   {:time 1557125670819 :type "MOR" :name "425" :long 77 :lat 25 :content [".----"]}
   {:time 1557125670824 :type "ENG" :name "NZ1" :long 166 :lat -78 :content ["two"]}
   {:time 1557125670827 :type "ENG" :name "324" :long 27 :lat 9 :content ["two" "nine"]}
   {:time 1557125670829 :type "GER" :name "460" :long 95 :lat 31 :content ["fünf" "sieben"]}
   {:time 1557125670831 :type "GER" :name "355" :long 42 :lat 14 :content ["sieben"]}
   {:time 1557125670832 :type "ENG" :name "159" :long -55 :lat -18 :content ["three" "five"]}
   {:time 1557125670837 :type "ENG" :name "426" :long 78 :lat 26 :content ["one"]}
   {:time 1557125670839 :type "GER" :name "85" :long -92 :lat -30 :content ["fünf" "fünf"]}
   {:time 1557125670840 :type "GER" :name "505" :long 117 :lat 39 :content ["eins" "null" "vier"]}
   {:time 1557125670841 :type "GER" :name "487" :long 108 :lat 36 :content ["eins" "null" "neun"]}
   {:time 1557125670842 :type "MOR" :name "20" :long -125 :lat -41 :content ["...--"]}
   {:time 1557125670843 :type "GER" :name "199" :long -35 :lat -11 :content ["eins" "vier"]}])

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

;; Confirm the timestamp extracted for a consumer record matches the time provided by the message
(deftest test-timestamp-extraction
  (let [message (first test-messages)
        record  (ConsumerRecord. "radio-logs" 0 0 (:name message) message)]
    (is (= 1557125670789 (.extract compute/extractor record 0)))))


;; Demonstrate the ability to filter out messages that return false to (translator/known? message)
(deftest test-filter-known
  (let [builder (StreamsBuilder.)]
    (-> ^KStream (compute/stream builder)
        (compute/filter-known)
        (.to "output"))

    (with-open [driver (TopologyTestDriver. (.build builder) compute/config)]

      (send-messages driver test-messages)

      (is (= [{:time 1557125670789 :type "GER" :name "85" :long -92 :lat -30 :content ["eins" "null" "sechs"]}
              {:time 1557125670794 :type "MOR" :name "425" :long 77 :lat 25 :content ["....." "----."]}
              {:time 1557125670799 :type "ENG" :name "NZ1" :long 166 :lat -78 :content ["two"]}
              {:time 1557125670807 :type "ENG" :name "159" :long -55 :lat -18 :content ["three" "five"]}
              {:time 1557125670812 :type "ENG" :name "426" :long 78 :lat 26 :content ["six" "three"]}]
             (read-output driver 5))))))

;; Split the stream in two, testing the new rest-of-world stream first
(deftest test-branch-rest-of-world
  (let [builder (StreamsBuilder.)]
    (-> ^KStream (compute/stream builder)
        (compute/filter-known)
        (compute/branch-scott-base)
        first
        (.to "output"))

    (with-open [driver (TopologyTestDriver. (.build builder) compute/config)]

      (send-messages driver test-messages)

      (is (= [{:time 1557125670789 :type "GER" :name "85" :long -92 :lat -30 :content ["eins" "null" "sechs"]}
              {:time 1557125670794 :type "MOR" :name "425" :long 77 :lat 25 :content ["....." "----."]}
              {:time 1557125670807 :type "ENG" :name "159" :long -55 :lat -18 :content ["three" "five"]}
              {:time 1557125670812 :type "ENG" :name "426" :long 78 :lat 26 :content ["six" "three"]}
              {:time 1557125670814 :type "GER" :name "85" :long -92 :lat -30 :content ["drei" "neun"]}
              {:time 1557125670819 :type "MOR" :name "425" :long 77 :lat 25 :content [".----"]}
              {:time 1557125670827 :type "ENG" :name "324" :long 27 :lat 9 :content ["two" "nine"]}
              {:time 1557125670829 :type "GER" :name "460" :long 95 :lat 31 :content ["fünf" "sieben"]}
              {:time 1557125670831 :type "GER" :name "355" :long 42 :lat 14 :content ["sieben"]}
              {:time 1557125670832 :type "ENG" :name "159" :long -55 :lat -18 :content ["three" "five"]}
              {:time 1557125670837 :type "ENG" :name "426" :long 78 :lat 26 :content ["one"]}
              {:time 1557125670839 :type "GER" :name "85" :long -92 :lat -30 :content ["fünf" "fünf"]}
              {:time 1557125670840 :type "GER" :name "505" :long 117 :lat 39 :content ["eins" "null" "vier"]}
              {:time 1557125670841 :type "GER" :name "487" :long 108 :lat 36 :content ["eins" "null" "neun"]}
              {:time 1557125670842 :type "MOR" :name "20" :long -125 :lat -41 :content ["...--"]}
              {:time 1557125670843 :type "GER" :name "199" :long -35 :lat -11 :content ["eins" "vier"]}]
             (read-output driver 16))))))

;; Split the stream in two, testing the new scott base stream second
(deftest test-branch-scott-base
  (let [builder (StreamsBuilder.)]
    (-> ^KStream (compute/stream builder)
        (compute/filter-known)
        (compute/branch-scott-base)
        second
        (.to "output"))

    (with-open [driver (TopologyTestDriver. (.build builder) compute/config)]

      (send-messages driver test-messages)

      (is (= [{:time 1557125670799 :type "ENG" :name "NZ1" :long 166 :lat -78 :content ["two"]}
              {:time 1557125670824 :type "ENG" :name "NZ1" :long 166 :lat -78 :content ["two"]}]
             (read-output driver 2))))))

;; Demonstrate the ability to mutate the stream, translating words into numeric strings
(deftest test-translate
  (let [builder (StreamsBuilder.)]
    (some-> (compute/stream builder)
            (compute/filter-known)
            (compute/translate)
            (.to "output"))

    (with-open [driver (TopologyTestDriver. (.build builder) compute/config)]

      (send-messages driver test-messages)

      (testing "Content is translated"
        (is (= [{:time 1557125670789 :type "GER" :name "85" :long -92 :lat -30 :content [106]}
                {:time 1557125670794 :type "MOR" :name "425" :long 77 :lat 25 :content [59]}
                {:time 1557125670799 :type "ENG" :name "NZ1" :long 166 :lat -78 :content [2]}
                {:time 1557125670807 :type "ENG" :name "159" :long -55 :lat -18 :content [35]}
                {:time 1557125670812 :type "ENG" :name "426" :long 78 :lat 26 :content [63]}]
               (read-output driver 5)))))))

;; Prove the result of grouping, windowing, and aggregating the stream into a k-table ("PT10S-Store")
(deftest test-correlate
  (let [builder (StreamsBuilder.)]
    (some-> (compute/stream builder)
            (compute/filter-known)
            (compute/translate)
            (compute/correlate "PT10S-Store"))

    (with-open [driver (TopologyTestDriver. (.build builder) compute/config)]

      (send-messages driver test-messages)

      (testing "Slice data from the "
        (with-open [iterator (.fetch (.getWindowStore driver "PT10S-Store")
                                     "85"
                                     ^Long (- 1557125670789 25000) ;; start 25s before the first message
                                     ^Long (+ 1557125670789 100000))] ;; end 100s after the first message
          (is (= [{:time 1557125670789 :type "GER" :name "85" :long -92 :lat -30 :content [106 39 55]}]
                 (mapv #(.value %) (iterator-seq iterator))))))

      (testing "Fetch from empty windows"
        (with-open [iterator (.fetch (.getWindowStore driver "PT10S-Store") "Unknown-Key" 0 10000)]
          (is (= []
                 (mapv #(.value %) (iterator-seq iterator)))))))))