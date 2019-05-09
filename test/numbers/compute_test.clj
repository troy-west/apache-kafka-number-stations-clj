(ns numbers.compute-test
  (:require [clojure.test :refer [deftest is testing]]
            [numbers.compute :as topology]
            [numbers.serdes :as serdes])
  (:import (org.apache.kafka.streams.test ConsumerRecordFactory)
           (org.apache.kafka.streams StreamsBuilder TopologyTestDriver)
           (org.apache.kafka.common.serialization StringSerializer)
           (org.apache.kafka.common.serialization StringDeserializer Serializer)
           (org.apache.kafka.streams.kstream KStream)))

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

(deftest filter-test
  (let [builder (StreamsBuilder.)]
    (-> ^KStream (topology/stream builder)
        (topology/filter-known)
        (.to "output"))

    (with-open [driver (TopologyTestDriver. (.build builder) topology/config)]

      (send-messages driver test-messages)

      (is (= [{:time 1557125670789 :type "GER" :name "85" :long -92 :lat -30 :content ["eins" "null" "sechs"]}
              {:time 1557125670794 :type "MOR" :name "425" :long 77 :lat 25 :content ["....." "----."]}
              {:time 1557125670799 :type "ENG" :name "NZ1" :long 166 :lat -78 :content ["two"]}
              {:time 1557125670807 :type "ENG" :name "159" :long -55 :lat -18 :content ["three" "five"]}
              {:time 1557125670812 :type "ENG" :name "426" :long 78 :lat 26 :content ["six" "three"]}]
             (read-output driver 5))))))

(deftest translate-test
  (let [builder (StreamsBuilder.)]
    (some-> (topology/stream builder)
            (topology/filter-known)
            (topology/translate)
            (.to "output"))

    (with-open [driver (TopologyTestDriver. (.build builder) topology/config)]

      (send-messages driver test-messages)

      (testing "Content is translated"
        (is (= [{:time 1557125670789 :type "GER" :name "85" :long -92 :lat -30 :content [106]}
                {:time 1557125670794 :type "MOR" :name "425" :long 77 :lat 25 :content [59]}
                {:time 1557125670799 :type "ENG" :name "NZ1" :long 166 :lat -78 :content [2]}
                {:time 1557125670807 :type "ENG" :name "159" :long -55 :lat -18 :content [35]}
                {:time 1557125670812 :type "ENG" :name "426" :long 78 :lat 26 :content [63]}]
               (read-output driver 5)))))))

(deftest correlate-test
  (let [builder (StreamsBuilder.)]
    (some-> (topology/stream builder)
            (topology/filter-known)
            (topology/translate)
            (topology/correlate "PT10S-Store"))

    (with-open [driver (TopologyTestDriver. (.build builder) topology/config)]

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

(deftest branch-test-scott-base
  (let [builder (StreamsBuilder.)]
    (-> ^KStream (topology/stream builder)
        (topology/filter-known)
        (topology/branch)
        first
        (.to "output"))

    (with-open [driver (TopologyTestDriver. (.build builder) topology/config)]

      (send-messages driver test-messages)

      (is (= [{:time 1557125670799 :type "ENG" :name "NZ1" :long 166 :lat -78 :content ["two"]}
              {:time 1557125670824 :type "ENG" :name "NZ1" :long 166 :lat -78 :content ["two"]}]
             (read-output driver 2))))))

(deftest branch-test-rest-of-world
  (let [builder (StreamsBuilder.)]
    (-> ^KStream (topology/stream builder)
        (topology/filter-known)
        (topology/branch)
        second
        (.to "output"))

    (with-open [driver (TopologyTestDriver. (.build builder) topology/config)]

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