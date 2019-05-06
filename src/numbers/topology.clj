(ns numbers.topology
  (:import (org.apache.kafka.streams.processor TimestampExtractor)
           (org.apache.kafka.streams.kstream Consumed)
           (org.apache.kafka.streams StreamsBuilder)
           (java.util Properties)))

(def config
  (let [props (Properties.)]
    (.putAll props {"application.id"      "stream-default"
                    "bootstrap.servers"   "localhost:9092"
                    "default.key.serde"   "org.apache.kafka.common.serialization.Serdes$StringSerde"
                    "default.value.serde" "numbers.serdes.JsonSerde"})
    props))

;; TODO implement me first
(def extractor
  (reify TimestampExtractor
    (extract [record k v])))

(defn stream
  [^StreamsBuilder builder]
  (.stream builder "radio-logs" (Consumed/with ^TimestampExtractor extractor)))

;; TODO implement me to get the tests running
(defn filter-recognized
  [events])

(defn translate
  [events])

(defn correlate
  [events])

(defn deduplicate
  [builder events])