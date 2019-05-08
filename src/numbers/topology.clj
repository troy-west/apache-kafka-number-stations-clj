(ns numbers.topology
  (:require [numbers.translate :as tx])
  (:import (org.apache.kafka.streams.processor TimestampExtractor)
           (org.apache.kafka.streams.kstream Consumed KStream Predicate ValueMapper Aggregator TimeWindows Initializer Materialized)
           (org.apache.kafka.streams StreamsBuilder)
           (java.util Properties)))

(def config
  (let [props (Properties.)]
    (.putAll props {"application.id"      "stream-default"
                    "bootstrap.servers"   "localhost:9092"
                    "default.key.serde"   "org.apache.kafka.common.serialization.Serdes$StringSerde"
                    "default.value.serde" "numbers.serdes.JsonSerde"})
    props))


(def extractor
  (reify TimestampExtractor
    (extract [_ record _]
      (:time (.value record)))))

(defn stream
  [^StreamsBuilder builder]
  (.stream builder "radio-logs" (Consumed/with ^TimestampExtractor extractor)))

(defn filter-known
  [^KStream events]
  (.filter events (reify Predicate
                    (test [_ _ v]
                      (tx/known? (:type v))))))

(defn translate
  [^KStream events]
  (.mapValues events (reify ValueMapper
                       (apply [_ v]
                         (-> (assoc v :number (tx/number (:type v) (:numbers v)))
                             (dissoc :numbers))))))

(defn correlate
  [^KStream events]
  (-> (.groupByKey events)
      (.windowedBy (TimeWindows/of 10000))
      (.aggregate (reify Initializer
                    (apply [_] []))
                  (reify Aggregator
                    (apply [_ _ v agg]
                      (conj agg v)))
                  (Materialized/as "PT10S-Store"))))