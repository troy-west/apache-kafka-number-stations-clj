(ns numbers.compute
  (:require [numbers.translate :as tx]
            [numbers.radio :as radio])
  (:import (java.util Properties)
           (org.apache.kafka.streams.processor TimestampExtractor)
           (org.apache.kafka.streams.kstream Consumed KStream Predicate ValueMapper Aggregator TimeWindows Initializer Materialized)
           (org.apache.kafka.streams StreamsBuilder StreamsConfig Topology KafkaStreams)
           (org.apache.kafka.streams.state QueryableStoreTypes ReadOnlyWindowStore)))

(def config
  (let [props (Properties.)]
    (.putAll props {"application.id"      "compute-radio-logs"
                    "bootstrap.servers"   "localhost:9092"
                    "default.key.serde"   "org.apache.kafka.common.serialization.Serdes$StringSerde"
                    "default.value.serde" "numbers.serdes.JsonSerde"})
    props))

(def ^TimestampExtractor extractor
  (reify TimestampExtractor
    (extract [_ record _]
      (:time (.value record)))))

(defn stream
  [^StreamsBuilder builder]
  (.stream builder "radio-logs" (Consumed/with ^TimestampExtractor extractor)))

(defn filter-known
  "Filter the input stream, keeping only tx/known? elements"
  [^KStream events]
  (.filter events (reify Predicate
                    (test [_ _ message]
                      (tx/known? message)))))

(defn translate
  "Translate the input stream, converting from text to numeric content"
  [^KStream events]
  (.mapValues events (reify ValueMapper
                       (apply [_ message]
                         (tx/translate message)))))

(defn correlate
  "Correlate the input stream, grouping by station-id then windowing every 10s"
  [^KStream events]
  (-> (.groupByKey events)
      (.windowedBy (TimeWindows/of 10000))
      (.aggregate (reify Initializer
                    (apply [_] nil))
                  (reify Aggregator
                    (apply [_ _ v agg]
                      (if (not agg)
                        v
                        (update agg :content conj (first (:content v))))))
                  (Materialized/as "PT10S-Store"))))

(defn topology
  []
  (let [builder (StreamsBuilder.)]
    (-> (stream builder)
        (filter-known)
        (translate)
        (correlate))
    (.build builder)))

(defn start!
  []
  (let [^KafkaStreams streams (KafkaStreams. ^Topology (topology) (StreamsConfig. config))]
    (.start streams)
    streams))

(defn store
  [streams]
  (.store streams "PT10S-Store" (QueryableStoreTypes/windowStore)))

(defn slice
  ([streams]
   (slice streams (radio/stations)))
  ([streams stations]
   (slice streams stations 1557125670763 1557135278803))
  ([streams stations ^long start ^long end]
   (let [store (.store streams "PT10S-Store" (QueryableStoreTypes/windowStore))]
     (reduce into [] (map (fn [station]
                            (iterator-seq (.fetch ^ReadOnlyWindowStore store station start end)))
                          stations)))))