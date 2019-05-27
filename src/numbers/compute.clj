(ns numbers.compute
  (:require [numbers.translate :as tx]
            [numbers.radio :as radio])
  (:import (java.util Properties)
           (org.apache.kafka.streams.processor TimestampExtractor)
           (org.apache.kafka.streams.kstream Consumed KStream Predicate ValueMapper Aggregator TimeWindows Initializer Materialized KTable ValueJoiner)
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
     ;; TODO: implement me. Return the time value from each message rather than 0
      0
     )))

(defn stream
  [^StreamsBuilder builder]
  ;; TODO: Implement me. Create a new stream with a timestamp extractor (implemented above)

  )

(defn filter-known
  "Filter the input stream, keeping only tx/known? elements"
  [^KStream messages]
  ;; TODO: Implement me. streams.filter only messages where translator.known?(message) is true

  )

(defn branch-scott-base-and-row
  "Branch between messages above and below -75 latitude"
  [^KStream messages]
  ;; TODO: Implement me. Split the stream in two with streams.branch. All messages above -75 latitude, and those below (Scott Base)

  )

(defn translate
  "Translate the input stream, converting from text to numeric content"
  [^KStream messages]
  ;; TODO: Implement me. Translate content from text to numeric with streams.map or streams.mapValues

  )

(defn correlate
  "Correlate the input stream, grouping by station-id then windowing every 10s"
  [^KStream messages store-name]
  ;; TODO: Implement me. Group messages by key, then window by 10s tumbling time windows, then aggregate each window into a single message with a content tuple of three numbers
  ;; TODO: finally, make sure the k-table is materialized to "PT10S-Store"
  )

(defn topology
  []
  (let [builder (StreamsBuilder.)]
    (-> (stream builder)
        (filter-known)
        (translate)
        (correlate "PT10S-Store"))
    (.build builder)))

(defn start!
  []
  (let [^KafkaStreams streams (KafkaStreams. ^Topology (topology) (StreamsConfig. config))]
    (.start streams)
    streams))

(defn slice
  ([streams]
   (slice streams "PT10S-Store"))
  ([streams store-name]
   (slice streams store-name (radio/stations)))
  ([streams store-name stations]
   (slice streams store-name stations 1557125660763 1557135288803))
  ([streams store-name stations start end]
   (let [store (.store streams store-name (QueryableStoreTypes/windowStore))]
     (map #(.value %1)
          (reduce into
                  []
                  (map (fn [station]
                         (with-open [iter (.fetch ^ReadOnlyWindowStore store station ^long start ^long end)]
                           (doall (iterator-seq iter))))
                       stations))))))