(ns number-stations.topology
  (:require [clojure.data.json :as json]
            [number-stations.translate :as translate])
  (:import java.time.Duration
           java.util.Properties
           [org.apache.kafka.common.serialization Deserializer Serde Serdes Serializer]
           [org.apache.kafka.streams KeyValue StreamsBuilder]
           [org.apache.kafka.streams.kstream Aggregator Consumed Initializer Materialized Predicate TimeWindows Transformer TransformerSupplier ValueMapper]
           org.apache.kafka.streams.processor.TimestampExtractor
           org.apache.kafka.streams.state.Stores))

(def pt10s-window (TimeWindows/of (Duration/ofSeconds 10)))
(def pt10s-store "components-pt10s-store")

(def deduplicate-store "deduplicate-store")

(def extractor
  (reify TimestampExtractor
    (extract [_ record _]
      (:time (.value record)))))

(deftype JsonSerializer []
  Serializer
  (configure [_ _ _])
  (serialize [_ _ data] (.getBytes (json/write-str data)))
  (close [_]))

(deftype JsonDeserializer []
  Deserializer
  (configure [_ _ _])
  (deserialize [_ _ data] (json/read-str (String. data) :key-fn keyword))
  (close [_]))

(deftype JsonSerde []
  Serde
  (configure [_ _ _])
  (close [_])
  (serializer [_] (JsonSerializer.))
  (deserializer [_] (JsonDeserializer.)))

(def default-config
  {"application.id"      "stream-default"
   "bootstrap.servers"   "localhost:9092"
   "default.key.serde"   "org.apache.kafka.common.serialization.Serdes$StringSerde"
   "default.value.serde" "number_stations.topology.JsonSerde"})

(defn config
  ([]
   (config default-config))
  ([props]
   (doto (Properties.)
     (.putAll (merge default-config props)))))

(defn translate
  [stream]
  (.mapValues stream (reify ValueMapper
                       (apply [_ message]
                         (-> (select-keys message [:time :name :latitude :longitude])
                             (assoc :number
                                    (translate/to-numbers message)))))))

(defn denoise
  [stream]
  (.filter stream (reify Predicate
                    (test [_ _ v]
                      (boolean (:number v))))))

(defn correlate
  [stream]
  (-> (.groupByKey stream)
      (.windowedBy ^TimeWindows pt10s-window)
      (.aggregate (reify Initializer
                    (apply [_] []))
                  (reify Aggregator
                    (apply [_ _ message messages]
                      (conj messages message)))
                  (Materialized/as ^String pt10s-store))))

(defn unique-id
  [message]
  (str (:time message) "-" (:name message)))

(defn non-duplicate
  [state-store timestamp window-size k v]
  (let [id          (unique-id v)
        duplicate?  (with-open [result (.fetch state-store
                                               id
                                               (- timestamp window-size)
                                               (+ timestamp window-size))]
                      (.hasNext result))]
    (.put state-store id timestamp timestamp)
    (when-not duplicate?
      (KeyValue. k v))))

(defn deduplicate
  [stream builder]
  stream
  (let [retention-period   (Duration/ofMinutes 10)
        window-size        (Duration/ofMinutes 10)
        retain-duplicates? false]
    (.addStateStore builder (-> (Stores/persistentWindowStore deduplicate-store retention-period window-size retain-duplicates?)
                                (Stores/windowStoreBuilder (Serdes/String) (Serdes/Long))
                                (.withCachingEnabled)))

    (.transform stream
                (reify TransformerSupplier
                  (get [_]
                    (let [ctx (atom nil)]
                      (reify Transformer
                        (init [_ context]
                          (reset! ctx context))
                        (transform [_ k v]
                          (non-duplicate (.getStateStore @ctx deduplicate-store)
                                         (.toMillis window-size)
                                         (.timestamp @ctx)
                                         k
                                         v))
                        (close [_])))))
                (into-array String [deduplicate-store]))))

(defn combined
  [stream builder]
  (some-> stream
          translate
          denoise
          (deduplicate builder)
          correlate))
