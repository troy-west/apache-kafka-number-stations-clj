(ns number-stations.topology
  (:require [clojure.data.json :as json]
            [number-stations.translate :as translate])
  (:import java.time.Duration
           java.util.Properties
           [org.apache.kafka.common.serialization Deserializer Serde Serdes Serializer]
           org.apache.kafka.streams.KeyValue
           [org.apache.kafka.streams.kstream Aggregator Initializer Materialized Predicate TimeWindows TransformerSupplier ValueMapper]
           org.apache.kafka.streams.processor.TimestampExtractor
           org.apache.kafka.streams.state.Stores))

(def pt10s-window (TimeWindows/of (Duration/ofSeconds 10)))
(def pt10s-store "components-pt10s-store")
(def pt1m-window (TimeWindows/of (Duration/ofMinutes 1)))
(def pt1m-store "components-pt1m-store")
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

(defn config
  [{:keys [bootstrap-servers application-id] :or {bootstrap-servers "localhost:9092"}}]
  ;; force JsonSerde it to exist
  (compile 'number-stations.topology)
  (doto (Properties.)
    (.putAll {"application.id"      application-id
              "bootstrap.servers"   bootstrap-servers
              "default.key.serde"   "org.apache.kafka.common.serialization.Serdes$StringSerde"
              "default.value.serde" "number_stations.topology.JsonSerde"})))

(defn translate
  [stream]
  (.mapValues stream (reify ValueMapper
                       (apply [_ message]
                         (-> (select-keys message [:time :name :latitude :longitude])
                             (assoc :translated
                                    (translate/ints-to-colour-component (translate/translate-to-numbers message))))))))

(defn denoise
  [stream]
  (.filter stream (reify Predicate
                    (test [_ _ v]
                      (boolean (:translated v))))))

(defn correlate
  [stream]
  (-> (.groupByKey stream)
      (.windowedBy ^TimeWindows pt10s-window)
      (.aggregate (reify Initializer
                    (apply [_] []))
                  (reify Aggregator
                    (apply [_ _ message colour-components]
                      (conj colour-components message)))
                  (Materialized/as ^String pt10s-store))))

(defn fetch
  [store metric-name start end]
  (with-open [iterator (.fetch store metric-name start end)]
    (doall (map #(.value %) (iterator-seq iterator)))))

(defn fetch-all
  [store start end]
  (with-open [iterator (.fetchAll store start end)]
    (doall (map #(.value %) (iterator-seq iterator)))))

(defn unique-id
  [message]
  (str (:time message) "-" (:name message)))

(defn non-duplicate
  [ctx window-size k v]
  (let [id          (unique-id v)
        timestamp   (.timestamp @ctx)
        state-store (.getStateStore @ctx deduplicate-store)
        duplicate?  (with-open [result (.fetch state-store
                                               id
                                               (- timestamp (.toMillis window-size))
                                               (+ timestamp (.toMillis window-size)))]
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
                          (non-duplicate ctx window-size k v))
                        (close [_])))))
                (into-array String [deduplicate-store]))))
