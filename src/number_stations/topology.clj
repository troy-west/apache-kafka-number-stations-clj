(ns number-stations.topology
  (:require [clojure.data.json :as json]
            [number-stations.translate :as translate])
  (:import java.time.Duration
           java.util.Properties
           [org.apache.kafka.common.serialization Deserializer Serde Serializer]
           [org.apache.kafka.streams.kstream Aggregator Initializer Materialized TimeWindows ValueMapper]
           org.apache.kafka.streams.processor.TimestampExtractor))

(def pt10s-window (TimeWindows/of (Duration/ofSeconds 10)))
(def pt10s-store "components-pt10s-store")

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

(defn denoise
  [stream])

(defn deduplicate
  [stream])

(defn translate
  [stream]
  {:pre [stream]}
  (.mapValues stream (reify ValueMapper
                       (apply [_ message]
                         (-> (select-keys message [:time :name :latitude :longitude])
                             (assoc :colour-component
                                    (translate/ints-to-colour-component (translate/translate-to-numbers message))))))))

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
