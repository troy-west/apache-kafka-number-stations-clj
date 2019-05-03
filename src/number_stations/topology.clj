(ns number-stations.topology
  (:require [clojure.data.json :as json]
            [number-stations.translate :as translate])
  (:import java.time.Duration
           java.util.concurrent.TimeUnit
           [org.apache.kafka.common.serialization Deserializer Serde Serializer]
           [org.apache.kafka.streams.kstream Aggregator Initializer Materialized Predicate SessionWindows TimeWindows ValueMapper]))

(def pt1m-session-window (SessionWindows/with (.toMillis TimeUnit/MINUTES 1)))
(def pt10s-window (TimeWindows/of (Duration/ofSeconds 10)))
(def pt10s-store "components-pt10s-store")

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

(defn translate-numbers
  [stream]
  {:pre [stream]}
  (.mapValues stream (reify ValueMapper
                       (apply [_ message]
                         (-> (select-keys message [:time :name :latitude :longitude])
                             (assoc :colour-component
                                    (translate/ints-to-colour-component (translate/translate-to-numbers message))))))))

(defn correlate-rgb
  [stream]
  (-> (.groupByKey stream)
      (.windowedBy ^TimeWindows pt10s-window)
      (.aggregate (reify Initializer
                    (apply [_] []))
                  (reify Aggregator
                    (apply [_ _ message colour-components]
                      (conj colour-components message)))
                  (Materialized/as ^String pt10s-store))))
