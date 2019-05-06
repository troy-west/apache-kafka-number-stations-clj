(ns numbers.serdes
  (:require [clojure.data.json :as json])
  (:import [org.apache.kafka.common.serialization Deserializer Serde Serializer]))

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

(defn json-serializer
  []
  (->JsonSerializer))

(defn json-deserializer
  []
  (->JsonDeserializer))

(defn json-serde
  []
  (->JsonSerde))