(ns numbers.serdes
  (:require [clojure.data.json :as json]
            [clojure.tools.logging :as log])
  (:import (org.apache.kafka.common.serialization Deserializer Serde Serializer))
  (:gen-class))

(deftype JsonSerializer []
  Serializer
  (configure [_ _ _])
  (serialize [_ _ data]
    (try
      (.getBytes (json/write-str data))
      (catch Exception ex
        (log/error ex "failed to serialize"))))
  (close [_]))

(deftype JsonDeserializer []
  Deserializer
  (configure [_ _ _])
  (deserialize [_ _ data]
    (try
      (json/read-str (String. data) :key-fn keyword)
      (catch Exception ex
        (log/error ex "failed to deserialize"))))
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