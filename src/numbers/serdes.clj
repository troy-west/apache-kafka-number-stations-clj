(ns numbers.serdes
  (:require [clojure.data.json :as json]
            [clojure.tools.logging :as log])
  (:import (org.apache.kafka.common.serialization Deserializer Serde Serializer))
  (:gen-class))

(deftype JsonSerializer []
  Serializer
  (configure [_ _ _])
  (serialize [_ _ data]
   ;; TODO: Implement me. What's the preferred action when a serialization exception occurs, and why?
   )
  (close [_]))

(deftype JsonDeserializer []
  Deserializer
  (configure [_ _ _])
  (deserialize [_ _ data]
   ;; TODO: Implement me. What's the preferred action when a serialization exception occurs, and why?
   )
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