(ns numbers.radio
  (:require [numbers.image :as image]
            [numbers.kafka :as kafka])
  (:import (org.apache.kafka.clients.producer ProducerRecord KafkaProducer)))

(defn listen
  []
  (image/obsfuscate image/source))

(defn sample
  []
  (take 20 (listen)))

(defn produce
  "Send the radio burst to the radio-logs topic on Kafka"
  []
  ;; implement me!
  (let [^KafkaProducer producer (kafka/producer)]
    (doseq [message (listen)]
      (.send producer (ProducerRecord. "radio-logs" (:name message) message)))))