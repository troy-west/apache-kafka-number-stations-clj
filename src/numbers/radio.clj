(ns numbers.radio
  (:require [numbers.image :as image]
            [numbers.kafka :as kafka])
  (:import (org.apache.kafka.clients.producer ProducerRecord KafkaProducer)))

(defn listen
  []
  (sort-by :time (reduce into [] (image/obsfuscate))))

(defn explore
  []
  (take 20 (listen)))

(defn produce
  "Send the radio burst to the radio-logs topic on Kafka"
  []
  ;; implement me!
  )