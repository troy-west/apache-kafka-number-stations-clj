(ns numbers.kafka
  (:require [numbers.serdes :as serdes])
  (:import (org.apache.kafka.clients.producer KafkaProducer)
           (org.apache.kafka.common.serialization StringSerializer Serializer)
           (java.util Map)))

(defn producer
  []
  (KafkaProducer. ^Map {"bootstrap.servers" "localhost:9092"}
                  ^Serializer (StringSerializer.)
                  ^Serializer (serdes/json-serializer)))