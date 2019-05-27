(ns numbers.serde-test
  (:require [clojure.test :refer :all]
            [numbers.serdes :as serdes])
  (:import (org.apache.kafka.common.serialization Serializer Deserializer)))

(deftest serialize

  (is (= "{\"time\":1557125670789,\"type\":\"GER\",\"name\":\"85\",\"long\":-92,\"lat\":-30,\"content\":[\"eins\",\"null\",\"sechs\"]}"
         (String.
          (.serialize ^Serializer (serdes/json-serializer)
                      "radio-logs"
                      {:time    1557125670789
                       :type    "GER"
                       :name    "85"
                       :long    -92
                       :lat     -30
                       :content ["eins" "null" "sechs"]})))))

(deftest deserialize

  (is (= {:time    1557125670789
          :type    "GER"
          :name    "85"
          :long    -92
          :lat     -30
          :content ["eins" "null" "sechs"]}
         (.deserialize ^Deserializer (serdes/json-deserializer)
                       "radio-logs"
                       (.getBytes "{\"time\":1557125670789,\"type\":\"GER\",\"name\":\"85\",\"long\":-92,\"lat\":-30,\"content\":[\"eins\",\"null\",\"sechs\"]}")))))