(ns number-stations.generator
  (:require [number-stations.topology :as topology]
            [number-stations.images :as images]
            [number-stations.translate :as translate])
  (:import java.util.Properties))

(def config (let [props (Properties.)]
              (.putAll props {"application.id"      "test-examples"
                              "bootstrap.servers"   "dummy:1234"
                              "default.key.serde"   "org.apache.kafka.common.serialization.Serdes$StringSerde"
                              "default.value.serde" "number_stations.generator.JsonSerde"})
              props))

;; number translation

(def rgb-window-duration (* 2 (.size topology/pt10s-window)))

(defn pad-to-three
  [components]
  (into (vec (repeat (- 3 (count components)) 0))
        components))

(defn generate-messages
  [image]
  (let [pixels                  (images/pixel-seq image)
        width                   (.getWidth image)
        rgb-window-row-duration (* 2 rgb-window-duration width)]
    (->> (map #(take 3 %) pixels)
         (partition width)
         (mapcat (fn [j row]
                   (let [station-name (str "E-" j)
                         row-time     (* j rgb-window-row-duration)]
                     (into [{:time row-time :name "E-123" :longitude 0 :latitude j :number-of-pixels (count row)}]
                           (mapcat (fn [i [r g b]]
                                     (let [time (+ row-time (* rgb-window-duration i))]
                                       [{:time time :name station-name :longitude 0 :latitude j :numbers (translate/translate-to-words {:name "E-123" :numbers (pad-to-three (mapv #(Long/parseLong (str %)) (str r)))})}
                                        {:time (+ time 2500) :name station-name :longitude 0 :latitude j :numbers (translate/translate-to-words {:name "E-123" :numbers (pad-to-three (mapv #(Long/parseLong (str %)) (str g)))})}
                                        {:time (+ time 5000) :name station-name :longitude 0 :latitude j :numbers (translate/translate-to-words {:name "E-123" :numbers (pad-to-three (mapv #(Long/parseLong (str %)) (str b)))})}]))
                                   (range)
                                   row))))
                 (range)))))
