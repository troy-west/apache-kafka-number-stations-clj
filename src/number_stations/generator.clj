(ns number-stations.generator
  (:require [clojure.java.io :as io]
            [clojure.data.json :as json]
            [clojure.test :refer :all]
            [java-time :as time])
  (:import (javax.imageio ImageIO)
           (java.awt.image BufferedImage)
           (java.awt Color)

           (java.util Date Properties)
           (org.apache.kafka.streams TopologyTestDriver StreamsBuilder)
           (org.apache.kafka.streams.kstream Predicate TimeWindows Windowed KeyValueMapper Initializer Aggregator Materialized ValueMapper Initializer Aggregator Materialized TimeWindows KStream Consumed)
           (org.apache.kafka.streams.test ConsumerRecordFactory)
           (org.apache.kafka.clients.producer ProducerRecord)
           (org.apache.kafka.common.serialization StringDeserializer StringSerializer Serializer Deserializer Serde)

           (java.time Duration)
           (org.apache.kafka.streams StreamsBuilder KafkaStreams StreamsConfig Topology KeyValue)
           (org.apache.kafka.streams.state QueryableStoreTypes ReadOnlyWindowStore)
           (org.apache.kafka.streams.processor TimestampExtractor)
           ))

(defn pixel-seq [buffered-img]
  (let [raster (.getData buffered-img)
        width  (.getWidth raster)
        height (.getHeight raster)]
    (partition 4 (.getPixels raster 0 0 width height (int-array (* width height 4))))))

(defn render-image [pixels width]
  (let [height       (int (Math/ceil (/ (count pixels) width)))
        buffered-img (BufferedImage. width height BufferedImage/TYPE_INT_ARGB)
        dst-array    (-> buffered-img
                         .getRaster
                         .getDataBuffer
                         .getData)
        src-array    (int-array (map (fn [[r g b a]] (.getRGB (Color. r g b a))) pixels))]
    (System/arraycopy src-array 0 dst-array 0 (alength src-array))
    buffered-img))

(defn write-output [buffered-img]
  (ImageIO/write buffered-img
                 "png"
                 (io/file "resources/output.png")))

;; let's add some context to the pixels

(defn rand-str [len]
  (apply str (take len (repeatedly #(char (+ (rand 26) 65))))))

(def station-names (repeatedly 50 #(str (rand-nth [\E \G \M]) "-" (rand-str 5))))

(def time-stamps (iterate #(time/plus % (time/minutes 1)) (time/instant)))

(defn enrich-stream [pixels]
  (map (fn [pixel time-stamp]
         {:time    time-stamp
          :numbers pixel
          :name    (rand-nth station-names)})
       pixels
      time-stamps))

(def station-cycle-index {\E 1
                          \G 2
                          \M 3})

(defn cycle-numbers [stream]
  (map (fn [message]
         (update message :numbers
                 (fn [numbers]
                   (take 4
                         (drop (get station-cycle-index (first (:name message)))
                               (cycle numbers))))))
       stream))



(defn add-duplication [prob stream]
  (let [[s1 s2] (partition-all (/ (count stream) 2) stream)]
    (concat
     s1
     (mapcat (fn [message]
               (if (< (rand) prob)
                 (repeat 2 message)
                 [message]))
             s2))))

(comment

  (def bi (ImageIO/read (io/resource "source.png")))

  ;; a seq of pixel tuples [r g b a]
  (def pixels (pixel-seq bi))


  (.getWidth bi)
  (.getHeight bi)


  ;; write the pixels back exactly as we got them
  (write-output (render-image pixels (.getWidth bi)))

  ;; if we don't know the exact width the image will be skewed
  (write-output (render-image pixels (+ (.getWidth bi) 5)))

  (write-output (render-image (->> pixels
                                   enrich-stream
                                   cycle-numbers
                                   (map :numbers))
                              (+ (.getWidth bi) 0)))

  (write-output (render-image (->> pixels
                                   enrich-stream
                                   (add-duplication 0.025)
                                   dedupe
                                   (map :numbers))
                              (+ (.getWidth bi) 0)))

  (write-output (render-image (->> pixels
                                   enrich-stream
                                   (add-duplication 0.01)
                                   (map :numbers))
                              (+ (.getWidth bi) 0)))


  )

(deftype JsonSerializer []
  Serializer
  (configure [_ _ _])
  (serialize [_ _ data] (.getBytes (json/write-str data)))
  (close [_]))

(deftype JsonDeserializer []
  Deserializer
  (configure [_ _ _])
  (deserialize [_ _ data] (json/read-str (String. data)
                                         :key-fn keyword))
  (close [_]))

(deftype JsonSerde []
  Serde
  (configure [_ _ _])
  (close [_])
  (serializer [_] (JsonSerializer.))
  (deserializer [_] (JsonDeserializer.)))

(def config (let [props (Properties.)]
              (.putAll props {"application.id"      "test-examples"
                              "bootstrap.servers"   "dummy:1234"
                              "default.key.serde"   "org.apache.kafka.common.serialization.Serdes$StringSerde"
                              "default.value.serde" "number_stations.generator.JsonSerde"})
              props))

;; number translation

(defn index-numbers [numbers]
  (zipmap numbers (range)))

(def number-lookup {\E (index-numbers ["zero" "one" "two" "three" "four" "five" "six" "seven" "eight" "nine"])
                    \G (index-numbers ["null" "eins" "zwei" "drei" "vier" "fünf" "sechs" "sieben" "acht" "neun"])

                    \M (index-numbers ["-----" ".----" "..---" "...--" "....-" "....." "-...." "--..." "---.." "----."])})

(defn translate-numbers
  [{:keys [numbers name]}]
  (mapv #(get-in number-lookup [(first name) %]) numbers))

(defn ints-to-colour-component
  [ints]
  (when (and (seq ints) (every? int? ints))
    (Integer/parseInt (apply str ints))))





(def pt10s-window (TimeWindows/of (Duration/ofSeconds 10)))

(defn correlate-rgb
  [^KStream stream output-topic]
  (-> (.groupByKey stream)
      (.windowedBy ^TimeWindows pt10s-window)
      (.aggregate (reify Initializer
                    (apply [_] []))
                  (reify Aggregator
                    (apply [_ _ message colour-components]
                      (conj colour-components message))))
      (.toStream)
      (.filter (reify Predicate
                 (test [_ _ colour-components]
                   (= (count colour-components) 3))))
      (.map (reify KeyValueMapper
              (apply [_ k v]
                (KeyValue. (:name (first v)) (sort-by :time v)))))
      (.to output-topic)))

(defn translate-numbers-topology
  [input-topic output-topic]
  (let [builder (StreamsBuilder.)]
    (-> (.stream builder input-topic)
        (.mapValues (reify ValueMapper
                      (apply [_ message]
                        (assoc message
                               :colour-component
                               (-> message
                                   translate-numbers
                                   ints-to-colour-component)))))
        (.filter (reify Predicate
                   (test [_ _ message]
                     (boolean (:colour-component message)))))
        (.to output-topic))
    (.build builder)))

(defn correlate-rgb-topology
  [input-topic output-topic]
  (let [builder   (StreamsBuilder.)
        extractor (reify TimestampExtractor
                    (extract [_ record _]
                      (:time (.value record))))
        events    (.stream builder ^String input-topic (Consumed/with extractor))]
    (correlate-rgb events output-topic)
    (.build builder)))
