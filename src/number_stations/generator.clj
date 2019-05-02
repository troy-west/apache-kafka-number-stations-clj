(ns number-stations.generator
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [java-time :as time])
  (:import java.awt.Color
           java.awt.image.BufferedImage
           java.time.Duration
           java.util.concurrent.TimeUnit
           java.util.Properties
           javax.imageio.ImageIO
           [org.apache.kafka.common.serialization Deserializer Serde Serializer]
           [org.apache.kafka.streams KeyValue StreamsBuilder]
           [org.apache.kafka.streams.kstream Aggregator Consumed Initializer KeyValueMapper KStream Merger Predicate SessionWindows TimeWindows ValueMapper]
           org.apache.kafka.streams.processor.TimestampExtractor))

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

(def pt1m-session-window (SessionWindows/with (.toMillis TimeUnit/MINUTES 1)))

(defn group-by-session
  [^KStream stream output-topic]
  ;; TODO this is getting quite complicated, so don't make it more complex, but see if it can be simpler.
  (-> (.groupByKey stream)
      (.windowedBy ^SessionWindows pt1m-session-window)
      (.aggregate (reify Initializer
                    (apply [_] {:received-primer   false
                                :expected-messages 0
                                :pixels            []}))
                  (reify Aggregator
                    (apply [_ _ message agg]
                      (if (:number-of-messages message)
                        (assoc agg
                               :expected-messages (:number-of-messages message)
                               :received-primer true)
                        (update agg :pixels conj message))))
                  (reify Merger
                    (apply [_ k v1 v2]
                      {:received-primer   (or (:received-primer v1)
                                              (:received-primer v2))
                       :expected-messages (+ (:expected-messages v1)
                                             (:expected-messages v2))
                       :pixels            (into (:pixels v1) (:pixels v2))})))
      (.toStream)
      (.map (reify KeyValueMapper
              (apply [_ k v]
                (KeyValue. (:name (first v)) (update v :pixels #(sort-by :time %))))))
      (.filter (reify Predicate
                 (test [_ k v]
                   (boolean (and v
                                 (:received-primer v)
                                 (< 0 (count (:pixels v)))
                                 (= (:expected-messages v)
                                    (count (:pixels v))))))))
      (.map (reify KeyValueMapper
              (apply [_ k v]
                (let [{:keys [pixels]} v
                      leader           (first pixels)]
                  (KeyValue. (:name leader)
                             (assoc (select-keys leader [:time :name :latitude :longitude])
                                    :pixels pixels))))))
      (.to output-topic)))

;; data stereotypes -- must be JSON encoded when going onto kafka,
;; so use primitive types which are serializable

;; TOPIC "number-stations"
(def number-station-message-numbers-stereotype
  {:time       0         ;; epoch timestamp (long)
   :name       "E-123"   ;; radio station name
   :session-id "xyz-123" ;; session-id (but randomized -- only last pixel in row has a valid session id)
   :latitude   37
   :longitude  144
   :numbers    ["one" "two" "three"]})

;; TOPIC "number-stations"
(def number-station-message-primer-stereotype
  {:time               0 ;; epoch timestamp (long)
   :name               "E-123" ;; radio station name
   :latitude           37
   :longitude          144
   :number-of-messages 100 ;; number of pixels
   })

;; TOPIC "translate-numbers"
(def translated-numbers-message-stereotype
  {:time             0
   :name             "E-123" ;; radio station name
   :latitude         37
   :longitude        144
   :colour-component 123})

;; TOPIC "rgb-stream"
(def rgb-stereotype
  {:time      0
   :name      "E-123"
   :latitude  37
   :longitude 144
   :rgb       [123 0 0]})

;; TOPIC "rgb-row-stream"
(def rgb-row-stereotype
  {:time      0
   :name      "E-123"
   :latitude  37
   :longitude 144
   :pixels    [{:time      0
                :name      "E-123"
                :latitude  37
                :longitude 144
                :rgb       [123 0 0]}]})

(defn group-by-session-topology
  [input-topic output-topic]
  (let [builder   (StreamsBuilder.)
        extractor (reify TimestampExtractor
                    (extract [_ record _]
                      (:time (.value record))))
        events    (.stream builder ^String input-topic (Consumed/with extractor))]
    (group-by-session events output-topic)
    (.build builder)))

;; TODO -group by special id of last pixel in row, this gives an image.
;; materialize to store via latitude -- overwrite
;; going back, insert dedup into topologies -- pixel repetitition
;; insert join into topologies -- (colour cycling) (join with colour ktable store)
