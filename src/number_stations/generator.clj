(ns number-stations.generator
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [java-time :as time]
            [clojure.string :as str])
  (:import java.awt.Color
           java.awt.image.BufferedImage
           java.time.Duration
           java.util.concurrent.TimeUnit
           java.util.Properties
           javax.imageio.ImageIO
           [org.apache.kafka.common.serialization Deserializer Serde Serializer]
           [org.apache.kafka.streams KeyValue StreamsBuilder]
           [org.apache.kafka.streams.kstream Aggregator Consumed ForeachAction Initializer KeyValueMapper KStream Merger Predicate SessionWindows TimeWindows UnlimitedWindows ValueMapper]
           org.apache.kafka.streams.processor.TimestampExtractor))

(defn topology
  ([input-topic stream-operations]
   (topology input-topic stream-operations nil))
  ([input-topic stream-operations output-topic]
   (let [extractor (reify TimestampExtractor
                     (extract [_ record _]
                       (:time (.value record))))]
     (topology input-topic stream-operations output-topic (Consumed/with extractor))))
  ([input-topic stream-operations output-topic consumed]
   (let [builder   (StreamsBuilder.)
         events    (if consumed
                     (.stream builder ^String input-topic consumed)
                     (.stream builder ^String input-topic))]

     (cond-> (stream-operations events)
       (seq output-topic)
       (.to output-topic))

     (.build builder))))

(def ^:dynamic *instrument* true)
(def ^:dynamic *instrument-stream-name* #{})

(defn instrument-stream
  [input-stream stream-name]
  (when (or *instrument*
            (get *instrument-stream-name* stream-name))
    (.foreach input-stream (reify ForeachAction
                             (apply [_ k v]
                               (println (format "%-40s => [key=%s, value=%s]" stream-name k v))))))
  input-stream)

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
        src-array    (int-array (map (fn [[r g b a]] (.getRGB (Color. r g b (or a 255)))) pixels))]
    (System/arraycopy src-array 0 dst-array 0 (alength src-array))
    buffered-img))

(defn write-output
  ([buffered-img]
   (write-output buffered-img nil))
  ([buffered-img filename]
   (ImageIO/write buffered-img
                  "png"
                  (io/file (or filename "resources/output.png")))))

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

(defn invert
  [number-lookup]
  (into {} (for [[letter words-numbers] number-lookup]
             [letter (into {} (for [[word number] words-numbers]
                                [number word]))])))

(def word-lookup
  (invert number-lookup))

(defn translate-to-numbers
  [{:keys [numbers name]}]
  (mapv #(get-in number-lookup [(first name) %]) numbers))

(defn translate-to-words
  [{:keys [numbers name]}]
  (mapv #(get-in word-lookup [(first name) %]) numbers))

(defn ints-to-colour-component
  [ints]
  (when (and (seq ints) (every? int? ints))
    (Integer/parseInt (apply str ints))))

(def pt10s-window (TimeWindows/of (Duration/ofSeconds 10)))

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
                              (+ (.getWidth bi) 0))))

(def small-image (ImageIO/read (io/resource "small.png")))
(def source-image (ImageIO/read (io/resource "source.png")))

(def rgb-window-duration (* 2 (.size pt10s-window)))

(defn pad-to-three
  [components]
  (into (vec (repeat (- 3 (count components)) 0))
        components))

(defn generate-messages
  [image]
  (let [pixels                  (pixel-seq image)
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
                                       [{:time time :name station-name :longitude 0 :latitude j :numbers (translate-to-words {:name "E-123" :numbers (pad-to-three (mapv #(Long/parseLong (str %)) (str r)))})}
                                        {:time time :name station-name :longitude 0 :latitude j :numbers (translate-to-words {:name "E-123" :numbers (pad-to-three (mapv #(Long/parseLong (str %)) (str g)))})}
                                        {:time time :name station-name :longitude 0 :latitude j :numbers (translate-to-words {:name "E-123" :numbers (pad-to-three (mapv #(Long/parseLong (str %)) (str b)))})}]))
                                   (range)
                                   row))))
                 (range)))))

#_(take 10 (generate-messages source-image))

(defn translate-numbers-stream-operations
  [^KStream stream]
  (-> stream
      (.filter (reify Predicate
                 (test [_ _ message]
                   (boolean (:numbers message)))))
      (instrument-stream :translate-numbers/filter1)
      (.mapValues (reify ValueMapper
                    (apply [_ message]
                      (-> message
                          (assoc :colour-component
                                 (ints-to-colour-component (translate-to-numbers message)))
                          (dissoc :numbers)))))
      (instrument-stream :translate-numbers/mapValues)
      (.filter (reify Predicate
                 (test [_ _ message]
                   (boolean (:colour-component message)))))
      (instrument-stream :translate-numbers/filter2)))

(defn correlate-rgb-stream-operations
  [^KStream stream]
  (-> (.groupByKey stream)
      (.windowedBy ^TimeWindows pt10s-window)
      (.aggregate (reify Initializer
                    (apply [_] []))
                  (reify Aggregator
                    (apply [_ _ message colour-components]
                      (conj colour-components message))))
      (.toStream)
      (instrument-stream :correlate-rgb/aggregate)
      (.filter (reify Predicate
                 (test [_ _ colour-components]
                   (= (count colour-components) 3))))
      (instrument-stream :correlate-rgb/filter)
      (.map (reify KeyValueMapper
              (apply [_ k v]
                (let [[leader :as components] (sort-by :time v)]
                  (KeyValue. (:name leader) (-> leader
                                                (assoc :rgb (mapv :colour-component components))
                                                (dissoc :colour-component)))))))
      (instrument-stream :correlate-rgb/map)))

(defn translate-numbers-topology
  [input-topic output-topic]
  (topology input-topic translate-numbers-stream-operations output-topic))

(defn correlate-rgb-topology
  [input-topic output-topic]
  (topology input-topic correlate-rgb-stream-operations output-topic))

(def pt1m-session-window (SessionWindows/with (.toMillis TimeUnit/MINUTES 1)))

(defn group-by-row-stream-operations
  "Stream key is :name of message (radio station name)"
  [^KStream stream]
  ;; TODO this is getting quite complicated, so don't make it more complex, but see if it can be simpler.
  ;; simpler in a meaningful way, which can be taught
  ;; identity "key is :name" constraint. Is this enforced? What happens if we key it incorrectly?
  (-> stream
      (instrument-stream :group-by-row/pre-aggregate)
      (.groupByKey)
      (.windowedBy ^SessionWindows pt1m-session-window)
      (.aggregate (reify Initializer
                    (apply [_] {:received-primer false
                                :expected-pixels 0
                                :pixels          []}))
                  (reify Aggregator
                    (apply [_ _ message agg]
                      (if (:number-of-pixels message)
                        (assoc agg
                               :expected-pixels (:number-of-pixels message)
                               :received-primer true)
                        (update agg :pixels conj message))))
                  (reify Merger
                    (apply [_ k v1 v2]
                      {:received-primer (or (:received-primer v1)
                                            (:received-primer v2))
                       :expected-pixels (+ (:expected-pixels v1)
                                           (:expected-pixels v2))
                       :pixels          (into (:pixels v1) (:pixels v2))})))
      (.toStream)
      (instrument-stream :group-by-row/aggregate)
      (.map (reify KeyValueMapper
              (apply [_ k v]
                (KeyValue. (:name (first v)) (update v :pixels #(sort-by :time %))))))
      (instrument-stream :group-by-row/map1)
      (.filter (reify Predicate
                 (test [_ k v]
                   (boolean (and v
                                 (:received-primer v)
                                 (< 0 (count (:pixels v)))
                                 ;; this requires exactly-once semantics to work.
                                 (= (:expected-pixels v)
                                    (count (:pixels v))))))))
      (instrument-stream :group-by-row/filter)
      (.map (reify KeyValueMapper
              (apply [_ k v]
                (let [{:keys [pixels]} v
                      leader           (first pixels)]
                  (KeyValue. (:name leader)
                             (-> leader
                                 (select-keys [:time :name :latitude :longitude])
                                 (assoc :pixels pixels)
                                 (dissoc :received-primer :expected-pixels)))))))
      (instrument-stream :group-by-row/map2)))

(defn group-by-row-topology
  [input-topic output-topic]
  (topology input-topic group-by-row-stream-operations output-topic))

(def unlimited-window (UnlimitedWindows/of))

(defn group-by-rows-stream-operations
  "Stream key is :name of message (radio station name)"
  [^KStream stream]
  ;; TODO this is getting quite complicated, so don't make it more complex, but see if it can be simpler.
  ;; simpler in a meaningful way, which can be taught
  ;; identity "key is :name" constraint. Is this enforced? What happens if we key it incorrectly?
  (-> (.groupBy stream ^KeyValueMapper (reify KeyValueMapper
                                         (apply [_ k v]
                                           "0")))
      (.windowedBy ^UnlimitedWindows unlimited-window)
      (.aggregate (reify Initializer
                    (apply [_] {}))
                  (reify Aggregator
                    (apply [_ _ message agg]
                      ;; NOTE: agg could already have been serialized!
                      (assoc agg (str (:latitude message)) message))))
      (.toStream)
      (.map (reify KeyValueMapper
              (apply [_ k rows]
                (let [rows (mapv second (sort-by (comp #(Long/parseLong %) name first) rows))]
                  (KeyValue. "0" {:time (:time (first rows))
                                  :rows rows})))))))

(defn group-by-rows-topology
  [input-topic output-topic]
  (topology input-topic group-by-rows-stream-operations output-topic))

(defn rows-to-image-stream-operations
  ([stream]
   (rows-to-image-stream-operations stream nil))
  ([stream filename]
   (.foreach stream (reify ForeachAction
                      (apply [_ _ rows]
                        (let [pixel-rows (->> rows
                                              :rows
                                              (map :pixels)
                                              (mapv (fn [pixels]
                                                      (mapv :rgb pixels))))
                              width      (apply max (map count pixel-rows))]
                          (-> (render-image (mapcat (fn [pixels]
                                                      ;; pad pixels to same length
                                                      (vec (concat pixels (repeat (- (count pixels) width) nil))))
                                                    pixel-rows)
                                            width)
                              (write-output filename))))))))

(defn rows-to-image-topology
  [input-topic]
  (topology input-topic rows-to-image-stream-operations))

(defn number-stations-to-image-stream
  [output-image-path ^KStream stream]
  (instrument-stream stream :number-stations)

  (let [[primer-stream number-stream] (.branch stream
                                               (into-array Predicate
                                                           [(reify Predicate
                                                              (test [_ k v]
                                                                (boolean (:number-of-pixels v))))
                                                            (reify Predicate
                                                              (test [_ k v]
                                                                (boolean (not (:number-of-pixels v)))))]))]

    (instrument-stream primer-stream :primer-stream)

    (let [correlated-rgb-stream (-> number-stream
                                    (instrument-stream :number-stream)
                                    translate-numbers-stream-operations
                                    (instrument-stream :translate-numbers)
                                    correlate-rgb-stream-operations
                                    (instrument-stream :correlate-rgb))]

      (-> (.merge primer-stream correlated-rgb-stream)
          (instrument-stream :merged/primer+rgb)
          group-by-row-stream-operations
          (instrument-stream :group-by-row)
          group-by-rows-stream-operations
          (instrument-stream :group-by-rows)
          (rows-to-image-stream-operations output-image-path)))))

(defn number-stations-to-image-topology
  [input-topic output-image-path]
  (topology input-topic (partial number-stations-to-image-stream output-image-path)))


;; TODO -group by special id of last pixel in row, this gives an image.
;; materialize to store via latitude -- overwrite
;; going back, insert dedup into topologies -- pixel repetitition
;; insert join into topologies -- (colour cycling) (join with colour ktable store)
