(ns numbers.image
  (:require [clojure.java.io :as io]
            [numbers.translate :as tx])
  (:import (javax.imageio ImageIO)
           (java.awt.image BufferedImage RenderedImage)
           (java.awt Color)))

(defonce source
  (ImageIO/read (io/resource "source.png")))                ;; Width 960, Height 540.

(defn width
  [^BufferedImage img]
  (.getWidth (.getData img)))

(defn height
  [^BufferedImage img]
  (.getHeight (.getData img)))

(defn pixels
  [^BufferedImage img]
  (let [raster (.getData img)
        width  (.getWidth raster)
        height (.getHeight raster)]
    (partition 4 (.getPixels raster 0 0 width height (int-array (* width height 4))))))

(defn reading
  [time type name long lat content]
  {:time    time
   :type    type
   :name    name
   :long    long
   :lat     lat
   :content content})

(defn scott-base
  [n]
  (reduce into
          []
          (map (fn [idx]
                 (let [time    (+ (* idx 10000) 1557125670799)
                       content (rand-int 3)]
                   [(reading time "ENG" "NZ1" 166 -78 content)
                    (reading (+ 25 time) "ENG" "NZ1" 166 -78 content)
                    (reading (+ 50 time) "ENG" "NZ1" 166 -78 content)]))
               (range n))))

(defn fuzz
  [secret idx station]
  (let [start-at 1557125670763
        s-type   (get tx/types (mod idx 3))
        s-name   (format "%03d" idx)
        s-long   (int (+ -135 (/ idx 2)))
        s-lat    (int (+ -45 (/ idx 6)))]
    (reduce-kv (fn [ret i item]
                 (let [time (+ start-at (* i 10000) (int (rand 8000)))
                       [e1 e2 e3] (map #(tx/words s-type %1) (take 3 item))]
                   (case (:content (nth secret (* 3 idx)))
                     0 (conj ret
                             (reading time s-type s-name s-long s-lat e1)
                             (reading (+ time 25) s-type s-name s-long s-lat e2)
                             (reading (+ time 50) s-type s-name s-long s-lat e3))
                     1 (conj ret
                             (reading time s-type s-name s-long s-lat e2)
                             (reading (+ time 25) s-type s-name s-long s-lat e3)
                             (reading (+ time 50) s-type s-name s-long s-lat e1))
                     2 (conj ret
                             (reading time s-type s-name s-long s-lat e3)
                             (reading (+ time 25) s-type s-name s-long s-lat e1)
                             (reading (+ time 50) s-type s-name s-long s-lat e2)))))
               []
               (vec station))))

(defn obsfuscate
  [image]
  (let [width    (width image)
        stations (partition width (pixels image))
        secret   (scott-base width)
        readings (sort-by :time (reduce into [] (conj (map-indexed (partial fuzz secret) stations)
                                                      (map (fn [reading]
                                                             (update reading :content #(tx/words "ENG" %1))) secret))))]
    (into (vec (interleave readings
                           [{:time (+ 1 (:time (first readings))) :type "UXX" :name "X-RAY"}
                            {:time (+ 1 (:time (second readings))) :type "UXX" :name "X-RAY"}]))
          (drop 2 readings))))                              ;; remove drop 2 for duplicates

(defn render
  [pixels]
  (let [img       (BufferedImage. 960 540 BufferedImage/TYPE_INT_ARGB)
        dst-array (.getData (.getDataBuffer (.getRaster img)))
        src-array (int-array (map (fn [[r g b]]
                                    (.getRGB (Color. ^int (or r 0) ^int (or g 0) ^int (or b 0) 255)))
                                  pixels))]
    (System/arraycopy src-array 0 dst-array 0 (alength src-array))
    img))

(defn persist
  ([img]
   (ImageIO/write ^RenderedImage img "png" (io/file (str "resources/public/generated.png"))))
  ([img rand-part]
   (ImageIO/write ^RenderedImage img "png" (io/file (str (format "resources/public/generated-%s.png" rand-part))))))

(defn roundtrip
  []
  (->> (obsfuscate source)
       (filter #(not= "UXX" (:type %1)))
       (filter #(not= "001-NZ" (:name %1)))
       (map tx/translate)
       (group-by :name)
       (vals)
       (reduce into [])
       (sort-by (juxt :name :time))
       (partition 3)
       (map #(reduce (fn [ret v] (into ret (:content v))) [] %1))
       (render)
       (persist)))