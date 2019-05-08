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

(defn pixels
  [^BufferedImage img]
  (let [raster (.getData img)
        width  (.getWidth raster)
        height (.getHeight raster)]
    (partition 4 (.getPixels raster 0 0 width height (int-array (* width height 4))))))

(defn reading
  [time type name long lat value]
  {:time  time
   :type  type
   :name  name
   :long  long
   :lat   lat
   :value value})

(defn scott-base
  [n]
  (reduce into
          []
          (map (fn [idx]
                 (let [time  (+ (* idx 10000) 1557125670799)
                       value (rand-int 3)]
                   [(reading time "ENG" "NZ-1" -78 166 value)
                    (reading (+ 25 time) "ENG" "NZ-1" -78 166 value)
                    (reading (+ 50 time) "ENG" "NZ-1" -78 166 value)]))
               (range n))))

(defn fuzz
  [secret idx station]
  (let [start-at 1557125670763
        s-type   (get tx/types (mod idx 3))
        s-name   (str (get tx/prefixes (mod idx 3)) "-" idx)
        s-long   (int (+ -135 (/ idx 2)))
        s-lat    (int (+ -45 (/ idx 6)))]
    (reduce-kv (fn [ret i item]
                 (let [time (+ start-at (* i 10000) (int (rand 8000)))
                       [e1 e2 e3] (map #(tx/words s-type %1) (take 3 item))]
                   (case (:value (nth secret (* 3 idx)))
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
                                                             (update reading :value #(tx/words "ENG" %1))) secret))))]
    (into (vec (interleave readings
                           [{:time (+ 1 (:time (first readings))) :type "UXX" :name "X-RAY"}
                            {:time (+ 1 (:time (second readings))) :type "UXX" :name "X-RAY"}]))
          (drop 2 readings))))                              ;; remove drop 2 for duplicates

(defn render
  [pixels]
  (let [img       (BufferedImage. 960 540 BufferedImage/TYPE_INT_ARGB)
        dst-array (.getData (.getDataBuffer (.getRaster img)))
        src-array (int-array (map (fn [[r g b]]
                                    (.getRGB (Color. r g b 255)))
                                  pixels))]
    (System/arraycopy src-array 0 dst-array 0 (alength src-array))
    img))

(defn persist
  [img rand-part]
  (ImageIO/write ^RenderedImage img "png" (io/file (str "generated-img" rand-part ".png"))))