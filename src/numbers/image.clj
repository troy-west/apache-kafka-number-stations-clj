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
  [time type name long lat content]
  {:time    time
   :type    type
   :name    name
   :long    long
   :lat     lat
   :content content})

(defn scott-base
  [n]
  (map (fn [idx]
         (reading (+ (* idx 10000) 1557125670763) "ENG" "NZ-1" -78 166 (rand-int 3)))
       (range n)))

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
                   (conj ret
                         (reading time s-type s-name s-long s-lat e1)
                         (reading (+ time 25) s-type s-name s-long s-lat e2)
                         (reading (+ time 50) s-type s-name s-long s-lat e3))))
               []
               (vec station))))

(defn obsfuscate
  [image]
  (let [width    (width image)
        stations (partition width (pixels image))
        secret   (scott-base width)
        readings (sort-by :time (reduce into [] (map-indexed (partial fuzz secret) stations)))]
    (into (vec (interleave readings
                           [{:time (+ 1 (:time (first readings))) :type "UXX" :name "X-RAY" :elts "base"}
                            {:time (+ 1 (:time (second readings))) :type "UXX" :name "X-RAY" :elts "base"}]))
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