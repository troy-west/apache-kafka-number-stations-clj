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

(defn fuzz
  [idx station]
  (let [start-at 1557125670763
        s-type   (get tx/types (mod idx 3))
        s-prec   (get tx/prefixes (mod idx 3))]
    (reduce into []
            (for [reading (reduce-kv (fn [ret i item]
                                       (conj ret {:time (+ start-at (* i 10000))
                                                  :type s-type
                                                  :name (str s-prec "-" idx)
                                                  :long (int (+ -135 (/ idx 2)))
                                                  :lat  (int (+ -45 (/ idx 4)))
                                                  :elts (map #(tx/words s-type %1) (take 3 item))}))
                                     []
                                     (vec station))]
              (let [new-time (+ (:time reading) (int (rand 8000)))]
                (map-indexed (fn [idx number]
                               (assoc reading :elts number :time (+ new-time (* idx 150))))
                             (:elts reading)))))))

(defn obsfuscate
  [image]
  (let [stations (partition (width image) (pixels image))
        readings (sort-by :time (reduce into [] (map-indexed fuzz stations)))]
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