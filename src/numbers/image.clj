(ns numbers.image
  (:require [clojure.java.io :as io]
            [numbers.translate :as tx])
  (:import (javax.imageio ImageIO)
           (java.awt.image BufferedImage RenderedImage)
           (java.awt Color)))



;; Width 960, Height 540.
(defonce source
  (ImageIO/read (io/resource "source.png")))

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
                                       (conj ret {:time      (+ start-at (* i 10000))
                                                  :type      s-type
                                                  :name      (str s-prec "-" idx)
                                                  ;;     :longitude (int (+ 45 (/ idx 2)))
                                                  ;;     :latitude  (int (- 315 (/ idx 2)))
                                                  :numbers   (map #(tx/words s-type %1) (take 3 item))}))
                                     []
                                     (vec station))]
              (let [new-time (+ (:time reading) (int (rand 8000)))]
                (map-indexed (fn [idx number]
                               (assoc reading :numbers number :time (+ new-time (* idx 150))))
                             (:numbers reading)))))))

(defn obsfuscate
  []
  (let [data      (pixels source)
        stations  (partition 960 data)]
    (map-indexed fuzz stations)))

(defn render
  [pixels width]
  (let [img       (BufferedImage. width 540 BufferedImage/TYPE_INT_ARGB)
        dst-array (.getData (.getDataBuffer (.getRaster img)))
        src-array (int-array (map (fn [[r g b]]
                                    (.getRGB (Color. r g b 255)))
                                  pixels))]
    (System/arraycopy src-array 0 dst-array 0 (alength src-array))
    img))

(defn persist
  [img rand-part]
  (ImageIO/write ^RenderedImage img "png" (io/file (str "generated-img" rand-file-part ".png"))))