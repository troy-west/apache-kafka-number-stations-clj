(ns number-stations.images
  (:require [clojure.java.io :as io])
  (:import java.awt.Color
           java.awt.image.BufferedImage
           javax.imageio.ImageIO
           org.apache.kafka.streams.state.ReadOnlyWindowStore))

(defonce small-image
  (ImageIO/read (io/resource "small.png")))

(defonce source-image
  (ImageIO/read (io/resource "source.png")))

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
        src-array    (int-array (map (fn [[r g b a]] (.getRGB (Color. (or r 0)
                                                                      (or g 0)
                                                                      (or b 0)
                                                                      (or a 255)))) pixels))]
    (System/arraycopy src-array 0 dst-array 0 (alength src-array))
    buffered-img))

(defn write-output
  ([buffered-img]
   (write-output buffered-img nil))
  ([buffered-img file]
   (ImageIO/write buffered-img
                  "png"
                  file)))

(defn fetch-row
  [^ReadOnlyWindowStore store ^String metric-name ^long start ^long end]
  (with-open [iterator (.fetch ^ReadOnlyWindowStore store metric-name start end)]
    (iterator-seq iterator)))

(defn fetch-rows
  [store radio-station-names start end]
  (reduce #(assoc %1 %2 (fetch-row store %2 start end)) {} radio-station-names))

(defn radio-stations-to-image
  [store radio-station-names start end file]
  (let [radio-station-rows (fetch-rows store radio-station-names start end)
        pixel-rows         (for [[radio-station-name pixels] radio-station-rows]
                             (for [key-value-pair pixels]
                               (mapv :colour-component (.value key-value-pair))))
        width              (apply max (map count pixel-rows))
        pixels             (mapcat (fn [pixel-row]
                                     ;; pad pixels to same length
                                     (concat pixel-row (repeat (- (count pixel-row) width) nil)))
                                   pixel-rows)]
    (write-output (render-image pixels width) file)))
