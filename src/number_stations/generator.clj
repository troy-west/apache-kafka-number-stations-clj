(ns number-stations.generator
  (:require [clojure.java.io :as io])
  (:import (javax.imageio ImageIO)
           (java.awt.image BufferedImage)
           (java.awt Color)))

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

(defn rand-str [len]
  (apply str (take len (repeatedly #(char (+ (rand 26) 65))))))

(defn write-output [buffered-img]
  (ImageIO/write buffered-img
                 "png"
                 (io/file "resources/output.png")))

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


  ;; let's add some context to the pixels

  (def station-names (repeatedly 50 #(str (rand-nth [\E \G \S \V]) "-" (rand-str 5))))

  (def station-stream (map (fn [pixel]
                             {:numbers pixel
                              :name    (rand-nth station-names)})
                           pixels))

  (def station-cycle-index {\E 1
                            \G 2
                            \S 3
                            \V 4})

  (def cycled-numbers (map (fn [message]
                             (update message :numbers
                                     (fn [numbers]
                                       (take 4
                                             (drop (get station-cycle-index (first (:name message)))
                                                   (cycle numbers))))))
                           station-stream))

  (write-output (render-image (map :numbers cycled-numbers) (+ (.getWidth bi) 5)))

  )
