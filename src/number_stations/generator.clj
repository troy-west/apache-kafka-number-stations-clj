(ns number-stations.generator
  (:require [clojure.java.io :as io])
  (:import (javax.imageio ImageIO)
           (java.awt.image BufferedImage
                           Raster)))

(defn pixel-seq [image-file]
  (let [raster (.getData (ImageIO/read image-file))
        width  (.getWidth raster)
        height (.getHeight raster)]
    (partition 4 (.getPixels raster 0 0 width height (double-array (* width height 4))))))

(comment
  (pixel-seq (io/resource "source.png")))
