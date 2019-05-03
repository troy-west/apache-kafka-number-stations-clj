(ns number-stations.stereotypes)

;; data stereotypes -- must be JSON encoded when going onto kafka,
;; so use primitive types which are serializable

;; TOPIC "number-stations"
(def number-station-message-numbers-stereotype
  {:time       0         ;; epoch timestamp (long)
   :name       "E-123"   ;; radio station name
   :latitude   37
   :longitude  144
   :numbers    ["one" "two" "three"]})

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

;; bursts with 30 second gaps. reverse burst
;;

;; remove primer; just use window
;; ;; TOPIC "number-stations"
;; (def number-station-message-primer-stereotype
;;   {:time             0 ;; epoch timestamp (long)
;;    :name             "E-123" ;; radio station name
;;    :latitude         37
;;    :longitude        144
;;    :number-of-pixels 100 ;; number of pixels
;;    })

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
