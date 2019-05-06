(ns number-stations.system
  (:require [clojure.java.io :as io]
            [integrant.core :as ig]
            [number-stations.generator :as generator]
            [number-stations.images :as images]
            [number-stations.topology :as topology]
            [org.httpkit.server :as httpkit]
            [reitit.ring :as reitit.ring]
            [ring.middleware.keyword-params :refer [wrap-keyword-params]]
            [ring.middleware.params :refer [wrap-params]]
            [hiccup.core :as hiccup])
  (:import java.util.Properties
           org.apache.kafka.common.serialization.StringSerializer
           [org.apache.kafka.streams StreamsBuilder TopologyTestDriver]
           org.apache.kafka.streams.kstream.Consumed
           org.apache.kafka.streams.processor.TimestampExtractor
           org.apache.kafka.streams.test.ConsumerRecordFactory)
  (:gen-class))

(defonce system (atom nil))

(defmethod ig/init-key :httpkit/server
  [_ {:keys [ring/app :httpkit/config]}]
  {:pre [(:port config)]}
  (let [server (httpkit/run-server app config)]
    (println "Serving on port" (:port config))
    server))

(defmethod ig/halt-key! :httpkit/server
  [_ stop-server-fn]
  (stop-server-fn))

(defn generate-image-output-stream
  [driver file start end]
  (images/radio-stations-to-image (.getWindowStore driver topology/pt10s-store)
                                  (vec (for [i (range 1000)]
                                         (str "E-" i)))
                                  start
                                  end
                                  file))

(defn index
  [start end]
  (hiccup/html [:html
                [:body
                 [:form {:method "GET"}
                  [:label {:for "start"} "Start:"]
                  [:input {:type "number" :name "start" :value (str (or start 0))}]

                  [:label {:for "end"} "End:"]
                  [:input {:type "number" :name "end" :value (str (or end 2000000000))}]

                  [:button "View image for time period"]]
                 [:div
                  [:img {:src (str "generated.png?" (rand-int 1000000)) :width "960"}]]]]))

(defn handler
  [test-driver]
  {:get {:parameters {:query {:start int?, :end int?}}
         :handler (fn [req]
                    (let [{:strs [start end]} (:query-params req)]
                      (let [start (try
                                    (Long/parseLong start)
                                    (catch Exception _
                                      0))
                            end   (try
                                    (Long/parseLong end)
                                    (catch Exception _
                                      2000000000))]
                        (generate-image-output-stream test-driver (io/file "resources/public/generated.png")
                                                      start end)
                        {:body    (index start end)
                         :status  200})))}})

(defmethod ig/init-key :ring/app
  [_ {:keys [kafkastreams/test-driver]}]
  (->> (reitit.ring/ring-handler
        (reitit.ring/router
         ["" ["/" (handler test-driver)]])
        (reitit.ring/routes
         (reitit.ring/create-resource-handler {:path "/"})
         (reitit.ring/create-default-handler)))
       wrap-keyword-params
       wrap-params))

(defmethod ig/init-key :kafkastreams/test-driver
  [_ {:keys [input-topic application-id]}]
  (let [builder   (StreamsBuilder.)
        factory   (ConsumerRecordFactory. "input"
                                          (StringSerializer.)
                                          (topology/->JsonSerializer))]

    (some-> (.stream builder input-topic (Consumed/with topology/extractor))
            topology/translate
            topology/denoise
            (topology/deduplicate builder)
            topology/correlate)

    (let [driver (TopologyTestDriver. (.build builder) (topology/config {:application-id application-id}))]
      (future (doseq [message (generator/generate-messages images/small-image)]
                (locking driver
                  (.pipeInput driver (.create factory "input" (:name message) message)))))

      driver)))

(defmethod ig/halt-key! :kafkastreams/test-driver
  [_ driver]
  (.close driver))

(defn start
  []
  {:pre [(not @system)]}
  (let [config {:httpkit/server           {:ring/app       (ig/ref :ring/app)
                                           :httpkit/config {:port 8080}}
                :ring/app                 {:kafkastreams/test-driver (ig/ref :kafkastreams/test-driver)}
                :kafkastreams/test-driver {:input-topic    "input"
                                           :application-id "number-stations"}}]
    (reset! system (ig/init config))))

(defn stop
  []
  (ig/halt! @system)
  (reset! system nil))

(defn restart
  []
  (stop)
  (start))

(defn -main
  [& args]
  (start))
