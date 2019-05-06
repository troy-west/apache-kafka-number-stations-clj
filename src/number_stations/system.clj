(ns number-stations.system
  (:gen-class)
  (:require [aero.core :as aero]
            [clojure.java.io :as io]
            [hiccup.core :as hiccup]
            [integrant.core :as ig]
            [number-stations.generator :as generator]
            [number-stations.images :as images]
            [number-stations.topology :as topology]
            [org.httpkit.server :as httpkit]
            [reitit.ring :as reitit.ring]
            [ring.middleware.keyword-params :refer [wrap-keyword-params]]
            [ring.middleware.params :refer [wrap-params]]
            [ring.middleware.file :refer [wrap-file]])
  (:import java.util.Properties
           [org.apache.kafka.clients.admin AdminClient NewTopic]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [org.apache.kafka.streams KafkaStreams KafkaStreams$State StreamsBuilder]
           org.apache.kafka.streams.kstream.Consumed
           org.apache.kafka.streams.state.QueryableStoreTypes))

(defonce system (atom nil))

(defmethod ig/init-key :httpkit/server
  [_ {:keys [ring/app :httpkit/config]}]
  {:pre [(:port config)]}
  (let [server (httpkit/run-server app (update config :port #(Integer/parseInt (str %))))]
    (println "Serving on port" (:port config))
    server))

(defmethod ig/halt-key! :httpkit/server
  [_ stop-server-fn]
  (stop-server-fn))

(defn index
  [start end]
  (hiccup/html [:html
                [:head
                 [:style "body { max-width: 38rem; padding: 2rem; margin: auto; }"]]
                [:body
                 [:form {:method "GET"}
                  [:fieldset
                   [:div
                    [:input {:type "number" :name "start" :value (str (or start 0))}]
                    [:label {:for "start"} "Start timestamp (milliseconds since epoch):"]]

                   [:div
                    [:input {:type "number" :name "end" :value (str (or end 2000000000))}]
                    [:label {:for "end"} "End timestamp (milliseconds since epoch):"]]

                   [:button "Regenerate and view image from time period"]]]
                 [:div
                  [:img {:src (str "generated.png?" (rand-int 1000000)) :width "480"}]]]]))

(defn handler
  [store base-filename]
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
                        (images/radio-stations-to-image store
                                                        start end
                                                        (io/file base-filename))
                        {:body    (index start end)
                         :status  200})))}})

(defmethod ig/init-key :ring/app
  [_ {:keys [store]}]
  {:pre [store]}
  (let [base-filename (format "generated-%s.png" (rand-int 1000000))]
    (->> (reitit.ring/ring-handler
          (reitit.ring/router

           [""
            ["/" (handler store base-filename)]
            ["/generated.png" (fn [req]
                                (println :image base-filename (.exists (io/file base-filename)))
                                {:status 200
                                 :headers {}
                                 :body    (io/file base-filename)})]])

          (reitit.ring/routes
           (reitit.ring/create-resource-handler {:path "/"})
           (reitit.ring/create-default-handler)))

         wrap-keyword-params
         wrap-params)))

(defmethod ig/init-key :correlate/store
  [_ {:keys [streams]}]
  {:pre [streams]}
  (.store streams topology/pt10s-store (QueryableStoreTypes/windowStore)))

(defmethod ig/init-key :number-stations/generator
  [_ {:keys [topic producer admin-client partitions replication-factor]}]
  {:pre [(seq topic) admin-client partitions replication-factor]}
  (try
    (println "Creating topic")
    @(.all (.createTopics (AdminClient/create admin-client)
                          [(NewTopic. topic partitions replication-factor)]))
    (catch Exception e
      (println e)))

  (let [producer (KafkaProducer. (doto (Properties.)
                                   (.putAll producer)))
        f        (future
                   (println "Generation begun.")
                   (try
                     (doseq [message (generator/generate-messages images/small-image)]
                       (.send producer (ProducerRecord. topic (:name message) message)))
                     (println "Generation finished.")
                     (catch Exception e
                       (println e)
                       (throw e))))]

    (when (future-done? f)
      (println @f))

    f))

(defmethod ig/halt-key! :number-stations/generator
  [_ f]
  (future-cancel f))

(defn wait-for-running-state
  [streams]
  (loop []
    (let [state (.state streams)]
      (println "Waiting on stream state to be RUNNING: " state)
      (when-not (= KafkaStreams$State/RUNNING state)
        (Thread/sleep 1000)
        (recur)))))

(defmethod ig/init-key :kafka/streams
  [_ {:keys [input-topic topology]}]
  (let [builder (StreamsBuilder.)]
    (-> (.stream builder input-topic (Consumed/with topology/extractor))
        (topology/combined builder))

    (doto (KafkaStreams. (.build builder)
                         (topology/config topology))
      (.start)
      (wait-for-running-state))))

(defmethod ig/halt-key! :kafka/streams
  [_ streams]
  (.close streams))

(defn start
  []
  {:pre [(not @system)]}
  (let [config (binding [*data-readers* (merge *data-readers* {'ig/ref ig/ref})]
                 (aero/read-config (io/resource "config.edn")))]
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
