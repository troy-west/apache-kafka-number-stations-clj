(ns number-stations.system
  (:require [integrant.core :as ig]
            [org.httpkit.server :as httpkit]
            [reitit.ring :as reitit.ring]
            [ring.middleware.keyword-params :refer [wrap-keyword-params]]
            [ring.middleware.params :refer [wrap-params]]))

(defn wrap-dir-index [handler]
  (fn [req]
    (handler
     (update-in req [:uri] #(if (= "/" %) "/index.html" %)))))

(defmethod ig/init-key :httpkit/server
  [_ {:keys [ring/app :httpkit/config]}]
  (httpkit/run-server app config))

(defmethod ig/init-key :ring/app
  [_ _]
  (->> (reitit.ring/ring-handler
        (reitit.ring/router
         [""
          ["/" {:get {:handler (constantly {:body "ok!" :status 200})}}]])
        (reitit.ring/routes
         (reitit.ring/create-resource-handler {:path "/"})
         (reitit.ring/create-default-handler)))
       wrap-keyword-params
       wrap-params
       wrap-dir-index))

(defonce system (atom nil))

(defn start
  []
  {:pre [(not @system)]}
  (reset! system (ig/init {:httpkit/server {:httpkit/server {:ring/app       (ig/ref :ring/app)
                                                             :httpkit/config {:port 8080}}}
                           :ring/app       {}})))

(defn stop
  []
  (ig/halt! @system))
