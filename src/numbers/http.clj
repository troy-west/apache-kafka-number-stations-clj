(ns numbers.http
  (:require [clojure.java.io :as io]
            [org.httpkit.server :as httpkit]
            [hiccup.core :as hiccup]
            [reitit.ring :as reitit.ring]
            [ring.middleware.keyword-params :refer [wrap-keyword-params]]
            [ring.middleware.params :refer [wrap-params]]
            [ring.middleware.file :refer [wrap-file]]))

(defn index
  [start end]
  (hiccup/html [:html
                [:head
                 [:style "body { max-width: 38rem; padding: 2rem; margin: auto; }"]]
                [:body
                 [:form {:method "GET"}
                  [:fieldset
                   [:div
                    [:input {:type "number" :name "start" :value (str (or start 1557125670763))}]
                    [:label {:for "start"} "Start timestamp (milliseconds since epoch):"]]

                   [:div
                    [:input {:type "number" :name "end" :value (str (or end 1557135269060))}]
                    [:label {:for "end"} "End timestamp (milliseconds since epoch):"]]

                   [:button "Regenerate and view image from time period"]]]
                 [:div
                  [:img {:src (str "generated.png?" (rand-int 1000000)) :width "480"}]]]]))

(defn handler
  [store base-filename]
  {:get {:parameters {:query {:start int?, :end int?}}
         :handler    (fn [req]
                       (let [{:strs [start end]} (:query-params req)]
                         (let [start (try (Long/parseLong start)
                                          (catch Exception _
                                            1557125670763))
                               end   (try (Long/parseLong end)
                                          (catch Exception _
                                            2000000000))]
                           ;; TODO: render image
                           {:body   (index start end)
                            :status 200})))}})

(defn start!
  [store]
  (let [base-filename (format "generated-%s.png" (rand-int 1000000))
        app           (->> (reitit.ring/ring-handler
                            (reitit.ring/router
                             [""
                              ["/" (handler store base-filename)]
                              ["/generated.png" (fn [req]
                                                  {:status  200
                                                   :headers {}
                                                   :body    (io/file base-filename)})]])
                            (reitit.ring/routes
                             (reitit.ring/create-resource-handler {:path "/"})
                             (reitit.ring/create-default-handler)))
                           wrap-keyword-params
                           wrap-params)
        server        (httpkit/run-server app {:port 8080})]
    (let []
      (println "Serving on port" 8080)
      server)))