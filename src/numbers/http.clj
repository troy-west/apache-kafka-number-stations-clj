(ns numbers.http
  (:require [clojure.java.io :as io]
            [numbers.compute :as compute]
            [numbers.image :as image]
            [org.httpkit.server :as httpkit]
            [hiccup.core :as hiccup]
            [reitit.ring :as reitit.ring]
            [ring.middleware.keyword-params :refer [wrap-keyword-params]]
            [ring.middleware.params :refer [wrap-params]]
            [ring.middleware.file :refer [wrap-file]]
            [clojure.tools.logging :as log]))

(defn index
  [start end]
  (hiccup/html [:html
                [:head
                 [:style "body { max-width: 38rem; padding: 2rem; margin: auto; }"]]
                [:body
                 [:form {:method "GET"}
                  [:fieldset
                   [:div
                    [:input {:type "number" :name "start" :value (str (or start 1557125660763))}]
                    [:label {:for "start"} "Start timestamp (milliseconds since epoch):"]]

                   [:div
                    [:input {:type "number" :name "end" :value (str (or end 1557135288803))}]
                    [:label {:for "end"} "End timestamp (milliseconds since epoch):"]]

                   [:button "Regenerate and view image from time period"]]]
                 [:div
                  [:img {:src "generated.png" :width "560"}]]]]))

(defn handler
  [streams rand-part]
  {:get {:parameters {:query {:start int?, :end int?}}
         :handler    (fn [req]
                       (let [{:strs [start end]} (:query-params req)]
                         (let [start (try (Long/parseLong start)
                                          (catch Exception _ 1557125660763))
                               end   (try (Long/parseLong end)
                                          (catch Exception _ 1557135288803))]
                           (log/info "generating image:" (format "resources/public/generated-%s.png" rand-part))
                           (image/persist (image/render (map :content (compute/slice streams)))
                                          rand-part)
                           {:body   (index start end)
                            :status 200})))}})

(defn start!
  [port streams]
  (let [rand-part (rand-int 10000)
        app       (->> (reitit.ring/ring-handler
                        (reitit.ring/router
                         [""
                          ["/" (handler streams rand-part)]
                          ["/generated.png"
                           (fn [_]
                             (let [filename (format "resources/public/generated-%s.png" rand-part)]
                               (log/info "fetching image :" filename)
                               {:status  200
                                :headers {}
                                :body    (io/file filename)}))]])
                        (reitit.ring/routes
                         (reitit.ring/create-resource-handler {:path "/"})
                         (reitit.ring/create-default-handler)))
                       wrap-keyword-params
                       wrap-params)
        server    (httpkit/run-server app {:port port})]
    (let []
      (println "Serving on port" port)
      server)))