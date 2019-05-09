(ns numbers.system
  (:require [clojure.tools.logging :as log]
            [numbers.compute :as compute]
            [numbers.http :as http])
  (:gen-class))

(defn start!
  [port]
  (http/start! port (compute/start!)))

(defn -main
  [& args]
  (Thread/setDefaultUncaughtExceptionHandler
   (reify Thread$UncaughtExceptionHandler
     (uncaughtException [_ thread ex]
       (log/error ex "uncaught exception on" (.getName thread)))))
  (start! (or (some-> (first args) Integer/parseInt) 8080)))