(ns numbers.system
  (:require [clojure.tools.logging :as log]
            [numbers.compute :as compute]
            [numbers.http :as http])
  (:gen-class))

(defn start!
  []
  (http/start! (compute/start!)))

(defn -main
  [& _]
  (Thread/setDefaultUncaughtExceptionHandler
   (reify Thread$UncaughtExceptionHandler
     (uncaughtException [_ thread ex]
       (log/error ex "uncaught exception on" (.getName thread)))))
  (start!))