(ns numbers.system
  (require [clojure.tools.logging :as log]
           [numbers.compute :as compute]
           [numbers.http :as http])
  (:gen-class))

(defn -main
  [& _]
  (Thread/setDefaultUncaughtExceptionHandler
   (reify Thread$UncaughtExceptionHandler
     (uncaughtException [_ thread ex]
       (log/error ex "uncaught exception on" (.getName thread)))))
  (let [streams (compute/start!)]
    (http/start! (compute/store streams))))
