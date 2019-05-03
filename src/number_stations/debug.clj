(ns number-stations.debug
  (:import org.apache.kafka.streams.kstream.ForeachAction))

(defn instrument-stream
  [input-stream stream-name]
  (.foreach input-stream (reify ForeachAction
                           (apply [_ k v]
                             (println (format "%-40s => [key=%s, value=%s]" stream-name k v)))))
  input-stream)
