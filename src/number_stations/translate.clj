(ns number-stations.translate)

(def number-lookup {\E {"six" 6 "three" 3 "two" 2 "seven" 7 "zero" 0 "five" 5 "eight" 8 "one" 1 "nine" 9 "four" 4}
                    \G {"fünf" 5 "acht" 8 "sechs" 6 "null" 0 "vier" 4 "eins" 1 "zwei" 2 "sieben" 7 "drei" 3 "neun" 9}
                    \M {"....-" 4 "----." 9 "...--" 3 "....." 5 "--..." 7 "..---" 2 "---.." 8 "-...." 6 "-----" 0 ".----" 1}})

(def word-lookup
  {\E {0 "zero" 7 "seven" 1 "one" 4 "four" 6 "six" 3 "three" 2 "two" 9 "nine" 5 "five" 8 "eight"}
   \G {0 "null" 7 "sieben" 1 "eins" 4 "vier" 6 "sechs" 3 "drei" 2 "zwei" 9 "neun" 5 "fünf" 8 "acht"}
   \M {0 "-----" 7 "--..." 1 ".----" 4 "....-" 6 "-...." 3 "...--" 2 "..---" 9 "----." 5 "....." 8 "---.."}})

(defn to-numbers
  [{:keys [numbers name]}]
  (let [numbers (mapv #(get-in number-lookup [(first name) %]) numbers)]
    (when (and (seq numbers) (every? int? numbers))
      (Integer/parseInt (apply str numbers)))))

(defn to-words
  [{:keys [numbers name]}]
  (mapv #(get-in word-lookup [(first name) %]) numbers))
