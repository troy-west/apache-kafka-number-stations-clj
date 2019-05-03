(ns number-stations.translate)

(defn index-numbers [numbers]
  (zipmap numbers (range)))

(defn invert
  [number-lookup]
  (into {} (for [[letter words-numbers] number-lookup]
             [letter (into {} (for [[word number] words-numbers]
                                [number word]))])))

(def number-lookup {\E (index-numbers ["zero" "one" "two" "three" "four" "five" "six" "seven" "eight" "nine"])
                    \G (index-numbers ["null" "eins" "zwei" "drei" "vier" "fünf" "sechs" "sieben" "acht" "neun"])
                    \M (index-numbers ["-----" ".----" "..---" "...--" "....-" "....." "-...." "--..." "---.." "----."])})

(def word-lookup
  (invert number-lookup))

(defn translate-to-numbers
  [{:keys [numbers name]}]
  (mapv #(get-in number-lookup [(first name) %]) numbers))

(defn translate-to-words
  [{:keys [numbers name]}]
  (mapv #(get-in word-lookup [(first name) %]) numbers))

(defn ints-to-colour-component
  [ints]
  (when (and (seq ints) (every? int? ints))
    (Integer/parseInt (apply str ints))))
