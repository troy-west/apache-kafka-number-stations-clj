(ns numbers.translate)

(def types ["ENG" "GER" "MOR"])

(def number-index {"ENG" {"six" "6" "three" "3" "two" "2" "seven" "7" "zero" "0" "five" "5" "eight" "8" "one" "1" "nine" "9" "four" "4"}
                   "GER" {"fünf" "5" "acht" "8" "sechs" "6" "null" "0" "vier" "4" "eins" "1" "zwei" "2" "sieben" "7" "drei" "3" "neun" "9"}
                   "MOR" {"....-" "4" "----." "9" "...--" "3" "....." "5" "--..." "7" "..---" "2" "---.." "8" "-...." "6" "-----" "0" ".----" "1"}})

(def word-index {"ENG" {\0 "zero" \7 "seven" \1 "one" \4 "four" \6 "six" \3 "three" \2 "two" \9 "nine" \5 "five" \8 "eight"}
                 "GER" {\0 "null" \7 "sieben" \1 "eins" \4 "vier" \6 "sechs" \3 "drei" \2 "zwei" \9 "neun" \5 "fünf" \8 "acht"}
                 "MOR" {\0 "-----" \7 "--..." \1 ".----" \4 "....-" \6 "-...." \3 "...--" \2 "..---" \9 "----." \5 "....." \8 "---.."}})

(def words
  (memoize (fn [type number]
             (let [tx (get word-index type)]
               (mapv #(get tx %) (str number))))))

(defn number
  [type words]
  (let [tx (get number-index type)]
    (Integer/parseInt (apply str (map #(get tx %) words)))))

(defn translate
  [message]
  (assoc message :content [(number (:type message) (:content message))]))

(defn known?
  [message]
  (contains? number-index (:type message)))