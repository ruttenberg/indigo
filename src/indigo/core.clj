(ns indigo.core
  (:gen-class))

(use 'clojure.pprint)

;-----
(defmacro futures
  [n & exprs]
  (vec (for [_ (range n)
             expr exprs]
         `(future ~expr))))


;-----
(defmacro wait-futures
  [& args]
  `(doseq [f# (futures ~@args)]
     @f#))

(def kv-store (ref {}))

(defn get-value [key]
  (get (deref kv-store) key))

(defn value-of [value-expiration]
  (case (seq? value-expiration)
    true (first value-expiration)
    false value-expiration))

(defn set-value-for-key [key value]
  (dosync
    (alter kv-store update-in [key] (constantly value))
    (count value)))

(defn add-to-kv-store [kv-pairs]
  (doall
    (map (fn [pair] (set-value-for-key (first pair) (second pair))) kv-pairs))
  @kv-store)

(defn get-or-create-inner-map [a-map key]
  (let [value-at-key (get a-map key)]
    (case (nil? value-at-key)
      true {}
      false value-at-key)))

(defn modify-inner-map [a-map key field value]
  (let [value-at-key (get-or-create-inner-map a-map key)]
    (update-in a-map [key] (constantly (update-in value-at-key [field] (constantly value))))))

(defn inner-field-new? [a-map key field]
  (nil? (get (get a-map key) field)))

(defn hset [key field value]
  (dosync
    (let [value-at-key (get (deref kv-store) key)]
      (case (nil? value-at-key)
        true (do
               (alter kv-store modify-inner-map key field value)
               1)
        false (case (map? value-at-key)
                false nil
                true (do
                       (let [new?
                             (case (inner-field-new? (deref kv-store) key field)
                               true 1
                               false 0)]
                         (alter kv-store modify-inner-map key field value)
                         new?)))))))

(defn pop-from-list [key list]
  (case (empty? list)
    true nil
    false (do
            (set-value-for-key key (pop list))
            (peek list))))


(defn lpop [key]
  (dosync
    (let [value-at-key (get-value key)]
      (case (seq? value-at-key)
        false nil
        true (pop-from-list key value-at-key)))))

(defn push-to-list [list item more-items]
  (let [list+item (conj list item)]
    (reduce
      (fn [list element] (conj list element))
      list+item
      more-items)))

(defn lpush [key item & rest]
  (dosync
           (let [value-at-key (get-value key)]
             (case (nil? value-at-key)
               true (set-value-for-key key (push-to-list () item rest))
               false (case (seq? value-at-key)
                       true (set-value-for-key key (push-to-list value-at-key item rest))
                       false nil)))))

(defn dump []
  (println "value of KV STORE")
  (pprint (deref kv-store))
  (println)
  )

(defn running [val]
  (pprint 'RUNNING...)
  (pprint val)
  (println))

(defn random-sleep []
  (Thread/sleep (rand-int 500)))

(defn do-with-random-sleep [func & rest]
  (random-sleep)
  (apply func rest))


(defn set-random-int [maximum]
  (let [value (str (rand-int maximum))]
    (set-value-for-key value value)))

(defn get-random-int [maximum]
  (let [key (str (rand-int maximum))]
    (get-value key)))

(defn clear []
  (running (quote
             (dosync (ref-set kv-store {}))
             ))
  (dosync (ref-set kv-store {}))
  )

(defn -main
  "I don't do a whole lot ... yet."
  [& args]

  (dump)

  (running (quote (add-to-kv-store (list
                                     '("string" "string-value")
                                     (list "list" (apply list (range 1 10)))
                                     (list "map" {"key1" "value1"
                                                  "key2" "value2"
                                                  "key3" "value3"})))))
  (add-to-kv-store (list
                     '("string" "string-value")
                     (list "list" (apply list (range 1 10)))
                     (list "map" {"key1" "value1"
                                  "key2" "value2"
                                  "key3" "value3"})))

  (dump)

  (running '(set-value-for-key "foo" "bar"))
  (set-value-for-key "foo" "bar")
  (dump)

  (running '(hset "map" "key3" "newval"))
  (hset "map" "key3" "newval")
  (dump)

  (println "hset on a list is a no-op")
  (running '(hset "list" "key3" "newval"))
  (hset "list" "key3" "newval")
  (dump)

  (println "hset on a string is a no-op")
  (running '(hset "string" "key3" "newval"))
  (hset "string" "key3" "newval")
  (dump)

  (running (quote
             (do
               (println (get-value "string"))
               (println (get-value "list"))
               (println (get-value "map"))
               )
           ))

  (do
    (println (get-value "string"))
    (println (get-value "list"))
    (println (get-value "map"))
    )

  (println)
  (dump)

  (running (quote
             (let [pop-result (lpop "list")]
               (println)
               (println pop-result)
               (println (get-value "list")))))

  (let [pop-result (lpop "list")]
    (println)
    (println pop-result)
    (println (get-value "list")))

  (println)
  (dump)

  (running (quote
             (time (wait-futures 1
                                 (do-with-random-sleep lpop "list")
                                 (pprint (do-with-random-sleep get-value "list"))
                                 (do-with-random-sleep set-value-for-key "foo" "fighters")))
             ))

  (time (wait-futures 1
                      (do-with-random-sleep lpop "list")
                      (pprint (do-with-random-sleep get-value "list"))
                      (do-with-random-sleep set-value-for-key "foo" "fighters")))

  (dump)

  (running (quote
             (dotimes [_ 8]
               (println (lpop "list")))
             ))

  (dotimes [_ 8]
    (println (lpop "list")))

  (println)
  (dump)

  (clear)
  (running (quote
             (time (wait-futures 1000
                                 (dotimes [_ 10] (set-random-int 10000))))
             ))

  (time (wait-futures 1000
                    (dotimes [_ 10] (set-random-int 10000))))

  (println (str "kv-store size: " (count @kv-store)))

  (clear)

  (running (quote
             (time (wait-futures 100
                                 (dotimes [_ 100] (set-random-int 10000))))
             ))

  (time (wait-futures 100
                      (dotimes [_ 100] (set-random-int 10000))))

  (println (str "kv-store size: " (count @kv-store)))

  (clear)

  (running (quote
             (time (wait-futures 1
                                 (dotimes [_ 10000] (set-random-int 10000))))
             ))

  (time (wait-futures 1
                      (dotimes [_ 10000] (set-random-int 10000))))

  (println (str "kv-store size: " (count @kv-store)))

(System/exit 0)
)


