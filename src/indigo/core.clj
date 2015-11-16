(ns indigo.core
  (:gen-class))

(def kv-store (ref {}))

(defn get-value [key]
  (get (deref kv-store) key))


(defn set-value-for-key [key value]
  (dosync
    (alter kv-store update-in [key] (constantly value))))

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


(defn lpop [key]
  (dosync
    (let [value-at-key (get-value key)]
      (case (seq? value-at-key)
        false nil
        true (do
               (set-value-for-key key (pop value-at-key))
               (peek value-at-key))))))


(defn -main
  "I don't do a whole lot ... yet."
  [& args]

  (add-to-kv-store (list
                     '("string" "string-value")
                     (list "list" (apply list (range 1 10)))
                     (list "map" {"key1" "value1"
                                  "key2" "value2"
                                  "key3" "value3"})))

  (set-value-for-key "foo" "bar")
  (println (deref kv-store))

  (hset "map" "key3" "newval")
  (println)
  (println (deref kv-store))

  (hset "list" "key3" "newval")
  (println)
  (println (deref kv-store))

  (hset "string" "key3" "newval")
  (println) (println (deref kv-store))
  (println (get-value "string"))
  (println (get-value "list"))
  (println (get-value "map"))

  (let [pop-result (lpop "list")]
    (println)
    (println pop-result)
    (println (get-value "list")))

  (println)
  (println (deref kv-store)))


