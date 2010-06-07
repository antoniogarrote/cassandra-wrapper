(ns cassandra-wrapper.core-test
  (:use [cassandra-wrapper.core] :reload-all)
  (:use [clojure.test]))

(defonce *port* 9160)
(defonce *host* "localhost")

(println (str "******************* Runnin cassandra-wrapper tests *******************\n"
              "Cassandra must be running in " *host* " and port " *port* "\n"
              "**********************************************************************\n"))

(defn with-test-schema-connection
  []
  (let [c (make-connection)
        ks "TestsCassandraWrapper"
        should-destroy (not (empty? (filter #(= ks %1) (schemas c))))
        ks-def (schema-ks ks :rack-unaware 1 [(schema-standard-cf ks "foods") (schema-super-cf ks "drinks")])]
    (when should-destroy
      (schema-drop-ks c ks))
    (schema-add-ks c ks-def)
    (use-ks c ks) c))

(deftest make-connection-test
  (let [c1 (make-connection)
        c2 (make-connection *host*)
        c3 (make-connection *host* *port*)]
    (is (instance? cassandra-wrapper.core.CassandraClientImpl c1))
    (is (instance? cassandra-wrapper.core.CassandraClientImpl c2))
    (is (instance? cassandra-wrapper.core.CassandraClientImpl c3))
    (close c1) (close c2) (close c3)))

(deftest make-schema-cf-test
  (let [cf (schema-cf "table" "Super" "name")]
    (is (= "table" (.getTable cf)))
    (is (= "name" (.getName cf)))
    (is (= "Super" (.getColumn_type cf)))))

(deftest make-schema-cf-super-test
  (let [cf (schema-super-cf "table" "name")]
    (is (= "table" (.getTable cf)))
    (is (= "name" (.getName cf)))
    (is (= "Super" (.getColumn_type cf)))))

(deftest make-schema-cf-standard-test
  (let [cf (schema-standard-cf "table" "name")]
    (is (= "table" (.getTable cf)))
    (is (= "name" (.getName cf)))
    (is (= "Standard" (.getColumn_type cf)))))

(deftest make-schema-ks-test
  (let [ks (schema-ks "tests" :rack-unaware 2 [(schema-standard-cf "tests" "a") (schema-super-cf "tests" "b")])]
    (is (= "tests" (.getName ks)))
    (is (= "org.apache.cassandra.locator.RackUnawareStrategy" (.getStrategy_class ks)))))

(deftest make-schema-test
  (let [c (make-connection)
        ks "TestsCassandraWrapper"
        should-destroy (not (empty? (filter #(= ks %1) (schemas c))))
        ks-def (schema-ks ks :rack-unaware 1 [(schema-standard-cf ks "a") (schema-super-cf ks "b")])]
    (when should-destroy
      (schema-drop-ks c ks))
    (schema-add-ks c ks-def)
    (is (not (empty? (filter #(= ks %1) (schemas c)))))
    (schema-add-cf c (schema-super-cf ks "c"))
    (schema-drop-cf c ks "c")
    (schema-drop-ks c ks)
    (is (empty? (filter #(= ks %1) (schemas c))))))

(deftest test-clock
  (let [c1 (clock)
        c2 (clock (java.util.Date.))
        c3 (now)]
    (is (instance? org.apache.cassandra.thrift.Clock c1))
    (is (instance? org.apache.cassandra.thrift.Clock c2))
    (is (instance? org.apache.cassandra.thrift.Clock c3))))

(deftest test-seriazers-deserializers
  (is (= {:a 1 :b 2} (java-deserializer (java-serializer {:a 1 :b 2}))))
  (is (= [1 2 3] (java-deserializer (java-serializer [1 2 3]))))
  (is (= '(1 2 3) (java-deserializer (java-serializer '(1 2 3)))))
  (is (= 2 (java-deserializer (java-serializer 2))))
  (is (= 2.0 (java-deserializer (java-serializer 2.0))))
  (is (= "hello world" (java-deserializer (java-serializer "hello world"))))
  (is (= 1 (aget (java-deserializer (java-serializer (int-array [1 2 3]))) 0)))
  (is (= 3 (alength (java-deserializer (java-serializer (int-array [1 2 3])))))))

(deftest test-columns
  (let [c1 (column "hola" "mundo" (now))
        c2 (column "hola2" "mundo2")]
    (is (= (String. (.getName c1)) "hola"))
    (is (= (String. (.getName c2)) "hola2"))
    (is (= (java-deserializer (.getValue c1)) "mundo"))
    (is (= (java-deserializer (.getValue c2)) "mundo2"))))

(deftest test-super-columns
  (let [sc1 (super-column "people" {"name" "antonio" "surname" "garrote hernandez"})]
    (is (= "name" (String. (.getName (first (.getColumns sc1))))))
    (is (= "antonio" (*deserializer-fn* (.getValue (first (.getColumns sc1))))))))

(deftest test-column-parent
  (let [cp1 (column-parent "ColumnFamily")
        cp2 (column-parent "ColumnFamily2" "SuperColumn")]
    (is (= (.getColumn_family cp1) "ColumnFamily"))
    (is (= (.getColumn_family cp2) "ColumnFamily2"))
    (is (= (String. (.getSuper_column cp2)) "SuperColumn"))))

(deftest test-slice-range
  (let [r1 (slice-range :start "hola" :finish "adios" :count 100 :reversed true)
        r2 (slice-range :start "hola" :finish "adios" :count 100)
        r3 (slice-range :count 100)]
    (is (= (String. (.getStart r1)) "hola"))
    (is (= (String. (.getStart r3)) ""))
    (is (= (String. (.getFinish r1)) "adios"))
    (is (= (.isReversed r2) false))
    (is (= (.isReversed r1) true))
    (is (= (.isReversed r3) false))
    (is (= 100 (.getCount r3)))))

(deftest test-slice-predicate
  (let [p1 (slice-predicate :first "hola")
        p2 (slice-predicate "a" "b")]
    (is (= 2 (count (.getColumn_names p2))))
    (is (instance? org.apache.cassandra.thrift.SliceRange (.getSlice_range p1)))))

(deftest test-insert-get-remove
  (let [c (with-test-schema-connection)
        *uuid* (uuid)]
    (insert-col c *uuid* ["foods" "desserts"] "ice cream" :all)
    (insert-col c *uuid* ["foods" "rices"] "paella" :all)
    (insert-col c *uuid* ["foods" "pasta"] "lassagna" :all)
    (let [read-column (get-col c *uuid* ["foods" "desserts"] :all)]
      (is (= (:value read-column) "ice cream"))
      (is (= (:name read-column) "desserts")))
    (remove-col c *uuid* ["foods" "desserts"] :all)
    (is (nil? (get-col c *uuid* ["foods" "desserts"] :all)))
    (is (not (nil? (get-col c *uuid* ["foods" "rices"] :all))))
    (is (not (nil? (get-col c *uuid* ["foods" "pasta"] :all))))
    (remove-col c *uuid* ["foods"] :all)
    (is (nil? (get-col c *uuid* ["foods" "rices"] :all)))
    (is (nil? (get-col c *uuid* ["foods" "pasta"] :all)))))

(deftest test-batch-ops-get-slice
  (let [c (with-test-schema-connection)
        *uuid* (uuid)]
    (batch-mutate c *uuid* {"foods" [(add-column-value-mutation "rices" "paella")
                                     (add-column-value-mutation "desserts" "ice-cream")]
                            "drinks" [(add-super-column-value-mutation "soft drinks" {"juices" "apple" "soda" "coke"})]} :all)
    (let [result (get-range-slices c ["foods"] [:start "rices"] [:start-key *uuid* :end-key *uuid*] :all)]
      (is (= (:name (first (get result *uuid*))) "rices"))
      (is (= (:value (first (get result *uuid*))) "paella")))))
