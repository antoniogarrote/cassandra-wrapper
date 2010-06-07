(ns cassandra-wrapper.core
  (:import [org.apache.thrift.transport TSocket]
	   [org.apache.thrift.protocol TBinaryProtocol]
	   [org.apache.cassandra.thrift Cassandra$Client ColumnPath SuperColumn
	    Column Mutation ColumnOrSuperColumn ColumnParent SlicePredicate NotFoundException
            SliceRange CfDef KsDef Clock Deletion KeyRange]))

;; Serializer

(defonce *byte-array-class* (class (byte-array [])))

(defn java-serializer
  ([x] (let [baos (java.io.ByteArrayOutputStream.)
             oos (java.io.ObjectOutputStream. baos)]
         (cond (integer? x) (.writeObject oos (java.lang.Integer. x))
               (float? x) (.writeObject oos (java.lang.Float. x))
               (char? x) (.writeObject oos (java.lang.Character. x))
               :else (.writeObject oos x))
         (.toByteArray baos))))

(defn java-deserializer
  ([bytes] (let [bais (java.io.ByteArrayInputStream. bytes)
                 ois (java.io.ObjectInputStream. bais)]
             (.readObject ois))))

(def *serializer-fn* java-serializer)

(def *deserializer-fn* java-deserializer)

(defn alter-serializer-fn [new-fn] (alter-var-root #'*serializer-fn* (fn [_] new-fn)))
(defn alter-deserializer-fn [new-fn] (alter-var-root #'*deserializer-fn* (fn [_] new-fn)))


;; Schema definition

(defn schema-cf
  [table type name]
  (let [cf (CfDef. table name)]
    (.setColumn_type cf type) cf))

(defn schema-standard-cf
  [table name] (schema-cf table "Standard" name))

(defn schema-super-cf
  [table name] (schema-cf table "Super" name))

(defn ks-strategy
  ([kw]
     (if (string? kw) kw
         (condp = kw
           :rack-unaware "org.apache.cassandra.locator.RackUnawareStrategy"
           :rack-aware   "org.apache.cassandra.locator.RackAwareStrategy"
           "org.apache.cassandra.locator.RackUnawareStrategy"))))

(defn schema-ks
  [name strategy replication-factor cfs]
  (KsDef. name (ks-strategy strategy) replication-factor cfs))

;; Clock

(defn clock
  ([date] (Clock. (.getTime date)))
  ([] (clock (java.util.Date.))))

(defn now
  ([] (clock)))

;; UUID
(defn uuid ([] (java.util.UUID/randomUUID)))

;; Column

(defn column
  ([name value clock] (Column. (.getBytes name) (*serializer-fn* value) clock))
  ([name value] (column name value (now))))

;; SuperColumn

(defn super-column
  ([name values-map clock] (let [columns (map (fn [[k v]] (column k v clock)) values-map)]
                             (SuperColumn. (.getBytes name) columns)))
  ([name values-map] (super-column name values-map (now))))


;; Column parent

(defn column-parent
  ([cf sc] (let [cp (ColumnParent. cf)]
             (.setSuper_column cp (.getBytes sc)) cp))
  ([cf] (ColumnParent. cf))
  ([] (ColumnParent.)))

;; Column path

(defn super-column-path
  ([cf sc] (let [cp (ColumnPath. cf)]
             (.setSuper_column cp (.getBytes sc)) cp)))

(defn column-path
  ([cf sc c] (if (nil? c)
               (super-column-path cf sc)
               (let [cp (ColumnPath. cf)]
                 (.setSuper_column cp (.getBytes sc))
                 (.setColumn cp (.getBytes c)) cp)))
  ([cf c] (let [cp (ColumnPath. cf)]
            (.setColumn cp (.getBytes c)) cp))
  ([cf] (ColumnPath. cf)))


;; Consistency levels

(defn get-consistency-level-constant
  ([kw]
     (condp = kw
       :all (org.apache.cassandra.thrift.ConsistencyLevel/ALL)
       :any (org.apache.cassandra.thrift.ConsistencyLevel/ANY)
       :dc-quorum (org.apache.cassandra.thrift.ConsistencyLevel/DCQUORUM)
       :dc-quorum-sync (org.apache.cassandra.thrift.ConsistencyLevel/DCQUORUMSYNC)
       :one (org.apache.cassandra.thrift.ConsistencyLevel/ONE)
       :quorum (org.apache.cassandra.thrift.ConsistencyLevel/QUORUM)
       :zero (org.apache.cassandra.thrift.ConsistencyLevel/ZERO)
       :else (throw (Exception. (str "Unknown consistency level: " kw))))))

(defonce *slice-range-keys* [:start :finish :reversed :count])

;; Slice ranges and predicates

(defn slice-range
  ([& opts]
     (let [opt-map-pre (apply hash-map opts)
           opt-map (reduce (fn [acum k] (condp = k
                                          :start (if (nil? (get opt-map-pre k)) (assoc acum :start "") acum)
                                          :finish (if (nil? (get opt-map-pre k)) (assoc acum :finish "") acum)
                                          :reversed (if (nil? (get opt-map-pre k)) (assoc acum :reversed false) acum)
                                          acum))
                           opt-map-pre *slice-range-keys*)
           sr (SliceRange.)]
       (do (.setStart sr (.getBytes (get opt-map :start)))
           (.setFinish sr (.getBytes (get opt-map :finish)))
           (.setReversed sr (get opt-map :reversed))
           (when-not (nil? (get opt-map :count)) (.setCount sr (get opt-map :count))) sr))))

(defn slice-predicate
  "Builds a slice predicate provided a list of column names or a list of
   key values for a slice range"
  [& opts]
  (let [pred (SlicePredicate.)]
    (if (keyword? (first opts))
      (.setSlice_range pred (apply slice-range opts))
      (if (instance? org.apache.cassandra.thrift.SliceRange (first opts))
        (.setSlice_range pred (first opts))
        (.setColumn_names pred (map #(.getBytes %1) opts))))
    pred))

(defonce *key-range-keys* [:start-key :end-key :start-token :end-token :count])

;; Slice ranges and predicates

(defn key-range
  ([& opts]
     (let [opt-map (apply hash-map opts)
           kr (KeyRange.)]
       (do (when (get opt-map :start-key) (.setStart_key kr (*serializer-fn* (get opt-map :start-key))))
           (when (get opt-map :end-key) (.setEnd_key kr (*serializer-fn* (get opt-map :end-key))))
           (when (get opt-map :start-token) (.setStart_token kr (get opt-map :start-token)))
           (when (get opt-map :end-token) (.setEnd_token kr (get opt-map :end-token)))
           (when (get opt-map :count) (.setCount kr (get opt-map :count))) kr))))

;; Deletion

(defn delete-column-predicate-mutation
  ([clock] (Deletion. clock))
  ([clock & opts]
     (let [del (Deletion. clock)
           mutation (Mutation.)]
       (.setPredicate del (apply slice-predicate opts))
       (.setDeletion mutation del) mutation)))

(defn add-column-value-mutation
  ([name value clock]
     (let [mutation (Mutation.)
           csc (let [csc-pre (ColumnOrSuperColumn.)] (.setColumn csc-pre (column name value clock)) csc-pre)]
       (.setColumn_or_supercolumn mutation csc) mutation))
  ([name value] (add-column-value-mutation name value (now))))

(defn add-super-column-value-mutation
  ([name values-map clock]
     (let [mutation (Mutation.)
           csc (let [csc-pre (ColumnOrSuperColumn.)] (.setSuper_column csc-pre (super-column name values-map clock)) csc-pre)]
       (.setColumn_or_supercolumn mutation csc) mutation))
  ([name values-map] (add-super-column-value-mutation name values-map (now))))

;; Client

(defprotocol CassandraClient
  "A connection to a Cassandra cluster"
  (close [c] "Closes the connection")
  ;; Schema manipulation
  (schema-add-ks [c ks-def] "Adds a new key space to the database")
  (schema-drop-ks [c ks-name] "Removes a key space and all the column families in that key space from the database")
  (schema-add-cf [c cf-def] "Adds a new column family to the key space in the database")
  (schema-drop-cf [c ks-name cf-name] "drops a column family from the provided key space")
  (use-ks [c ks-name] "Select a key space where operation will be applied")
  ;; Column manipulations and retrieval
  (insert-col [c key path value consistency-level]
              [c key path value clock consistency-level]
              "Adds a new column to the database")
  (get-col [c key path consistency-level] "Retrieves the column at the given path for the provided key")
  (get-super-col [c key path consistency-level] "Retrieves the super column at the given path for the provided key")
  (remove-col [c key path clock consistency-level]
              [c key path consistency-level] "Removes data from the row identified by key, path can contain from only the cf to cf sc, cf sc c or cf c")
  (batch-mutate [c key cf-mutations-map consistency-level] "Performs a list of mutations in a certain key space for a given key")
  (get-slice [c key path slice-pred consistency-level] "Return the group of columns at path matching the provided predicate")
  (get-range-slices [c path slice-pred key-range consistency-level])
  ;; Schemas description
  (schemas [c] "Returns an array with all the key spaces in the database"))


(defn- check-path
  ([path-args]
     (if (string? path-args)
       (column-parent path-args)
       (apply column-parent path-args))))

(defn- parse-result-column
  ([result-column]
     (let [column (.getColumn result-column)
           column-name (String. (.getName column))
           column-value (*deserializer-fn* (.getValue column))
           clock (.getClock column)]
       {:name column-name :value column-value :clock clock})))

(defn- parse-single-column
  ([column]
     (let [column-name (String. (.getName column))
           column-value (*deserializer-fn* (.getValue column))
           clock (.getClock column)]
       {:name column-name :value column-value :clock clock})))

(defn- parse-result-super-column
  ([result-column]
     (let [name (String. (.getName (.getSuper_column result-column)))
           columns (map #(parse-single-column %1) (.getColumns (.getSuper_column result-column)))]
       {:name name :columns columns})))

(defn- parse-key-slices
  ([result-slices]
     (reduce (fn [acum key-slice]
               (let [key (*deserializer-fn* (.getKey key-slice))
                     columns (map #(parse-result-column %1) (.getColumns key-slice))]
                 (assoc acum key columns)))
             {}
             result-slices)))

(defrecord CassandraClientImpl [client transport protocol]
  CassandraClient

  (close [c] (.close transport))

  ;; Schema manipulation
  (schemas [c] (vec (.describe_keyspaces client)))

  (schema-add-ks [c ks-def] (.system_add_keyspace client ks-def))

  (schema-drop-ks [c ks-name] (do (.set_keyspace client ks-name) (.system_drop_keyspace client ks-name)))

  (schema-add-cf [c cf-def] (do (.set_keyspace client (.getTable cf-def)) (.system_add_column_family client cf-def)))

  (schema-drop-cf [c ks-name cf-name] (do (.set_keyspace client ks-name)(.system_drop_column_family client ks-name cf-name)))

  (use-ks [c ks-name] (.set_keyspace client ks-name))

  ;; Column manipulations and retrieval
  (insert-col [c key path value clock consistency-level]
              (let [cp (check-path (drop-last path))
                    key-bytes (*serializer-fn* key)
                    column (column (last path) value clock)
                    cl (get-consistency-level-constant consistency-level)]
                (.insert client key-bytes cp column cl)))

  (insert-col [c key path value consistency-level]
              (insert-col c key path value (now) consistency-level))

  (get-super-col [c key path consistency-level]
                 (let [cp (apply super-column-path path)
                       key-bytes (*serializer-fn* key)
                       cl (get-consistency-level-constant consistency-level)]
                   (try
                    (parse-result-super-column (.get client key-bytes cp cl))
                    (catch org.apache.cassandra.thrift.NotFoundException ex
                      nil))))

  (get-col [c key path consistency-level]
           (let [cp (apply column-path path)
                 key-bytes (*serializer-fn* key)
                 cl (get-consistency-level-constant consistency-level)]
             (try
              (parse-result-column (.get client key-bytes cp cl))
              (catch org.apache.cassandra.thrift.NotFoundException ex
                nil))))

  (remove-col [c key path clock consistency-level]
              (let [cp (apply column-path path)
                    key-bytes (*serializer-fn* key)
                    cl (get-consistency-level-constant consistency-level)]
                (.remove client key-bytes cp clock cl)))

  (remove-col [c key path consistency-level] (remove-col c key path (now) consistency-level))

  (get-slice [c key path slice-pred consistency-level]
             (let [results (.get_slice client (*serializer-fn* key)
                                       (apply column-parent path)
                                       (apply slice-predicate slice-pred)
                                       (get-consistency-level-constant consistency-level))]
               (map #(parse-result-column %1) results)))

  (get-range-slices [c path slice-pred key-rang consistency-level]
                    (let [results (.get_range_slices client
                                                     (apply column-parent path)
                                                     (apply slice-predicate slice-pred)
                                                     (apply key-range key-rang)
                                                     (get-consistency-level-constant consistency-level))]
                      (parse-key-slices results)))

  (batch-mutate [c key cf-mutations-map consistency-level]
                (doseq [[cf muts] cf-mutations-map] (doseq [m muts] (when (not (nil? (.getDeletion m)))
                                                                      (throw (Exception. "Slice ranges not supported yet in deletions in Cassandra server (v 0.6.3)")))))
                (.batch_mutate client {(*serializer-fn* key) cf-mutations-map} (get-consistency-level-constant consistency-level))))

(defn make-connection
  "Make a closeable Cassandra client connection to the db."
  ([host port]
     (let [t (TSocket. host port)
           p (TBinaryProtocol. t)
           c (do (.open t)
                 (proxy [Cassandra$Client] [p]))]
       (CassandraClientImpl. c t p)))
  ([host] (make-connection host 9160))
  ([] (make-connection "localhost" 9160)))
