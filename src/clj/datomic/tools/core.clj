(ns
  ^{:author "Peter Feldtmann"
    :doc "A ton of convenience functionality for the Datomic database.
         This is still work in progress."}
  clj.datomic.tools.core
  (:use     [clojure.pprint]
            [clojure.test :only [deftest testing]])
  (:require [datomic.api :as d] 
            [clj.datomic.tools.util :as dtu]
            [clojure.java.io :as io])
  (:import  [datomic Util Datom]
            [java.util.concurrent LinkedBlockingQueue TimeUnit])
  )

(set! *warn-on-reflection* true)


;; some naming conventions and abbreviations,
;; used especially in the naming of functions:
;;
;; 'database'   denotes a Datomic database, identified by uri & name
;; 'conn'       denotes a connection to a 'database'
;; 'db'         denotes a database as a value
;; 'part'       denotes a partition
;; 'ns'         denotes a namespace
;; 'q'          denotes a (raw) query
;; 'tx'         denotes a transaction, the ACID thing
;; 'tx-data'    denotes the data of a transaction, i.e. a list, vector or map
;; 'tempid'     denotes a temporary id, later to be resolved to an 'eid'
;; 'eid'        denotes an entity id
;; 'e'          denotes an entity, i.e. the map-like entity
;; 'es'         denotes a list of entities
;; 'a'          denotes an attribute
;; 'v'          denotes a value
;; 't'          denotes a transaction id
;; 'k'          denotes a key, i.e. an attibute used like a key
;; 'm'          denotes a map
;; 'e-m'        denotes an entity converted to a regular map
;; 'es-m'       denotes a list of entities, each converted to a regular map
;; 'card'       denotes the cardinality of an attribute 


;; handle multiple databases

(defrecord ^:private Database [name uri conn])

(defonce ^:dynamic *databases*         (atom {}))
(defonce ^:dynamic *current-database*  (atom nil))

;; dynamic vars, e.g. used by with-* functions
(defonce ^:dynamic *current-db*         (atom nil))
(defonce ^:dynamic *latest-async-tx*    (atom nil))
(defonce ^:dynamic *latest-tx-result*   (atom nil))
(defonce ^:dynamic *tx-report-queue*    (atom nil))

;; from Demonic:
;; vars for running a transaction in demarcation

(defonce ^:dynamic *in-demarcation*    false)
(defonce ^:dynamic *pending-tx-data*  (atom []))
(defonce ^:dynamic *db-test-mode*      false)

;; the rule base
(defonce ^:dynamic *rule-base*        (atom []))


;; handle database names and uris

(defn defdatabase 
  "Set up a database by uri and name."
  [uri name]
  (let [full-name (str uri "/" name)
        database  (Database. name full-name nil)] 
    (swap! *databases* assoc name database)
    (reset! *current-database* database)
    name)
  )

(defmacro def-free-database 
  "Set up a free in-memory database."
  [name]
  `(defdatabase "datomic:free://localhost:4334" ~name)
  )

(defmacro def-dev-database 
  "Set up a local-storage database."
  [name]
  `(defdatabase "datomic:dev://localhost:4334" ~name)
  )

(defmacro def-mem-database
  "Set up a in-memory database."
  [name]
  `(defdatabase "datomic:mem:/" ~name)
  )

(defn get-database 
  "Get a named database."
  [name]
  (get @*databases* name)
  )

(defn get-database-uri 
  "Get the URI of the current or a named database." 
  ([]     (:uri @*current-database*))
  ([name] (or (:uri (get-database name)) name ))
  )


;; handle databases 

(defn open-database 
  "Prepare the named database for use."
  [name]
  (let [full-name (get-database-uri name)
        conn      (d/connect full-name) 
        database  (Database. name full-name 
                             conn)] 
    (swap! *databases* assoc name database)
    (reset! *current-database* database)
    name)
  )

(defn- create-database* [name] 
  "Helper. See create-database." 
  (d/create-database (get-database-uri name))
  (open-database name))

(defn create-database 
  "Create a new database."
  ([]     (create-database* (:name  @*current-database*)))
  ([name] (create-database*  name))
  )

(defn- delete-database* [name]
  "Helper. See delete-database." 
  (d/delete-database (get-database-uri name))
  (swap! *databases* dissoc name )
  (when (= name (:name @*current-database*))
    (reset! *current-database* nil))
  )

(defn delete-database 
  "Delete a database."
  ([]     (delete-database* (:name  @*current-database*)))
  ([name] (delete-database* name))
  )

(defn- rename-database* [old-name new-name]
   "Helper. See rename-database." 
  (when (d/rename-database (get-database-uri old-name) new-name)
     (swap! *databases* dissoc old-name)
     (when (= old-name (:name @*current-database*))
       (reset! *current-database* nil))
     (open-database new-name)
    ))

(defn rename-database 
  "Rename a database." 
  ([new-name]          (rename-database* (:name @*current-database*)
                                         new-name))
  ([old-name new-name] (rename-database* old-name
                                         new-name)))

;; use for testing or showcase purposes

(defn scratch-database
  "Make a throw-away-database, e.g. for testing or showcase use."
  []
  (defdatabase "datomic:mem:/" (str "scratch-db-" (d/squuid)))
  (create-database) 
  (println (str "Created database " (get-database-uri))) 
  (get-database-uri))

(defmacro with-scratch-database 
  "Use throw-away-database for the forms, e.g. for testing or showcase use."
  [& forms]
  `(do
     (scratch-database)
     ~@forms
     (delete-database)))


;; handle database connections

(defn get-conn
  "Gets the current or named database connection." 
  ([]     (:conn @*current-database*))
  ([name] (:conn (get-database name)))
  )

(defmacro with-database 
  "Eval forms with the named database set as current database." 
  [name & forms]
  `(binding [*current-database* (get-database ~name)]
    ~@forms))


;; handle db as values

(defn set-db
  "Set the current db." 
  [db]
  (reset! *current-db* db)
  db)

(defn db
  "Get the current db."
  []
  (@*current-db*))

(defn fresh-db
  "Sets a fresh db as current and returns it." 
  ([]     (set-db (d/db (get-conn))))
  ([name] (d/db (get-conn name))))

(defn get-db 
  "Gets the current db, if set, or a fresh one." 
  ([]  (or @*current-db*
          (set-db (d/db (get-conn)))))
  ([name] (d/db (get-conn name))))


(defmacro with-db 
  "Eval forms with the given db set as current db." 
  [db & forms]
  `(binding [*current-db* ~db]
    ~@forms))

(defmacro with-fresh-db 
  "Eval forms with a fresh db set as current db." 
  [& forms]
  `(binding [*current-db* (d/db (get-conn))]
    ~@forms))


;; handle temporary ids

(defn new-tempid 
  "Make a temporary id."
  ([]           (d/tempid ":db.part/user"))
  ([part]       (d/tempid part))
  ([part value] (d/tempid part value)))

(defn new-tx-tempid 
  "Make a temporary id for a transaction attribute."
  []  
  (d/tempid ":db.part/tx"))


;; handle database transactions

(defn transact
  "Prepare a transaction future."
   [tx-data]
   (d/transact (get-conn) tx-data
     ))

(defn transact!
  "Do transaction, don't store latest tx result."
  [tx-data]
  (if *in-demarcation*
    (let [res (d/with (get-db) tx-data)]
      (swap! *pending-tx-data* concat tx-data)
      (set-db (:db-after res))
      res)
    @(transact tx-data)
    ))

(defn tx! 
  "Do transaction, store latest tx result."
  [tx-data]
   (reset! *latest-tx-result* nil) 
   ;; may throw exception
   (reset! *latest-tx-result* 
         (transact! tx-data))
   (fresh-db))

;; get results from latest transaction

(defn get-db-before []
  "Get the beforde-db of the latest transcaction."
  (when @*latest-tx-result*
    (:db-before @*latest-tx-result*)))

(defmacro with-db-before 
  "Eval forms with a the before-tx-db set as current db." 
  [& forms]
  `(binding [*current-db* (get-db-before)]
    ~@forms))

(defn get-db-after []
   "Get the after-db of the latest transcaction."
  (when @*latest-tx-result*
    (:db-after @*latest-tx-result*)))

(defmacro with-db-after 
  "Eval forms with a the after-tx-db set as current db." 
  [& forms]
  `(binding [*current-db* (get-db-after)]
    ~@forms))

(defn get-tx-result-data
  "Get the result data of the latest transcaction."
  []
  (when @*latest-tx-result*
    (:tx-data @*latest-tx-result*)))

(defn get-tx-result-tempids
  "Get the result tempids of the latest transcaction."
  []
  (when @*latest-tx-result*
    (:tempids @*latest-tx-result*)))

(defn tempid->eid
  "Resolve a temporary id to entity id."
  [temp-id]
  (d/resolve-tempid (get-db)
                    (get-tx-result-tempids) temp-id))

(defn prepare-tx-async
  "Initiate an async transcaction"
  [tx-data]
  (reset! *latest-async-tx* 
          (d/transact-async (get-conn) tx-data))
  )

(defn tx-async! 
  "Realize the prepared transaction,
   maintaining latest tx result."
  []
  (when @*latest-async-tx*
    (reset! *latest-tx-result* nil) 
    ;; the following may throw exception
    (reset! *latest-tx-result* 
         @@*latest-async-tx*)
    (reset! *latest-async-tx* nil))
  )


;; from project 'Demonic':
;; run transaction in demarcation

(defn commit-pending-transaction
  "Commit transaction run in a demarcation"
  []
  (when-not (or *db-test-mode* 
                (empty? @*pending-tx-data*)) 
    (binding [*in-demarcation* false]
      (tx! @*pending-tx-data*)) 
    ))
  
(defn run-in-demarcation 
  "Run transaction in a demarcation"
  [thunk]
  (binding [*in-demarcation*  true
            *pending-tx-data* (atom [])
           ]
    (let [res (thunk)]
      (commit-pending-transaction)
      res)))

(defmacro in-demarcation 
  "Eval forms within a transaction demarcation."
  [& forms]
  `(run-in-demarcation (fn [] ~@forms)))

(defn wrap-demarcation
  "A Ring middleware providing tx-demarcation."
  [handler]
  (fn [request]
    (if-not *db-test-mode*
      (in-demarcation (handler request))
      (handler request))))

;; for testing using demarcations

(defmacro with-demarcation 
  [in-test? body]
  `(binding [*db-test-mode* ~in-test?]
     (in-demarcation ~@body)))

(defmacro deftest-with-demarcation 
  [test-name & body]
  `(deftest ~test-name
     (with-demarcation true ~body)))

(defmacro testing-with-demarcation
  [message & body]
  `(testing ~message
     (with-demarcation true ~body)))


;; handle partitions

(defn defpartition
  "Create a new partition."
  [name]
  (let [data 
        `[{:db/id ~(new-tempid :db.part/db),
          :db/ident ~name,
          :db.install/_partition :db.part/db}]
       ]
  (tx! data))
  )

(defn delete-partition
  "Delete the named partiton."
  [name]
  (let [data 
        [[:db/retract :db.part/db 
          :db.install/partition name]]
       ]
  (tx! data))
  )

(defn eid->part 
  "Return the partition associated with an entity id."
  [eid]
  (d/part eid) 
  )

(declare e->eid)

(defn e->part 
  "Return the partition associated with an entity."
  [e]
  (-> e e->eid eid->part) 
  )

;; handle entities

(defn e->eid 
   "Returns the entity id of an entity."
  [e]
  (:db/id e))

(defn delete-entity 
  "Remove an entity with all its attributes from the database."
  [entity-id]
 (tx! [[:db.fn/retractEntity entity-id]])
  nil)

(defn delete-entities
  "Deletes given entities keyed by ids."
  [& ids]
   (tx! (map #(vec [:db.fn/retractEntity %]) ids)))

(defn e->touched 
  "entities are lazy, so touch the entity, 
   meaning eager loading of all attributes."
  [e]
  (d/touch e))

(defn e->map 
  "Converts an entity into a map with :db/id added.
   Caution: looses lazyness."
  ( [e]  (select-keys e (conj (keys e) :db/id)))
  ( [db eid] (e->map (d/entity db eid)))
)

(defn eid->e 
  "get an entity via its id."
  [entity-id]
  (when entity-id
     (d/entity (get-db) entity-id)))

(defn eid->touched 
  "get a touched entity by its id."
  [entity-id]
  (when entity-id 
    (when-let [e (d/entity (get-db) entity-id)]
      (e->touched e))))

(defn eid->map
  "get an entity as a map."
  [eid]
  (-> eid eid->e e->map))

(defn key->eid 
  "get an entities id by a key value."
  [key value]
  (and value
       (ffirst
        (d/q `[:find ?eid
               :in ~'$ ?key
               :where
               [?eid ~key ?key]]
             (get-db) value))))

(defn key->e 
  "get an entity by a key value other than id."
  [key value]
  (when-let [eid (key->eid key value)]
    (eid->e eid))
  )

(defn key->touched 
  "get a touched entity by a key value other than id."
  [key value]
  (when-let [eid (key->eid key value) ]
    (eid->touched eid))
  )

;; handle attributes

(defn defattr 
  "Define a new attribute."
  [attr-name & {:keys 
               [ns type card doc unique index? ext-key? 
                upsertable?
                fulltext? is-component? no-history?]}]
   (let [name (if ns (keyword (str ns "/" (name attr-name))) 
                     attr-name)
         type   (or (and type (if (namespace type)
                                 type
                                (keyword "db.type" type)))
                    :db.type/string)
         card   (cond (= card :one)  :db.cardinality/one
                      (= card :many) :db.cardinality/many
                      card           card
                      :else          :db.cardinality/one)
         doc    (or doc "no doc specified")
         unique (or unique (when ext-key? :db.unique/value)
                           (when upsertable? :db.unique/identity))
         index  (or index? (when ext-key? true)
                           (when upsertable? true) 
                           false)
         fulltext (or fulltext? false)
         is-component (or is-component? false)
         no-history (or no-history? false)
         tx-data-map 
         { :db/id (new-tempid :db.part/db)
           :db/ident name
           :db/valueType type
           :db/cardinality card
           :db/doc doc 
           :db/index index 
           :db/fulltext fulltext
           :db/isComponent is-component
           :db/noHistory no-history
           :db.install/_attribute :db.part/db}
         tx-data-vec (vector
         (if unique 
           (assoc tx-data-map :db/unique unique )
           tx-data-map))
       ]
       #_(pprint tx-data-vec)
       (tx! tx-data-vec)
     )
  )

(defmacro defattribute
     "Define a new attribute."
     [& args]
     `(defattr (map #(quote %) '~args))
     )

(defn remove-attr
  "Forget the named attribute."
  [attr]
  (let [tx-data 
        [[:db/retract :db.part/db 
          :db.install/attribute attr]]
       ]
  (tx! tx-data))
  )

(declare find-eid)

(defn get-cardinality-of-attr
  "Returns the cardinality (:db.cardinality/one or
   :db.cardinality/many) of the attribute."
  [attr]
  (find-eid '[:find ?v
        :in $ ?attr
        :where
        [?attr :db/cardinality ?card]
        [?card :db/ident ?v]]
      attr))

(defn has-attribute?
  "Does database have an attribute named attr-name?"
  [attr-name]
  (-> (eid->e attr-name)
      :db.install/_attribute
      boolean))


;; handle enums

(defn add-enum-value [ns val]
  "Add a new enum value."
  (let [tx-data 
        [[:db/add #db/id[:db.part/user]
          :db/ident (keyword (str ns "/" (name val)))]]
       ]
  (tx! tx-data))
  )

(defn defenums
  "Define a new enum with values."
  [ns & vals]
  (doseq [val vals]
    (add-enum-value ns val)
    ))

(defn remove-enum-value
  "Delete an enum value."
  [ns val]
  (let [tx-data 
        [[:db/retract :db.part/user
          :db/ident (keyword (str ns "/" (name val)))]]
       ]
  (tx! tx-data))
  )

;; wrappers for datomic.Util functions

(defn read-item 
  "Reads one item from source, returning it."
  [^String source]
  (Util/read source)
  )

(defn read-all
  "Read all forms in f, where f is any resource that can
   be opened by io/reader."
  [f]
  (Util/readAll (io/reader f)))

(defn transact-all!
  "Load and run all transactions from f, where f is any
   resource that can be opened by io/reader."
  [f]
  (doseq [txdata (read-all f)]
    (tx! txdata))
  :done)

(defn save-tx-data
  "Opens f with writer, writes tx-data to f, then
   closes f. Options passed to clojure.java.io/writer."
  [f tx-data & options]
  (apply spit (list* f tx-data options)))


;; Entity-Id protocol

(defprotocol Eid
  (eid [_]))

(extend-protocol Eid
  java.lang.Long
  (eid [n] n)

  datomic.Entity
  (eid [entity] (:db/id entity))
)

;; handle queries

(defn q 
  "Simple wrapper on datomic/q.
   Uses the current database implicit."
  [query & args]
  (apply d/q (list* query (get-db) args)))

(defn find-eids
  "Returns the list of entity-ids of each query result."
  [query & args]
  (->> (apply d/q (list* query (get-db) args))
       (mapv first)))
;; alias
(defonce qeids find-eids)

(defn find-es
  "Returns the entities returned by a query, assuming that
   all :find results are entity ids."
  [query & args]
  (->> (apply d/q (list* query (get-db) args))
       (mapv (fn [items]
               (mapv (partial d/entity (get-db)) items)))))
;; alias
(defonce qes find-es)


;; this from project 'datomic-simple'
(defn find-es-for-ns
  "Returns all entities for a given namespace."
  [nsp]
  (find-es '[:find ?e
             :in $ ?nsp
             :where [?e ?attr]
                    [?attr :db/ident ?name]
                    [(#(= (namespace %1) (name %2)) ?name ?nsp)]]
             nsp))

(defn find-es-m
  "Returns the entities returned by a query, assuming that
   all :find results are entity ids.
   Caution: looses entities lazyness."
  [query & args]
  (->> (apply d/q (list* query (get-db) args) )
       (map (fn [item]
              (-> (first item) eid->map)))))
;; alias
(defonce qesm find-es-m)

(defn find-eid
  "Gets the first returned entity id of a query."
  [query & args]
  (ffirst (apply d/q (list* query (get-db) args))))
;; alias
(defonce qeid find-eid)

(defn find-e
  "Gets the first returned entity of a query."
  [query & args]
  (eid->e (ffirst (apply d/q (list* query (get-db) args)))))
;; alias
(defonce qe find-e)

(defn find-e-m
  "Gets the first returned entity of a query."
  [query & args]
  (eid->map (ffirst (apply d/q (list* query (get-db) args)))))
;; alias
(defonce qem find-e-m)

(defn count-items
  "Returns the number of items in the query result."
  [query & args]
  (let [res (apply d/q (list* query (get-db) args))]
     (count res))
  )

(defn is-unique?
  "Checks if the query returns exactly one item."
  [query & args]
  (let [res (apply d/q (list* query (get-db) args))]
     (dtu/solo? (lazy-seq res))))


;; handle values

;; project 'day of datomic' name : "maybe"
(defn get-value
  "Returns the value of attr for e, or default if e does not possess
   any values for attr. Cardinality-many attributes will be
   returned as a set"
  [eid a default]
  (let [result (d/q '[:find ?v
                    :in $ ?e ?a
                    :where [?e ?a ?v]]
                  (get-db) eid a)]
    (if (seq result)
      (case (get-cardinality-of-attr (get-db) a)
            :db.cardinality/one (ffirst result)
            :db.cardinality/many (into #{} (map first result)))
      default)))

(defn get-all-values
  "Returns all attributes and values of an entity."
  [eid]
  (eid->e eid))


(defn get-all-values-as-map
  "Returns all attributes and values of an entity in a map."
  [eid]
  (eid->map eid))

(defn set-value
  "Sets the value v of attr a for an entity."
   [eid a v]
  (let [tx-data 
        [[:db/add eid a v]]
       ]
  (tx! tx-data))
  )

(defn set-values-by-map 
  "Sets the values of attrs for an entity, using a map."
  [eid av-map] 
  (let [tx-data 
        (vec (map #(vector :db/add eid (first %) (second %))
                  ;; id is not "updateable":
                  (dissoc av-map :db/id)))]  
    #_(pprint tx-data)
    (tx! tx-data))
  )

(defn set-values 
  "Sets the values of attributes for an entity."
   [eid & av-pairs] 
   (set-values-by-map eid (apply hash-map av-pairs))
   eid)
  
(defn change-values-in-map
  "Change values of selected attributes of an entity."
  [map & av-pairs]
  (apply assoc (list* map av-pairs)))

(defn defentity
  "Sets the values of attrs for a new entity."
   [& av-pairs]
   (let [temp-id (new-tempid)]
     (set-values-by-map temp-id(apply hash-map av-pairs))
     (tempid->eid temp-id)
    ))

;; handle tx-data 

(defn new-tx-map 
  [eid]
  {:db/id eid})

(defn add-to-tx-map
  [tx-data & attrs-and-vals]
  (merge tx-data (apply hash-map attrs-and-vals)))

(defn tx-map
  [eid attrs-and-vals]
  (apply add-to-tx-map (new-tx-map eid) attrs-and-vals))


;; handle ids

(defn add-id
  "add an entities id to the map of attributes."
  [eid attr-map]
  (assoc attr-map :db/id eid))
    
(defn add-new-id
  "add a new entity id to the map of attributes."
   ([attr-map] (add-new-id :db.part/user attr-map))
   ([part attr-map] (assoc attr-map :db/id (new-tempid part)))) 

(defn inject-new-ids 
  "add new entity ids to all the entities in a list."
  [part & es]
  (let [partition (or part :db.part/user)]
    (doseq [e es]
      (add-new-id partition e))))

(defn make-tempids 
  "Generate a list of n temp-ids."
  ([n]      (make-tempids :db.part/user n))
  ([part n] (take n (repeatedly #(new-tempid part)))))

;; misc.

(defn install
  "Install txdata and return the single new entity possessing attr"
  [txdata attr]
  (let [t (d/basis-t (:db-after (tx! txdata)))]
    (find-e '[:find ?e
              :in $ ?attr ?t
              :where [?e ?attr _ ?t]]
            attr (d/t->tx t))))

;; from project 'day of datomic'
(defn existing-values
  "Returns subset of values that already exist as unique
   attribute attr in db"
  [attr vals]
  (->> (d/q '[:find ?val
            :in $ ?attr [?val ...]
            :where [_ ?attr ?val]]
            attr vals)
       (map first)
       (into #{})))

;; from project 'day of datomic'
(defn assert-new-values
  "Assert emaps whose attr value does not already exist in db.
   Returns transaction result or nil if nothing to assert."
  [conn part attr emaps]
  (let [vals (mapv attr emaps)
        existing (existing-values (get-db) attr vals)]
    (when-not (= (count existing) (count vals))
      (->> emaps
           (remove #(existing (get attr %)))
           (map (fn [emap] (add-new-id part emap)))
           (tx!)
           ))))

(defn dump-entities-by-attr
  "dump all entities with a common attribute.
   TODO: How to dump ALL entities?"
  [attr]
  (let [entities 
        (find-es '[:find ?e
                   :in $ ?a
                   :where [?e ?a _]
                   ]
                 attr)]
    (doseq [i entities] 
      (pprint (-> i first e->map)))))
  
(defn dump-entity-by-eid
  "dump all datoms of an entity."
  [eid]
  (seq (d/datoms (get-db) :eavt eid)))

(defn dump-whole-db
  "dump all datoms in the database."
  []
  (seq (d/datoms (get-db) :eavt)))


(defn keyword->eid 
   "Returns the entity id associated with a symbolic keyword, or the id
    itself if passed."
  [k]
  (d/entid (get-db) k))

(defn eid->keyword 
   " Returns the keyword associated with an id, or the key itself if passed."
  [eid]
  (d/ident (get-db) eid))

;; some simple wrappers

(defn basis-t 
  "Returns the t of the most recent transaction 
   reachable via the current db value."
  []
  (d/basis-t (get-db)))

(defn next-t
  "Returns the t one beyond the highest reachable via this db value."
  []
  (d/next-t (get-db)))

(defn t->tx
  " Return the transaction id associated with a t value."
  [t]
  (d/t->tx t))

(defn tx->t
  "Return the t value associated with a transaction id."
  [tx]
  (d/tx->t tx))

(defn as-of-t
  "Returns the as-of point, or nil if none."
  []
  (d/as-of-t (get-db)))

(defn since-t
  "Returns the since point, or nil if none."
  []
  (d/since-t (get)))

(defn as-of
  "Returns the value of the database as of some point t, inclusive.
   t can be a transaction number, transaction ID, or Date."
  [t]
  (d/as-of (get-db) t))

(defn since
  "Returns the value of the database since some point t, exclusive
   t can be a transaction number, transaction ID, or Date."
  [t]
  (d/since (get-db) t))

(defn with
  "Applies tx-data to the database. It is as if the data was
   applied in a transaction, but the source of the database is
   unaffected. Takes data in the same format expected by transact, and
   returns a map similar to the map returned by transact."
  [tx-data]
  (d/with (get-db) tx-data))
;; aliases 
(def as-if   with)
(def what-if with)

(defn gc-storage
  "Allow storage to reclaim garbage older than a certain age."
  [older-than]
  (d/gc-storage (get-conn) older-than))

(defn request-index
  "Schedules a re-index of the database. The re-indexing happens
   asynchronously. Returns true if re-index is scheduled."
  []
  (d/request-index (get-conn)))

(defn index-range
  "Returns an Iterable range of datoms in index named by attrid,
   starting at start, or from beginning if start is nil, and ending
   before end, or through end of attr index if end is nil."
  [a start end]
  (d/index-range (get-db) start end))

(def add-listener d/add-listener)

(def
  ^{:doc "Constructs a semi-sequential UUID."} 
  squuid d/squuid)

(def 
  ^{:doc "Get the time part of a squuid (a UUID created by squuid), in
   the format of System.currentTimeMillis."}
   squuid-time-millis d/squuid-time-millis)


;; handling transaction queues

(defn get-tx-queue
  "Gets the data queue associated with the current connection, 
   creating one if necessary."
  []
  (if @*tx-report-queue*
      @*tx-report-queue*
      (reset! *tx-report-queue*
              (d/tx-report-queue (get-conn)))
      ))

(defn remove-tx-queue
  "Removes the queue associated with the current connection."
  []
  (d/remove-tx-report-queue (get-conn))
  (reset! *tx-report-queue* (atom nil)))

(defn get-tx-queue-entry
  "Polls for a queue entry, which is a record with the following keys:
   :db-before    value of database before the transaction
   :db-after     value of database after the transaction
   :tx-data      the transaction data in E/A/V/Tx form.
   Returns nil, if no entry is available, otherwise consumes 
   and returns the entry."
  [& [timeout, ^TimeUnit timeout-unit]]
  (when (get-tx-queue)
    (if timeout
      (.poll ^LinkedBlockingQueue (get-tx-queue), 
        (long timeout), timeout-unit)
      (.poll ^LinkedBlockingQueue (get-tx-queue)))))

(defn peek-tx-queue-entry
  "Looks for a queue entry, without consuming it.
   See also 'get-tx-queue-entry'"
  []
  (.peek ^LinkedBlockingQueue (get-tx-queue)))

(defn count-tx-queue-entries
  "Returns the number of entries in the transaction queue."
  []
  (.size ^LinkedBlockingQueue (get-tx-queue)))

(defn has-tx-queue-entry?
  "Checks if the transaction queue has entries."
  []
  (-> (count-tx-queue-entries) zero? not))

(defn print-tx-report
  "Sample of using a transaction report.
   Taken from datomic/samples/seattle/getting-started.clj ."
  []
  (when-let [report (get-tx-queue-entry)]
    (pprint (seq (d/q '[:find ?e ?aname ?v ?added
                        :in $ [[?e ?a ?v _ ?added]]
                        :where
                        [?e ?a ?v _ ?added]
                        [?a :db/ident ?aname]]
                      (:db-after report)
                      (:tx-data report))))))


;; handle database filtering

(defn filtered-db
  "Returns the value of the database containing only datoms
   satisfying the predicate."
  ([pred-fn] (d/filter (get-db) pred-fn))
  ([db pred-fn] (d/filter db pred-fn))
  )

(defn set-filtered-db 
  "Sets the current database as a filtered db."
  [pred-fn]
  (set-db (filtered-db (get-db) pred-fn)))

(defn is-filtered-db?
  "Returns true if db has had a filter set."
  ([] (d/is-filtered (get-db))) 
  ([db] (d/is-filtered db)))

(defmacro pred-fn
  "Creates a function suited as a predicate function.
   It captures the argument names as 'db' and 'datom'."
  [& body]
  `(fn [~'db ^Datom ~'datom] ~@body))


;; aggregates

(defmacro aggregate-fn
  "Creates a simple aggregate function on the fly.
   It captures the name of the only argument,
   which holds the aggregated list, as 'coll'."
  [& body]
  `(fn [~'coll] ~@body))


;; handle db history

(defn get-history
  "Returns a special database containing all assertions and
   retractions across time. See datomic.api/history for details."
  ([] (d/history (get-db)))
  ([name] (d/history (get-db name)))
  )

(defmacro with-history
  [& forms]
  `(binding [*current-db* (get-history)]
                ~@forms))

(defmacro with-history-of-db
  [db-name & forms]
  `(binding [*current-db* (get-history db-name)]
                ~@forms))

(defn is-history? 
  "true if db is returned from get-history."
  []
  (d/is-history (get-db))
  )

;; handling of rules and the rule base

(defn set-rulebase
  "Sets current rule base."
  [rules]
  (reset! *rule-base* rules))

(defn get-rulebase
  "Gets current rule base."
  []
  @*rule-base*)

(defn add-rule-to-rulebase 
  [rule]
  (swap! *rule-base* conj rule)
  rule
  )

(defn read-rulebase
  "Read all forms in f, where f is any resource that can
   be opened by io/reader."
  [f]
  (reset! *rule-base* (slurp f)))

(defn save-rulebase
  "Opens f with writer, writes current-ruleset to f, then
   closes f. Options passed to clojure.java.io/writer."
  [f & options]
  (apply spit f (get-rulebase) options))

(defn save-rules
  "Opens f with writer, writes rules to f, then
   closes f. Options passed to clojure.java.io/writer."
  [f rules & options]
  (apply spit f rules options))

(defn build-rule
  [name vars clauses]
  (apply vector
        (apply vector name vars)
        clauses))

(defn new-rule
  [name vars clauses]
  (add-rule-to-rulebase (build-rule name vars clauses)))

(defmacro defrule 
  [name vars & clauses]
  `(new-rule '~name '~vars '~clauses)
  )
   
(defmacro defrules 
  [& rules]
   `(doseq [rule# '~rules]
      (println rule#)
      (if (and (or (list? rule#)
                     (vector? rule#))
                 (> (count rule#) 2))
        (new-rule (first rule#) (second rule#) (nnext rule#))
        (println (str "Rule '" rule# "' ignored, wrong syntax." )))
    )
  )

;; database and transaction functions

(defmacro deffunction
  "Create and install a new database function."
  [name doc func-map]
  `(let [func# (d/function '~func-map)
         tx-data# 
          [{:db/id    (new-tempid)
            :db/ident '~name
            :db/doc   ~doc
            :db/fn    func#
           }]]
    (tx! tx-data#)
    func#))

(defn get-function
  "Looks up the current database for the named function."
  [name]
  (when-let [func (eid->e name)]
   (:db/fn func)
  ))

(defmacro invoke-function
  "Invoke a data function directly."
  [func & args]
  `(.invoke ~func ~@args ))

(defn invoke
  "Lookup the database function, and call it with args."
  [func-name & args]
 (d/invoke (get-db) func-name args))

(defn add-invoke 
  "Adds a data function call to a transaction."
  [tx-data func-name & args]
  (conj tx-data (vec (list* func-name args))))
  
;; transaction attributes

(defn add-tx-attr 
  "Adds an attribute that annotates the transaction."
  [tx-data attr val]
  (conj tx-data {":db/id" (new-tx-tempid) 
                 attr val}))

(defn get-tx-attr-value
  "Get an attribute value of a transaction."
  [tx attr]
  (let [val 
        (q '[:find ?v
          :in $ ?tx ?a
          :where [?tx :db/txInstant]
                 [?tx ?a ?v]
          ]
        tx attr)]
    (ffirst val))
  )


;; example from project 'day of datomic'

;; how many attributes and value types does this
;; schema use?
(defn count-attr-value-types 
  "How many attributes and value types does this
   schema use?"
  []
  (dtu/solo (q '[:find (count ?a) (count-distinct ?vt)
       :where
       [?a :db/ident ?ident]
       [?a :db/valueType ?vt]]
     (get-db))))


; from https://gist.github.com/3150938

(defn wrap-datomic
  "A Ring middleware that provides a request-consistent database connection and
  value for the life of a request."
  [handler db-uri db-name]
  (fn [request]
    (defdatabase db-uri db-name)
    (open-database db-name)
    (handler request)))

;;; Rules-based predicates

(defn predicate-from-rules
  "Returns a predicate fn that will return true when the predicate-rules (a subset of
   the source-rules) are satisfied, and false otherwise."
  [source-rules predicate-rules]
  (fn [entity]
    (when entity
      (boolean
        (ffirst
          (q (concat '[:find ?e
                       :in $ $id %
                       :where [(= ?e $id)]]
                     predicate-rules)
             (:db/id entity)
             source-rules))))))

;;; Map => Transaction Helpers

(defn entity?
  [entity]
  (boolean (:db/id entity)))

(defn entity-collection?
  [coll]
  (and
    (coll? coll)
    (not (map? coll))
    (every? :db/id coll)))

(defn children
  [entity]
  (filter entity? (vals entity)))

(defn unpack-ids
  [entity]
  (into {}
        (map (fn [[k v]]
               [k (cond
                    (entity? v) (:db/id v)
                    (entity-collection? v) (map :db/id v)
                    :default v)])
             entity)))

(defn e->tx-data
  "Flattens an entity with tree-like structure into
   transaction data."
  [entity]
  (->> entity
    (tree-seq #(or (entity? %)
                   (entity-collection? %))
              #(cond
                 (entity? %) (children %)
                 (entity-collection? %) (map children %)
                 :default nil))
    reverse
    (map unpack-ids)
    (into [])))

(defn eid->tx-data
  "Flattens an entity, identified by id, into
   transaction data."
  [eid]
  (e->tx-data (eid->e eid)))

;;; Query

(defn entities
  "Returns a set of entities from a [:find ?e ...] query."
  [query-results]
  (into #{}
        (map (comp eid->e first)
             query-results)))

(defn find-all
  "Returns the set of all results of query over sources."
  [query & sources]
  (entities
    (apply q query sources)))

;; from project 'lewis'

(def schema-query '[:find ?e :where [?e :db/valueType]])

(defn get-schema 
  []
  (q schema-query))

(defn get-schema-fully-realized
  []
  (find-es-m schema-query))

;; todo:
;; attribute history
;; Neuerungen in datomic: Aggregatfunktionen - max, random, sample ... 
;; Tests/Beispiele schreiben!

;;;;; trickery

;; find all attributes ?
;; [:find ?entity
;;  :where [_ :db.install/attribute ?v]
;;         [(.entity $ ?v) ?entity]] <---- !!!

;; find all entities  ?
;; [:find ?entity
;;  :in $ ?s 
;;  :where [?e :db/valueType]
;;         [?e :db/ident ?a]
;;         [(namespace ?a) ?ns] <--- !!
;;         [(= ?ns ?s)]
;;         [(.entity $ ?e) ?entity]]  <--- !!!

