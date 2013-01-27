;; This file contains code examples for the datomic-tools project. They are
;; written in clojure, for use with Datomic's interactive repl. You can
;; start the repl by running 'bin/repl' from the datomic directory.
;; Once the repl is running, you can copy code into it or, if invoke it
;; directory from your editor, based on your configuration.

(ns
  ^{:author "Peter Feldtmann"
    :doc "Examples of using the convenience functionality for the Datomic database"}
  clj.datomic.tools.samples.demo
  (:use clojure.pprint)
  (:require [clj.datomic.tools.core :as dt]))


(set! *print-length* 20)

(defn reload []
  "Helper for easy development at the repl."
   (use 'datomic-tools.samples.demo :reload-all))


;; create a temporary database 
(dt/scratch-database)

;; parse and load schema file
(dt/transact-all! "src/resources/samples/seattle/seattle-schema.dtm")
  
;; parse and load seed data file
(def tx-datas (dt/read-all "src/resources/samples/seattle/seattle-data0.dtm"))

; read-all returns a vector of tx-data, so lets get the first
(def tx-data (first tx-datas)) 

;; submit seed data transaction
(dt/tx! tx-data)

;; find all communities, return entity ids
(def results (dt/find-eids '[:find ?c :where [?c :community/name]]))

(count results)

;; get first entity id in results and make an entity 
(def id (first results))
(def entity (dt/eid->e id))

;; display the entity's keys
(keys entity)

;; display the value of the entity's community name
(:community/name entity)

;; make it a regular map
(def entity-map (dt/e->map entity))

(keys entity-map)
(vals entity-map)


;; find all communities, return entities

;; lazy
(def results (dt/find-es '[:find ?c :where [?c :community/name]]))
(count results)

;; not lazy
(def results (dt/find-es-m '[:find ?c :where [?c :community/name]]))
(count results)
(second results)

;; this works! - example from: day-of-datomic, hello_world.clj
(def tx-result
  (dt/tx!
   [[:db/add (dt/new-tempid :db.part/user)
     :db/doc "Hello world"]]))  

(def q-result (dt/q '[:find ?e
                      :where [?e :db/doc "Hello world"]]
                    ))

;; schema itself is data
(def doc-entity (dt/eid->e :db/doc))

(dt/e->touched doc-entity)

;; examples finished.

;; delete the temporary database
(dt/delete-database)

'done

