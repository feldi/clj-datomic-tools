(ns clj.datomic.tools.samples.sql
  ^{:author "Peter Feldtmann"
    :doc "SQL-style examples using the convenience functionality for the Datomic database"}
  (:use clojure.pprint)
  (:require [clj.datomic.tools.core :as dt]))

(set! *print-length* 20)

(defn reload []
  "Helper for easy development at the repl."
   (use 'datomic-tools.samples.sql :reload-all)
  )

;; create a temporary database 
(dt/scratch-database)

;; define schema: "table" named 'address' with "fields"
(dt/defattr 'oid     :ns 'address 
  :ext-key? true 
  :doc "object id, the 'primary key'")
(dt/defattr 'code  :ns 'address
  :doc "address code")
(dt/defattr 'name    :ns 'address
  :doc "name of address")
(dt/defattr 'street  :ns 'address
  :doc "street name")
(dt/defattr 'zipcode :ns 'address
  :doc "zip code")
(dt/defattr 'city    :ns 'address
  :doc "city name")

;; fill "table" with some data
(dt/defentity 
  :address/oid "oid1"
  :address/code "addr1"
  :address/name "adress one")
(dt/defentity
  :address/oid "oid2"
  :address/code"addr2" 
  :address/name "address two")
(dt/defentity
  :address/oid "oid3"
  :address/code "addr3"
  :address/name "address three")

;; "select" from "table" by "primary key"
(def e (dt/find-e '[:find ?e :in $ ?v :where [?e :address/oid ?v]] "oid2"))

;; some stuff dealing with VO's = Value Objects:

;; get VO from entity
(def vo (dt/get-all-values-as-map (dt/eid e)))
;; change some data in VO
(def changed-vo (dt/change-values-in-map vo 
                   :address/name "name2"
                   :address/code "cd2"))
;; write the changed data back to DB
(dt/set-values-by-map (dt/eid e) changed-vo)

;; verify manually if it has changed
(pprint (dt/key->touched :address/oid "oid2")) 

;; delete the temporary database
#_(dt/delete-database)

'done


 