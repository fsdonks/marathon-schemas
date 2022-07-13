;;This is an initial hack at writing generative
;;testing and validation specs for various
;;MARATHON constructs.  note: much of the
;;primitives, particularly stuff that wraps
;;spork tables and schemas, will likely
;;be moved to spork proper.  Only the
;;MARATHON-specific stuff will live
;;here long term.
(ns marathon.spec
  (:require [clojure.spec.alpha :as s]
            [irresponsible.spectra :as ss]
            [clojure.spec.gen.alpha :as gen]
            [clojure.spec.test.alpha :as stest]
            [clojure.string :as str]
            [marathon.schemas :as schemas]
            [clojure.set :as cset])
  )

;;let's start by validating demand records.
;;We'll defin a function that can map a
;;spork schema to a spec.

;;This has the benefit of validating and
;;providing a basis for generating
;;random records via s/conform.

(defn maybe-type
  "Allows us to return a value for pred or sometimes return nil
  instead such as when a user leaves a field blank in Excel.  tag is
  just an identifier for s/or and pred is the datatype.  Defines the
  spec and returns the qualified keyword for the spec."
  [tag pred]
  (let [k (keyword "marathon.spec" (name tag))]
  (eval `(s/def ~k (s/or ~tag ~pred
                         :nil nil?)))
  k))

(s/def ::clojure-number
  number?)

(s/def ::clojure-primitive
  (s/or
   :keyword   keyword?
   :symbol    symbol?
   :string    string?
   :character char?
   :numeric   ::clojure-number))

(s/def ::clojure-collection
  (s/or
   :list       list?
   :vector     vector?
   :hash-map   map?
   :hash-set   set?
   :double     double?
   :sequence   seq?
   :collection coll?))

(s/def ::clojure-expression
  (s/or :primitive ::clojure-primitive
        :collection ::clojure-collection))

;;[:forward 3]
(s/def :marathon.spec/bin (s/cat :bin-name (s/or :key? keyword?
                                                 :string?  string?)
                                 :count int?))
(s/def :marathon.spec/bins (s/+ :marathon.spec/bin))

;;Preprocessing supply functions
;;Dispatch on the preprocessing supply function symbol for now.
(defmulti pre-function (fn [[func-symbol & args]]  func-symbol))

(defmethod pre-function 'align-units [_]
  ;;bin spec here
  ;;Only one arg of bins
  (s/tuple #{'align-units} (s/coll-of
  :marathon.spec/bins :count 1)))

(s/def :marathon.spec/preprocessor (s/multi-spec pre-function
                                                ;;Return the value
                                                ;;that was generated
                                                ;;by the spec
                                                (fn retag
                                                  [gen-val
                                                   dispatch-tag]
                                                  gen-val)
                                                ))

(s/def :marathon.spec/preprocess (s/and
                                      (s/*
                                       :marathon.spec/preprocessor)
                                      ;;Don't repeat the same
                                      ;;preprocessing function for
                                      ;;now.  Maybe there will be a
                                      ;;use case for that in the future.
                                      #(if (empty? %) true (apply
                                                            distinct?
                                      (map first %)))))

                                      
;; (s/def ::text string?)
;; (s/def ::double double?)
;; (s/def ::double? (maybe-type :double? float?))
;; (s/def ::date inst?)
;; (s/def ::long integer?)
;; (s/def ::int integer?)
;; (s/def ::int? (maybe-type :int? integer?))
;; (s/def ::number number?)

;;parsing defaults defined by spork.util.parsing
(def specs
  '{:float? (maybe-type :float? float?)
    :date   inst?
    :long   integer?
    :double double?
    :double? (maybe-type :double? float?)
    :int    integer?
    :number number?
    :int?   (maybe-type :int? integer?)
    :clojure ::clojure-expression
    :float  float?
    :symbol symbol?
    :string string?
    :long?  (maybe-type :long? integer?)
    :keyword keyword?
    :literal ::clojure-expression
    :code    ::clojure-expression
    :boolean boolean?
    :text   string?
    })

(defn kw->vec [k]
  [(name k) (namespace k)])
;;the trick with specs is to use qualified keys.
;;what we'll do is create a fake namespace,
;;and prepend it to all the keywords.
;;when validating, we'll use the schema's
;;namespace to check the keys.
(defmacro schema->spec
  "Defines a named schema in the current namespace,
   according to name.  Similar to s/def, except
   that the spec now references a schema map, or
   a var bound to a schema map.  When validating,
   the schema will check for the presence of
   the keys in the map, and will validate using
   the specs associated with the parsers defined in
   marathon.spec/specs.
   Returns a spec defined as name.  If name is already
   a qualified keyword, acts identically to s/def,
   otherwise creates a ns-qualified keyword from the
   textual or unqualified keyword name."
  [name schema]
  (let [[name space] (if (string? name)
                       [name nil]
                       (kw->vec name))
        qualified    (keyword (or space (str *ns*))
                              name)
        schema (cond (map? schema) schema
                     (symbol? schema) (eval schema)
                     :else (throw (Exception.
                                   (str [:invalid-schema schema]))))
        optional (:optional (meta schema))
        required (cset/difference (set (keys schema)) optional)
        ]
    `(do (ss/ns-defs ~name
                     ;;name and schema
         ~@(apply concat
             (for [[fld parser] (seq schema)]
               [fld (or (get specs parser)
                        (throw (Exception. (str [:unknown-field-type parser
                                                 :for-field
                                                 fld]))))])))
         ;;just send qualified and schema
       (s/def ~qualified (ss/ns-keys ~name :req-un [~@required]
                                     :opt-un [~@optional]
                                     )))))

;;Fields to ignore in specs.
(def ignored-fields
  {:PolicyRecords [:TimeStamp :Remarks]})

(defn patch-schema [nm schema]
  [nm (if-let [ignored (get ignored-fields nm)]
        (reduce dissoc schema ignored)
        schema)])

(def marathon-specs
  (doseq [[k v]  schemas/known-schemas]
    (let [[nm schema] (patch-schema k v)]
      (eval `(schema->spec ~nm ~schema)))))

(defn known-specs []
  (into []
        (for [nm (keys schemas/known-schemas)]
          (keyword (str *ns*) (name nm))
          )))

(defn validate-records
  "Validate a set of records using an associated m4 spec.
   If any invalid records are found, throws and exception
   and explanation.  Otherwise, returns nil."
  [nm xs]
  (let [spec (s/get-spec (keyword "marathon.spec" (name nm)))
        valid-row? (fn [r]
                     (s/valid? spec r))
        explain!  (fn [r]
                    (s/explain spec r))]
    (some->> (transduce (map-indexed vector)
                        (completing (fn [acc [idx r]]
                                      (if (valid-row? r)
                                        acc
                                        (do (explain! r)
                                            (reduced {:err (Exception. (str [:invalid-row!
                                                                             :at idx
                                                                             :in nm
                                                                             ]))
                                                      :idx idx})))))
                        nil
                        xs)
             (:err)
             (throw)
             )))

(defn validate-tables [xs]
  (doseq [[nm t] xs]
    (validate-records nm t)))

;;I think we should really define a MARATHON project spec as a map of
;;the project tables instead of doing this manually with this function.
(defn validate-project
  "Beta implementation of project-level
   validation.  Soon to be introduced into
   the load-project pipeline for marathon.project."
  [p]
  (validate-tables (dissoc (:tables p) :Parameters)))



;;Write spec for requirements analysis such that forward stationed
;;supply is >= forward stationed demand.

(s/def :marathon.spec/Tags (s/keys :opt-un
                                   [:marathon.spec/preprocess]))
;;can't redefine Tags this way, so we need another solution
;;commit for now. Then look into searching for marathon.spec/Tags when
;;defining the marathon specs
(s/def :DemandRecords/Tags2 (s/and :marathon.spec/Tags :DemandRecords/Tags ))

;;SupplyRecords/Component #{"AC", "NG", "RA"}
;;SupplyRecords can't have two records for the same SRC, Component
