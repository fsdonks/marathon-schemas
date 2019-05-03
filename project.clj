(defproject marathon-schemas "4.1.6-SNAPSHOT"
  :description "data schemas, specifications, and validation tools for marathon"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.0"]                 
                 [spork "0.2.1.1-SNAPSHOT"]
                 ;;used to generate ns keys for spec...
                 ;;will likely shift to metosin spec-tools
                 ;;in very near future to obviate this.
                 [irresponsible/spectra "0.2.1"]
                 [metosin/spec-tools "0.9.2-alpha1"]
                 ;;maintaining backwards compat with 1.8
                 ;;will likely migrate in near future.
                 #_[clojure-future-spec "1.9.0-beta4"]
                 [org.clojure/test.check "0.10.0-alpha4"]])
