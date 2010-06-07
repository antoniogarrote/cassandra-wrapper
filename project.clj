(defproject cassandra-wrapper "0.6.3-SNAPSHOT"
  :description "A wrapper for the cassandra Thrift interface trying to keep up with cassandra trunk"
  :dependencies [[org.clojure/clojure "1.2.0-master-SNAPSHOT"]
                 [org.clojure/clojure-contrib "1.2.0-SNAPSHOT"]
                 [cassandra-wrapper-deps "0.6.3-SNAPSHOT"]
		 [org.slf4j/slf4j-log4j12 "1.5.8"]
		 [log4j "1.2.14"]
		 [org.slf4j/slf4j-api "1.5.8"]]
  :dev-dependencies [[leiningen/lein-swank "1.2.0-SNAPSHOT"]])
