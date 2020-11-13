(defproject metabase/kylin-driver "1.1.0-SNAPSHOT-3.25.2"
            :min-lein-version "2.5.0"

            :dependencies
            [[org.apache.kylin/kylin-jdbc "2.6.4"]]


            :profiles
            {:provided
             {:dependencies [[metabase-core "1.0.0-SNAPSHOT"]]}


             :uberjar
             {:auto-clean    true
              :aot           :all
              :javac-options ["-target" "1.8", "-source" "1.8"]
              :target-path   "target/%s"
              :uberjar-name  "kylin.metabase-driver.jar"}})
