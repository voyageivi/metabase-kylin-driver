(ns metabase.driver.kylin
  "Driver for Kylin databases"
  (:require [clojure.java.jdbc :as jdbc]
    ;[jdbc.pool.c3p0 :as pool]
            [clojure.string :as str]
            [honeysql.core :as hsql]
            [java-time :as t]
            [metabase
             [driver :as driver]]
            [metabase.driver.sql-jdbc
             [common :as sql-jdbc.common]
             [connection :as sql-jdbc.conn]
             [sync :as sql-jdbc.sync]
             [execute :as sql-jdbc.exec]]
            [metabase.driver.sql.query-processor :as sql.qp]
            [metabase.util
             [honeysql-extensions :as hx]]
            [clojure.tools.logging :as log]
            [metabase.util :as u]
            [metabase.connection-pool :as connection-pool]
            [metabase.mbql.util :as mbql.u]
            [metabase.driver.sql.util.unprepare :as unprepare]
            [metabase.query-processor.util :as qputil]
            [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
            [metabase.query-processor.timezone :as qp.timezone]
            [metabase.driver.hive-like :as hive-like])
  (:import [java.sql DatabaseMetaData PreparedStatement]
           (java.time OffsetDateTime ZonedDateTime LocalDate)
           (java.util Date)))



(driver/register! :kylin, :parent :hive-like)

(defmethod sql-jdbc.conn/connection-details->spec :kylin
  [_ {:keys [user password dbname host port]
      :or   {user "ADMIN", password "KYLIN", dbname "", host "localhost", port "7070"}
      :as   details}]
  (-> {:classname   "org.apache.kylin.jdbc.Driver"
       :subprotocol "kylin"
       :subname     (str "//" host ":" port "/" dbname)
       :password    password
       :user        user}
      (sql-jdbc.common/handle-additional-options details, :seperator-style :url)))

(defn- get-tables
  [^DatabaseMetaData md, ^String schema-or-nil, ^String db-name-or-nil]
  (vec (jdbc/metadata-result (.getTables md db-name-or-nil schema-or-nil "%" (into-array ["TABLE"])))))

(defn- post-filtered-active-tables
  [driver, ^DatabaseMetaData md, & [db-name-or-nil]]
  (set (for [table (filter #(not (contains? (sql-jdbc.sync/excluded-schemas driver) (:table_schem %)))
                           (get-tables md db-name-or-nil nil))]
         (let [remarks (:remarks table)]
           {:name        (:table_name table)
            :schema      (:table_schem table)
            :description (when-not (str/blank? remarks) remarks)}))))

(defmethod driver/describe-database :kylin
  [driver database]
  (jdbc/with-db-metadata [md (sql-jdbc.conn/db->pooled-connection-spec database)]
                         {:tables (post-filtered-active-tables driver md)}))

(defmethod driver/describe-table :kylin [driver database table]
  (jdbc/with-db-metadata [md (sql-jdbc.conn/db->pooled-connection-spec database)]
                         (->>
                           (assoc (select-keys table [:name :schema]) :fields (sql-jdbc.sync/describe-table-fields md driver table))
                           (sql-jdbc.sync/add-table-pks md))))

(defn- sub-type-name
  [type_name]
  (def pos (.indexOf (name type_name) "("))
  (if (> pos 0)
    (keyword (subs (name type_name) 0 pos))
    type_name))

(defmethod sql.qp/date [:kylin :day] [_ _ expr] expr)

(defmethod driver/describe-table-fks :kylin
  [_ database table & [^String db-name-or-nil]]
  (jdbc/with-db-metadata [md (sql-jdbc.conn/db->pooled-connection-spec database)]
                         (with-open [rs (.getImportedKeys md db-name-or-nil, ^String (:schema table), ^String (:name table))]
                           (set
                             (for [result (jdbc/metadata-result rs)]
                               {:fk-column-name   (:fkcolumn_name result)
                                :dest-table       {:name   (:pktable_name result)
                                                   :schema (:pktable_schem result)}
                                :dest-column-name (:pkcolumn_name result)})))))

(defmethod sql.qp/quote-style :kylin [_] :mysql)

(defmethod driver/supports? [:kylin :full-join] [_ _] false)
(defmethod driver/supports? [:kylin :right-join] [_ _] false)
(defmethod driver/supports? [:kylin :set-timezone] [_ _] true)

(defmethod driver/supports? [:kylin :expressions] [_ _] true)
(defmethod driver/supports? [:kylin :native-parameters] [_ _] true)
(defmethod driver/supports? [:kylin :expression-aggregations] [_ _] true)
(defmethod driver/supports? [:kylin :binning] [_ _] true)



(defmethod unprepare/unprepare-value [:kylin Date]
  [_ t]
  (format "date '%s'" (t/format "yyyy-MM-dd" t)))
;
(defmethod unprepare/unprepare-value [:kylin LocalDate]
  [_ t]
  (format "date '%s'" (t/format "yyyy-MM-dd" t)))

(defmethod unprepare/unprepare-value [:kylin OffsetDateTime]
  [_ t]
  (format "date '%s'" (t/format "yyyy-MM-dd" t)))

(defmethod unprepare/unprepare-value [:kylin ZonedDateTime]
  [_ t]
  (format "date '%s'" (t/format "yyyy-MM-dd" t)))


;(defmethod sql-jdbc.execute/set-parameter [:kylin LocalDate]
;  [driver ps i t]
;  (log/info "test" i t)
;  (let [param (t/local-date-time t (t/local-time 0))]
;    (log/info "param:" t param)
;    (sql-jdbc.execute/set-parameter driver ps i (unprepare/unprepare-value :kylin t)))
;  )
;
;(defmethod sql-jdbc.execute/set-parameter [:kylin Date]
;  [driver ps i t]
;  (log/info "test2" i t)
;  (sql-jdbc.execute/set-parameter driver ps i (t/local-date-time t (t/local-time 0))))




(defmethod driver/execute-reducible-query :kylin
  [driver {:keys [database settings], {sql :query, :keys [params], :as inner-query} :native, :as outer-query} context respond]
  (let [inner-query (-> (assoc inner-query
                          :remark (qputil/query->remark :kylin outer-query)
                          :query (if (seq params)
                                   (binding [hive-like/*param-splice-style* :friendly]
                                     (unprepare/unprepare driver (cons (str/replace sql "`" "") params)))
                                   (str/replace sql "`" ""))
                          :max-rows (mbql.u/query->max-rows-limit outer-query))
                        (dissoc :params))
        query (assoc outer-query :native inner-query)]
    ((get-method driver/execute-reducible-query :sql-jdbc) driver query context respond)))
