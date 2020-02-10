(ns metabase.driver.sql-jdbc.execute
  "Code related to actually running a SQL query against a JDBC database and for properly encoding/decoding types going
  in and out of the database. Old, non-reducible implementation can be found in
  `metabase.driver.sql-jdbc.execute.old-impl`, which will be removed in a future release; implementations of methods
  for JDBC drivers that do not support `java.time` classes can be found in
  `metabase.driver.sql-jdbc.execute.legacy-impl`. "
  (:require [clojure.core.async :as a]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [metabase
             [driver :as driver]
             [util :as u]]
            [metabase.driver.sql-jdbc
             [connection :as sql-jdbc.conn]
             [sync :as sql-jdbc.sync]]
            [metabase.driver.sql-jdbc.execute.old-impl :as execute.old]
            [metabase.mbql.util :as mbql.u]
            [metabase.query-processor
             [context :as context]
             [interface :as qp.i]
             [store :as qp.store]
             [timezone :as qp.timezone]
             [util :as qputil]]
            [metabase.query-processor.util.reducible :as qp.util.reducible]
            [metabase.util.i18n :refer [trs]]
            [potemkin :as p])
  (:import [java.sql Connection JDBCType PreparedStatement ResultSet ResultSetMetaData Types]
           javax.sql.DataSource))

;; when we finally remove `execute.old`, we can move `set-parameter` into this namespace (and delete the rest)
(p/import-vars
 [execute.old set-parameter])

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                        SQL JDBC Reducible QP Interface                                         |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmulti connection-with-timezone
  "Fetch a Connection for a `database` with session time zone set to `timezone-id` (if supported by the driver.) The
  default implementation:

  1. Calls util fn `datasource` to get a c3p0 connection pool DataSource
  2. Calls `.getConnection()` the normal way
  3. Executes `set-timezone-sql` if implemented by the driver.

  `timezone-id` will be `nil` if a `report-timezone` Setting is not currently set; don't change the session time zone
  if this is the case.

  For drivers that support session timezones, the default implementation and `set-timezone-sql` should be considered
  deprecated in favor of implementing `connection-with-timezone` directly. This way you can set the session timezone
  in the most efficient manner, e.g. only setting it if needed (if there's an easy way for you to check this), or by
  setting it as a parameter of the connection itself (the default connection pools are automatically flushed when
  `report-timezone-id` changes).

  Custom implementations should set transaction isolation to the least-locking level supported by the driver, and make
  connections read-only (*after* setting timezone, if needed)."
  {:added    "0.35.0"
   :arglists '(^java.sql.Connection [driver database ^String timezone-id])}
  driver/dispatch-on-initialized-driver
  :hierarchy #'driver/hierarchy)

(defmulti ^PreparedStatement prepared-statement
  "Create a PreparedStatement with `sql` query, and set any `params`. You shouldn't need to override the default
  implementation for this method; if you do, take care to set options to maximize result set read performance (e.g.
  `ResultSet/TYPE_FORWARD_ONLY`); refer to the default implementation."
  {:added "0.35.0", :arglists '(^java.sql.PreparedStatement [driver ^java.sql.Connection connection ^String sql params])}
  driver/dispatch-on-initialized-driver
  :hierarchy #'driver/hierarchy)

(defmulti execute-query!
  "Execute a `PreparedStatement`, returning a `ResultSet`. Default implementation simply calls `.executeQuery()`. It is
  unlikely you will need to override this."
  {:added "0.35.0", :arglists '(^java.sql.ResultSet [driver ^java.sql.PreparedStatement stmt])}
  driver/dispatch-on-initialized-driver
  :hierarchy #'driver/hierarchy)

(defmulti column-metadata
  "Return a sequence of maps containing information about the corresponding columns in query results. The default
  implementation fetches this information via the result set metadata. It is unlikely you will need to override this."
  {:added "0.35.0", :arglists '([driver ^java.sql.ResultSetMetaData rsmeta])}
  driver/dispatch-on-initialized-driver
  :hierarchy #'driver/hierarchy)

(defmulti read-column-thunk
  "Return a zero-arg function that, when called, will fetch the value of the column from the current row."
  {:added "0.35.0", :arglists '([driver rs rsmeta i])}
  (fn [driver _ ^ResultSetMetaData rsmeta ^long col-idx]
    [(driver/dispatch-on-initialized-driver driver) (.getColumnType rsmeta col-idx)])
  :hierarchy #'driver/hierarchy)

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                  Default Impl                                                  |
;;; +----------------------------------------------------------------------------------------------------------------+

(defn datasource
  "Fetch the connection pool `DataSource` associated with `database`."
  {:added "0.35.0"}
  ^DataSource [database]
  (:datasource (sql-jdbc.conn/db->pooled-connection-spec database)))

(defn set-time-zone-if-supported!
  "Execute `set-timezone-sql`, if implemented by driver, to set the session time zone. This way of setting the time zone
  should be considered deprecated in favor of implementing `connection-with-time-zone` directly."
  {:deprecated "0.35.0"}
  [driver ^Connection conn ^String timezone-id]
  (when timezone-id
    (when-let [format-string (execute.old/set-timezone-sql driver)]
      (try
        (let [sql (format format-string (str \' timezone-id \'))]
          (log/debug (trs "Setting {0} database timezone with statement: {1}" driver (pr-str sql)))
          (.setReadOnly conn false)
          (with-open [stmt (.createStatement conn)]
            (.execute stmt sql)
            (log/tracef "Successfully set timezone for %s database to %s" driver timezone-id)))
        (catch Throwable e
          (log/error e (trs "Failed to set timezone ''{0}'' for {1} database" timezone-id driver)))))))

;; TODO - since we're not running the queries in a transaction, does this make any difference at all?
(defn set-transaction-level!
  "Set the connection transaction isolation level to the least-locking level supported by the DB. See
  https://docs.oracle.com/cd/E19830-01/819-4721/beamv/index.html for an explanation of these levels."
  {:added "0.35.0"}
  [driver ^Connection conn]
  (let [dbmeta (.getMetaData conn)]
    (loop [[[level-name ^Integer level] & more] [[:read-uncommitted Connection/TRANSACTION_READ_UNCOMMITTED]
                                                 [:repeatable-read  Connection/TRANSACTION_REPEATABLE_READ]
                                                 [:read-committed   Connection/TRANSACTION_READ_COMMITTED]]]
      (cond
        (.supportsTransactionIsolationLevel dbmeta level)
        (do
          (log/tracef "Set transaction isolation level for %s database to %s" (name driver) level-name)
          (try
            (.setTransactionIsolation conn level)
            (catch Throwable e
              (log/error e (trs "Error setting transaction isolation level for {0} database to {1}" (name driver) level-name)))))

        (seq more)
        (recur more)))))

(defmethod connection-with-timezone :sql-jdbc
  [driver database ^String timezone-id]
  (let [conn (.getConnection (datasource database))]
    (try
      (set-transaction-level! driver conn)
      (set-time-zone-if-supported! driver conn timezone-id)
      (doto conn
        (.setReadOnly true))
      (catch Throwable e
        (.close conn)
        (throw e)))))

(defn set-parameters!
  "Set parameters for the prepared statement by calling `set-parameter` for each parameter."
  {:added "0.35.0"}
  [driver stmt params]
  (dorun
   (map-indexed
    (fn [i param]
      (log/tracef "Set param %d -> %s" (inc i) (pr-str param))
      (set-parameter driver stmt (inc i) param))
    params)))

(defmethod prepared-statement :sql-jdbc
  [driver ^Connection conn ^String sql params]
  (let [stmt (.prepareStatement conn sql
                                ResultSet/TYPE_FORWARD_ONLY
                                ResultSet/CONCUR_READ_ONLY
                                ResultSet/CLOSE_CURSORS_AT_COMMIT)]
    (try
      (.setFetchDirection stmt ResultSet/FETCH_FORWARD)
      (set-parameters! driver stmt params)
      stmt
      (catch Throwable e
        (.close stmt)
        (throw e)))))

(defn- prepared-statement*
  ^PreparedStatement [driver conn sql params canceled-chan]
  ;; if canceled-chan gets a message, cancel the PreparedStatement
  (let [^PreparedStatement stmt (prepared-statement driver conn sql params)]
    (a/go
      (when (a/<! canceled-chan)
        (log/debug (trs "Query canceled, calling PreparedStatement.cancel()"))
        (u/ignore-exceptions
          (.cancel stmt))))
    stmt))

(defmethod execute-query! :sql-jdbc
  [_ ^PreparedStatement stmt]
  (.executeQuery stmt))

(defmethod read-column-thunk :default
  [_ ^ResultSet rs _ ^long i]
  ^{:name (format "(.getObject rs %d)" i)}
  (fn []
    (.getObject rs i)))

(defn- get-object-of-class-thunk [^ResultSet rs, ^long i, ^Class klass]
  ^{:name (format "(.getObject rs %d %s)" i (.getCanonicalName klass))}
  (fn []
    (.getObject rs i klass)))

(defmethod read-column-thunk [:sql-jdbc Types/TIMESTAMP]
  [_ rs _ i]
  (get-object-of-class-thunk rs i java.time.LocalDateTime))

(defmethod read-column-thunk [:sql-jdbc Types/TIMESTAMP_WITH_TIMEZONE]
  [_ rs _ i]
  (get-object-of-class-thunk rs i java.time.OffsetDateTime))

(defmethod read-column-thunk [:sql-jdbc Types/DATE]
  [_ rs _ i]
  (get-object-of-class-thunk rs i java.time.LocalDate))

(defmethod read-column-thunk [:sql-jdbc Types/TIME]
  [_ rs _ i]
  (get-object-of-class-thunk rs i java.time.LocalTime))

(defmethod read-column-thunk [:sql-jdbc Types/TIME_WITH_TIMEZONE]
  [_ rs _ i]
  (get-object-of-class-thunk rs i java.time.OffsetTime))

(defn- column-range [^ResultSetMetaData rsmeta]
  (range 1 (inc (.getColumnCount rsmeta))))

(defn- log-readers [driver ^ResultSetMetaData rsmeta fns]
  (log/trace
   (str/join
    "\n"
    (for [^Integer i (column-range rsmeta)]
      (format "Reading %s column %d %s (JDBC type: %s, DB type: %s) with %s"
              driver
              i
              (pr-str (.getColumnName rsmeta i))
              (or (u/ignore-exceptions
                    (.getName (JDBCType/valueOf (.getColumnType rsmeta i))))
                  (.getColumnType rsmeta i))
              (.getColumnTypeName rsmeta i)
              (let [f (nth fns (dec i))]
                (or (:name (meta f))
                    f)))))))

(defn- old-read-column-thunk
  "Implementation of deprecated method `old/read-column` if a non-default one is available."
  [driver rs ^ResultSetMetaData rsmeta ^Integer i]
  (let [col-type (.getColumnType rsmeta i)
        method   (get-method execute.old/read-column [driver col-type])
        default? (some (fn [dispatch-val]
                         (= method (get-method execute.old/read-column dispatch-val)))
                       [:default
                        [::driver/driver col-type]
                        [:sql-jdbc col-type]])]
    (when-not default?
      ^{:name (format "old-impl/read-column %s %d" driver i)}
      (fn []
        (method driver nil rs rsmeta i)))))

(defn- read-row-fn [driver rs ^ResultSetMetaData rsmeta]
  (let [fns (for [i (column-range rsmeta)]
              (or (old-read-column-thunk driver rs rsmeta i)
                  (read-column-thunk driver rs rsmeta (long i))))]
    (log-readers driver rsmeta fns)
    (apply juxt fns)))

(defmethod column-metadata :sql-jdbc
  [driver ^ResultSetMetaData rsmeta]
  (mapv
   (fn [^Integer i]
     (let [col-name     (.getColumnLabel rsmeta i)
           db-type-name (.getColumnTypeName rsmeta i)
           base-type    (sql-jdbc.sync/database-type->base-type driver (keyword db-type-name))]
       (log/tracef "Column %d '%s' is a %s which is mapped to base type %s for driver %s\n"
                   i col-name db-type-name base-type driver)
       {:name      col-name
        ;; TODO - disabled for now since it breaks a lot of tests. We can re-enable it when the tests are in a better
        ;; state
        #_:original_name #_(.getColumnName rsmeta i)
        #_:jdbc_type #_ (u/ignore-exceptions
                          (.getName (JDBCType/valueOf (.getColumnType rsmeta i))))
        #_:db_type   #_db-type-name
        :base_type   base-type}))
   (column-range rsmeta)))

(defn reducible-rows
  "Returns an object that can be reduced to fetch the rows and columns in a `ResultSet` in a driver-specific way (e.g.
  by using `read-column-thunk` to fetch values)."
  {:added "0.35.0"}
  [driver ^ResultSet rs ^ResultSetMetaData rsmeta canceled-chan]
  (let [read-row (read-row-fn driver rs rsmeta)
        row-fn   (fn []
                   (when (.next rs)
                     (read-row)))]
    (qp.util.reducible/reducible-rows row-fn canceled-chan)))

(defn execute-reducible-query
  "Default impl of `execute-reducible-query` for sql-jdbc drivers."
  {:added "0.35.0", :arglists '([driver query context respond])}
  [driver {{sql :query, params :params} :native, :as outer-query} context respond]
  (let [remark   (qputil/query->remark outer-query)
        sql      (str "-- " remark "\n" sql)
        max-rows (or (mbql.u/query->max-rows-limit outer-query)
                     qp.i/absolute-max-results)]
    (with-open [conn (connection-with-timezone driver (qp.store/database) (qp.timezone/report-timezone-id-if-supported))
                stmt (doto (prepared-statement* driver conn sql params (context/canceled-chan context))
                       (.setMaxRows max-rows))
                rs   (execute-query! driver stmt)]
      (let [rsmeta           (.getMetaData rs)
            results-metadata {:cols (column-metadata driver rsmeta)}]
        (respond results-metadata (reducible-rows driver rs rsmeta (context/canceled-chan context)))))))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                       Convenience Imports from Old Impl                                        |
;;; +----------------------------------------------------------------------------------------------------------------+

(p/import-vars
 [execute.old
  ;; interface (set-parameter is imported as well at the top of the namespace)
  set-timezone-sql
  read-column
  ;; util fns
  do-with-try-catch
  read-columns
  set-parameters
  ;; default impl of driver method
  execute-query])
