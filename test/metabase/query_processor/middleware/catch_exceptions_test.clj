(ns metabase.query-processor.middleware.catch-exceptions-test
  (:require [clojure.core.async :as a]
            [clojure.test :refer :all]
            [metabase
             [query-processor :as qp]
             [test :as mt]]
            [metabase.models
             [permissions :as perms]
             [permissions-group :as group]]
            [metabase.query-processor.error-type :as error-type]
            [metabase.query-processor.middleware.catch-exceptions :as catch-exceptions]
            [metabase.test.data :as data]
            [metabase.test.data.users :as test-users]
            [schema.core :as s]))

(deftest exception-chain-test
  (testing "Should be able to get a sequence of exceptions by following causes, with the top-level Exception first"
    (let [e1 (ex-info "1" {:level 1})
          e2 (ex-info "2" {:level 2} e1)
          e3 (ex-info "3" {:level 3} e2)]
      (is (= [e1 e2 e3]
             (#'catch-exceptions/exception-chain e3))))))

(deftest exception-response-test
  (testing "Should nicely format a chain of exceptions, with the top-level Exception appearing first"
    (testing "lowest-level error `:type` should be pulled up to the top-level"
      (let [e1 (ex-info "1" {:level 1})
            e2 (ex-info "2" {:level 2, :type error-type/qp} e1)
            e3 (ex-info "3" {:level 3} e2)]
        (is (= {:status     :failed,
                :class      clojure.lang.ExceptionInfo
                :error      "1"
                :stacktrace true
                :error_type :qp
                :ex-data    {:level 1}
                :via        [{:status     :failed
                              :class      clojure.lang.ExceptionInfo
                              :error      "2"
                              :stacktrace true
                              :ex-data    {:level 2, :type :qp}
                              :error_type :qp}
                             {:status     :failed
                              :class      clojure.lang.ExceptionInfo
                              :error      "3"
                              :stacktrace true
                              :ex-data    {:level 3}}]}
               (-> (#'catch-exceptions/exception-response e3)
                   (update :stacktrace sequential?)
                   (update :via (fn [causes]
                                  (for [cause causes]
                                    (update cause :stacktrace sequential?)))))))))))


(defn- catch-exceptions
  ([run]
   (catch-exceptions run {}))

  ([run query]
   (mt/test-qp-middleware catch-exceptions/catch-exceptions query {} [] {:run run})))

(deftest no-exception-test
  (testing "No Exception -- should return response as-is"
    (is (= {}
           (catch-exceptions
            (fn []))))))

(deftest sync-exception-test
  (testing "if the QP throws an Exception (synchronously), should format the response appropriately"
    (is (= {:status     :failed
            :class      java.lang.Exception
            :error      "Something went wrong"
            :stacktrace true
            :json_query {}
            :row_count  0
            :data       {:rows []
                         :cols []}}
           (-> (catch-exceptions (fn [& _] (throw (Exception. "Something went wrong"))))
               (update :stacktrace boolean))))))

#_(deftest async-exception-test
  (testing "if an Exception is returned asynchronously by `raise`, should format it the same way"
    (is (= {:status     :failed
            :class      java.lang.Exception
            :error      "Something went wrong"
            :stacktrace true
            :json_query {}
            :row_count  0
            :data       {:rows []
                         :cols []}}
           (-> (catch-exceptions (fn [_ _ {:keys [raisef]}] (a/>!! raise-chan (Exception. "Something went wrong"))))
               (update :stacktrace boolean))))))

(deftest include-query-execution-info-test
  (testing "Should include info from QueryExecution if sent to our secret `query-execution-chan`"
    (is (= {:status     :failed
            :class      java.lang.Exception
            :error      "Something went wrong"
            :stacktrace true
            :json_query {}
            :row_count  0
            :data       {:rows []
                         :cols []}
            :a          100
            :b          200}
           (-> (catch-exceptions
                (fn [_ _ {:keys [query-execution-chan raise-chan]}]
                  {}
                  (a/>!! query-execution-chan
                         {:a            100
                          :b            200
                          ;; these keys should all get removed
                          :result_rows  300
                          :hash         400
                          :executor_id  500
                          :card_id      600
                          :dashboard_id 700
                          :pulse_id     800
                          :native       900})
                  (a/>!! raise-chan (Exception. "Something went wrong"))))
               (update :stacktrace boolean))))))

(deftest permissions-test
  (data/with-temp-copy-of-db
    (perms/revoke-permissions! (group/all-users) (data/id))
    (perms/grant-permissions! (group/all-users) (data/id) "PUBLIC" (data/id :venues))
    (testing (str "If someone doesn't have native query execution permissions, they shouldn't see the native version of "
                  "the query in the error response")
      (is (schema= {:native (s/eq nil), :preprocessed (s/pred map?), s/Any s/Any}
                   (test-users/with-test-user :rasta
                     (qp/process-userland-query
                      (data/mbql-query venues {:fields [!month.id]}))))))

    (testing "They should see it if they have ad-hoc native query perms"
      (perms/grant-native-readwrite-permissions! (group/all-users) (data/id))
      (is (schema= {:native       (s/eq {:query  (str "SELECT parsedatetime(formatdatetime(\"PUBLIC\".\"VENUES\".\"ID\", 'yyyyMM'), 'yyyyMM') "
                                                      "AS \"ID\" FROM \"PUBLIC\".\"VENUES\" LIMIT 1048576")
                                         :params nil})
                    :preprocessed (s/pred map?)
                    s/Any         s/Any}
                   (test-users/with-test-user :rasta
                     (qp/process-userland-query
                      (data/mbql-query venues {:fields [!month.id]}))))))))
