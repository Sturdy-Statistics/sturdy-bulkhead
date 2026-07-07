(ns sturdy.bulkhead-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [sturdy.bulkhead :as bulkhead]))

(deftest invalid-options-test
  (testing "num-workers validation"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #":num-workers must be a positive integer"
                          (bulkhead/start-pool! {:num-workers 0})))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #":num-workers must be a positive integer"
                          (bulkhead/start-pool! {:num-workers -1})))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #":num-workers must be a positive integer"
                          (bulkhead/start-pool! {:num-workers 1.5}))))
  (testing "queue-size validation"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #":queue-size must be a non-negative integer"
                          (bulkhead/start-pool! {:queue-size -1})))))

(deftest successful-request-test
  (let [pool (bulkhead/start-pool! {:num-workers 1 :queue-size 1})
        handler (fn [_req] {:status 200 :body "OK"})
        wrapped (bulkhead/wrap-compute-bound handler pool)]
    (is (= {:status 200 :body "OK"} (wrapped {:request-method :get})))
    (bulkhead/stop-pool! pool)))

(deftest reject-queue-full-test
  (let [pool (bulkhead/start-pool! {:num-workers 1 :queue-size 1})
        p (promise)
        started1 (promise)
        handler (fn [_req] (deliver started1 true) @p {:status 200})
        wrapped (bulkhead/wrap-compute-bound handler pool)

        f1 (future (wrapped {:request-id "1"}))]

    @started1

    (let [f2 (future (wrapped {:request-id "2"}))]
      ;; Wait a tiny bit for f2 to enter the queue. Since f2 doesn't have a hook
      ;; into the worker, it just sits in the core.async channel.
      (Thread/sleep 50)

      (let [res3 (wrapped {:request-id "3"})]
        (is (= 503 (:status res3)))
        (is (= "3" (-> res3 :body :id)))

        (deliver p true)
        @f1
        @f2
        (bulkhead/stop-pool! pool)))))

(deftest shutdown-test
  (let [pool (bulkhead/start-pool! {:num-workers 1 :queue-size 2})
        p (promise)
        started (promise)
        handler (fn [_req] (deliver started true) @p {:status 200})
        wrapped (bulkhead/wrap-compute-bound handler pool)

        f1 (future (wrapped {:request-id "1"}))]
    @started

    (let [f2 (future (wrapped {:request-id "2"}))]
      (Thread/sleep 50) ;; Let f2 enter the queue

      (bulkhead/stop-pool! pool)

      ;; f2 should immediately fail with 503 because the pool was stopped
      (let [res2 @f2]
        (is (= 503 (:status res2)))
        (is (= "2" (-> res2 :body :id))))

      ;; new requests should also fail immediately
      (let [res3 (wrapped {:request-id "3"})]
        (is (= 503 (:status res3)))
        (is (= "3" (-> res3 :body :id))))

      ;; f1 is still executing, let it finish
      (deliver p true)
      (is (= 200 (:status @f1))))))

(deftest timeout-test
  (let [pool (bulkhead/start-pool! {:num-workers 1 :queue-size 1})
        p (promise)
        started (promise)
        finished (promise)
        handler (fn [_req]
                  (deliver started true)
                  @p
                  (deliver finished true)
                  {:status 200})
        wrapped (bulkhead/wrap-compute-bound handler pool {:timeout-ms 10})
        f1 (future (wrapped {:request-id "to-1"}))]
    @started

    ;; We know it's started, wait for the timeout to hit the caller
    (let [res @f1]
      (is (= 504 (:status res)))
      (is (= "to-1" (-> res :body :id))))

    ;; Unblock the worker, proving it was still occupied running the handler
    (deliver p true)
    @finished

    (is (= 1 (:timeouts @(:stats pool))))
    ;; processed should still increment since it finished successfully from the worker's perspective
    (is (= 1 (:processed @(:stats pool))))
    (bulkhead/stop-pool! pool)))

(deftest exceptions-thrown-test
  (let [pool (bulkhead/start-pool! {:num-workers 1 :queue-size 1})
        handler (fn [_req] (throw (ex-info "Boom" {:foo "bar"})))
        wrapped (bulkhead/wrap-compute-bound handler pool)]
    (is (thrown-with-msg? Exception #"Boom" (wrapped {:request-method :get})))
    (bulkhead/stop-pool! pool)))

(deftest errors-thrown-test
  (let [pool (bulkhead/start-pool! {:num-workers 1 :queue-size 1})
        handler (fn [_req] (throw (AssertionError. "Severe Error")))
        wrapped (bulkhead/wrap-compute-bound handler pool)]
    (is (thrown-with-msg? AssertionError #"Severe Error" (wrapped {:request-method :get})))
    (let [handler-2 (fn [_req] {:status 200 :body "OK"})
          wrapped-2 (bulkhead/wrap-compute-bound handler-2 pool)]
      (is (= {:status 200 :body "OK"} (wrapped-2 {:request-method :get}))))
    (bulkhead/stop-pool! pool)))

(deftest phantom-pop-test
  (let [pool (bulkhead/start-pool! {:num-workers 1 :queue-size 10})
        p (promise)
        started (promise)
        handler (fn [_req] (deliver started true) @p {:status 200})
        wrapped (bulkhead/wrap-compute-bound handler pool {:timeout-ms 10})

        f1 (future (wrapped {:request-id "1"}))]

    @started

    ;; f2 queues up and will eventually time out (timeout is 10ms)
    (let [f2 (future (wrapped {:request-id "2"}))]
      (is (= 504 (:status @f2)))

      (deliver p true)
      @f1

      ;; Give worker a moment to pull the timed-out task and phantom pop it
      (Thread/sleep 50)

      (is (= 1 (:phantom-pops @(:stats pool))))
      (is (= 1 (:processed @(:stats pool))))
      (bulkhead/stop-pool! pool))))
