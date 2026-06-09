(ns sturdy.bulkhead-test
  (:require
   [clojure.test :refer [deftest is]]
   [sturdy.bulkhead :as bulkhead]))

(deftest successful-request-test
  (let [pool (bulkhead/start-pool! {:num-workers 1 :queue-size 1})
        handler (fn [_req] {:status 200 :body "OK"})
        wrapped (bulkhead/wrap-compute-bound handler pool)]
    (is (= {:status 200 :body "OK"} (wrapped {:request-method :get})))
    (bulkhead/stop-pool! pool)))

(deftest reject-queue-full-test
  (let [pool (bulkhead/start-pool! {:num-workers 1 :queue-size 1})
        ;; This handler blocks forever so the worker is busy
        p (promise)
        handler (fn [_req] @p {:status 200})
        wrapped (bulkhead/wrap-compute-bound handler pool)

        ;; Start the first request which will tie up the only worker
        f1 (future (wrapped {:request-id "1"}))

        _ (Thread/sleep 50)

        ;; Second request enters the queue (size 1)
        f2 (future (wrapped {:request-id "2"}))

        _ (Thread/sleep 50)

        ;; Third request should be rejected because queue is full and worker is busy
        res3 (wrapped {:request-id "3"})]

    (is (= 503 (:status res3)))
    (is (= "3" (-> res3 :body :id)))

    ;; Unblock the worker and clean up
    (deliver p true)
    @f1
    @f2
    (bulkhead/stop-pool! pool)))

(deftest timeout-test
  (let [pool (bulkhead/start-pool! {:num-workers 1 :queue-size 1})
        ;; This handler takes longer than the timeout
        handler (fn [_req] (Thread/sleep 100) {:status 200})
        wrapped (bulkhead/wrap-compute-bound handler pool {:timeout-ms 10})
        res (wrapped {:request-id "to-1"})]
    (is (= 504 (:status res)))
    (is (= "to-1" (-> res :body :id)))
    (bulkhead/stop-pool! pool)))

(deftest exceptions-thrown-test
  (let [pool (bulkhead/start-pool! {:num-workers 1 :queue-size 1})
        handler (fn [_req] (throw (ex-info "Boom" {:foo "bar"})))
        wrapped (bulkhead/wrap-compute-bound handler pool)]
    (is (thrown-with-msg? Exception #"Boom" (wrapped {:request-method :get})))
    (bulkhead/stop-pool! pool)))
