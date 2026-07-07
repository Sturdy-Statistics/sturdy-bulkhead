(ns sturdy.bulkhead
  (:require
   [clojure.core.async :as a]))

(defn worker-loop [job-chan pool-stats stop-chan]
  (a/thread
    (loop []
      (let [[job port] (a/alts!! [job-chan stop-chan])]
        (when (= port job-chan)
          (when job
            (let [{:keys [thunk promise-chan]} job
                  ;; non-blocking read from promise-chan
                  ;; - channel closed: receive nil from promise-chan
                  ;; - channel open but empty: receive :continue as default
                  ;; (channel should never be non-empty here)
                  [_ p] (a/alts!! [promise-chan] :default :continue)]
              (if (= p promise-chan)
                ;; channel closed -> skip job
                (swap! pool-stats update :phantom-pops inc)
                ;; channel open -> run job
                (let [result (try
                               {:result (thunk)}
                               (catch Throwable e
                                 {:error e}))]
                  ;; job complete -> send result
                  ;; NB if channel closed while running, put simply drops the value
                  (a/>!! promise-chan result)
                  (swap! pool-stats update :processed inc))))
            (recur)))))))

(defn start-pool!
  "Starts a new worker pool for processing compute-bound requests.
  Opts:
  - :num-workers (default: cores - 1, or 1). Must be a positive integer.
  - :queue-size (default: 10). Must be a non-negative integer. If 0,
    requests are only accepted if a worker is immediately available.

  Returns a pool object (a map) which can be passed to `wrap-compute-bound`
  or `stop-pool!`."
  ([] (start-pool! {}))
  ([{:keys [num-workers queue-size]
     :or {num-workers (max 1 (dec (.availableProcessors (Runtime/getRuntime))))
          queue-size 10}}]
   (when-not (pos-int? num-workers)
     (throw (ex-info ":num-workers must be a positive integer" {:num-workers num-workers})))
   (when-not (nat-int? queue-size)
     (throw (ex-info ":queue-size must be a non-negative integer" {:queue-size queue-size})))
   (let [job-chan (if (zero? queue-size) (a/chan) (a/chan queue-size))
         stop-chan (a/chan)
         stats (atom {:processed 0
                      :rejections 0
                      :timeouts 0
                      :phantom-pops 0})
         workers (doall
                  (for [_ (range num-workers)]
                    (worker-loop job-chan stats stop-chan)))]
     {:job-chan job-chan
      :stop-chan stop-chan
      :stats stats
      :workers workers})))

(defn stop-pool!
  "Stops the worker pool."
  [pool]
  (let [{:keys [job-chan stop-chan]} pool]
    (a/close! job-chan)
    (loop []
      (when-let [job (a/poll! job-chan)]
        (a/close! (:promise-chan job))
        (recur)))
    (a/close! stop-chan)
    pool))

(defn default-reject-handler [request]
  (let [req-id (:request-id request)]
    {:status 503
     :body {:error "Service Unavailable"
            :message "The compute queue is at capacity. Please try again later."
            :id req-id}}))

(defn default-timeout-handler [request]
  (let [req-id (:request-id request)]
    {:status 504
     :body {:error "Gateway Timeout"
            :message "The request took too long to process."
            :id req-id}}))

(defn wrap-compute-bound
  "Ring middleware that limits concurrent compute-intensive requests
  by queueing them to a worker pool.

  Accepts a `pool` created by `start-pool!` and `opts`:
  - :timeout-ms (default: 30000)
  - :on-reject (fn [request] -> response, called when queue is full)
  - :on-timeout (fn [request] -> response, called when queue timeout happens)"
  ([handler pool]
   (wrap-compute-bound handler pool {}))
  ([handler pool {:keys [timeout-ms on-reject on-timeout]
                  :or {timeout-ms 30000
                       on-reject default-reject-handler
                       on-timeout default-timeout-handler}}]
   (let [job-chan (:job-chan pool)
         stats (:stats pool)]
     (fn [request]
       (let [promise-chan (a/promise-chan)
             thunk #(handler request)]
         (if (a/offer! job-chan {:thunk thunk :promise-chan promise-chan})
           ;; sleep on either result or timeout
           (let [[val port] (a/alts!! [promise-chan (a/timeout timeout-ms)])]
             ;; close the channel
             ;; - if timeout and worker hasn't started, signals to skip this task
             ;; - if timeout and worker has started, signals to drop the result
             (a/close! promise-chan)
             (if (= port promise-chan)
               (if (nil? val)
                 (do
                   (swap! stats update :rejections inc)
                   (on-reject request))
                 (if (contains? val :error)
                   (throw (:error val))
                   (:result val)))
               (do
                 (swap! stats update :timeouts inc)
                 (on-timeout request))))
           (do
             (swap! stats update :rejections inc)
             (on-reject request))))))))
