# sturdy-bulkhead

[![Clojars Project](https://img.shields.io/clojars/v/com.sturdystats/sturdy-bulkhead.svg)](https://clojars.org/com.sturdystats/sturdy-bulkhead)

`sturdy-bulkhead` implements middleware to queue compute-intensive requests within a Ring API.
It prevents CPU starvation by offloading work to dedicated worker pools using `core.async`.

## Features

- **Bounded Concurrency**: Set an exact limit on the number of concurrent tasks for each pool.
- **Queueing**: Automatically queues excess requests up to a configurable limit.
- **Fast Failures**: Rejects requests with a 503 Service Unavailable when the queue is saturated.
- **Timeouts**: Configurable timeouts to send a 504 Gateway Timeout.
- **Multiple Pools**: Create multiple pools to bound different resources independently within the same application.
- **Observability**: Tracks basic statistics like processed tasks, rejections, and timeouts.

## Installation

Add to `deps.edn`:

```clojure
{:deps com.sturdystats/sturdy-bulkhead {:mvn/version "VERSION"}}
```

## Usage

### 1. Start a Worker Pool

Initialize a worker pool when your application starts.

```clojure
(require '[sturdy.bulkhead :as bulkhead])

(def my-pool 
  (bulkhead/start-pool! {:num-workers 4    ; 4 concurrent queries max
                         :queue-size 20})) ; queue up to 20 requests
```

### 2. Wrap Your Ring Handlers

Use the provided middleware to wrap computationally expensive routes.

#### Standard Ring

```clojure
(def app
  (-> heavy-query-handler
      (bulkhead/wrap-compute-bound my-pool {:timeout-ms 10000})))
```

#### Reitit

If you use `reitit`, you can attach the middleware to specific routes using its vector syntax:

```clojure
(require '[reitit.ring :as ring])

(def app
  (ring/ring-handler
   (ring/router
    ["/api"
     ["/heavy" {:get {:middleware [[bulkhead/wrap-compute-bound my-pool {:timeout-ms 10000}]]
                      :handler heavy-query-handler}}]
     ["/light" {:get {:handler light-query-handler}}]])))
```

### 3. Stop the Pool

Call `stop-pool!` as part of application shutdown, after the server has stopped accepting new traffic:

```clojure
(bulkhead/stop-pool! my-pool)
```

Stopping a pool closes it permanently; a stopped pool is not intended to be restarted or reused.
Queued and subsequent requests receive the same `503 Service Unavailable` response as requests rejected because the queue is full, and all of these cases increment the `:rejections` statistic.
The pool deliberately does not track separate lifecycle state or distinguish shutdown from saturation.
If that distinction matters for operational metrics, stop accepting traffic before calling `stop-pool!` and exclude the shutdown window from saturation alerts.

## Tuning & Production Considerations

It is critical to tune the worker pool relative to your web server's thread pool:
The sum of your `num-workers` and `queue-size` should be strictly less than your web server's maximum thread pool size.

Because `wrap-compute-bound` is synchronous middleware, it blocks the incoming Jetty request thread while waiting for the bulkhead worker to finish (or for the timeout to occur). 

- **Worker Count (`num-workers`)**: This should be less than your total CPU cores.  This ensures the OS and web server always have dedicated resources to process network I/O, health checks, and fast endpoints without CPU starvation.
- **Queue Size (`queue-size`)**: Determine how many requests you are willing to hold in memory during a traffic burst.  If the queue is full, the middleware immediately returns a `503 Service Unavailable`, freeing the jetty thread. Note that you may configure `:queue-size 0`, which means requests will only be accepted if a worker is immediately available for direct handoff; otherwise they are rejected.
- **Jetty Max Threads**: Ring-Jetty defaults to a maximum of 200 threads.  If you set `num-workers = 30` and `queue-size = 250`, all of jetty's threads could be sleeping on responses from bulkhead.  This would render the server unresponsive until the queue drains.
- **Timeouts and Capacity**: When a request times out (via `:timeout-ms`), the caller immediately receives a `504 Gateway Timeout`. However, if the worker pool has already begun executing your handler, that handler will continue running to completion in the background, occupying a worker thread until it finishes. Timeouts only free the incoming web server thread, they do not interrupt or cancel the underlying worker pool task.

## Development & Testing

This project uses `deps.edn` aliases for testing and validation.

```bash
# Run unit tests
clj -X:test

# Generate code coverage report (in target/coverage/index.html)
clj -X:coverage

# Scan dependencies for known vulnerabilities (clj-watson)
clj -X:vuln
```

<!-- Local Variables: -->
<!-- fill-column: 10000000 -->
<!-- End: -->
