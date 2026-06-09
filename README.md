# sturdy-bulkhead

`sturdy-bulkhead` implements middleware to queue compute-intensive requests (such as DuckDB queries) within a Ring API.
It prevents CPU starvation by offloading work to dedicated worker pools using `core.async`.

## Features

- **Bounded Concurrency**: Set an exact limit on the number of concurrent tasks for each pool.
- **Queueing**: Automatically queues excess requests up to a configurable limit.
- **Fast Failures**: Rejects requests with a 503 Service Unavailable when the queue is saturated.
- **Timeouts**: Configurable timeouts to send a 504 Gateway Timeout.
- **Multiple Pools**: Create multiple pools to bound different resources independently within the same application.
- **Observability**: Tracks basic statistics like processed tasks, rejections, and timeouts.

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

When shutting down your application, clean up the resources:

```clojure
(bulkhead/stop-pool! my-pool)
```

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
