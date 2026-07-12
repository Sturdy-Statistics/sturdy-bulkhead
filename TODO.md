# TODO

## Optional test hardening

The current concurrency tests use short `Thread/sleep` calls to allow asynchronous state changes to settle, and this approach has been reliable in practice.
If these tests become flaky in CI, replace the sleeps with promises, latches, or other deterministic synchronization using bounded waits.

Additional low-priority coverage could verify zero-capacity direct handoff, the maximum active handler count under contention, multiple-worker shutdown races, custom callbacks, complete statistics, and the zero-arity pool constructor.
These are regression-hardening opportunities rather than known production bugs.
If new coverage reveals a production defect, track and fix that defect separately with a red/green test.
