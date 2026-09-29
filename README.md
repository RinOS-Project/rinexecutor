# RinExecutor

Header-only cooperative task queue with bounded priorities, requeue, cancellation, and explicit run results.

## Public API contract

| Requirement | Contract |
| --- | --- |
| Purpose | Header-only cooperative task queue with bounded priorities, requeue, cancellation, and explicit run results. |
| Supported API | include/rinexecutor/executor.hpp in namespace RinExecutor. |
| Unsupported API | Does not create OS threads or promise background execution, real-time scheduling, or unbounded queues. |
| ownership | Executor and task records are caller-owned; callback/context lifetime must cover queued execution. |
| thread-safety | Treat an executor as single-owner unless the embedder serializes queue operations. |
| limits | 64 queued tasks, four priority levels, high-priority burst bound of eight. |
| errors | TaskResult/RunResult distinguish completion, requeue, failure, idle, and cancellation; callback exceptions are contained as task failure; full queues reject admission. |
| ABI stability | Header-only C++ source API; templates/layout require rebuild and provide no binary ABI. |
| security | Callbacks are trusted in-process code; queue bounds do not sandbox callbacks. |
| build | Header-only; include include/rinexecutor/executor.hpp. |
| test | Root `tests/rinexecutor_test.cpp` is compiled by the host manifest and covers queue, scheduling, cancellation, shutdown, and callback-exception retirement. |
