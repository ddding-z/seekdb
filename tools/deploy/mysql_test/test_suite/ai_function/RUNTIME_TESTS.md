# AI Function Runtime Regression

Latest validation (2026-09-28): all **86 SQL contracts**, **five fixture self-tests**, the expanded [resource reliability tests](#resource-reliability-tests), and a separate server-shutdown contract passed with [SQL worker suspension](#sql-worker-suspension-2026-09-28). Eligible single-statement AI pipeline SELECTs now return their worker while waiting and resume from the retained result cursor. Earlier results are retained below as historical evidence.

Run from the repository root after a Debug build:

```bash
python tools/deploy/mysql_test/test_suite/ai_function/test_runtime.py \
  --binary "$PWD/build_debug/bin/src/observer/seekdb"
```

Requires Linux, Python 3, PyMySQL and a runnable seekdb binary with its runtime libraries available. In the current workspace, `/volume/xicksys/.venv/bin/python` has PyMySQL installed. Allow several GiB of temporary disk space and at least 2GiB of memory for the isolated database.

The script creates a private temporary database directory, starts seekdb with TCP SQL and RPC disabled, and runs a loopback-only HTTP mock on a random port. It does not connect to an existing database or a real model. The database process and temporary data are cleaned up on exit; the model access key is a dummy test value. No global proxy settings are changed.

Assertions cover request/output equivalence, batch-sized concurrency, whole-batch submission, reordered completion, per-request retry, constant/dynamic parameters, CASE skips, repeated expressions, NULL/JSON/LOB behavior, malformed responses, size limits, deadline and query cancellation. Timing compares identical mocked work through batch and scalar paths. Reported process CPU and peak RSS include database bootstrap and the whole suite, not a per-query resource comparison.

Client-owned allocation failures and low-level `no_wait` lifetime checks are covered by the resource tests below. Whole-heap sanitizer coverage, AI_RERANK, real embedding providers and detailed resource comparisons remain separate verification work. These standalone scripts are not registered in the mysqltest `.test` runner.

## Independent Contract Tests

The original script is a smoke/performance comparison, not the acceptance oracle. Some of its historical negative cases accept any MySQL error; its timeout and cancellation checks now require error codes 4012 and 1317 and evidence of submitted requests. Run the stricter suite separately:

```bash
python tools/deploy/mysql_test/test_suite/ai_function/test_runtime_contracts.py --self-test
python tools/deploy/mysql_test/test_suite/ai_function/test_runtime_contracts.py \
  --binary "$PWD/build_debug/bin/src/observer/seekdb"
```

The contract suite uses stdlib `unittest` to collect failures and continue through independent cases. It reuses only the isolated database launcher, not the smoke test's assertions. Each SQL test gets a separate loopback server and port, and joins the server's request threads during cleanup. Requests already sent before cancellation cannot contaminate the next case.

Its SQL connection waits 75 seconds, exceeding the existing 60-second SQL deadline used by the 2048-row test. Other per-query deadlines are unchanged. The mock listens with backlog 4096 to accept a whole-batch connection burst; this changes test-service capacity, not the seekdb concurrency policy or result assertions.

### Oracle Design

Expected behavior comes from the execution plan, SQL evaluation boundaries, HTTP semantics, and the public error definitions in [ob_errno.def](../../../../../src/share/ob_errno.def), not from recorded output of the modified client.

| Area | Independent expectation | Defect the case can expose |
| --- | --- | --- |
| Input preservation | Explicit model, one user message per row, unchanged options, exact independent result | Both scalar and batch making the same wrong request |
| Default parallelism | Hold all 72 responses with plan batch size 128; all 72 requests must arrive before any response is released | A leftover fixed 50/64-request limit or waiting for responses before submitting the rest of the batch |
| Whole-batch submission | Hold the first response until at least 33 requests arrive; all 64 results remain aligned, with no duplicate submissions | A fixed preparation boundary blocking later requests |
| Large batch | 2048 rows with `OPT_PARAM('rowsets_max_rows', 2048)`, exact outputs and one request per row | A leftover 1024-request limit, missing rows or duplicate submissions |
| Logical row set | Duplicate prompts still sent per row; CASE skips invalid unselected inputs; LIMIT 0/3 sends only needed rows in the tested plan | Hidden deduplication or unnecessary remote work |
| Strings | Exact Unicode, NUL, quotes, backslash and newline round trip | Truncation, encoding or double escaping |
| Error classification | Invalid argument 1210; HTTP/uncertain transport 4216; provider format 4070; size 4019; non-object JSON 3140 | Treating any exception as a passing negative test |
| JSON syntax | Either documented JSON error 3140 or parser syntax error 5447, never arbitrary failure | Misclassifying transport/timeouts as malformed JSON |
| Retry-After | Second attempt after advertised four-second delay or absolute HTTP-date | Merely seeing two requests even when the header is ignored |
| Retry policy | Temporary 429/500/502/503/504 retry; successful rows once; maximum four attempts | Whole-batch retries or infinite attempts |
| Uncertain POST result | Mock reads the request then drops/truncates response; no retry | Duplicate model execution after uncertain transport failure |
| Fail-fast | Fatal HTTP, JSON and provider errors must return before held peers' deadline and cancel remaining local transfers; submitted requests need not stop remotely | Deferred validation hidden by other requests' timeout |
| Constrained output | Forward the exact Schema; reject malformed/unsupported configuration before HTTP; validate content, finish reason and refusal on each completed response | Prompt-only constraints, silently ignored Schema keywords, truncation, invalid structured results or delayed validation |
| JSON string length | Count Unicode code points, including supplementary characters and embedded NUL, in both AI output and JSON_SCHEMA_VALID | Treating UTF-8 bytes or displayed graphemes as JSON Schema string length |
| Byte limits | Valid response JSON at 8MiB minus one, exactly 8MiB and plus one; nine individually valid large responses exceed 64MiB total | Oversize fixture failing only because it was invalid JSON, or per-response-only accounting |
| Serialized request | 1MiB control-character input expands above 4MiB when JSON escaped | Checking prompt character length instead of request bytes |
| Deadline/cancel | Long Retry-After cannot extend query deadline; cancel during backoff returns 1317 promptly and next query succeeds | Ignoring cancel while no transfer is active |
| Resource recovery | 12 rounds each of cancel, provider error and Schema error, with eight held requests; model socket FDs disappear before peers are released and the same SQL connection recovers | Local transfers or connections retained after errors, unexpected retries, or state leaking into the next query |
| Native embedding | 74 eligible rows, including duplicates and Unicode/NUL, form one HTTP request; indexed responses arrive reversed and return distinct known vectors in SQL row order | Accidental scalar requests, deduplication, nested output arrays or wrong row mapping |
| Embedding splitting | Raw texts fit 4MiB but JSON escaping does not; two native subrequests are formed, only the failing one retries | Raw-byte-only sizing, unnecessarily splitting every row or retrying successful subrequests |
| Embedding validation | Exact counts, unique in-range integer indices, nonempty finite numeric vectors and consistent/requested dimensions; malformed response cancels a held subrequest | Unsafe casts, ignored indices, dimension drift or late validation |
| Shared client | Dynamic-model scalar embedding preserves vectors/dimensions and 502 retry; native requests preserve deadline, cancellation and socket cleanup | Native batching breaking fallback or resource ownership |
| Cross-batch overlap | Hold every response for the first two SQL batches; both batches must reach the service before a response, with no third pending batch; completion remains one request per row | A synchronous per-batch barrier, unintended prompt merging or an unbounded prefetch queue |
| Pipeline lifecycle | Release the first batch while its peer remains held and require slot refill; check later-batch fatal errors, cancellation, deadlines, large LOB side columns and row-mode fallback | Waiting for the whole window, hidden errors, reused datum pointers or scalar-mode initialization failures |
| Worker release | Hold more AI queries than the initial request-thread count; every model request must arrive, a control SELECT must finish, and no additional request thread may appear | Moving HTTP work to the background while leaving SQL workers blocked, or relying on replacement threads |
| Suspended client disconnect | Close the SQL connection while both batches are held; model socket FDs disappear before responses are released and other queries remain usable | Retaining a session, request or model connection after the client disappears |
| Completion pipeline inputs | Preserve static Schema/options, retry only a failed second-batch request, and expand stored JSON prompt objects exactly; reject malformed first-batch input before HTTP | Lost configuration, a retry blocked by the preceding batch, duplicate calls or broken JSON-column handling |
| Credentials | Endpoint key rotation applies to the same SQL on next execution | Stale credentials retained across executions |

Five fixture self-tests check exact valid-JSON byte lengths, scripted HTTP responses, case-insensitive header names, independently specified reordered embedding vectors, and negative controls: wrong SQL codes and unexpected success must fail. Threading events hold peer responses for fail-fast tests; no arbitrary sleep is used to infer their completion. Retry timing assertions allow 50ms scheduling/measurement tolerance. HTTP-date tests assume the local wall clock is not stepped during the run.

The default batch-sized concurrency, retry count (3 retries), and byte limits are versioned first-stage policies. They are deliberately not imported from the implementation under test; an intentional policy change requires updating the specification and these expectations together. Timing ratios in the smoke benchmark are not semantic correctness gates. LIMIT and reused-expression checks demonstrate the concrete tested query shapes, not every possible optimizer plan. This suite does not prove constant total process memory, exactly-once remote execution, or absence of resource leaks.

### Results: 2026-09-17

- Fixture self-tests: 4 passed.
- Existing smoke suite with stricter timeout/cancel checks: passed.
- Independent SQL suite: 26 tests, 25 passed, 1 failed (46.584s in the recorded run).
- No production code was modified during this testing task; the current Debug binary from the prior build was tested.

The failing case was `RuntimeContracts.test_provider_error_stops_refill_without_waiting_for_peers`. The mock responds HTTP 200 with `{"unexpected":true}` for one row, while holding other responses. The single-request control confirms error 4070. With peers active, that version returned timeout 4012 after 2.002s and submitted 9 requests. The expectation remained 4070 before 1.5s and no refill after the permanent error was observed. The test was not skipped or marked expected-failure, so that run exited with status 1.

The cause was provider parsing in [call_completion_vector](../../../../../src/sql/engine/expr/ob_expr_ai/ob_ai_func_utils.cpp) only after `send_post_batch` finished the entire batch. HTTP and JSON validation already failed promptly, but provider validation could not stop refill or outrank a later peer timeout. The constrained-output change below validates each completion response immediately and fixes this defect without accepting timeout as the expected error.

An earlier apparent LIMIT failure was traced to late requests from a prior case entering shared mock counters. Per-case ports and joined request threads removed that test-fixture defect without weakening the LIMIT assertion.

Remaining gaps at that time included allocator fault injection, low-level `no_wait` cleanup, leak/sanitizer evidence, DNS/connection failure, diagnosis mode, denied-access/multi-tenant isolation, AI_RERANK and non-OpenAI providers, and per-query CPU/RSS/tail-latency comparisons. The later resource tests below cover bounded allocator/cleanup cases, not all of these gaps. The embedding case is a focused regression, not full embedding coverage.

### Whole-Batch Scheduling: 2026-09-17

AI_COMPLETE prepares all eligible rows in the current SQL batch and submits them through one client window, following Sema's whole-batch queueing approach. The former 32-row preparation barriers are removed. The HTTP window was 8 at this stage and was subsequently raised to 50, then changed to batch-sized concurrency as recorded below. At this stage, 2048 input rows did not mean 2048 concurrent requests. These changes do not introduce prompt batching, lazy request preparation or cross-batch asynchronous execution.

The fixed 1024-request limit has been replaced with byte-budgeted request state. The 64MiB client budget includes `sizeof(Request)` per request, serialized bodies and retained raw responses. SQL preparation also rejects aggregate prompt bytes above 64MiB. Per-request 4MiB and per-response 8MiB limits are unchanged. JSON trees, allocator capacity and curl internals are not included, so this is not an RSS hard cap.

The budget now covers the whole SQL batch, not each former 32-row segment. An aggregate payload that previously fit segment by segment can now fail with size error 4019; it is not silently split. All requests and responses remain materialized until the call finishes. Model/endpoint resolution and header preparation occur once per SQL batch, while config JSON parsing is still per row.

- Debug build and editor diagnostics: passed; `git diff --check` clean.
- Cross-boundary gate test: failed against the original binary, passed after removing the preparation barrier.
- Focused window, 2048-row and response-budget checks: 4 passed.
- Fixture self-tests: 4 passed; existing smoke suite passed.
- Full independent SQL suite: 28 tests, 27 passed, 1 failed (61.911s).
- The only failure remains provider fail-fast: timeout 4012 after 2.001s with 9 requests submitted, rather than expected provider error 4070. Its assertions are unchanged.

The latest 24-row mock comparison was batch 0.971s versus scalar 6.102s. The database process including bootstrap and the smoke suite used 16.90s CPU and 354084KiB peak RSS. This input is smaller than the removed preparation boundary, so the timing ratio is not evidence of a speedup from this change. No real model service was called in this run.

### Default Window Raised to 50: 2026-09-17

At this stage, the client default was raised to 50 active requests and the configurable range remained 1-64; retry, deadline, byte limits and connection-pool lifetime were unchanged. This was a per-client request limit, not a curl connection limit or database-wide quota.

The new gate test starts 51 logical inputs, holds all responses, observes 50 active requests with the 51st still queued, then releases one response and requires the queued request to start. Existing fail-fast and cancellation inputs exceed the new window so those tests still exercise pending requests.

Debug build and editor diagnostics passed. Eight focused SQL contracts passed (12.820s): the default window, continuous refill, 2048 rows, aggregate response budget, fatal HTTP/JSON errors, retry deadline and cancellation recovery. The smoke suite also passed. The full contract suite, the known provider fail-fast failure and connection-reuse tests were not rerun for this default-value change. No real model service was called.

### Default Parallelism Follows the Batch: 2026-09-18

The client now defaults to `max_parallel_ = 0`, meaning the effective activity limit is recalculated from the current HTTP batch's request count. Every eligible initial request is added to curl multi without waiting for another request to finish. For AI_COMPLETE, skipped/already-evaluated rows are excluded; scalar calls and dynamic-parameter fallback still submit one request at a time. The C++ setter accepts zero for this default policy or a positive explicit limit; negative values are rejected, and the former fixed 64 ceiling is removed. No SQL system variable is added.

This delegates inference admission and queueing to the service rather than a fixed seekdb window. It does not guarantee simultaneous arrival or inference: network, protocol and provider capacity still apply. Retry backoff remains client-side, and service queueing consumes the existing query deadline. There is no database-wide quota; connection/handle memory and remote work can grow with the batch size. The 4MiB request, 8MiB response and 64MiB aggregate budgets remain unchanged and do not include curl internals. Local cancellation cannot retract requests already accepted remotely. The SQL worker still waits for the whole batch.

- The new 72-request response gate failed against the prior binary with only 50/72 requests received, then passed against the rebuilt binary with all 72 requests received before any response.
- Debug build passed; both Python test files passed Pylance syntax checks. Four fixture self-tests passed.
- Full SQL contract suite: 29 tests, 28 passed, 1 failed (78.077s), including passing 2048-row, aggregate-budget, HTTP/JSON fail-fast, retry and cancellation cases.
- At this stage the only failure remained `test_provider_error_cancels_active_peers`: timeout 4012 after 2.007s with 72 requests submitted, instead of provider error 4070 before 1.5s. The error-code and latency assertions were unchanged; that run exited with status 1.
- Smoke suite passed. The 24-row mock comparison was batch 0.828s versus scalar 6.101s; database CPU including bootstrap and the suite was 18.67s, peak RSS 367056KiB. This is not a real-provider benchmark or proof of a speedup over the prior 50-request policy.

No real model service, existing debug database or Notebook was used or modified for these tests. At that stage, explicit positive client limits and low-level `no_wait` lifetime behavior were not independently exercised.

## Resource Reliability Tests

[test_runtime_resources.py](test_runtime_resources.py) builds and runs [test_runtime_resources.cpp](test_runtime_resources.cpp) against the existing Linux Debug Bazel objects. It compiles only the test entry point, queries the actual final-link action as JSON, and replaces the server entry point in a temporary test executable. The production binary is not overwritten. Rebuild Debug and refresh `compile_commands.json` after production changes before running this test; the runner does not rebuild stale production objects.

```bash
# Requires the existing Debug build, compile_commands.json and its compiler/runtime environment.
python tools/deploy/mysql_test/test_suite/ai_function/test_runtime_resources.py \
  --build-dir "$PWD/build_debug"

# Includes the real SQL process/socket lifecycle checks.
python tools/deploy/mysql_test/test_suite/ai_function/test_runtime_contracts.py \
  --binary "$PWD/build_debug/bin/src/observer/seekdb"
```

In this workspace, source `.vscode/seekdb-env.sh` and use `/volume/xicksys/.venv/bin/python`. The native runner uses a loopback mock and temporary build directory, without starting a database. The SQL runner starts its own temporary database as described above. Neither uses a real model service or the existing debug instance.

### Failure Injection And Ownership

The native test calls the production client with a tracking allocator and link-time wrappers around curl resource APIs. It also constructs the production AI operator with a controlled child to exercise diagnosis and empty-input rescan. Except for the selected injected failure, curl calls execute the real implementation. No production fault switch or alternate client implementation is added.

| Check | Required result |
| --- | --- |
| URL and two header-copy allocation sites | Return the allocation error; repeated reset and destruction release all client-owned allocations; subsequent initialization succeeds |
| Every reachable request-preparation allocation | Fail each allocation in turn until a successful preparation is reached; six failure sites for the three-row fixture; no residual request memory or handles |
| Eight curl faults | First/second header append, multi creation, first/second request-handle creation, second handle attachment, perform and poll failures return the expected error and roll back resources |
| Response buffer allocation | Propagate allocation failure, expose no partial results and release peer requests and buffers |
| Shared pipeline budget | Two clients charge one 64MiB counter together with simulated retained row data, in both manual and background modes; reject overflowing preparation and response writes, return exactly the owning client's quota on failure/cancel, and allow resubmission after quota is released |
| `no_wait` lifetime | 32 cancellation cycles with an explicit window of two for three rows; pending reads return EAGAIN, queued work stays unsubmitted, repeated cleanup is safe |
| Reinitialization/destruction while pending | Release the old allocator's resources when switching allocators, and detach/clean requests before destroying their multi handle |
| Background progress | Three requests finish while the caller waits only for the curl resource observer, without calling poll; all results are subsequently readable |
| Background cancellation | 16 submit/reset races release caller memory; repeated reset is safe, duplicate submission returns INIT_TWICE, and subsequent work exposes no stale results |
| Background deadline and peer progress | A held batch expires without caller polling while an independent batch completes first; timeout exposes no partial results |
| Bounded admission | Fill all 64 scheduler slots with held batches; a 65th submission honors its original deadline, and cancellation restores admission capacity |
| Scheduler shutdown | Stop with held requests, wait for cancellation, release caller memory, allow repeated stop and reject subsequent submissions |
| Request await protocol | Default-off authorization, matching execution-context ownership, readiness, nested TLS isolation, cancellation without re-suspension, and reset without stale authorization |
| Exception on resume | A retained processor throws on its second dispatch; preserve the error code, do not translate another processor, and release it exactly once instead of retaining an unwakeable request |
| Failure followed by success | 16 cycles complete actual loopback requests using the same client after a failed preparation; returned JSON remains valid after client destruction because the caller still owns its allocator |
| Operator diagnosis/rescan | For both AI function types, an empty stream can be rescanned repeatedly and must pull its child again; diagnosis passes child rows through without preparing AI requests, including after rescan |
| Detector negative controls | Deliberately omit one allocation release, one request-handle cleanup, or background scheduling in separate test processes; each must fail with its specific leak/progress diagnostic |

Request memory is compared with the post-initialization baseline before releasing the caller's allocator. Client-owned easy handles, multi handles and header nodes must be balanced at client destruction. A live multi may retain connections; its internally created cache handles are not counted as application request handles because their internal cleanup bypasses the public easy-cleanup API. The test does not pretend to instrument every libcurl allocation.

The SQL resource case uses Linux `SO_PEERCRED` to identify the database process, then correlates its `/proc/<pid>/fd` socket descriptors with the mock endpoint in `/proc/<pid>/net/tcp`. Each round first observes live model sockets while eight responses are held. It then cancels the query or releases one invalid provider/Schema response. Error 1317 or 4070 must return promptly with no model socket FDs remaining, even while peers are still held. A successful follow-up on the same SQL connection must return all eight answers and also leave no model socket FDs. Kernel TIME_WAIT entries without a process FD are not counted as leaks. Socket-table access is required; inaccessible evidence is not silently skipped.

### Resource Results: 2026-09-20

- All native fault, lifetime and recovery checks passed. Both deliberately broken cleanup controls failed as required and were recognized by the runner.
- SQL: 36 failed batches and 36 successful recoveries passed, with eight model socket FDs observed in flight and zero after every query. Request-count and exact error-code assertions were retained.
- Complete independent SQL suite: **44/44 passed in 230.861s**. Database CPU including bootstrap and all tests was 62.51s, peak RSS 481396KiB; these are context measurements, not a memory-leak oracle or a performance comparison.
- No production code, Notebook or existing database was changed. No real-model requests were made. This round did not rerun the separate smoke timing comparison.

The 2026-09-20 results provide bounded resource-lifetime evidence, not a whole-process leak proof. Those checks used one owner thread and did not establish cross-thread safety. The background owner/caller cancellation checks were added on 2026-09-28 below. Global allocator failures, JSON parser/Schema compiler allocation failures, and failures inside every curl option or allocator remain outside these checks. No whole-heap sanitizer was run, and the 64MiB logical budget still is not an RSS cap.

## Native Embedding Batches

AI_EMBED now groups the eligible rows of the **current SQL batch** into a native multi-input request. The model and optional dimension must be static scalar constants for the batch evaluator; dynamic model/dimension expressions and diagnosis mode choose scalar evaluation before submitting requests. Scalar and batch paths share input handling and output validation. SQL syntax and the per-row one-dimensional JSON vector result remain unchanged:

```sql
SELECT id, AI_EMBED('my_embedding_model', prompt, 1536) AS embedding
FROM inputs;
```

The model/endpoint must already be registered and support the optional requested dimension. Omitting the third argument uses the service's default dimension. Inputs are not concatenated into a prompt, deduplicated or cached; CASE/skip and already-evaluated rows are excluded. The batch evaluator itself synchronously waits for the batch. The separate pipeline below can overlap eligible SQL batches without merging their input arrays.

### Request And Result Contract

- One request is used whenever the whole eligible input array fits the existing **4MiB serialized-body limit**. Otherwise, consecutive inputs are grouped into subrequests using the JSON printer's actual escaped sizes, provider envelope and separators. All subrequests are prepared before any are submitted. A single input that cannot fit is rejected with 4019, without sending earlier rows.
- All subrequests share the existing client, query deadline and **64MiB aggregate budget** for request state, serialized bodies and retained raw responses. Splitting does not reset this budget. The **8MiB per-response limit** remains; JSON trees, conversion buffers, allocator capacity and curl internals are not an RSS hard cap.
- OpenAI-compatible endpoints receive an `input` array and must return `data[].index`. Native `aliyun-dashscope` receives `input.texts` and must return `output.embeddings[].text_index`. The registered embedding providers `openai`, `aliyun-openai`, `hunyuan-openai`, `siliconflow` and `aliyun-dashscope` were checked against protocol mocks. Ollama has a legacy helper class but is not registered as an SQL provider; this change does not enable it.
- Every completed response is checked immediately against that subrequest's input count. Indices must be present, integer, unique and in range. Vectors must be nonempty arrays of finite numbers, with the requested dimension or a consistent inferred dimension across subrequests. Responses are reordered by index before flattening to one JSON vector per original SQL row.
- Wrong shape/count/index/non-numeric values return 4070; an explicit dimension mismatch retains error 1210. Malformed JSON may return 3140/5447. Invalid results are not repaired or regenerated, and permanent errors cancel other local transfers. No partial-success mode is introduced.
- A retry replays only the failed HTTP subrequest, including all inputs it contains; successful subrequests are not resent. The existing bounded retry policy and no-retry rule for uncertain POST outcomes remain. Cancellation cannot guarantee that the service stops computation or billing.

Only the local serialized-byte bound drives splitting. There is no model-specific tokenizer, input-count negotiation, or speculative retry using smaller batches after an endpoint rejection. Service-specific input/token limits and the response size still apply; an accepted protocol format is not evidence that every deployed model accepts every SQL batch size. No real embedding service or performance benchmark was run.

### Native Batch Results: 2026-09-20

- The new 74-row test failed against the previous binary with **74 requests instead of 1**. Its identical request-count, content and result assertions passed after implementation, including reversed response indices and duplicate inputs.
- The final embedding subset passed **19/19** in 25.355s. Cases cover all registered embedding provider formats, 270 rows spanning SQL batches with a tail, optional/dynamic dimensions, dynamic-model fallback, CASE/empty inputs, malformed results, byte splitting, selective retry, lost responses, response limits, deadline, cancellation and same-connection recovery.
- The complete suite passed **62/62** in 92.516s, including the existing completion/Schema contracts and 36 failed batches plus 36 socket-verified recoveries. Database CPU including bootstrap and the suite was 52.09s; peak RSS was 487740KiB. These are context measurements, not embedding speedup or leak-proof claims.
- Native allocator/curl fault and lifecycle tests passed again against the rebuilt production objects, including both deliberately broken cleanup controls. Debug build, five fixture self-tests and Python syntax validation passed.
- During implementation, the first new SQL run exposed a missing allocator on JSON measurement buffers (4152); the production buffers were corrected and the same tests rerun. Provider tests initially used unregistered short names; they now use the declared provider registry and explicitly retain rejection of Ollama. Result assertions were not relaxed.

No existing debug database or Notebook was accessed or modified. The separate smoke timing comparison and real-model tests were not rerun for this change.

## Cross-Batch Embedding Pipeline

Simple projections can now send the next SQL batch while the preceding batch is waiting for its response. Existing SQL needs no new option:

```sql
EXPLAIN SELECT id, AI_EMBED('my_embedding_model', prompt, 1536)
FROM inputs ORDER BY id;
```

An eligible plan now contains `AI FUNCTION PIPELINE` (originally `AI EMBED PIPELINE`). The [logical operator](../../../../../src/sql/optimizer/ob_log_ai_func.cpp) owns the AI expression so the child cannot evaluate it synchronously. The [physical operator](../../../../../src/sql/engine/basic/ob_ai_func_op.cpp) maintains a configurable number of owned batch slots, defaulting to two, each with its own persistent `AIFuncBatch` request state. Native embedding input arrays, byte splitting, provider validation and per-request retries are unchanged; batches are not combined and duplicate inputs are retained. See [multi-slot configuration](#configurable-multi-slot-pipelines-2026-09-29) for current controls and limits.

### Scope And Scheduling

- The pipeline accepts one top-level AI_EMBED or AI_COMPLETE projection over a single basic table, a static model and static/omitted dimension or completion configuration, and a column input optionally wrapped in casts. Completion also accepts a stored JSON prompt object. Other projected values must be columns or static constants. Eligible physical inputs are single-worker scan/sort plans; ordinary operator-only filters and column ordering are allowed.
- LIMIT, CASE/complex projections, dynamic parameters, multiple AI projections, joins, aggregates, subqueries and AI-dependent filters/orderings retain the existing path. AI_RERANK is not connected to the pipeline. A plan with `rowsets_max_rows=0` runs the scalar path without allocating batch state; diagnosis mode retains synchronous evaluation.
- Before pulling another child batch, the operator polls every active batch and returns the front result if ready. It does not wait for the entire window to fill before checking completion. After the parent consumes a batch, that slot can be refilled while other batches are pending.
- Results remain in input order. A later completed batch is validated and its request memory released, but its owned results wait behind the first batch. This deliberately retains bounded head-of-line blocking. Later-batch permanent errors are checked even while the first batch is pending and cancel all local clients.
- Rows, side columns and completed results outlive reused evaluation frames. `ObDatum::deep_copy` owns row payloads; `ObBatchResultHolder` restores the child's original datum pointers before another pull. Request allocation is separate from retained output allocation. Close, rescan, error and destruction reset requests before releasing their allocators.

At the cross-batch-only milestone, the SQL thread drove both curl multi instances and a blocking child read could delay network progress. The [background scheduler](#background-network-scheduling-2026-09-28) now advances submitted batches independently. Eligible requests use [worker suspension](#sql-worker-suspension-2026-09-28); other callers retain their synchronous wait, checking all batches at most 20ms apart. Each slot retains its curl multi for its own lifetime; model/endpoint resolution and authentication still happen for each batch, without a cross-query configuration cache.

### Shared Budget

All slots share **one configurable per-operator logical budget**, defaulting to 64MiB, including copied row datums/payloads, retained result strings, request-state objects, serialized bodies and raw responses. The same charges also count toward the server's pipeline-wide limit, defaulting to 1GiB. The existing 4MiB request, 8MiB response and 64MiB per-client batch limits also apply. Retry response reset, failure, cancellation and slot reuse return the owning allocation's quota rather than clearing another slot or query's usage.

The initial two-slot implementation failed whenever combined usage exceeded its budget. The current implementation stops prefetching at half the per-operator limit and can retain the next child batch without submitting it when its copied rows cannot yet fit alongside active batches. This does not guarantee every response will fit: a first batch, prepared request, raw response or retained result that exceeds a hard budget still returns 4019 and cancels local requests. There is no spilling or restart as scalar after submission.

JSON trees, temporary conversions, SQL evaluation frames/snapshots, container capacity, arena slack and curl internals are not fully counted. The server budget covers pipeline charges, not all SQL or scalar AI memory. Neither budget is a hard heap/RSS cap or proof that remote work stops when a query is canceled.

### Pipeline Results: 2026-09-20

- The 270-row gate test failed against the previous binary with only one SQL batch submitted before the first response. The same assertions now observe two pending batches, no third pending batch, and all rows including the final tail exactly once.
- Nine pipeline contracts cover slot refill, later-batch HTTP/provider failures, both sockets closing on cancellation/deadline, same-connection recovery, reversed completion with 400 rows of greater-than-10KiB LONGTEXT and other columns, plan eligibility, filtering/LIMIT, and rowsets of 0/1.
- Final complete suite: **71/71 passed in 98.875s**. Database CPU including bootstrap and all tests was 57.19s, peak RSS 491244KiB. These are context measurements, not a pipeline speedup or leak-proof claim.
- Shared-budget native tests and all earlier allocation/curl lifetime checks passed, including both deliberate cleanup-leak negative controls. Debug build, five fixture self-tests and Pylance syntax validation passed.
- Tests caught and fixed two ownership/execution defects: corrupted tail IDs from overwriting child datum pointers, and zero-length snapshot allocation in scalar mode. The result, request-count and error assertions were not weakened.

At this milestone, diagnosis mode and forced physical-operator rescan had not been independently exercised. The later shared-pipeline work below adds bounded native coverage. Fault injection covers the client, not every new operator/JSON allocation; no whole-heap sanitizer, real embedding endpoint, performance benchmark or worker-release test was run. The existing debug database and Notebook were not used or modified.

### Shared Completion Pipeline: 2026-09-24

AI_COMPLETE now uses the same `LogAIFunc` / `AIFuncSpec` / `AIFuncOp` pipeline as AI_EMBED. A single scheduling loop owns row copies, two slots, the shared 64MiB budget, ordered output, error propagation and cleanup. Function-specific branches retain their existing input preparation and request construction. `AIFuncBatch` owns provider validation and request state; the synchronous completion batch helper also drives this same object to completion, rather than maintaining a separate request implementation.

```sql
EXPLAIN SELECT id, AI_COMPLETE('my_completion_model', prompt)
FROM inputs ORDER BY id;
```

Each completion prompt remains an independent HTTP request with unchanged options and Schema validation. Two SQL batches do not mean two HTTP requests: a batch of N rows may submit N requests, and both batches share the operator budget. Default per-batch parallelism is unchanged. At this milestone the SQL thread still owned network progress, without a background scheduler or worker release. The following milestone changes network ownership, not plan eligibility, byte-overflow behavior or support for other AI functions.

- Negative control: the new completion gate contract failed against the old binary with only 16 requests arriving before any response. The unchanged contract passed after integration, proving overlap beyond one 16-row SQL batch.
- Eleven completion pipeline cases cover submission, refill before a peer completes, later-batch HTTP/provider/Schema errors, 32 live model socket FDs on cancel and zero afterward, deadline/recovery, out-of-order large LOB side columns, selective retry with unchanged Schema/options, plan eligibility, rowsets 0/1, filter/LIMIT boundaries, and stored JSON prompt objects with invalid-input rejection.
- Full SQL suite: **82/82 passed in 113.991s**. Database CPU including bootstrap and all contracts was 62.52s, peak RSS 495616KiB. These are context measurements, not a performance comparison. Five fixture self-tests also passed.
- Debug build and native resource tests passed, including both deliberately broken cleanup controls. Native operator tests directly exercise diagnosis passthrough and repeated empty-stream `rescan()` for both AI function types. They do not claim to exercise full SQL diagnostic reporting or rescan with retained rows/in-flight model requests.
- No real model service, existing debug database or Notebook was used. Active-request rescan, all operator allocation failures, whole-heap sanitizers and real-provider performance remain verification gaps.

## Background Network Scheduling: 2026-09-28

This section records the original network-scheduling milestone. Pipeline callers now use the [nonblocking admission and shared byte limits](#configurable-multi-slot-pipelines-2026-09-29) described below; synchronous callers retain their existing admission behavior.

The completion and native embedding batch entry points now call `start_async()` after preparing their existing client state. A process-wide `AIFuncScheduler` lazily starts one `AINetwork` thread. The low-level `send_post_batch_no_wait()` API itself remains caller-driven unless explicitly handed to the scheduler. Direct scalar completion and AI_RERANK paths have not been converted.

### Ownership And Admission

- The scheduler owns network progress for at most **64 registered batches**, each retaining its existing curl multi. This is not a 64-HTTP-request limit: per-batch request parallelism is unchanged. A full scheduler blocks admission, checks the original deadline/cancellation state and wakes when capacity becomes available.
- Pending clients are advanced on a 5ms timer, with notification on submission, cancellation and completion; an empty scheduler sleeps. This is a shared polling owner, not a unified socket-event loop or a per-query thread pool.
- Authentication, endpoint lookup and request preparation stay on the SQL thread. The client captures the submitting session and absolute deadline instead of reading the network thread's `THIS_WORKER` as the original query context. The synchronous caller keeps the borrowed session, allocator and validator alive until ownership is returned.
- The network owner handles transfers, retry timing and response validation. It publishes completion before the SQL thread reads results. Only the SQL thread writes evaluation frames. Cancel/reset waits for ownership to return before freeing provider or request memory; a client still requires one submitting/consuming caller, not arbitrary concurrent API access.
- Shared byte reservations/releases are atomic because the SQL thread can retain another batch while the network owner receives responses. The existing 64MiB per-operator logical budget and 4MiB/8MiB request/response limits are unchanged.
- AI service stop/destroy rejects new scheduler submissions, cancels active work and joins the network thread. Stop is idempotent and terminal for this process-local scheduler; in-process scheduler restart is not implemented.

Admission waiters have already prepared their requests. The 64 slots therefore do not cap all waiting request memory, HTTP connections or database RSS. There is no tenant fairness, endpoint quota, global byte budget, adaptive single-batch fallback or shared connection pool across clients.

**Background scheduling alone does not release the SQL worker.** At this milestone the operator and synchronous batch wrappers still waited on their existing call stacks. The subsequent [worker-suspension milestone](#sql-worker-suspension-2026-09-28) adds an opt-in continuation protocol for a restricted request path; it does not turn `Worker::sched_wait()`/`sched_run()` into general coroutine primitives.

### Verification And Limits

- Debug build, **82/82 SQL contracts** (117.063s), and **five fixture self-tests** passed on the final binary. Database CPU including bootstrap was 66.10s and peak RSS was 482340KiB; these are context measurements, not a performance comparison.
- Native resource tests passed, including independent progress without caller polling, 16 background cancel/reuse races, duplicate submission rejection, autonomous deadline, independent peer progress, full admission/deadline/recovery, both shared-budget modes and in-flight shutdown. All three omitted-action negative controls were detected.
- SQL resource checks again observed 36 failed batches and 36 recoveries, with eight model socket FDs in flight and zero after each query. Existing request-count, ordering, retry, Schema and error-code assertions were unchanged.
- Physical rescan coverage remains diagnosis passthrough and empty input, not retained rows or in-flight requests. No ASan/LSan/TSan, complete operator allocation sweep, real-provider benchmark or worker-release test was run. Local cancellation does not prove remote inference or billing stopped.
- No real model service, existing debug database or Notebook was used or modified.

## SQL Worker Suspension: 2026-09-28

The [AI operator](../../../../../src/sql/engine/basic/ob_ai_func_op.cpp) can now register a `RequestAwait` and return EAGAIN when its pipeline has no deliverable result. This is only a suspension when the request layer explicitly authorizes that exact execution context and a pending readiness probe is registered. Other EAGAIN errors and unmanaged callers retain their existing behavior.

### Request Ownership And Resume

- Only single-statement `COM_QUERY` SELECTs with a vectorized `AI FUNCTION PIPELINE` root are eligible. Multi-statement/batched requests, prepared execution/cursors, internal SQL, diagnosis, debug-sync and statement retries retain synchronous execution. The existing optimizer eligibility rules still apply.
- Each request worker retains at most **64 suspended requests**, with readiness checked on a 5ms queue wait. It executes other requests while AI responses are pending and resumes a ready request on the original worker. A worker with pending requests does not shrink out of the pool. There is no new SQL execution thread or cross-worker migration.
- Request memory is rooted independently of the temporary per-dispatch arena and does not borrow thread-local pages. The query processor no longer occupies the reusable TLS query buffer. Result set, driver, row count, header/open state, SQL statistics, session reference, original deadline and memory-statistics baseline survive suspension.
- Resume reacquires the session lock and restores warning/trace/allocator state. It does not parse, optimize, execute or open the plan again, resend metadata, or retry the whole SQL after a suspension. Completion/error releases the result and session before destroying request memory. The network thread still never writes SQL evaluation frames.
- Original deadline, KILL QUERY, disconnected sessions and network failures make the retained batches resumable through the existing background client. Worker stop cancels retained requests and drains them before exit. Cancellation/reset returns network ownership before releasing provider and request storage.
- If a worker's request slots are full or a retained context cannot be allocated, that new request uses the original synchronous path. The separate **64 network-batch slots** can also block submission before suspension; this change does not make admission, input preparation, child scans or response encoding nonblocking.

At this milestone the 64MiB logical quota was per AI operator, without a global byte budget, and full network admission could block before suspension. The [multi-slot implementation](#configurable-multi-slot-pipelines-2026-09-29) adds configurable local/global logical budgets and nonblocking pipeline admission. There is still no tenant fairness, work stealing or unified socket-event scheduler. Scalar/nonpipeline callers do not gain worker release, even where their HTTP client uses the network thread.

### Worker Suspension Verification

```bash
python tools/deploy/mysql_test/test_suite/ai_function/test_runtime_contracts.py \
  --shutdown-test --binary "$PWD/build_debug/bin/src/observer/seekdb"
```

- Complete SQL suite: **86/86 passed in 121.091s**, including worker release and client-disconnect checks for both completion and embedding. Database CPU including bootstrap was 65.38s, peak RSS 511628KiB; these are context measurements, not a performance or leak oracle.
- Worker-release cases hold every response, admit more concurrent queries than the original request-thread count, run a control SELECT, reject additional request threads, and then require exact rows and model-call counts. Queries enter the held state one at a time so ordinary burst-driven pool growth cannot masquerade as AI waiting. The disabled suspension path failed this same gate; the enabled path passed.
- Existing multi-batch refill, out-of-order/LOB output, later fatal error, deadline, cancellation, same-connection recovery and 36 failure/36 recovery socket checks pass unchanged. Disconnect checks observe zero model FDs before releasing held responses.
- The separate shutdown case holds both AI function types and requires normal process exit within 9 seconds after SIGUSR1, before the 20-second SQL deadline. SIGUSR1 includes a fixed 5-second preparation period and up to a 3-second main-loop wait. SIGTERM deliberately forces SIGKILL in this server and is not a graceful-cleanup oracle. The test observes exit without reaping the launcher's child process.
- Final Debug build, native resource/protocol tests and five fixture self-tests passed. Native protocol checks cover nested authorization, readiness, cancellation/reset and an exception on processor resume. The exception test first failed because the error was reported as success; the fixed path preserves the error and releases exactly once. Client fault tests and all three omitted-action negative controls still pass.
- Remaining gaps: a full per-worker request-slot exhaustion test, every retained-request/operator allocation failure, active-request physical rescan, whole-heap ASan/LSan/TSan, and real-provider performance. The shutdown check proves bounded normal server exit, not exhaustive cleanup instrumentation at every shutdown phase. No real model, existing debug database or Notebook was used.

## Configurable Multi-Slot Pipelines: 2026-09-29

Eligible AI_COMPLETE and AI_EMBED plans can retain more than two SQL batches without changing batch boundaries, model options, prompt contents, provider validation or per-request retry rules. The controls are independent of `rowsets_max_rows` and of the number of HTTP requests in a batch.

### Pipeline Configuration

| Control | Scope | Default | Range |
| --- | --- | --- | --- |
| `ai_pipeline_slots` | Session; global default for new sessions | 2 | 1-1024 |
| `ai_pipeline_memory_limit` | Session; global default for new sessions | 67108864 bytes (64MiB) | 1MiB-1TiB, specified in bytes |
| `ai_pipeline_total_memory_limit` | Dynamic server parameter, shared across pipelines | 1GiB | 1MiB-1TiB, capacity suffixes accepted |

```sql
SET SESSION ai_pipeline_slots = 8;
SET SESSION ai_pipeline_memory_limit = 268435456;

SELECT @@ai_pipeline_slots, @@ai_pipeline_memory_limit;

EXPLAIN SELECT id, AI_COMPLETE('my_completion_model', prompt)
FROM inputs ORDER BY id;
```

An administrator can adjust the instance-wide pipeline budget separately:

```sql
ALTER SYSTEM SET ai_pipeline_total_memory_limit = '2G';
SHOW PARAMETERS LIKE 'ai_pipeline_total_memory_limit';
```

The operator reads its session settings at open, including when reusing a cached plan. Changing the session does not resize an already executing operator. `SET GLOBAL` changes defaults for new sessions; it does not alter the separate server parameter. Increasing a pipeline's budget does not raise its clients' existing per-batch or individual HTTP limits. The configured slot count is a maximum, not a promise that every slot will be filled.

### Backpressure And Ownership

- Slots use a dynamic FIFO ring; arbitrary valid counts, including non-powers of two, are supported. Ready head results are returned before further prefetch; later errors remain visible. No whole-query retry or previously submitted batch replay is introduced.
- Additional prefetch stops once local charged bytes reach half the operator budget, leaving headroom for responses. Before copying a new batch, the operator measures retained row bytes. If those bytes cannot be admitted while this operator has active batches, it preserves the child position and waits for its existing work to progress. A first batch that cannot fit fails instead of waiting indefinitely for other queries.
- Local and global reservations are atomic. A global admission failure rolls back only the attempted local charge. Requests, responses and retained row/result bytes release their own charges on failure, retry cleanup, cancellation and reuse. Dynamic lowering of the server limit does not evict existing buffers; subsequent reservations must fit the new limit.
- At most 64 batches have a background network owner. A pipeline that reaches this limit retains one prepared, unadmitted batch and stops further prefetch. It checks capacity on the existing readiness interval, preserving the original deadline and cancellation state. Eligible SQL requests can suspend while waiting; synchronous/nonpipeline callers retain their old behavior.
- The worker's 64 suspended-request slots, the network's 64 active-batch slots, and each operator's configured batch slots are different limits. Full worker capacity or retained-context allocation failure still falls back to synchronous execution. No new SQL workers, thread per batch or cross-worker request migration is introduced.

This remains a bounded logical-buffer policy, not exact heap accounting, fair scheduling or a guaranteed-success memory estimator. Prepared requests and unpredictable responses can still exceed hard limits and return 4019. There is no spill-to-disk path. More slots can submit more work before a failure or cancellation is observed; closing local HTTP requests does not prove remote inference or billing stopped.

### Multi-Slot Verification

- Final Debug build and **94/94 SQL contracts** passed, along with **five fixture self-tests**, the native resource/protocol suite and the separate normal-shutdown test. All use private temporary databases and loopback mocks, not real providers or the existing debug database/Notebook.
- Configuration tests cover defaults, invalid ranges, session isolation and 1/3/5-slot windows for both AI functions. Held responses verify exact in-flight batch counts; complete outputs, tails, ring reuse and logical input counts must match.
- The byte-pressure test uses inline VARBINARY side columns, not the external payload size of LOB locators. With five configured slots, 1MiB allows one held batch while 4MiB allows all four input batches; both paths preserve rows and request counts after release.
- A two-connection SQL test sets the server limit to 1MiB: the second query fails before sending a model request, the first query remains intact, and released quota supports a subsequent successful execution. Native tests check cross-query accounting, exact rollback and 1MiB/64MiB/128MiB local budgets.
- A 65-slot embedding query fills all 64 network positions. More queries than the original request-worker count then wait for admission while a control SELECT completes and the worker thread set remains unchanged. Native checks cover immediate nonblocking admission, duplicate submission rejection, deadline, cancellation and capacity reuse.
- Five-slot completion/embedding disconnects and a disconnect while the 65th batch is awaiting admission release model sockets before mock responses are allowed. Existing ordering, LOB, retry, error, cancellation/recovery and shutdown contracts remain in the complete suite.
- Remaining gaps include exhaustive operator allocation injection, active-request physical rescan, all 1024 configured slots under pressure, complete per-worker request-slot exhaustion, ASan/LSan/TSan and real-provider performance. The fixed 50% prefetch watermark is conservative, not a learned model-memory estimate.

## Constrained Output

AI_COMPLETE accepts explicit JSON Schema constraints through its existing third argument on the OpenAI-compatible completion path. It sends `response_format.type = "json_schema"` and the original Schema to the inference service, which must support structured output generation. This follows BlendSQL's separation between output constraints and independent per-row requests, but uses the standard JSON Schema request format rather than compiling a grammar or sending legacy `guided_*` fields.

The model must already be registered with a compatible endpoint. Given an `inputs(id, prompt)` table, replace `my_model` with its registered model name:

```sql
SELECT id, AI_COMPLETE('my_model', prompt, '{
  "response_format": {
    "type": "json_schema",
    "json_schema": {
      "name": "classification",
      "strict": true,
      "schema": {
        "type": "object",
        "properties": {
          "label": {"type": "string", "enum": ["ENGINE", "BODY"]}
        },
        "required": ["label"],
        "additionalProperties": false
      }
    }
  }
}') AS result
FROM inputs;
```

An accepted result is JSON text such as `{"label":"ENGINE"}`. AI_COMPLETE retains its SQL string result type and preserves the returned content, including whitespace and case. It does not strip code fences, repair JSON, coerce a wrong value, or regenerate invalid content. An enum constrains the output domain, not the semantic correctness of the selected label.

### Supported Contract

Every Schema node must be an object with an explicit, single `type`. Root scalars and arrays are locally supported, but a particular service may require an object root or impose additional restrictions. Schema property names, enums and descriptions are sent unchanged; the local compiled Schema uses a separate copy and is reused across a constant-config batch.

| Area | Supported keywords or values |
| --- | --- |
| Types | `object`, `array`, `string`, `integer`, `number`, `boolean`, `null` |
| Allowed values | Nonempty `enum` with distinct JSON values |
| Objects | Recursive `properties`, string-array `required`, boolean `additionalProperties` |
| Arrays | One recursive `items` Schema, `minItems`, `maxItems`, `uniqueItems` |
| Strings | `minLength`, `maxLength`, measured in Unicode code points, not UTF-8 bytes or graphemes |
| Numbers | Inclusive `minimum`, `maximum` |
| Metadata | String `title`, `description` |

Unsupported keywords are rejected rather than ignored. This includes `$ref`, `$defs`, `definitions`, `$schema`, `const`, `pattern`, `format`, combinators such as `anyOf`, union-type arrays and boolean Schemas. Length/count constraints must be nonnegative integers. This is a deliberately limited Schema subset, not a claim of full JSON Schema support.

The `json_schema` definition requires a nonempty string `name`, `strict: true` and an object `schema`. Streaming must be omitted or false; `n` must be omitted or 1. Combining this mode with `structured_outputs`, `guided_json`, `guided_grammar`, `guided_regex` or `guided_choice` is rejected. No automatic SQL type inference, new SQL function or new runtime dependency is added. Existing unconstrained calls and `json_object` mode do not gain Schema guarantees.

### Failure Semantics

The [completion provider](../../../../../src/sql/engine/expr/ob_expr_ai/ob_ai_func_utils.cpp#L140) checks configuration before HTTP submission. When a transfer completes, the [HTTP client](../../../../../src/sql/engine/expr/ob_expr_ai/ob_ai_func_client.cpp#L350) invokes provider validation before recording success. Constrained responses require exactly one choice, `finish_reason = "stop"`, string content, no non-null `refusal`, valid JSON and a matching Schema. Temporary validation data is freed after each check. Successful results are still returned only after the entire batch completes and are aligned with their original rows.

| Failure | SQL behavior |
| --- | --- |
| Invalid configuration or unsupported Schema | 1210 before sending a request |
| Native `aliyun-dashscope` provider with `response_format` | 1235 before HTTP; register an OpenAI-compatible endpoint for this feature |
| Schema violation, missing/wrong finish reason, refusal or wrong response shape | 4070; no output-repair or regeneration attempt |
| Invalid JSON content | JSON parser error, including 3140/5447; never silently converted to NULL or accepted text |
| Endpoint rejects structured output with HTTP 400 | 4216; no retry without the constraints |

Permanent validation failures terminate the local batch without waiting for held peers. Normal HTTP retry policy remains unchanged. Canceling local transfers does not retract requests already accepted by the service. Default concurrency still equals the current batch's eligible request count; constrained decoding does not add deduplication, prompt batching, cross-batch overlap or an independent task queue.

**Service boundary:** local validation cannot prove that a service applied a token mask. A service that ignores `response_format` but happens to return valid content cannot be distinguished by this check alone. Real constrained generation requires a supporting model/backend and a compatible deployed API, such as a correctly configured vLLM structured-output endpoint. The copilot-api checks recorded below establish acceptance of requests carrying a Schema and valid SQL results, not token-level enforcement. No feature-negotiation handshake, GPU backend or token-level trace is included here.

### Validation: 2026-09-18

- The first negative control returned valid JSON with `"engine"` outside the `"ENGINE"`/`"BODY"` enum. It failed against the earlier binary because no SQL error was raised, then passed after local Schema validation was added.
- A Unicode boundary test exposed byte-based string length in the shared JSON Schema validator. Both validator paths now count code points; direct JSON_SCHEMA_VALID and AI tests cover ASCII, Chinese, supplementary-plane characters, combining marks, empty strings and embedded NUL.
- The first full run hit a 40-second client timeout in the 60-second large-batch query, invalidating the connection for subsequent tests. After correcting the client budget, the same query hit its SQL deadline with only 1470/2048 requests received. Changing only the mock listen backlog from 64 to 4096 completed all 2048 requests in 8.403s. Query deadlines, input counts and result assertions were not relaxed.
- Full SQL contract suite: **43/43 passed** in 65.074s after the fixture corrections, including all 14 new constrained-output cases and the existing provider fail-fast case. The provider error-code and latency assertions remain strict.
- Smoke suite passed: 24-row batch 0.824s versus scalar 6.105s. Database CPU including bootstrap and the smoke suite was 18.86s, peak RSS 365228KiB. This is mock evidence, not a constrained-decoding performance measurement.
- Debug build and focused Unicode checks passed before the complete run. No real model service, existing debug database or Notebook was used or modified.

Remaining work includes server-side decoding evidence, broader real-endpoint Schema coverage, wider allocator-failure/sanitizer coverage and broader provider testing. The resource tests above cover client-owned resources and repeated SQL failure recovery, not Schema compiler allocation faults. Schema compilation/validation allocations are not included in the HTTP client's 64MiB logical byte budget; no constant-RSS or arbitrary-Schema complexity guarantee is claimed. The historical live JSON failure below has not been rerun and is not retroactively marked fixed.

## Real Model Service Tests

[test_runtime_live.py](test_runtime_live.py) uses the actual API configured in [seekdb-debug.ipynb](../../../../../../seekdb-debug.ipynb), not the mock HTTP service. It starts a separate temporary Debug seekdb, registers a temporary OpenAI-compatible model/endpoint, and executes SQL against that database. It does not connect to or restart the Notebook's database. The Notebook and production C++ code are unchanged.

Real requests can incur charges. Only generated arithmetic inputs, row identifiers and the Notebook's synthetic marker prompt leave the machine. No table data from an existing database is used. This suite deliberately excludes fault injection, giant payloads, timeout/cancellation stress and retry-exhaustion tests against the real provider.

```bash
# Offline oracle and opt-in guard checks; no model requests.
python tools/deploy/mysql_test/test_suite/ai_function/test_runtime_live.py --self-test

# One real request using the original Notebook prompt.
SEEKDB_AI_LIVE_CASE=scalar python tools/deploy/mysql_test/test_suite/ai_function/test_runtime_live.py \
  --binary "$PWD/build_debug/bin/src/observer/seekdb" --allow-live

# Constrained single-row and three-row batch tests only: four planned requests.
SEEKDB_AI_LIVE_CASE=constrained python tools/deploy/mysql_test/test_suite/ai_function/test_runtime_live.py \
  --binary "$PWD/build_debug/bin/src/observer/seekdb" --allow-live

# Full live suite, stops at the first failing test.
python tools/deploy/mysql_test/test_suite/ai_function/test_runtime_live.py \
  --binary "$PWD/build_debug/bin/src/observer/seekdb" --allow-live
```

Use `/volume/xicksys/.venv/bin/python` in the current workspace. The explicit `--allow-live` flag is mandatory for live execution; omission is tested to fail before database startup. Do not put real credentials in command-line arguments or commit them to files.

| Environment variable | Default / accepted values |
| --- | --- |
| `SEEKDB_AI_API_BASE` | `http://copilot-api:4141/v1` |
| `SEEKDB_AI_MODEL` | `gemini-3.8-flash`, sent verbatim |
| `SEEKDB_AI_API_KEY` | `dummy`; provide real credentials via the process environment when needed |
| `SEEKDB_AI_LIVE_CASE` | `all`, `scalar`, `batch`, `fallback`, `json`, `constrained-scalar`, `constrained-batch`, or the two-case group `constrained`; default `all` |

The tests append `/chat/completions` to the API base. For the default `copilot-api` host, the isolated database process bypasses the proxy, matching the Notebook's native debugging setup. Existing proxy exceptions are retained; global shell settings are not changed. Custom endpoint hosts follow the caller's proxy settings.

| Case | Planned logical requests | Independent check |
| --- | --- | --- |
| `scalar` | 1 | Original Notebook prompt returns exactly `SEEKDB_AI_OK`, ignoring surrounding whitespace only |
| `batch` | 3 | Three rows with constant model/config each return the correct random row ID and arithmetic answer; a fourth NULL prompt in an unselected CASE branch yields `skipped` |
| `fallback` | 3 | Dynamic model column query returns each row's independently computed answer |
| `json` | 1 | `AI_PROMPT('{0}', ...)` passes a prompt yielding the expected row ID and sum |
| `constrained-scalar` | 1 | Explicit Schema requires the generated row ID from an enum and an integer answer, with both fields required and no extra keys; independent arithmetic check |
| `constrained-batch` | 3 | Same output contract for three row IDs; exact per-row answers and a fourth NULL input skipped by CASE |

The full suite now plans twelve logical completions, or four when selecting only `constrained`. These are planned SQL inputs, not measured upstream attempt counts: the existing client's retries may increase network calls. The full runner starts with scalar, batch, fallback and JSON, followed by the two constrained cases. Each SQL query has a 180-second budget and the SQL connection waits 210 seconds, unlike the Notebook's 30-minute debugger allowance. This is a bounded live-test budget, not a claim about the provider's latency SLA. Errors remain failures; the script does not silently change models, retry the whole suite or extend the budget.

Arithmetic cases use `temperature=0` and `max_tokens=256`. Expected answers are computed before calling the model. The oracle rejects wrong row IDs, wrong sums, empty text, extra keys and string/boolean answers; it does not strip Markdown fences to make invalid JSON pass. Model instruction-following failures are integration failures and require diagnosis, not automatic attribution to seekdb. Exact equality between two nondeterministic generations and speedup thresholds are not asserted.

Without provider-side instrumentation, this suite cannot prove actual upstream concurrency, request counts or absence of unseen extra requests. Those checks remain in the controlled mock suite. Real-call SQL durations include the network and provider and are reported as timings, not database performance conclusions.

### Live Results: 2026-09-17

- Shared launcher changes passed the existing offline smoke regression; four live-test oracle/opt-in checks passed without network access.
- Initial real batch/CASE execution reached the 180-second SQL deadline and failed with 4012. Fail-fast prevented fallback and JSON cases from running.
- A non-inference connectivity check returned HTTP 200 from `/v1/models` at peer `192.168.3.3` in about 3ms.
- The isolated `scalar` case subsequently passed with the exact `SEEKDB_AI_OK` response; SQL wall time was 160.140s.
- At that point, no successful live batch result had been established, and live fallback/JSON cases remained unexecuted. A reachable model-list endpoint does not establish inference health, and the batch timeout alone does not isolate a seekdb bug from provider latency or queuing.

Temporary registrations, test database processes and directories were cleaned up after both live runs. This does not guarantee that an upstream model stops computation after a local timeout. At that time the provider fail-fast defect in the independent mock contract suite remained unresolved; it is fixed by the later per-response validation described above.

### Retest After Network Recovery: 2026-09-17

The user reported that the network issue had been resolved. The full live suite was rerun with the same API, `gemini-3.8-flash`, 180-second SQL budget, inference options and assertions. No production or test code was changed.

| Case | SQL wall time | Result |
| --- | --- | --- |
| Scalar Notebook prompt | 2.759s | Passed; exact `SEEKDB_AI_OK` |
| Three-row batch and CASE skip | 4.228s | Passed; expected row IDs, arithmetic answers and skipped row |
| Dynamic-model scalar fallback | 11.757s | SQL returned, but at least one result failed JSON parsing with `Unterminated string starting at: line 1 column 12` |
| JSON prompt | 3.658s | Passed in a separate invocation after the full suite stopped at the fallback failure |

Across the two invocations, all four cases were exercised: three passed and one failed, with eight planned logical completions and no query timeouts. The fallback case was not repeatedly retried until it passed, and its JSON assertion was not weakened. The actual upstream attempt count was not measured.

The remaining failure is an output-format/integration failure, not the earlier network timeout. Its origin is not established: returned SQL text was not complete valid JSON, but this alone cannot distinguish model output or generation truncation from a database output issue. The existing output-token limit and all other test settings were left unchanged. Both isolated database instances and their temporary registrations were cleaned up.

### Constrained Live Results: 2026-09-20

With user authorization, the real `http://copilot-api:4141/v1/chat/completions` endpoint and `gemini-3.8-flash` model were tested using `temperature=0`, `max_tokens=256` and the existing 180-second budget. Only synthetic arithmetic prompts and random row IDs were sent. No mock service participated in these calls.

| Check | Planned requests | Wall time | Result |
| --- | --- | --- | --- |
| Direct HTTP Schema probe, without seekdb | 1 | 4.137s | HTTP 200, `finish_reason="stop"`, no refusal, exact row ID and integer answer |
| SQL `constrained-scalar` | 1 | 3.709s | Passed seekdb's output-contract validation and the independent row-ID/arithmetic oracle |
| SQL `constrained-batch` | 3 | 4.147s | All three results matched their original rows; the fourth NULL input returned `skipped` |

The SQL run used the `constrained` selector and an isolated temporary database. Both tests passed on the first run; registrations, process and temporary data were cleaned up. Five offline oracle/opt-in tests and Python syntax/editor diagnostics also passed. No production C++ code, Notebook or existing debug database was changed for this testing step.

The direct probe issued one HTTP request with no retry. The SQL runner planned four requests; its existing per-request retry policy and the gateway's upstream attempts were not instrumented, so five logical calls must not be presented as a measured upstream request count. There was no retry-until-pass, model switch, token-budget increase or constraint downgrade.

This establishes that the endpoint accepts this object/enum/integer Schema request and that seekdb can consume valid real-model results through single-row and batch SQL. It does not establish support for the entire local Schema subset, prove the exact remote concurrency, or prove token masking: the prompts also requested the same JSON format, and no decoding backend trace or constrained/unconstrained comparison was captured. The previous unconstrained dynamic-model fallback failure was not rerun or marked fixed.
