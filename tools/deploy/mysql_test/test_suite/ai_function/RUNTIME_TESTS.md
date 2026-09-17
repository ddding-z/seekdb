# AI Function Runtime Regression

Run from the repository root after a Debug build:

```bash
python tools/deploy/mysql_test/test_suite/ai_function/test_runtime.py \
  --binary "$PWD/build_debug/bin/src/observer/seekdb"
```

Requires Linux, Python 3, PyMySQL and a runnable seekdb binary with its runtime libraries available. In the current workspace, `/volume/xicksys/.venv/bin/python` has PyMySQL installed. Allow several GiB of temporary disk space and at least 2GiB of memory for the isolated database.

The script creates a private temporary database directory, starts seekdb with TCP SQL and RPC disabled, and runs a loopback-only HTTP mock on a random port. It does not connect to an existing database or a real model. The database process and temporary data are cleaned up on exit; the model access key is a dummy test value. No global proxy settings are changed.

Assertions cover request/output equivalence, bounded concurrency, window refill across the SQL batch, reordered completion, per-request retry, constant/dynamic parameters, CASE skips, repeated expressions, NULL/JSON/LOB behavior, malformed responses, size limits, deadline and query cancellation. Timing compares identical mocked work through batch and scalar paths. Reported process CPU and peak RSS include database bootstrap and the whole suite, not a per-query resource comparison.

Allocation failure, low-level `no_wait` lifetime checks, other AI functions/providers and detailed resource comparisons remain separate verification work. This standalone script is not registered in the mysqltest `.test` runner.

## Independent Contract Tests

The original script is a smoke/performance comparison, not the acceptance oracle. Some of its historical negative cases accept any MySQL error; its timeout and cancellation checks now require error codes 4012 and 1317 and evidence of submitted requests. Run the stricter suite separately:

```bash
python tools/deploy/mysql_test/test_suite/ai_function/test_runtime_contracts.py --self-test
python tools/deploy/mysql_test/test_suite/ai_function/test_runtime_contracts.py \
  --binary "$PWD/build_debug/bin/src/observer/seekdb"
```

The contract suite uses stdlib `unittest` to collect failures and continue through independent cases. It reuses only the isolated database launcher, not the smoke test's assertions. Each SQL test gets a separate loopback server and port, and joins the server's request threads during cleanup. Requests already sent before cancellation cannot contaminate the next case.

### Oracle Design

Expected behavior comes from the execution plan, SQL evaluation boundaries, HTTP semantics, and the public error definitions in [ob_errno.def](../../../../../src/share/ob_errno.def), not from recorded output of the modified client.

| Area | Independent expectation | Defect the case can expose |
| --- | --- | --- |
| Input preservation | Explicit model, one user message per row, unchanged options, exact independent result | Both scalar and batch making the same wrong request |
| Default window | Hold 51 responses; exactly 50 requests may be active, and the 51st starts only after one slot is released | An unchanged default of 8, unbounded submission or failure to refill |
| Continuous window | Hold the first response until at least 33 requests arrive; all 64 results remain aligned, with peak activity at most 50 | A fixed preparation boundary idling available concurrency |
| Large batch | 2048 rows with `OPT_PARAM('rowsets_max_rows', 2048)`, exact outputs and one request per row | A leftover 1024-request limit, missing rows or duplicate submissions |
| Logical row set | Duplicate prompts still sent per row; CASE skips invalid unselected inputs; LIMIT 0/3 sends only needed rows in the tested plan | Hidden deduplication or unnecessary remote work |
| Strings | Exact Unicode, NUL, quotes, backslash and newline round trip | Truncation, encoding or double escaping |
| Error classification | Invalid argument 1210; HTTP/uncertain transport 4216; provider format 4070; size 4019; non-object JSON 3140 | Treating any exception as a passing negative test |
| JSON syntax | Either documented JSON error 3140 or parser syntax error 5447, never arbitrary failure | Misclassifying transport/timeouts as malformed JSON |
| Retry-After | Second attempt after advertised four-second delay or absolute HTTP-date | Merely seeing two requests even when the header is ignored |
| Retry policy | Temporary 429/500/502/503/504 retry; successful rows once; maximum four attempts | Whole-batch retries or infinite attempts |
| Uncertain POST result | Mock reads the request then drops/truncates response; no retry | Duplicate model execution after uncertain transport failure |
| Fail-fast | Fatal HTTP, JSON and provider errors must return before held peers' deadline and stop refill | Deferred validation hidden by other requests' timeout |
| Byte limits | Valid response JSON at 8MiB minus one, exactly 8MiB and plus one; nine individually valid large responses exceed 64MiB total | Oversize fixture failing only because it was invalid JSON, or per-response-only accounting |
| Serialized request | 1MiB control-character input expands above 4MiB when JSON escaped | Checking prompt character length instead of request bytes |
| Deadline/cancel | Long Retry-After cannot extend query deadline; cancel during backoff returns 1317 promptly and next query succeeds | Ignoring cancel while no transfer is active |
| Shared client | Known embedding vector and dimensions survive a 502 retry | Completion-only tests missing embedding regressions |
| Credentials | Endpoint key rotation applies to the same SQL on next execution | Stale credentials retained across executions |

Four fixture self-tests check exact valid-JSON byte lengths, scripted HTTP responses, case-insensitive header names, and negative controls: wrong SQL codes and unexpected success must fail. Threading events hold peer responses for fail-fast tests; no arbitrary sleep is used to infer their completion. Retry timing assertions allow 50ms scheduling/measurement tolerance. HTTP-date tests assume the local wall clock is not stepped during the run.

The default window (50), retry count (3 retries), and byte limits are versioned first-stage policy values. They are deliberately not imported from the implementation under test; an intentional policy change requires updating the specification and these expectations together. Timing ratios in the smoke benchmark are not semantic correctness gates. LIMIT and reused-expression checks demonstrate the concrete tested query shapes, not every possible optimizer plan. This suite does not prove constant total process memory, exactly-once remote execution, or absence of resource leaks.

### Results: 2026-09-17

- Fixture self-tests: 4 passed.
- Existing smoke suite with stricter timeout/cancel checks: passed.
- Independent SQL suite: 26 tests, 25 passed, 1 failed (46.584s in the recorded run).
- No production code was modified during this testing task; the current Debug binary from the prior build was tested.

The failing case is `RuntimeContracts.test_provider_error_stops_refill_without_waiting_for_peers`. The mock responds HTTP 200 with `{"unexpected":true}` for one row, while holding other responses. The single-request control confirms error 4070. With peers active, the query instead returns timeout 4012 after 2.002s and has submitted 9 requests. The expectation remains 4070 before 1.5s and no refill after the permanent error is observed. The test is not skipped or marked expected-failure, so the suite currently exits with status 1.

The cause is visible in [call_completion_vector](../../../../../src/sql/engine/expr/ob_expr_ai/ob_ai_func_utils.cpp#L1431): provider parsing happens only after `send_post_batch` finishes the entire batch. HTTP and JSON validation already fail promptly, but provider validation cannot stop refill or outrank a later peer timeout. This is a known failed acceptance criterion, not permission to redefine the expected result as a timeout.

An earlier apparent LIMIT failure was traced to late requests from a prior case entering shared mock counters. Per-case ports and joined request threads removed that test-fixture defect without weakening the LIMIT assertion.

Remaining gaps: allocator fault injection, low-level `no_wait` cleanup, leak/sanitizer evidence, DNS/connection failure, diagnosis mode, denied-access/multi-tenant isolation, AI_RERANK and non-OpenAI providers, and per-query CPU/RSS/tail-latency comparisons. The embedding case is a focused regression, not full embedding coverage. These are not reported as passed.

### Whole-Batch Scheduling: 2026-09-17

AI_COMPLETE prepares all eligible rows in the current SQL batch and submits them through one client window, following Sema's whole-batch queueing approach. The former 32-row preparation barriers are removed. The HTTP window was 8 at this stage and was subsequently raised to 50 as recorded below; this is not 2048 concurrent requests, prompt batching, lazy request preparation or cross-batch asynchronous execution.

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

The client now defaults to 50 active requests. The configurable range remains 1-64; retry, deadline, byte limits and connection-pool lifetime are unchanged. This is a per-client request limit, not a curl connection limit or database-wide quota.

The new gate test starts 51 logical inputs, holds all responses, observes 50 active requests with the 51st still queued, then releases one response and requires the queued request to start. Existing fail-fast and cancellation inputs exceed the new window so those tests still exercise pending requests.

Debug build and editor diagnostics passed. Eight focused SQL contracts passed (12.820s): the default window, continuous refill, 2048 rows, aggregate response budget, fatal HTTP/JSON errors, retry deadline and cancellation recovery. The smoke suite also passed. The full contract suite, the known provider fail-fast failure and connection-reuse tests were not rerun for this default-value change. No real model service was called.

## Real Model Service Tests

[test_runtime_live.py](test_runtime_live.py) uses the actual API configured in [seekdb-debug.ipynb](../../../../../../seekdb-debug.ipynb), not the mock HTTP service. It starts a separate temporary Debug seekdb, registers a temporary OpenAI-compatible model/endpoint, and executes SQL against that database. It does not connect to or restart the Notebook's database. The Notebook and production C++ code are unchanged.

Real requests can incur charges. Only generated arithmetic inputs, row identifiers and the Notebook's synthetic marker prompt leave the machine. No table data from an existing database is used. This suite deliberately excludes fault injection, giant payloads, timeout/cancellation stress and retry-exhaustion tests against the real provider.

```bash
# Offline oracle and opt-in guard checks; no model requests.
python tools/deploy/mysql_test/test_suite/ai_function/test_runtime_live.py --self-test

# One real request using the original Notebook prompt.
SEEKDB_AI_LIVE_CASE=scalar python tools/deploy/mysql_test/test_suite/ai_function/test_runtime_live.py \
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
| `SEEKDB_AI_LIVE_CASE` | `all`, `scalar`, `batch`, `fallback`, `json`; default `all` |

The tests append `/chat/completions` to the API base. For the default `copilot-api` host, the isolated database process bypasses the proxy, matching the Notebook's native debugging setup. Existing proxy exceptions are retained; global shell settings are not changed. Custom endpoint hosts follow the caller's proxy settings.

| Case | Planned logical requests | Independent check |
| --- | --- | --- |
| `scalar` | 1 | Original Notebook prompt returns exactly `SEEKDB_AI_OK`, ignoring surrounding whitespace only |
| `batch` | 3 | Three rows with constant model/config each return the correct random row ID and arithmetic answer; a fourth NULL prompt in an unselected CASE branch yields `skipped` |
| `fallback` | 3 | Dynamic model column query returns each row's independently computed answer |
| `json` | 1 | `AI_PROMPT('{0}', ...)` passes a prompt yielding the expected row ID and sum |

The full suite normally requests eight logical completions. These are planned SQL inputs, not measured upstream attempt counts: the existing client's retries may increase network calls. In the final runner, the scalar case runs first, followed by batch, fallback and JSON. Each SQL query has a 180-second budget and the SQL connection waits 210 seconds, unlike the Notebook's 30-minute debugger allowance. This is a bounded live-test budget, not a claim about the provider's latency SLA. Errors remain failures; the script does not silently change models, retry the whole suite or extend the budget.

Arithmetic cases use `temperature=0` and `max_tokens=256`. Expected answers are computed before calling the model. The oracle rejects wrong row IDs, wrong sums, empty text, extra keys and string/boolean answers; it does not strip Markdown fences to make invalid JSON pass. Model instruction-following failures are integration failures and require diagnosis, not automatic attribution to seekdb. Exact equality between two nondeterministic generations and speedup thresholds are not asserted.

Without provider-side instrumentation, this suite cannot prove actual upstream concurrency, request counts or absence of unseen extra requests. Those checks remain in the controlled mock suite. Real-call SQL durations include the network and provider and are reported as timings, not database performance conclusions.

### Live Results: 2026-09-17

- Shared launcher changes passed the existing offline smoke regression; four live-test oracle/opt-in checks passed without network access.
- Initial real batch/CASE execution reached the 180-second SQL deadline and failed with 4012. Fail-fast prevented fallback and JSON cases from running.
- A non-inference connectivity check returned HTTP 200 from `/v1/models` at peer `192.168.3.3` in about 3ms.
- The isolated `scalar` case subsequently passed with the exact `SEEKDB_AI_OK` response; SQL wall time was 160.140s.
- At that point, no successful live batch result had been established, and live fallback/JSON cases remained unexecuted. A reachable model-list endpoint does not establish inference health, and the batch timeout alone does not isolate a seekdb bug from provider latency or queuing.

Temporary registrations, test database processes and directories were cleaned up after both live runs. This does not guarantee that an upstream model stops computation after a local timeout. The previously identified provider fail-fast defect in the independent mock contract suite remains unresolved.

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
