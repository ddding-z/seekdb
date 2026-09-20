# AI Function Runtime Regression

Latest validation (2026-09-18): all 43 SQL contracts and the smoke suite passed after adding [constrained output](#constrained-output). Earlier failures below are retained as historical evidence.

Run from the repository root after a Debug build:

```bash
python tools/deploy/mysql_test/test_suite/ai_function/test_runtime.py \
  --binary "$PWD/build_debug/bin/src/observer/seekdb"
```

Requires Linux, Python 3, PyMySQL and a runnable seekdb binary with its runtime libraries available. In the current workspace, `/volume/xicksys/.venv/bin/python` has PyMySQL installed. Allow several GiB of temporary disk space and at least 2GiB of memory for the isolated database.

The script creates a private temporary database directory, starts seekdb with TCP SQL and RPC disabled, and runs a loopback-only HTTP mock on a random port. It does not connect to an existing database or a real model. The database process and temporary data are cleaned up on exit; the model access key is a dummy test value. No global proxy settings are changed.

Assertions cover request/output equivalence, batch-sized concurrency, whole-batch submission, reordered completion, per-request retry, constant/dynamic parameters, CASE skips, repeated expressions, NULL/JSON/LOB behavior, malformed responses, size limits, deadline and query cancellation. Timing compares identical mocked work through batch and scalar paths. Reported process CPU and peak RSS include database bootstrap and the whole suite, not a per-query resource comparison.

Allocation failure, low-level `no_wait` lifetime checks, other AI functions/providers and detailed resource comparisons remain separate verification work. This standalone script is not registered in the mysqltest `.test` runner.

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
| Shared client | Known embedding vector and dimensions survive a 502 retry | Completion-only tests missing embedding regressions |
| Credentials | Endpoint key rotation applies to the same SQL on next execution | Stale credentials retained across executions |

Four fixture self-tests check exact valid-JSON byte lengths, scripted HTTP responses, case-insensitive header names, and negative controls: wrong SQL codes and unexpected success must fail. Threading events hold peer responses for fail-fast tests; no arbitrary sleep is used to infer their completion. Retry timing assertions allow 50ms scheduling/measurement tolerance. HTTP-date tests assume the local wall clock is not stepped during the run.

The default batch-sized concurrency, retry count (3 retries), and byte limits are versioned first-stage policies. They are deliberately not imported from the implementation under test; an intentional policy change requires updating the specification and these expectations together. Timing ratios in the smoke benchmark are not semantic correctness gates. LIMIT and reused-expression checks demonstrate the concrete tested query shapes, not every possible optimizer plan. This suite does not prove constant total process memory, exactly-once remote execution, or absence of resource leaks.

### Results: 2026-09-17

- Fixture self-tests: 4 passed.
- Existing smoke suite with stricter timeout/cancel checks: passed.
- Independent SQL suite: 26 tests, 25 passed, 1 failed (46.584s in the recorded run).
- No production code was modified during this testing task; the current Debug binary from the prior build was tested.

The failing case was `RuntimeContracts.test_provider_error_stops_refill_without_waiting_for_peers`. The mock responds HTTP 200 with `{"unexpected":true}` for one row, while holding other responses. The single-request control confirms error 4070. With peers active, that version returned timeout 4012 after 2.002s and submitted 9 requests. The expectation remained 4070 before 1.5s and no refill after the permanent error was observed. The test was not skipped or marked expected-failure, so that run exited with status 1.

The cause was provider parsing in [call_completion_vector](../../../../../src/sql/engine/expr/ob_expr_ai/ob_ai_func_utils.cpp) only after `send_post_batch` finished the entire batch. HTTP and JSON validation already failed promptly, but provider validation could not stop refill or outrank a later peer timeout. The constrained-output change below validates each completion response immediately and fixes this defect without accepting timeout as the expected error.

An earlier apparent LIMIT failure was traced to late requests from a prior case entering shared mock counters. Per-case ports and joined request threads removed that test-fixture defect without weakening the LIMIT assertion.

Remaining gaps: allocator fault injection, low-level `no_wait` cleanup, leak/sanitizer evidence, DNS/connection failure, diagnosis mode, denied-access/multi-tenant isolation, AI_RERANK and non-OpenAI providers, and per-query CPU/RSS/tail-latency comparisons. The embedding case is a focused regression, not full embedding coverage. These are not reported as passed.

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

No real model service, existing debug database or Notebook was used or modified for these tests. Explicit positive client limits and low-level `no_wait` lifetime behavior were not independently exercised.

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

Remaining work includes server-side decoding evidence, broader real-endpoint Schema coverage, resource-failure/sanitizer coverage and broader provider testing. Schema compilation/validation allocations are not included in the HTTP client's 64MiB logical byte budget; no constant-RSS or arbitrary-Schema complexity guarantee is claimed. The historical live JSON failure below has not been rerun and is not retroactively marked fixed.

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
