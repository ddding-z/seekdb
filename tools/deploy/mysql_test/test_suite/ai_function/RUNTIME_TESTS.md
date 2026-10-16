# AI Function Runtime Regression

Legacy baseline validation (2026-09-29): the Debug build, all **105 SQL contracts** (including ten [global SOLO contracts](#global-solo-prompt-reordering-2026-09-29)), **five fixture self-tests**, the [native resource tests](#resource-reliability-tests), and the separate server-shutdown contract passed. The earlier [suspended-statement cache lifetime fix](#suspended-statement-cache-lifetime-2026-09-29) remains covered. Eligible single-statement AI pipeline SELECTs return their worker while waiting and resume from the retained result cursor. The newer semantic-operator validation is recorded below.

For the current architecture, code map, maintenance invariants and handoff checklist, see the [Chinese maintenance handoff](../../../../../docs/developer-guide/zh/ai-function-runtime-handoff.md).

## Native Semantic Rank: Numeric Per-Row Scoring

`AI_RANK(model, input[, options])` implements SemBench's numeric `AI.SCORE`
semantics. It returns a native SQL **DOUBLE**, not text or a SQL cast of
`AI_MAP`. It is not the existing `AI_RERANK` interface, pairwise comparison
ranking, or a specialized Top-K search algorithm.

SemBench movie Q9 scores each review; Q10 averages those scores per movie:

```sql
SELECT reviewId,
       AI_RANK('judge_model',
               AI_INPUT('Rate this review from 1 to 5: ', reviewText)) AS reviewScore
FROM reviews;

SELECT movieId,
       AVG(AI_RANK('judge_model',
                   AI_INPUT('Rate this review from 1 to 5: ', reviewText))) AS movieScore
FROM reviews
GROUP BY movieId;

SELECT reviewId,
       AI_RANK('judge_model',
               AI_INPUT('Rate this review from 1 to 5: ', reviewText)) AS reviewScore
FROM reviews
ORDER BY reviewScore DESC, reviewId
LIMIT 10;
```

The prompt defines the rubric and intended score range; the engine does not
enforce 1-5, clamp scores, or supply a default score on failure. Negative,
fractional and large finite scores are valid. SQL explicitly selects ascending
or descending order. Ties and duplicate SQL rows retain normal SQL semantics.
Score-dependent Top-K must still score all demanded candidates. In contrast,
a projection after a relational ORDER BY/LIMIT only scores surviving rows.

The provider must support strict structured output. The engine installs:

```json
{"type":"json_schema","json_schema":{"name":"ai_rank","strict":true,"schema":{
  "type":"object","properties":{"value":{"type":"number"}},
  "required":["value"],"additionalProperties":false
}}}
```

Only one numeric `value` is accepted. Scalars, strings, booleans, nulls, extra
or duplicate fields, nonfinite values, DOUBLE overflow and nonzero underflow
to zero fail explicitly. Ordinary DOUBLE rounding is allowed. Refusal,
truncation and malformed provider responses remain errors, including after a
client-visible prefix; no invalid score is cached as a successful result.
Provider errors may precede the numeric parser and retain their original SQL
error code. A matching schema with strict=true and a nonempty caller name is
preserved; conflicting schemas and the reserved `Schema` option are rejected.

Models/options must be static. Text, ordered `AI_INPUT`, tuple and eligible
FILE inputs use the same native request protocol as MAP. CASE and Boolean
guards retain demand semantics; constant prompts are not folded into one
query-wide model call when caching is off. Scoring materialization appears as
`SEMANTIC RANK`; a rank-dependent WHERE/HAVING remains `SEMANTIC FILTER`.
This reuses the bounded asynchronous runtime, cancellation, query cache and
eligible resumable local PX path. Cache identity includes function kind even
when MAP and RANK have identical HTTP bodies.

Serial TEXT and FILE RANK are accepted by `enable_semantic_row_async`, with the
same ordered-prefix contract below; local TEXT PX additionally requires the
AUTO policy described in [Local PX Row-Asynchronous Execution](#local-px-row-asynchronous-execution).
FILE preparation and stage-level automatic output policy are described next.
An independent FILTER proxy can reject rows before a separate RANK stage
scores survivors. RANK itself cannot use `semantic_proxy`. Existing lifting
guards still apply. Cached scoring comparisons admit finite, nonthrowing
implicit integer-to-DOUBLE coercions, including parameterized thresholds;
explicit casts, potentially failing conversions and guarded recipes do not
gain general lifting permission.

Deterministic SQL/mock entry point:

```sh
python tools/deploy/mysql_test/test_suite/ai_function/test_semantic_rank.py \
  --binary build_debug/bin/src/observer/seekdb
```

`--list-cases` starts no services; repeat `--case` for focused selection.
`--binary` owns a private deployment; `--socket` never stops the supplied
server and skips owned-deployment-only FILE/PX evidence. `--log-dir` retains
private deployment logs in a new directory.

Validation:

- Debug build and **27/27 rank SQL/mock contracts** passed: Q9/Q10,
  protocol DOUBLE metadata, numeric sort/Top-K, aliases, guards, dependencies,
  exact cache/lifting request counts and SQL bags, strict errors, retries,
  cancellation, visible prefixes, FILE, actual local PX, FILTER proxy and
  native JOIN/FILTER followed by per-pair scoring and grouped AVG.
- **83 existing SQL contracts** passed during this change: runtime 39,
  row-async 20, cache/lift/JOIN 16 and eight targeted placement safety gates.
  The cache suite and placement gates were rerun on the final build.
- Native input/provider/proxy/scoring **22/22** and native resources
  **18 cases repeated twice (36 executions)** passed.

These are functional contracts, not a real-model accuracy, SemBench quality,
latency or specialized Top-K pruning result. Sanitizer validation is not claimed.

## FILE Row-Asynchronous Media Preparation (P1)

```sql
SELECT /*+ PARALLEL(1)
           OPT_PARAM('enable_semantic_row_async','true')
           OPT_PARAM('semantic_output_order','unordered') */
       id, AI_MAP('vision_model', AI_INPUT('Describe this image: ', image_file))
FROM images
LIMIT 5;
```

Serial MAP/FILTER/RANK now accept native FILE inputs and mixed text/FILE
`AI_INPUT` or tuple inputs with static models/options. Async eligibility is
separate from function-cache eligibility; enabling the cache is not required.
Default/`ordered` still emits completed input prefixes. Explicit `unordered`
can emit a later ready row while an earlier remote download is held. `auto`
uses the stage policy below. Other unordered query-shape limits are unchanged.

This reuses the existing bounded media runtime and incremental model-response
publication. Each row waits for all of its own FILE parts to be read, validated
and encoded before model submission, but not for other rows' files. A ready
image can reach its vision model, then a dependent text MAP/RANK, without
waiting for a slow sibling download. This is neither download-to-provider
streaming nor token streaming. Mixed content-part order, model modality
checks, MIME/size/digest validation, demand guards and media admission budgets
are unchanged.

FILE-dependent **model results are not memoized** by the text function cache.
Text calls remain cacheable, including text calls dependent on a completed
vision result. Existing query-scoped media reuse keeps its URI/version
identity and validation rules; it is not a model-answer cache keyed by URL.
Slots, task credits and retained bytes are still bounded. Media handles and
request arenas are not released per completed row while a request group may
still borrow them; a slow row can retain its slot's memory.

Downloads, model errors, deadline and cancellation remain query failures,
including after a visible ready row. Teardown cancels pending work before
releasing input owners; no failure becomes a successful default result.

```sh
python tools/deploy/mysql_test/test_suite/ai_function/test_semantic_file_row_async.py \
  --binary build_debug/bin/src/observer/seekdb
```

The suite owns private loopback download/model fixtures and a private database.
It tests SQL visibility rather than inferring progress from HTTP completion.

## Stage-Level Automatic Output Policy (P2)

```sql
SELECT /*+ PARALLEL(1)
           OPT_PARAM('enable_semantic_row_async','true')
           OPT_PARAM('semantic_output_order','auto') */
       id, AI_MAP('summarizer', content)
FROM documents
WHERE AI_FILTER('selector', content)
ORDER BY AI_RANK('ranker', content) DESC, id
LIMIT 10;
```

`auto` is explicit, quoted and case-insensitive. It requires row-async; it
does not change the default `ordered` policy or weaken strict `unordered`.
One plan can select:

```text
Scan -> Filter[UNORDERED] -> Rank[UNORDERED] -> Sort -> Limit -> Map[ORDERED]
```

The planner clears order properties before choosing dependent parent plans,
then propagates order requirements down the admitted plan and recomputes
ordering bottom-up. A full sort restores order; operators above it that must
preserve final SQL order use ordered-prefix delivery. This also applies if
an index/scan already supplies the final order and no separate SORT exists.
Projection placement and LIMIT demand boundaries are not moved by this pass.

EXPLAIN shows both the effective `row_async` mode and
`output_policy=AUTO, order_reason=...`:

| Reason | Effective mode |
| --- | --- |
| `NO_ORDER_REQUIREMENT` | `UNORDERED_READY` |
| `SORT_RESTORES_ORDER` | `UNORDERED_READY` below an ordinary full sort/exact Top-N |
| `FINAL_SQL_ORDER` | `ORDERED_PREFIX` in the final order-preserving region |

The initial P2 scope admits serial single-input MAP/FILTER/RANK plans, ordinary full sort,
exact Top-N, LIMIT and simple derived scans. The resolver checks original
query-block boundaries before rewrites. Derived inputs must have no ORDER BY
or LIMIT; stored views, CTEs/materialization, scalar subqueries, JOIN,
grouping/aggregates, DISTINCT, windows, sets, locking/output SELECT and PX are
not in that initial scope; the P3 extension below adds verified hash relational
boundaries. Prefix/local-merge/partition sorts, WITH TIES,
approximate Top-K and pushed Top-N filters fail explicitly. Both merged
derived inputs and retained vectorized SUBPLAN SCAN are covered; a retained
scan suspends only before consuming child output, not midway through its
CPU projection.

The later runtime-filter allocation pass also checks auto admission: a
statistics-enabled pushed Top-N filter must not bypass the earlier semantic
plan check. To explicitly select an ordinary exact Top-N without this optional
filter, use `OPT_PARAM('enable_topn_runtime_filter','false')`; no policy silently
changes that setting.

Score-dependent Top-N normally scores every demanded candidate; unordered
scoring is not a pruning algorithm. A final sort still blocks client rows
until it has enough input. Conversely, a projection after relational
ORDER BY/LIMIT calls models only for selected rows and preserves that final
order. Order-free LIMIT remains latency-biased, not stable pagination.

```sh
python tools/deploy/mysql_test/test_suite/ai_function/test_semantic_output_policy.py \
  --binary build_debug/bin/src/observer/seekdb
```

Both new suites accept `--list-cases`, repeatable `--case`, private `--log-dir`
and `--socket` with the existing deployment-ownership rules. The auto suite
checks mixed stage policies, numeric exact Top-N and demand counts, sort-free
index ordering, guarded/shared results, retained derived scans, final errors
and deadline/kill/recovery. These are deterministic functional contracts, not
real-model latency, SemBench quality or sanitizer results.

Final P1/P2 Debug validation:

- FILE row-async **18/18** and automatic policy **22/22** SQL/mock contracts
  passed, including AUTO FILE scoring before sort and selected-only FILE
  projection after relational ORDER BY/LIMIT.
- **113 existing SQL contracts** passed: ordered row-async 20, strict
  unordered 22, RANK 27, default runtime 39, local media 3 and remote media 2.
  Obsolete FILE-row-async rejection checks were updated to the new contract.
- Six critical automatic-policy visibility/order/derived-resume gates passed
  again in a fresh private deployment.
- Native input/provider/proxy/eligibility **24/24**, and resource/policy/sort/
  derived-scan capability **21 cases repeated twice (42 executions)** passed.
- A 512-row, statistics-enabled negative control exposed late Top-N filter
  insertion bypassing early plan admission before the final fix. Both EXPLAIN
  and SELECT now reject it before model work; ordered mode and explicit
  filter-disabled auto exact Top-N remain covered positive controls.

## Relational Automatic Output Boundaries (P3)

The extension is to **row-async/auto plan eligibility**, not new SQL syntax or
a claim that ordinary GROUP BY/JOIN were previously unexecutable. Default
batch and ordered paths keep their existing contracts. Strict `unordered`
keeps its original single-base-table scope; use explicit `auto` for these
verified relational plans.

### Ordinary hash aggregation

```sql
SELECT /*+ PARALLEL(1) USE_HASH_AGGREGATION
           OPT_PARAM('enable_semantic_row_async','true')
           OPT_PARAM('semantic_output_order','auto') */
       movie_id, AVG(AI_RANK('judge_model', review_text)) AS average_score
FROM reviews
GROUP BY movie_id
ORDER BY average_score DESC, movie_id
LIMIT 10;
```

Ordinary COUNT/SUM/AVG/MIN/MAX hash aggregation accepts unordered semantic
input. Aggregate DISTINCT, aggregate ORDER BY, rollup, AI_AGG, adaptive/
pushed/three-stage aggregation and scalar/merge aggregation are not included.
Simple HAVING and late semantic projections retain their existing demand
placement. Hash aggregation is a blocking boundary: allowing its child to
advance does not make final groups available before collection is complete.

### Streaming Hash DISTINCT

Top-level streaming hash DISTINCT can consume materialized semantic values without
requiring input order. SQL duplicates are removed by the relational operator,
not by incorrectly merging model calls: with the function cache disabled,
separate demanded source rows still call models separately. Blocking, merge or
pushed DISTINCT are not admitted by AUTO or treated as resumable equivalents.

Group By and streaming DISTINCT reevaluate suspension capability on each child
pull. Dynamic spill switches to protected inline waiting inside the operator;
this is not asynchronous spill support or a change of output/cache policy.
Allocation, spill I/O, model, deadline and cancellation errors still propagate.
Both the live forced chunk-store override and the store's captured memory limit
are checked: resetting the dynamic global override does not retroactively make
an already-initialized forced-limit store resumable.

Grouping and DISTINCT retain SQL null/collation rules. A chosen representative
among collation-equivalent values is not a stable source-row identity. As with
ordinary unordered SQL aggregation, floating SUM/AVG rounding can depend on
input order; auto is not a bitwise floating-point reproducibility option.

### Restricted INNER HASH JOIN

AUTO admits ordinary serial INNER equijoins with a relational build input
and eligible semantic probe input, or semantic operators above the join.
The build subtree must contain no native semantic stage. Use appropriate
LEADING/USE_HASH/NO_SWAP_JOIN_INPUTS hints when an actual hash plan and relational
build side are required; a nested-loop or merge alternative is not silently
relabeled as supported hash execution. This phase validates constructed physical
paths; it does not yet prune every unsupported candidate and search for another
eligible alternative automatically.
Outer/semi/anti joins, cross products, shared/PX joins, parameterized rescans,
late materialization and join runtime filters are not in this first scope.
This does not expand native AI_JOIN.

The default-false physical join flag is enabled only for admitted AUTO plans.
Pure build/hash-table completion precedes the first semantic probe pull; only a
fresh probe fetch can suspend. Bucket traversal and output assembly stay
protected. This first scope excludes disk partition replay, dumped partitions,
cache-aware processing and nested-loop hash-join processing. Unlike guarded
aggregation spill, reaching an unsupported flagged join state is an explicit
SQL error, not an inline-wait claim of supported asynchronous spilling.

Join multiplicity remains normal SQL bag multiplicity. The policy pass does
not move a model call from per-source-row to per-pair demand, or vice versa.
Projection after relational ORDER BY/LIMIT only calls models for selected
pairs. A hash join provides no inherited input-order promise; explicit
sorting restores final SQL order.

EXPLAIN adds input-region reasons:

| Reason | Meaning |
| --- | --- |
| `HASH_AGGREGATE_INPUT` | Ordinary hash aggregation does not require ordered input |
| `HASH_DISTINCT_INPUT` | Hash DISTINCT does not require ordered input |
| `HASH_JOIN_PROBE` | The admitted hash join can consume an unordered probe region |

The resolver still checks original query-block boundaries before rewriting.
Simple derived relational blocks are admitted, but inner ORDER BY/LIMIT,
CTE/materialization, stored views, scalar subqueries, windows, sets and
locking/output SELECT remain excluded. Physical-path checks apply as well:
permitting a SQL shape is not permission to unwind every implementation.
After semantic placement the selected plan is checked again and effective
ordering is recomputed, so lifting cannot leave stale stage policies.

Focused deterministic SQL entry points:

```sh
python tools/deploy/mysql_test/test_suite/ai_function/test_semantic_relational_aggregate.py \
  --binary build_debug/bin/src/observer/seekdb
python tools/deploy/mysql_test/test_suite/ai_function/test_semantic_relational_join.py \
  --binary build_debug/bin/src/observer/seekdb
```

Provider failures, cancellation and deadline remain final SQL errors,
including after any visible prefix. No new coroutine framework, model-result
cache policy, query-wide request credit or specialized semantic Top-K pruning
is introduced.

### Reproducible asynchronous microbenchmark

`benchmark_semantic_async.py` uses only the loopback TEXT fixture. It compares
default batch, ordered row-async, strict unordered and auto with the same batch/
slot limits and serial DOP. Its five cases cover all-row MAP, MAP after relational
ORDER BY/LIMIT, FILTER/MAP/LIMIT, exact RANK sort and exact RANK Top-N.

```sh
python tools/deploy/mysql_test/test_suite/ai_function/benchmark_semantic_async.py --list-cases
python tools/deploy/mysql_test/test_suite/ai_function/benchmark_semantic_async.py --self-test
python tools/deploy/mysql_test/test_suite/ai_function/benchmark_semantic_async.py \
  --binary build_debug/bin/src/observer/seekdb --run-label local_fixture \
  --rows 19 --limit 3 --offset 1 --first-k 3 \
  --batch-rows 8 --slots 2 --repeats 2 \
  --fast-delay-ms 15 --slow-delay-ms 150 --query-timeout-s 20 \
  --output /tmp/semantic-async-smoke.json \
  --log-dir /tmp/semantic-async-smoke-logs
```

Output and log paths must be fresh: existing paths are rejected, and versioned
JSON publication is atomic/no-clobber. Inputs, scores, selectivity and latency
assignments are deterministic; no random seed or independent concurrency knob
is needed. Omitting case/mode selectors records 17 supported combinations and
three intentionally unsupported unordered/ORDER BY combinations.

Measurements use an actual SSCursor: first returned row, first K rows, complete
EOF time, returned rows, model requests and observed provider peak concurrency.
First-K timing is null if fewer than K rows return. Exact bags/order and request
demand are validated where comparable. Unordered FILTER/MAP/LIMIT may select a
different legal subset, so this case is not eligible for an exact-answer speedup
claim. Fixture timings are not real-model SemBench performance/quality results;
memory, cancellation credits and unobserved resource counters are not invented.

### P3 verification snapshot

The coordinated Debug build and final private-deployment runs passed:

| Coverage | Passed |
| --- | ---: |
| Ordinary aggregation / streaming DISTINCT | 18 |
| Restricted INNER HASH JOIN | 23 |
| FILE row-async, including the three new relational compositions | 21 |
| AUTO output policy | 22 |
| Existing ordered / strict unordered | 20 / 22 |
| Native RANK / default semantic runtime | 27 / 39 |
| Native input / aggregate eligibility | 25 |
| Native resources / hash capabilities / captured store limits | 24 cases, twice (48 executions) |
| Benchmark service-free tests | 16 |
| Final benchmark SQL trials | 17 supported combinations, twice (34 trials) |

The SQL contract suites total 192 methods. FILE compositions were also run with
`enable_semantic_lift='true'`, including a fresh three-gate deployment, to verify
post-placement input-region reasons, SQL visibility and exact demand. Join
coverage includes nullable NSEQ keys, nested hash joins, carried large semantic
datums through duplicate chains, grouped RANK follow-up, empty builds, errors
after visible rows, kill/deadline/recovery, and unchanged legacy execution.

A real serial-plan negative control caught an invalid runtime guard:
`is_use_px()` is set unconditionally by codegen and is not proof of actual PX
execution. The corrected guard uses physical DOP and the absence of an SQC
handler, alongside logical/physical admission and all mode/state/spill checks.
Both text and FILE positives were rerun after that correction.

Final fixture observations below use 19 rows, two repeats, batch 8, slots 2,
15/150 ms fast/slow replies and first-K=3. Values are observed medians in ms:

| Case | Default first row | AUTO first row | Default total | AUTO total |
| --- | ---: | ---: | ---: | ---: |
| MAP all rows | 337.42 | 29.84 | 337.57 | 337.00 |
| MAP after relational LIMIT | 169.28 | 33.90 | 169.32 | 169.14 |
| FILTER/MAP/LIMIT, different allowed subsets | 330.01 | 56.35 | 330.05 | 75.35 |
| RANK exact sort | 333.18 | 338.75 | 333.37 | 338.93 |
| RANK exact Top-N | 334.31 | 337.08 | 334.35 | 337.12 |

The main demonstrated benefit is earlier visible progress, not a universal total
runtime reduction. FILTER/LIMIT timings are descriptive only: default/ordered
selected IDs {0,3,6}, unordered/auto selected {3,6,9}. Each relational-LIMIT MAP
trial made exactly three MAP calls; both RANK cases scored all 19 candidates.
Across the 34 trials, 550 HTTP attempts were audited. Unsupported combinations
have no samples; observed outstanding responses at EOF are not cancellation
counts. Private services, model sockets, gates and worker cleanup passed.
No sanitizer, real-model quality, asynchronous spilling or PX expansion is claimed.

## Local PX Row-Asynchronous Execution

```sql
SELECT /*+ PARALLEL(4)
           OPT_PARAM('enable_semantic_row_async','true')
           OPT_PARAM('semantic_output_order','auto')
           OPT_PARAM('rowsets_max_rows',16) */
       id, AI_MAP('extractor', content)
FROM partitioned_documents
WHERE AI_FILTER('selector', content);
```

This opt-in extension admits a single-server, vectorized TEXT MAP/FILTER/RANK
producer pipeline below a simple FIFO Gather:

```text
FIFO COORDINATOR
  REDUCE TRANSMIT
    SEMANTIC MAP / FILTER / RANK
      optional simple GRANULE ITERATOR
        TABLE SCAN
```

Each semantic stage reports `execution_scope=LOCAL_PX_WORKER`,
`row_async=UNORDERED_READY` and AUTO reason `PX_GATHER_INPUT`. Merely requesting
`PARALLEL(n)` does not prove model work ran in multiple SQL workers.
Serial stages report `execution_scope=SERIAL`; ordered and strict-unordered
parallel row-async remain rejected. Default batch PX is unchanged.

The admitted original query has one base table, static models/options and no
ORDER BY, LIMIT, grouping, DISTINCT, HAVING, JOIN, derived/view/CTE/subquery,
window/set or locking/output SELECT boundary. FILE, embedding proxy, native
AI_JOIN/AI_AGG, repartition/shared hash, access-all or parameterized GI, unsafe
rescan and fulltree/inline task execution are not admitted. Original-shape
eligibility survives rewriting; logical, complete physical and actual
SQC/task-topology validation each guard their own boundary. Unsupported
execution fails explicitly rather than silently reverting to batch execution.

In seekdb, multiple local tablets use DISTRIBUTED sharding/location metadata
to retain PX planning. This is not proof of remote execution. Conversely,
`is_use_px()` is not proof of actual parallel workers. Admission relies on the
checked Gather pipeline and actual local SQC/task topology, not either proxy.
A partitioned DOP=1 fixture can use excluded fulltree/inline PX; the serial
comparison uses an unpartitioned mirror with identical IDs and prompts.

### Ownership, dependency capacity and visibility

- Each logical producer task owns its ExecContext, frames, slots, row queues,
  cursors and continuation. It may resume on another physical SQL thread;
  one task never polls/evaluates another task's frames.
- All tasks of one execution share memory/task admission, query cache and the
  first fatal failure. An uncached QC lease also spans early/late worker gaps.
  The global wave/task limit remains `ai_pipeline_slots * distinct query
  semantic-call count`, not that value multiplied by DOP.
- In this new scope, each physical semantic call additionally shares a
  query-wide `ai_pipeline_slots` wave window. Its stable key is physical
  operator ID plus expression index, not model/cache identity or thread ID.
  Otherwise four workers can fill all four credits with held inner calls,
  starving a ready nested outer call. Per-call windows leave dependency
  capacity without increasing the existing global limit. Cache-on flights
  use the same accounting, including reused models at different depths.
- Credits count request waves/tasks, not individual HTTP requests. With
  batch 16 and slots 2, the held single-MAP fixture admits exactly 32 HTTP
  attempts at DOP 1, 2 and 4; this is a fixture observation, not a general
  request-credit API.
- Incremental delivery crosses three boundaries: producer commits/flushes
  copied DTL data on an owned semantic wait without sending EOF; verified
  FIFO QC returns an available partial batch; the MySQL driver flushes once
  that output batch is exhausted before the next potentially blocking pull.
  No producer continuation or semantic await is propagated through Exchange.

Successful execution preserves exact values and bag multiplicity, not input
or global completion order. A nonbuffering client can see a tiny ready row
while all other replies remain held, but must still consume the final SQL
status. Provider failure, KILL, deadline and memory overflow remain errors,
including after visible rows. This does not promise token streaming or
linear DOP speedup.

```sh
python tools/deploy/mysql_test/test_suite/ai_function/test_semantic_px_row_async.py \
  --binary build_debug/bin/src/observer/seekdb \
  --log-dir /tmp/semantic-px-row-logs
```

The suite requires its own private `--binary` deployment for actual task,
thread, quiescence and cleanup evidence; it does not attach to external
instances. Repeatable `--case` and `--list-cases` select its 14 local methods.

The Debug candidate passed all **14 SQL/mock methods in two fresh deployments
(28 executions)**, including equal-budget
serial/DOP2/DOP4 bags, actual 2/4 producer tasks and preparation threads,
tiny-row SSCursor visibility with a default-batch negative control,
cross-worker cache reuse/fresh-query isolation, cache-off/on two- and
three-level nested calls with a reused model, guarded demand, FILTER-to-MAP,
native DOUBLE RANK, error-after-prefix, kill/deadline/memory recovery and
zero-demand unsupported-shape rejection. Native resources passed **30 cases
twice (60 executions)**, including concurrent per-call admission and
task-private polling/migration; native input passed **25 cases**.

The same binary also passed all **252 existing SQL methods**:

| Regression coverage | Passed |
| --- | ---: |
| Default local PX | 15 |
| FILE row-async | 21 |
| Ordered / strict unordered | 20 / 22 |
| Serial AUTO | 22 |
| Native RANK / default semantic runtime | 27 / 39 |
| Relational hash aggregate / join | 18 / 23 |
| Query cache / semantic lifting | 16 / 29 |

Validation binary SHA-256:
`c91a30c651475398e095000264e88995ab18647fec7f2c4479e799e44f3994ae`.
These are deterministic functional/resource contracts, not real-model
SemBench performance, quality, sanitizer or general PX-plan coverage.

## Opt-in Unordered Ready-Row Output

```sql
SELECT /*+ PARALLEL(1)
           OPT_PARAM('enable_semantic_row_async','true')
           OPT_PARAM('semantic_output_order','unordered')
           OPT_PARAM('rowsets_max_rows',16) */
       id, AI_RANK('judge_model', AI_INPUT('Score relevance: ', content)) AS score
FROM documents
WHERE AI_FILTER('selector', AI_INPUT('Relevant? ', content))
LIMIT 5;
```

`semantic_output_order` accepts quoted, case-insensitive `ordered`,
`unordered` or `auto`. The default is `ordered`; the default for row-async
remains false. Invalid policy values, numeric values and `unordered`/`auto` without
`enable_semantic_row_async='true'` fail with 1210 rather than falling back.

| Row-async | Output policy | Delivery |
| --- | --- | --- |
| Off/default | Ordered/default | Existing batch-completion path |
| On | Ordered/default | `row_async=ORDERED_PREFIX` |
| On | Unordered | `row_async=UNORDERED_READY` |
| On | Auto | Per-stage policy described above |

Unordered delivery does not wait for an input prefix: any accepted terminal
row can be delivered, including from a later slot while the first slot is
held. A SQL-worker-owned, duplicate-protected ready-slot queue rotates
between ready slots. Fully consumed terminal slots are reclaimed independently
of the head by permuting slot pointers, not moving slot objects. Returned
datums stay pinned until the next pull; a partial slot retains its arena and
outstanding work. Rejected rows count toward completion but are never output.
This is not a strict ordering of HTTP completion timestamps.

Strict unordered supports a serial, single-base-table TEXT/FILE MAP/FILTER/RANK pipeline, optional
ordinary filters/projection/LIMIT, static models/options, guarded/nested
expressions, query caching and independent reject-only FILTER proxies. Explicit
ORDER BY/Top-N, GROUP BY/HAVING/aggregate, DISTINCT, JOIN (including ordinary
joins), AI_JOIN/AI_AGG, PX/exchange, derived tables/CTEs, scalar subqueries,
set/window queries and locking/output SELECTs are rejected with 1235 before
model requests. The resolver checks shape before rewriting can erase a
boundary; the planner also checks the serial operator whitelist. Unordered
logical stages clear inherited ordering, local-order and range-order
properties before parents compute theirs.

The existing fixed HTTP groups, retries, query-wide cache ownership and
slot/task/byte budgets are reused; no coroutine runtime or new HTTP
request-credit limit is introduced. Active retained slots remain bounded,
but total requests admitted while the first slot is held can exceed two
batches: a later slot can be consumed and reused repeatedly. This cumulative
count must not be mistaken for the instantaneous resource bound. A slow row
can still retain its own slot's memory.

Full, successful execution preserves row values and bag multiplicity, not
input order. LIMIT may select fast matches and is latency-biased, not random
sampling or stable pagination; cancellation can leave speculative requests.
Projection-only MAP/RANK after a relational LIMIT still evaluates only the
selected rows. This option does not move model work across that LIMIT.
`AI_RANK` remains scoring, not an unordered specialized Top-K algorithm.
Client buffering can hide progress. After any visible rows, the client must
still consume the final status: validation errors, deadlines and cancellation
remain failures, not successful partial results.

```sh
python tools/deploy/mysql_test/test_suite/ai_function/test_semantic_unordered.py \
  --binary build_debug/bin/src/observer/seekdb
```

The entry point reuses the existing loopback cache/rank/proxy fixtures.
`--list-cases`, repeatable `--case`, `--socket` and private `--log-dir` have the
same ownership rules as the rank suite. Validation on the final Debug build:

- **22/22 new SQL/mock contracts** passed, including actual small-row
  SSCursor visibility past a held first row, overtaking a held first slot,
  repeated non-head slot reuse, FILTER-to-MAP/RANK, LIMIT/cancellation,
  duplicate bags/cache on and off, guarded branches, retry, large carried
  values, proxy composition, quota failures, errors, timeout/kill/recovery,
  option validation and zero-request unsupported-shape rejection.
- Six critical gate contracts passed in two additional fresh deployments:
  **12/12 repeat executions**.
- **86 existing SQL contracts** passed: ordered row-async 20, rank 27 and
  default semantic runtime 39.
- Native input/provider/proxy/scoring **22/22** and resources **20 cases
  repeated twice (40 executions)** passed. Resource coverage includes output
  policy defaults, strict hint typing and serialized spec round trips.

These are historical V1 functional results, not a real-model latency,
SemBench quality or sanitizer claim. The P1/P2 sections above describe the
subsequent FILE and ordinary order-restoring-plan extensions.

## Opt-in Ordered Row-Asynchronous MAP/FILTER/RANK

```sql
SELECT /*+ PARALLEL(1)
           OPT_PARAM('enable_semantic_row_async','true')
           OPT_PARAM('rowsets_max_rows',16) */
       id, AI_MAP('extractor', AI_INPUT('Summarize: ', content))
FROM documents
WHERE AI_FILTER('selector', AI_INPUT('Relevant? ', content))
LIMIT 5;
```

The new option defaults to **false**. It accepts quoted, case-insensitive
`true`/`false`; invalid values and explicitly unsupported shapes fail rather
than silently using the old publication path. EXPLAIN identifies enabled
stages with `row_async=ORDERED_PREFIX` when `semantic_output_order` is omitted
or `ordered`; unordered and auto policies above are separately opt-in. Current scope is serial native TEXT/FILE
MAP/FILTER/RANK with static model/options, with or without query caching and the
existing reject-only text FILTER proxies. AI_JOIN, AI_AGG and PX/exchange
plans are not enabled by this option. Ordinary SQL joins remain possible in
serial plans, subject to their existing suspension guards.

The HTTP scheduler still multiplexes fixed request groups. Only terminal,
validated responses are published, once per request; retries are not results.
SQL collects each ready response into isolated caller-owned memory, marks its
row dirty, and reevaluates that row's demanded expressions without waiting for
its siblings. A nested call for a later row can therefore start while an
earlier row's inner call is held. This is not token streaming or a coroutine
rewrite.

The physical operator emits only its **contiguous completed input prefix**.
Rejected rows advance that prefix without output; an all-rejected prefix is
not EOF. FILTER can pass a true prefix to a separate MAP before a slow tail,
but a slow FILTER head still prevents later rows from reaching that separate
MAP. Input order, bag multiplicity, guarded branches and response indexes
remain intact. Slot-owned output datums remain live until the next pull;
partial output never resets a still-needed slot.

The existing slot/task/byte limits and scheduler admission remain in force.
This version does not shrink SQL batches to one, force a scheduler client per
row, introduce appendable HTTP groups, or add an independent query-wide HTTP
request-credit limit. A partly consumed slot remains pinned until its ordered
tail drains. LIMIT can finish from a prefix and normal query teardown cancels
unneeded work, but request lookahead is still the existing bounded window,
not a new selectivity-aware LIMIT policy. Query-cache work remains owned by
the query, not canceled merely because one subscriber finishes.

On a safe SQL-worker suspension, newly generated row packets are flushed for
this opt-in mode. Small prefixes are thus observable with an unbuffered
client, not just inside the operator. Blocking sort/aggregate and a buffering
client can still hide progress. An error after a visible prefix remains a SQL
error; clients must consume the final status, and the server cannot retry
after sending rows.

Deterministic SQL/mock entry point (no real model or speedup claim):

```sh
python tools/deploy/mysql_test/test_suite/ai_function/test_semantic_row_async.py \
  --binary build_debug/bin/src/observer/seekdb
```

`--list-cases` starts no services; repeat `--case` for focused selection.
The suite holds sibling responses in an unchanged 16-row batch and checks
downstream demand, LIMIT cancellation, bounded head-of-line lookahead,
actual small-row SSCursor visibility, exact bag/counts, retries, cache/proxy
composition, invalid settings, errors, deadline, kill and connection recovery.
`--log-dir` retains only the owned deployment's logs in a new directory.

Historical validation for the initial MAP/FILTER implementation (the RANK
extension is validated separately above):

- Debug seekdb build and **20/20 SQL/mock contracts** passed, including
  two-stage EXPLAIN markers and rejection of a serial semantic stage above
  a parallel SQL subtree.
- Seven held-request/client-visibility cases passed on three additional
  fresh private deployments (**21/21 repeated executions**).
- **84 existing SQL contracts** passed: MAP/FILTER runtime 39, cache/lift/JOIN
  16, expression composition 10, legacy completion/embedding 12, JOIN proxy 3,
  and targeted local PX 4.
- Native resource contracts passed **17 cases twice (34 executions)**;
  native input/provider/proxy contracts passed **20/20**. Fixture self-tests
  passed **11 + 27 = 38**.

With the pre-change binary, the default/off barrier control passed, while
the new small-prefix and nested-dependency oracles failed as expected.
These checks distinguish SQL row progress from HTTP completion order.
Real-model latency, SemBench quality, sanitizer runs and exhaustive rescan
or failure-injection coverage are not claimed by this validation.

## Per-FILTER Proxy Composition (2026-10-14)

The earlier V0 sections below describe their historical single-call scope.
The current serial/cache-enabled implementation also supports independent
positive FILTER predicates with different proxy settings, and FILTER followed
by an independent MAP projection. Each FILTER can supply constant engine-only
options; missing fields inherit the corresponding query-hint defaults:

```sql
SELECT /*+ PARALLEL(1)
           OPT_PARAM('rowsets_max_rows',8)
           OPT_PARAM('enable_semantic_cache','true')
           OPT_PARAM('enable_semantic_proxy','true') */
       id, AI_MAP('judge_model', AI_INPUT('Extract a short title: ', content),
                  '{"temperature":0}') AS title
FROM documents
WHERE AI_FILTER('judge_model', AI_INPUT('Discusses database deadlocks? ', content),
                '{"temperature":0,"semantic_proxy":{"model":"embed_model","query":"database deadlocks","threshold":0.4}}')
  AND AI_FILTER('judge_model', AI_INPUT('Discusses transactions? ', content),
                '{"temperature":0,"semantic_proxy":{"model":"embed_model","query":"database transactions","threshold":0.3}}');
```

These numbers illustrate syntax only. Rejected rows do not run MAP. The native
operator tracks one proxy configuration per FILTER through optimization,
code generation, serialization, cache binding and asynchronous tasks.
`semantic_proxy` is removed before calling the completion provider; remaining
generation options retain their original meaning. Malformed/unknown proxy
fields and use on MAP/JOIN/AGG are errors. The master proxy switch remains off
by default; merely supplying per-call options does not enable approximation.
The small rowset is explicit: callers must respect their embedding provider's
input cap; there is no automatic provider-cap batch splitting.

This does not enable projected/shared FILTER values, NOT/OR/CASE/comparison
recipes, FILE, AGG or PX proxies. JOIN still requires a single semantic call.
There is no general cross-FILTER document-vector cache, automatic acceptance,
ANN candidate pruning or engine-side threshold learning. Individual FILTER
recall targets do not imply a recall guarantee for their conjunction.

Validation: Debug build, **5/5 FILTER plus 3/3 JOIN SQL/mock contracts** and
**20/20 native input/provider/options/scoring contracts** pass. These are
functional contracts, not new real-model effectiveness or speedup results.

### External conservative calibration and native score parity

[semantic_proxy_score.cpp](semantic_proxy_score.cpp) and
[build_semantic_proxy_score.py](build_semantic_proxy_score.py) provide an
experimental JSONL scorer linked against the same Debug parser/cosine objects.
Its manifest binds kernel semantics, scorer/seekdb hashes and CPU dispatch
flags. String IDs and ordered pair IDs survive JSON round trips; bad vectors
remain errors. No model requests or Python/scalar score fallback occur.

The enclosing workspace's
[calibration workflow](../../../../../../experiments/sembench/README.md#10-原生评分独立验证的-reject-only-proxy-校准)
fits on learning only, freezes the threshold, and uses disjoint validation
with a one-sided Clopper–Pearson lower bound. Defaults 0.95 recall / 0.05
failure probability need at least 59 distinct validation positives even with
zero misses. Failed/insufficient evidence explicitly disables proxy routing.
This certificate is relative to the frozen main-model oracle, conditional on
sampling assumptions and binding; it does not certify composed SQL or human
truth. Provider/format errors still fail rather than disable silently.

53 offline calibration/profile tests pass. The focused native workflow also
passes scorer → certified FILTER SQL and scorer → abstained exact-only SQL,
including threshold equality, raw SQL ID restoration, zero embedding work
when abstained, stripped engine options and actual Unix-socket peer/binary
attestation. These use loopback mocks, not real-model benchmarks. The external
tool consumes already collected labels/vectors; it does not add automatic
engine learning, provider splitting or a new cost-based optimizer rule.

## Experimental FILTER Embedding Proxy V0 (2026-10-13)

This is explicit **approximate** execution, not an equivalent optimizer rewrite.
A low cosine score can reject a row that the main model would accept. There is
no learned threshold, recall guarantee or high-score automatic acceptance.
The default remains the original semantic execution.

```sql
SELECT /*+ PARALLEL(1)
           OPT_PARAM('enable_semantic_cache', 'true')
           OPT_PARAM('enable_semantic_lift', 'true')
           OPT_PARAM('enable_semantic_proxy', 'true')
           OPT_PARAM('semantic_proxy_model', 'embed_model')
           OPT_PARAM('semantic_proxy_query', 'database deadlocks')
           OPT_PARAM('semantic_proxy_threshold', '0.25') */
       id
FROM documents
WHERE AI_FILTER('judge_model',
               AI_INPUT('Does this text discuss database deadlocks? ', content));
```

`embed_model` must be a registered dense-embedding model; `judge_model` remains
a completion model. All three proxy parameters are required when enabled.
The threshold must be a quoted finite number in `[-1,1]`; `0.25` is only an
interface example, not a calibrated recommendation. Enabling the proxy requires
`enable_semantic_cache='true'`. Lifting is optional and uses the existing rules.

V0 requires exactly one positive native FILTER call in one semantic stage,
with constant model/options and `AI_INPUT(fixed text instruction, text column)`.
Projected/shared FILTER values, NOT/OR/CASE/comparison recipes, additional
semantic calls, FILE, AI_AGG and parallel/exchange plans are unsupported and
explicitly fail when the proxy is enabled. Empty proxy documents also fail;
normal argument validation, including SQL NULL handling, still precedes lookup.

The cache first coalesces effective-call misses. Only unique misses are sent
to the asynchronous embedding batch API. The retrieval text is embedded once
per query binding; subsequent batches wait for that vector, rather than
duplicating its request. The document embedding uses only the second text
part, not the concatenated main-model instruction/prompt. Existing provider
response-index validation preserves original input positions.

Cosine scores strictly below the threshold publish `{"value":false}` to that
call's future. Equal/higher scores go through the original asynchronous
AI_FILTER request and strict Boolean response parser. Main-model prompts and
settings are unchanged. SQL rows and duplicate bags are not deduplicated.
Proxy model/query/threshold are included in cache identity; proxied rejections
are not ordinary non-proxy model answers. Both model snapshots are query-owned.

Scoring uses seekdb's native raw-cosine kernel and its existing runtime CPU
dispatch (AVX2/AVX512 where supported by the build and CPU), not a separate
scalar dot/norm implementation or the normalized SQL similarity interface.
Input dimension/finite/nonzero checks and finite-result validation precede
publication; kernel errors propagate without a scalar fallback. Scores remain
clamped to `[-1,1]`, with the same strict reject threshold and support for
non-normalized vectors. Native floating-point accumulation may differ slightly
from the earlier double-accumulation loop near a threshold. This reuses vector
computation only: it does not read stored VECTOR columns or build/use an ANN
index, and does not reduce the number of embedding requests.

The query vector is charged to the non-evicting cache quota. Temporary vector/
JSON parsing allocations, request waves and response buffers use the existing
query budget. Document vectors are released after scoring. Invalid counts,
dimensions, nonnumeric/nonfinite/zero vectors and provider failures propagate
as errors, never as low-score rejections or successful fallbacks.

EXPLAIN adds `embedding_proxy=REJECT_ONLY, approximate=true, threshold=...`.
The existing INFO cache summary additionally reports submitted query/document
embedding inputs, rejected unique calls and forwarded unique calls. Submitted
completion counts exclude proxy rejections; these are not HTTP retry counts.
No prompts, retrieval text, vectors or credentials are logged.

The small [proxy smoke module](test_semantic_proxy.py) uses loopback-only models:

```bash
python tools/deploy/mysql_test/test_suite/ai_function/test_semantic_proxy.py \
  --binary "$PWD/build_debug/bin/src/observer/seekdb"
```

Validation: the Debug build, **3/3 SQL/mock smoke cases**, and **18/18 native
input/provider/scoring contracts** passed. The native set adds three focused
proxy scoring cases covering non-normalized raw cosine, unaligned inputs and
dimension tails, the threshold boundary, invalid vectors and numeric failure.
Run it with `test_semantic_input.py --build-dir build_debug`. This delivery is
limited to compilation and these small native/SQL smoke sets, not a full
historical regression run. No real-model accuracy, threshold sweep,
SemBench effectiveness, latency or GPU benchmark is part of this version.
There is no ANN index, precomputed embedding, cross-query vector cache,
automatic calibration or high-score automatic acceptance. JOIN support is
described separately below.

## Experimental AI_JOIN Embedding Proxy V0

JOIN uses the same opt-in cache/proxy/model/threshold hints, but deliberately
does **not** use `semantic_proxy_query`; supplying that hint for JOIN is an
explicit invalid-argument error.

```sql
SELECT /*+ PARALLEL(1)
           OPT_PARAM('enable_semantic_cache', 'true')
           OPT_PARAM('enable_semantic_proxy', 'true')
           OPT_PARAM('semantic_proxy_model', 'embed_model')
           OPT_PARAM('semantic_proxy_threshold', '0.5') */
       l.id, r.id
FROM left_inputs l
JOIN right_inputs r ON l.group_id = r.group_id
  AND AI_JOIN('judge_model', 'Describe the same issue', l.content, r.content);
```

This is **approximate, reject-only** execution of all existing SQL candidate
pairs. Native INNER JOIN candidate generation and duplicate multiplicities
remain unchanged. There is no ANN index, Top-K candidate generation, candidate
reordering or AI_JOIN lifting. A score below the threshold rejects the pair;
equal/higher scores still require the original AI_JOIN completion request.
Similarity alone cannot reliably proxy an arbitrary matching rule, so a low
score can reject a true match. The example threshold is not calibrated.

V0 accepts one positive native AI_JOIN predicate in an INNER JOIN `ON`, a static
text rule, constant model/options, and scalar text or ordered all-text
`AI_INPUT`/ROW sides. A standalone outer WHERE predicate is not supported.
The existing serial/cache requirements and unsupported semantic recipe,
FILE, aggregation and parallel/exchange restrictions still apply.
Left and right embedding inputs are rendered separately from the original
arguments, never recovered by parsing the main-model prompt. Ordered text
parts are concatenated with the same native text-rendering behavior.
The rule is **not** embedded. Original rule, side roles, tuple boundaries,
completion prompts and completion options retain their pair-cache identity
and main-model semantics.

After pair-result cache lookup/coalescing, unique pair misses subscribe to
query-owned side-vector futures keyed by the frozen embedding model and exact
rendered text bytes. Identical text shares a vector across pairs, batches and
left/right roles, including pending requests. Only newly claimed side inputs
enter asynchronous embedding batches. Cosine uses the same native raw-cosine
kernel as FILTER. The completion subset preserves original pair positions.

Side-vector metadata, text keys and vectors use the existing half-query
non-evicting cache quota; they are retained only until query completion.
Temporary parsing and request/response waves use the existing query budget.
Embedding production reuses the pair flight's wave credit rather than
acquiring a separate credit per side. Bad vectors, provider failures, quota
exhaustion, cancellation and deadlines propagate to dependent pair futures,
never as false results or successful main-model fallbacks.

EXPLAIN retains `candidate_policy=ALL_SQL_CANDIDATES` and adds
`embedding_proxy=REJECT_ONLY, proxy_input=LEFT_RIGHT_TEXT, approximate=true,
threshold=...`. The cache summary additionally counts submitted unique JOIN
side embeddings, completed-vector hits and pending-vector coalescing.

The small [JOIN proxy smoke module](test_semantic_join_proxy.py) uses only
loopback mocks:

```bash
python tools/deploy/mysql_test/test_suite/ai_function/test_semantic_join_proxy.py \
  --binary "$PWD/build_debug/bin/src/observer/seekdb"
```

Validation: the final Debug build, **3/3 JOIN SQL/mock smoke cases**, the
existing **3/3 FILTER proxy regression cases**, and **18/18 native
input/provider/scoring contracts** passed. Fixed mocks verify pending-vector
coalescing, cross-batch reuse, asymmetric pair answers, duplicate bags, raw
threshold equality, reordered response indexes, ordered side rendering,
explicit unsupported/invalid options, and embedding failure with fresh-query
recovery. The routing fixture has 15 SQL candidates and 10 distinct ordered
pairs: proxy-on uses five distinct side embeddings and forwards eight pairs;
proxy-off uses no embeddings and submits all ten distinct pairs.
These are functional routing/count assertions, not effectiveness evidence.
No real-model accuracy, latency, throughput, threshold or SemBench experiment
is included.

## Query Function Caching and Cached Greedy Lifting (2026-10-13)

```sql
SELECT /*+ OPT_PARAM('enable_semantic_cache', 'true')
           OPT_PARAM('enable_semantic_lift', 'true') */
       a.id, b.category
FROM semantic_inputs a
JOIN selected_rows b ON a.id = b.input_id
WHERE b.enabled = 1 AND AI_FILTER('classifier', a.prompt);
```

Both options default to false and require quoted, case-insensitive `true` or
`false`; invalid values explicitly fail. Caching can be enabled without lifting.
The new placement policy is greedy: move each eligible MAP/FILTER stage as high
as its existing safety boundaries permit, repeating to a fixed point. It does
not compare relational and inference costs or implement model-aware join CBO.

With caching disabled, the opposite JOIN input must still be provably unique.
With both options enabled, that restriction can be relaxed only when **every**
semantic call in the moved stage supports reliable memoization. The supported
movement remains directly adjacent ordinary INNER HASH/MERGE JOINs with
same-type direct equality keys. Existing guards, shared/carried-value,
model-dependent-consumer, NLJ, GROUP/DISTINCT, LIMIT/Top-N and semantic-subtree
protections remain. AI_JOIN's position and immediate candidate-pair boundary
are unchanged. Cached FILE-dependent row-multiplying movement is not supported.
Cached comparison recipes additionally admit the resolver's implicit
UTF8MB4-to-UTF8MB4_BIN coercion of constant string literals when the type stays
unchanged and the length does not shrink. This permits predicates such as
`AI_MAP(...) = 'keep'` without allowing explicit casts or arbitrary comparison
conversions. Cache-disabled recipe eligibility is unchanged.

The cache supports demanded TEXT MAP/FILTER/AI_JOIN calls with constant model
and options and statically known text-only input layouts. It is shared by
operators, batches and local PX workers for one execution on one server, not
across executions, tenants or servers. The first demand freezes the model/
endpoint metadata for that model argument. The effective options are encoded
with sorted JSON object keys and retained scalar types. Function kinds, model
arguments, settings, ordered argument roles and tuple-part boundaries remain
distinct; text equality is byte equality, not SQL collation equality.

Atomic PENDING entries coalesce in-flight duplicates; completed entries retain
the original result. Only unique misses consume network wave credits. A query-
owned dispatch copies its request data and holds its own allocator, so a result
does not borrow a producer Slot, expression frame or worker session. A root
execution-context lease preserves the cache while local PX producers finish.
Completion collection is shared with the existing semantic runtime, including
non-cached semantic/aggregate operators that share its resource budget.

Boolean results pass the same strict parser as uncached execution before
publication. Failures remain FAILED futures and propagate to subscribers;
they never become false or empty answers. Arguments still undergo normal
validation before lookup, and undemanded expressions do not create entries.
NULL/empty-input and FILE behavior are not replaced by PLOP's NULL convention.
Caching deliberately means one sampled answer per effective call within this
execution; it does not prove real-model determinism across separate queries.

Persistent keys, values, model metadata and allocator pages have a non-evicting
quota of **half `ai_pipeline_memory_limit`**, charged to the same query budget.
Active dispatches and response buffers also use that budget. Exhaustion raises
an explicit resource error: there is no LRU recomputation, silent bypass or
unbounded wait for permanent cache memory to become free. Increase the existing
query memory limit or disable the cache for workloads whose distinct inputs/
answers exceed the quota. Disk spill is not part of this version.

EXPLAIN adds `function_cache=LOCAL_QUERY_TEXT, memoized_calls=N`; this is the
number of eligible bound call sites, not measured inference calls. Cache-only
AI_JOIN retains `candidate_policy=ALL_SQL_CANDIDATES`. Existing placement
markers still show real ancestry changes. The INFO execution summary reports
distinct keys, ready hits, coalesced subscribers, submitted unique calls and
retained cache bytes without logging prompts or credentials. HTTP retries
remain separate from logical-call counts.

The [cache/lifting contracts](test_semantic_cache.py) use deterministic,
loopback-only models and private database instances:

```bash
python tools/deploy/mysql_test/test_suite/ai_function/test_semantic_cache.py --self-test
python tools/deploy/mysql_test/test_suite/ai_function/test_semantic_cache.py --list-cases
python tools/deploy/mysql_test/test_suite/ai_function/test_semantic_cache.py \
  --binary "$PWD/build_debug/bin/src/observer/seekdb"
```

The existing default-off [lifting contracts](test_semantic_lift.py) and native
[ordered-input contracts](../../../../../unittest/sql/engine/test_semantic_input.cpp)
also cover preserved demand, legacy text concatenation, and the shared strict
Boolean parser. AI_JOIN relocation, AI_AGG answer caching, row-level publication
and cross-server cache coordination are out of scope. The cache-only contract
does not route by embeddings; the experimental opt-in FILTER proxy is described
separately above.

Validation on the final Debug binary: **16/16 cache SQL contracts** passed
without skips, with **27/27 fixture self-tests**. Measured deterministic cases:

| Shape | Original | Cache only | Cache + lift |
|---|---:|---:|---:|
| Nonunique HASH/FILTER | 12 requests | 6 requests | 3 requests |
| MAP comparison recipe | 12 requests | — | 3 requests |
| MERGE/FILTER | 12 requests | 6 requests | 2 requests |
| Two consecutive eligible joins | 24 requests | — | 4 requests, two crossings |
| Cross-operator FILTER reuse | 70 requests | 1 request | — |

Held cross-batch MAP duplicates issued exactly two requests for two distinct
keys before any reply. Different models/options/function kinds remained
separate. A local PX query used four actual tasks/threads but one shared model
request; cancellation returned 1317 before the held response was released,
then independent recovery succeeded. Boolean/provider failures coalesced to
one request and propagated their original errors. Answers were uniform within
one cached query, refreshed in the next execution. The half-budget quota
contract returned 4019 without recomputation and recovered in a fresh query.
AI_JOIN role/tuple-boundary and candidate-protection contracts passed unchanged.

Current additional gates passed: native input/parser **15/15**, existing
conservative lifting **29/29**, and targeted default-off runtime **9**,
composition **10**, local PX **4**, for **52** existing SQL regressions.
These are not all historical/legacy suites or a real-model/cloud/GPU benchmark.
The suite does not independently prove FAILED-future delivery after an early
producer close; ordinary LIMIT drain does not close its semantic child. No
early-detach behavior was added solely to manufacture that scenario.

## Opt-in Semantic Late Evaluation (2026-10-13)

```sql
SELECT /*+ OPT_PARAM('enable_semantic_lift', 'true') */ a.id, b.category
FROM semantic_inputs a
JOIN selected_ids b ON a.id = b.id
WHERE b.enabled = 1 AND AI_FILTER('classifier', a.prompt);
```

The option defaults to **false**. Its quoted `true`/`false` values are
case-insensitive; other values fail with `OB_INVALID_ARGUMENT`, rather than
silently enabling or ignoring this new behavior. This is a placement rule after
ordinary join planning, not a model-latency-aware join CBO.

After complete lowering and projection/shared-value registration, a post-order
placement pass operates on the plan already selected by ordinary CBO. Other
candidate plans are discarded before mutation because their operator subtrees
can be shared. The pass can move a directly adjacent MAP/FILTER stage
above an ordinary INNER HASH/MERGE JOIN, from either operand, and repeat over
consecutive eligible joins. Without function caching, the **opposite** input must be provably unique on
the complete ordinary `=` column keys using existing FD/equality/constant
metadata. Computed/differently typed keys and null-safe equality are rejected.
No foreign-key existence assumption is required: eliminating unmatched rows
is exactly the useful work this rule puts before inference.

The first version rejects nested loops, parameterized/correlated or late
materialization joins, shared/carried model values, semantic input/sibling
subtrees, model-dependent join consumers, and unsafe ordinary expressions.
CASE/AND/OR guards inside an individual semantic recipe stay intact and are
not lifted. Only conservative column/constant/boolean/comparison recipes,
known-lossless implicit casts, native ordered inputs and direct `TO_FILE`
recipes are admitted; explicit casts, JSON extraction, UDFs and volatile
recipes are not. Existing ordinary scan predicates remain in place.
LIMIT/Top-N, GROUP, DISTINCT, outer/semi/anti joins, subquery and AI_JOIN
candidate boundaries are not crossed. Existing projection-after-LIMIT demand
and AI_AGG/AI_JOIN rewrite safeguards are unchanged.

Lifting happens before producer allocation and code generation, never during
partial lowering before HAVING/projection reuse or AI_JOIN bindings are known.
The JOIN
retains its path-derived FD/order/distribution properties and its updated
child costs. The relocated stage recomputes properties over the JOIN, so its
input columns, materialization cost and shared query-task count are wired to
the new topology. Changed ancestors refresh their properties/costs without
using unary FD inheritance for joins. AI_JOIN's immediate candidate JOIN
is explicitly protected during this traversal. EXPLAIN reports
`semantic_placement=LIFTED, crossed_inner_joins=N`; rejected attempts report
`KEPT` and `lift_stop_reason`. Internal `OPT_TRACE` instrumentation also records
the applied/blocked rule when that tracer is active; this is not the separate
`information_schema.OPTIMIZER_TRACE` JSON interface. EXPLAIN is the public
placement verification entrypoint.
Estimated rows/time are still ordinary CBO estimates, not measured model calls
or inference latency.

Opting in deliberately suppresses **model/media demand for unmatched rows**.
Those rows make no provider request or object GET; their model/media-specific
validation failures are therefore not raised. Surviving rows retain strict
validation and error propagation. This is not an error-to-false fallback,
model-result caching by itself, fetch streaming or removal of asynchronous batch barriers.

The measured private baselines require changed operator ancestry and placement
markers as well as exact deterministic SQL tuples and request payload counts:

| Shape | Disabled | Enabled |
|---|---:|---:|
| Unique HASH JOIN, either operand | 100 calls | 10 calls |
| Larger unique HASH JOIN | 1000 calls | 100 calls |
| Unique MERGE JOIN | 12 calls | 2 calls |
| Two consecutive eligible joins | 24 calls | 4 calls, two crossings |
| Local/remote FILE filter | 12 model calls | 3 model calls |
| Remote FILE object GETs | 12 GETs | 3 GETs |

Disabled: `HASH JOIN -> SEMANTIC FILTER -> SCAN`.
Enabled: `SEMANTIC FILTER -> HASH JOIN -> SCAN`.
Filtered-out objects have zero GETs; malformed surviving bytes still fail
before model submission. Generated fixtures do not establish real-model quality,
GPU overlap or an end-to-end SemBench speedup.

The focused [semantic placement runner](test_semantic_lift.py) provides strict
positive/negative plan and counter oracles, generated loopback media, private
database lifecycle and fixture self-tests:

```bash
(
  binary="$PWD/build_debug/bin/src/observer/seekdb"
  cd tools/deploy/mysql_test/test_suite/ai_function
  python -m test_semantic_lift --self-test
  python -m test_semantic_lift --list-cases
  python -m test_semantic_lift --binary "$binary"
)
```

The final Debug build passed **29/29 focused SQL contracts** and **18/18 fixture
self-tests**. Coverage includes false/default demand, both HASH operands,
MERGE/repeated joins, complete/incomplete composite uniqueness, multiplicity and
null-safe hazards, blocked NLJ/CAST/JSON/volatile/dependent/shared recipes,
AI_JOIN roles/candidate boundaries, guarded and three-valued demand, local/remote
FILE pruning/validation, and surviving GROUP/DISTINCT output. Shared FILTER/HAVING
without ORDER/LIMIT stays below the JOIN; grouping/distinct positives still
require 100-to-10 requests and matching grouped/duplicate-eliminated results.

Targeted existing default-off regressions also passed: composition **10**,
MAP/FILTER **9**, AI_AGG **5**, AI_JOIN **4**, local FILE **6**, actual PX **4**,
and remote zero-demand/cancellation **2**, for **40** cases. This is not a rerun
of every historical/native/legacy suite. All fixtures own private databases
and generated/loopback inputs; normal shutdown and resource cleanup completed.
No commit, cloud account, real inference service or GPU performance validation
is implied by these results.

## Native Semantic Aggregation (2026-10-12)

`AI_AGG(model, input, instruction[, options])` is a native aggregate expression,
not pointwise `AI_MAP` or a `GROUP_CONCAT` wrapper. Logical GROUP BY lowers to
`SEMANTIC AGG`; its physical operator retains SQL hash/collation/NULL grouping
and reuses the native aggregate processor for ordinary COUNT/SUM/AVG/MIN/MAX.
Outputs are nullable UTF8MB4_BIN LONGTEXT.

```sql
SELECT department,
       AI_AGG('summarizer', feedback,
              'Summarize recurring concerns and preserve disagreements.',
              '{"context_window":8192,"tokenizer":"byte_bpe",
                "template_tokens":256,"max_output_tokens":512,
                "max_partial_bytes":2048,
                "provider_options":{"temperature":0}}') AS summary,
       COUNT(*) AS row_count
FROM feedbacks
GROUP BY department;
```

### Reduction and scheduling contract

- Non-NULL inputs, including empty strings and duplicates, each contribute once.
  SQL NULL inputs are skipped. Empty scalar/all-NULL groups produce SQL NULL
  without model calls; empty grouped input produces no rows. A successful empty
  model string is a non-NULL result.
- Small groups use one DIRECT request after sealing. Larger groups incrementally
  EXTRACT relevant evidence, COMBINE adjacent notes in a fixed binary tree, then
  FINALIZE the answer. EOF tails are folded deterministically. The tree is tied
  to the input stream, not HTTP arrival/completion order; SQL without an ordering
  guarantee does not acquire one from this operator.
- Ready subtrees merge without a whole-layer barrier. Independent groups and
  aggregates overlap; upstream/downstream MAP/FILTER stages share the same
  execution registry and budget. A retained row/binding cursor prevents
  backpressure retries from duplicating ordinary or semantic contributions.
  Completion counting keeps whole-operator readiness checks constant-time.
- The query credit limit is `ai_pipeline_slots * query semantic expression count`;
  this aggregate stage also caps jobs at `ai_pipeline_slots * stage AI_AGG count`.
  Group count does not multiply the concurrency limit.
- This first implementation uses serial SQL policy and conservative rewrite
  protection. It has no spill or distributed partial/final aggregation.
  Safe ancestor paths use the shared worker continuation protocol; other paths
  keep the stack while awaiting asynchronous HTTP, rather than invoking a
  synchronous model fallback.

### Explicit context and output contract

The three-argument form binds, but execution currently requires fourth-argument
options containing `context_window` and `tokenizer: "byte_bpe"`. There is no
automatic model-context/tokenizer discovery. Complete serialized UTF-8 user
prompt bytes are a conservative token upper bound for the caller-declared
byte-BPE tokenizer:

```text
user_prompt_bytes <= context_window - template_tokens
                     - max_output_tokens - system_prompt_bytes
```

Instruction, JSON escaping, phase and source-count framing are included.
`template_tokens` defaults to 256; the caller must supply a valid upper bound for
its provider chat framing. This is not a general exact-tokenizer implementation
or a safety promise for incorrect tokenizer/framing declarations.
The engine owns `max_tokens = max_output_tokens`.

`max_partial_bytes` defaults to 1024 and applies to every output phase, including
DIRECT/FINALIZE. `max_calls=4096` and `max_levels=32` bound each group/aggregate
reduction graph; transport retries retain the same logical task. Unshrinkable
notes or an oversized row/combine/finalize prompt fail rather than being
truncated or silently discarded. Options text is capped at 64 KiB; unknown keys
and invalid bounds are errors. Provider options only allow `temperature`,
`top_p`, `top_k`, `seed`, `presence_penalty` and `frequency_penalty`, not request,
streaming, output-format or token-budget overrides.

Only static model/instruction/options, text/NULL input and OpenAI-compatible
completion endpoints are supported. Direct FILE/JSON/ROW input, dynamic config,
AI_AGG DISTINCT/ordered/window forms, ROLLUP, PX partial/final and external spill
are rejected. Ordinary DISTINCT/ordered aggregates in the same dedicated
aggregate stage are also protected, not silently implemented by the new path.

AGG opts into plain-text, complete-response validation: exactly one choice,
`finish_reason="stop"`, object message, absent/JSON-NULL refusal, and string
content. Truncated, refused or malformed responses fail without semantic
success-shaped defaults. Existing MAP/COMPLETE parsing retains its legacy
behavior. Quota, deadline, cancellation and provider failures cancel remaining
jobs and release registry/task/buffer ownership.

Controllers, group metadata/key copies, held native aggregate arenas, frame
metadata, request/response arenas and wire buffers are accounted in the shared
logical memory budget. Some arenas are checked after allocation; this is not an
RSS hard cap or a claim that all normal SQL allocations belong to this budget.

### Test entrypoints

```bash
# Requires a current complete Debug build, compile metadata and Bazel toolchain.
python tools/deploy/mysql_test/test_suite/ai_function/test_semantic_aggregation.py \
  --build-dir build_debug --repeat 3
python tools/deploy/mysql_test/test_suite/ai_function/test_semantic_aggregate.py \
  --self-test
python tools/deploy/mysql_test/test_suite/ai_function/test_semantic_aggregate.py \
  --binary "$PWD/build_debug/bin/src/observer/seekdb"
```

The native runner links current cached Debug objects, including the controller,
instead of compiling another duplicate implementation. SQL tests create a new
private unix-socket database and loopback endpoints; no GPU or real model is
started. Repeat `--case` for targeted SQL cases. This suite opts into normal
SIGUSR1 shutdown, requires exit status zero, enables warning-diagnosis logging,
and checks semantic resource teardown; the legacy launcher default is unchanged.

The mock supplies deterministic compact evidence and SQL source-count oracles.
Tests verify complete source coverage, duplicate/NULL behavior, mixed native
aggregates, grouping collation and LOB ownership, multi-aggregate backpressure,
phase/context bounds, independent overlap, dependency-ready merging, reverse
completion graph equality, strict provider envelopes, retry, abort and recovery.
These checks do not establish real-model factual recall, answer quality, GPU
overlap, optimal call count, prefix-cache gains or SemBench performance.

### Validation: 2026-10-12

| Validation | Result |
| --- | --- |
| Complete Debug seekdb build and refreshed compile metadata | PASS |
| Native reduction controller | 12 cases repeated 3 times: 36 PASS |
| Shared budget/scope/registry/completion resources | 15 cases repeated 3 times: 45 PASS |
| New AI_AGG SQL/mock contracts | 27 PASS |
| Controlled overlap/ready-subtree/reverse-completion repeat | 3 cases repeated 3 times: 9 PASS |
| Aggregate protocol/oracle self-tests | 3 PASS |
| Existing MAP/FILTER/native-aggregate composition regression subset | 5 PASS |
| Existing MAP/FILTER/provider/cancellation/retry regression subset | 5 PASS |
| AI_AGG private database normal shutdown | Exit 0, no semantic resource leak diagnostics |

The SQL run includes 18 KiB plain-text output and out-of-row LOB grouping-key
content equality, multiple hierarchical aggregates with skipped NULL inputs,
many all-NULL groups before a ready group, dependency-ready merges while a leaf
is held, and reversed completion without a different reduction graph.
Retry scripts are per actual HTTP attempt, not per mock dictionary lookup.
Warning-diagnosis logging is enabled for SQL and shutdown so resource-log checks
are not silently suppressed by the default INFO threshold.
No real model/GPU was started. Full historical FILE/PX/legacy suites, production
model quality, arithmetic performed by a model and performance gains remain
outside this validation record.

## Native FILE SQL/DDL

`FILE` is a native nullable SQL value containing an owned reference descriptor,
not file contents or a JSON column alias. The keyword is context-sensitive and
nonreserved; existing tables and columns named `file` remain valid.

```sql
CREATE TABLE file (id BIGINT PRIMARY KEY, file FILE DEFAULT NULL);
INSERT INTO file VALUES (1, TO_FILE('/srv/seekdb-media/photo.png'));
SELECT id, FILE_URI(file), FILE_META(file) FROM file ORDER BY id;
```

- `TO_FILE(path_or_json)` accepts an absolute local path, a local `file:///` URI,
  JSON descriptor text, or native SQL JSON. SQL NULL produces NULL. Construction
  validates and owns the descriptor without opening media or calling a model.
- JSON descriptors require `format_version: 1` and `uri`; optional fields are
  `mime_type`, `object_version`, and nonnegative `size_bytes`. Unsupported fields,
  invalid field types, duplicate text fields, and invalid references error.
  References are at most 16 KiB, URIs at most 4096 bytes; descriptor input
  materialization is bounded at 128 KiB, including LOB-backed inputs.
- `FILE_URI(file)` returns the URI as a UTF8 varchar; `FILE_META(file)` returns
  native SQL JSON for descriptor interchange. Both preserve SQL NULL.
- Projections, derived tables/CTEs, FILE/NULL CASE and COALESCE branches,
  IFNULL/NVL, INSERT SELECT, and UNION ALL retain FILE typing. IS NULL,
  IS NOT NULL, COUNT(file), and ANY_VALUE(file) are supported.
- Whole FILE values cannot be implicitly converted to strings, compared,
  concatenated, used in arithmetic, sorted/grouped/deduplicated, or aggregated
  other than COUNT/ANY_VALUE. Use the explicit accessors for URI or JSON operations.
  Sorting/grouping by other columns does not reject unrelated FILE payloads.
- Semantic tuple lowering uses the internal `AI_INPUT` packer for typed text/FILE
  parts. Its strict JSON envelope is a semantic-input transport format, not
  FILE's stored representation or a new DDL type.
- FILE primary/unique/index/partition keys, foreign keys and index storing
  columns are rejected. Defaults are limited to literal NULL. Generated FILE
  columns and FILE functions in generated expressions are deliberately disabled.
  ALTER does not convert existing columns to or from FILE.

Media loading is controlled independently of `secure_file_priv`:

| Variable | Scope | Default | Bounds |
| --- | --- | --- | --- |
| `ai_media_root` | administrator-controlled GLOBAL only | empty (loading disabled) | varchar |
| `ai_media_max_bytes` | GLOBAL / SESSION | 2097152 | 1 through 67108864 |
| `ai_media_parallel` | GLOBAL / SESSION | 4 | 1 through 64 |

The root is a global loading policy, not a user-overridable session directory.
`SET GLOBAL ai_media_root` uses the existing ALTER SYSTEM privilege check;
`SET SESSION ai_media_root` (including bare `SET`) is rejected. The root is never
inferred from `secure_file_priv`, even when that setting is `/`.
`ObBasicSessionInfo::get_global_sys_variable` reads a runtime global-schema
snapshot; the ordinary session getter retains its session snapshot.
The per-file and parallel settings follow query/PX session configuration. The
per-file limit does not relax the existing 4 MiB serialized HTTP request,
8 MiB response and 64 MiB client-batch limits or the configured query memory
budget; base64 and intermediate copies consume additional memory.

The focused native type-map regression compiles from existing Debug compile
metadata without rebuilding observer. It checks all twelve SQL type maps,
including FILE/NULL merges, rejection entries, and every pre-FILE entry:

```bash
python tools/deploy/mysql_test/test_suite/ai_function/test_file_sql_types.py
```

With a cached Debug link manifest, the optional parser mode regenerates the
parser and checks twelve FILE DDL/identifier AST contracts. It links only cached
parser/core libraries, not a new observer:

```bash
python tools/deploy/mysql_test/test_suite/ai_function/test_file_sql_types.py \
  --parser --link-manifest /path/to/cached_debug_link_action.json
```

These focused checks do not substitute for the FILE live SQL/runtime contracts.

### Ordered multimodal semantic inputs

```sql
CALL DBMS_AI_SERVICE.CREATE_AI_MODEL(
  'vision_model',
  '{"type":"completion","model_name":"vision","input_modalities":["text","image"]}'
);
-- Register its endpoint with CREATE_AI_MODEL_ENDPOINT and provider=openai.

SET GLOBAL ai_media_root = '/data/animals';
CREATE TABLE image_data (id BIGINT PRIMARY KEY, image FILE NOT NULL);
INSERT INTO image_data VALUES (1, TO_FILE('/data/animals/zebra.png'));
SELECT id FROM image_data
WHERE AI_FILTER('vision_model', ('Does this image contain a zebra?', image));
```

A semantic input can be FILE directly, or a tuple whose first argument is text
and whose remaining arguments are text/FILE, with at most 64 tuple parts. Text
paths remain ordinary text; there is no filename-extension guessing. The
internal AI_INPUT recipe and `$seekdb_ai_input` envelope are implementation
details, not FILE's storage format or a stable public JSON API. MAP/FILTER keep
their existing model/options positions. JOIN keeps its rule and left/right
bindings, using explicit left/right labels for media blocks. Ordinary text/JSON
JOIN calls retain their original JSON prompt.

Model JSON accepts `input_modalities`, containing unique lowercase `text`,
`image`, and/or `audio` values, with `text` required. Omission preserves the
text-only default. Media declarations are allowed only for completion models.
FILE requests currently require the explicit `openai` provider, rather than
assuming every adapter sharing an OpenAI-like text protocol supports media.
Images use `image_url` data URIs; audio uses `input_audio` with base64 data and a
format. There is no automatic transcription or fallback from failed media loads
to literal text or Boolean false. Declared modalities do not prove the remote
model supports them or native strict Boolean schemas.

File contents are resolved only when a semantic call is actually demanded.
Media reads/encoding run outside SQL frames, using a query-shared bounded loader,
in-flight deduplication and reusable buffers separately from HTTP task credits.
Ready rows may dispatch while another row is still loading, including while an
earlier subset's replies remain held. Each dispatch owns its body arena, client
batch and row mapping. Completed dispatches release their buffers and byte
charges; the logical wave retains its query task credit until all its queued
and in-flight rows finish. Across its dispatches, in-flight rows cannot exceed
the owned SQL batch size. Media readiness is not success: the completed task's
result must be checked before publishing MIME/base64 blocks.
Cancellation, deadline, rescan and normal server shutdown must drain owned work
and balance shared byte accounting.

Absolute local paths / `file:///` require the configured local root. Pre-signed
HTTPS GET URLs require the separate GLOBAL-only `ai_media_remote_root` described
below. Arbitrary HTTP URLs, path escapes, symlinks and nonregular files are not allowed.
Missing, corrupt, oversized, changed or metadata-inconsistent sources error.
JPEG/PNG/WAV/MP3 checks are bounded structural/container validation, not full
pixel or audio decoding; a provider can still reject structurally valid media.
External files do not gain database MVCC; deleting a SQL reference does not
delete media. No native storage credential/signing API, remote stage, upload,
implicit ASR or generic media decoder is implied. FILE data requires a server supporting the native type;
these checks are not mixed-version/downgrade compatibility evidence.

The [FILE runtime contracts](test_file_runtime.py) cover typed SQL/storage,
persistence/reopen and loopback multimodal payloads. Native
[ordered-input/provider-block checks](../../../../../unittest/sql/engine/test_semantic_input.cpp)
exercise recipe bounds, role order and strict output validation. Synthetic
fixtures do not prove real-model quality, full SemBench coverage or a speedup.

```bash
python tools/deploy/mysql_test/test_suite/ai_function/test_file_runtime.py --self-test
python tools/deploy/mysql_test/test_suite/ai_function/test_file_runtime.py \
  --binary "$PWD/build_debug/bin/src/observer/seekdb"
python tools/deploy/mysql_test/test_suite/ai_function/test_file_native.py \
  --build-dir build_debug --link-manifest /path/to/cached_debug_link_action.json
python tools/deploy/mysql_test/test_suite/ai_function/test_semantic_input.py \
  --build-dir build_debug --link-manifest /path/to/cached_debug_link_action.json
```

### Pre-signed object-storage FILE reads (2026-10-12)

```sql
SET GLOBAL ai_media_remote_root = 'https://media.example/bucket/images/';
-- Insert short-lived signed URLs through TO_FILE using application bind parameters.
SELECT id, AI_MAP('vision_model', ('Describe this image.', image))
FROM image_data;
```

Both local and remote policies default to deny, are administrator-controlled,
and are read from GLOBAL configuration rather than writable session overrides.
The remote policy is one complete, canonical origin/path prefix ending in `/`,
without a query. Invalid policies are rejected at `SET GLOBAL`; empty/DEFAULT
disables remote reads. Disabled/nonmatching source policies retain the existing
`OB_OP_NOT_ALLOW` contract. Only matching URLs are admitted; prefix boundaries cannot be
expanded by userinfo, fragments, dot segments, escaped slash/backslash or
unescaped control/space/non-ASCII bytes. HTTPS scheme and DNS host must be
lowercase. Numeric-loopback HTTP (`127.0.0.1` or `[::1]`) is available for private
fixtures only; ordinary remote HTTP and `localhost` are rejected. Empty local
root can coexist with enabled remote reads and must still deny local FILE.

FILE constructors perform no I/O and retain remote escaped paths and queries
byte for byte, including signature parameter order and percent-escape case.
The URI cap remains 4096 bytes and the FILE binary layout/version is unchanged.
FILE expressions support dynamic result buffers: metadata/URLs beyond the
128-byte reserved result buffer must not fail with a misleading memory error.
The loader issues GET, never HEAD; verifies TLS certificates/hostname using
system trust; disables redirects, environment proxies, cookies and transparent
content decoding. Error statuses/transport failures are explicit errors, with
no new download retry or text/false fallback. `object_version` remains a
`sha256:<hex>` content check, not an ETag or storage SDK version token; a provider
versionId belongs in the signed URL. Cached/live coalescing requires the same
full URL/version and consistent declared MIME/size. Expired URLs are not refreshed.

A separate single-thread curl multi executor, requiring asynchronous DNS,
holds at most 64 active fetches and 128 queued jobs. It does not occupy the four
local read/validation/encoding workers while waiting on network input.
Downloaded bytes pass through the existing bounded preparation queue,
container validation, hash and Base64 path. Query `ai_media_parallel` covers
queued/fetching/preparing tasks; media remains separate from inference credits.
Known/unknown/chunked lengths obey the same actual byte cap and declared-size
checks. Request logical overhead, source capacity, temporary buffer growth and
encoded output consume the query budget; source buffers are released before
publishing completed results. Curl internals/RSS are not fully accounted.
Cancellation/deadline remove active transfers without requiring a server reply;
query teardown and normal shutdown drain fetch/preparation work.
Shared readiness checks query/session termination even before any model HTTP
request exists. It wakes the owning SQL worker; background threads never evaluate
SQL frames or use thread-dependent execution-context status checks.

Media loader error logs omit the full signed URL/descriptor. FILE accessors,
application logs and SQL/audit records can still expose signed credentials;
protect FILE columns and avoid valid URL literals in stored code or diagnostics.
This backend does not yet provide fetch-to-provider streaming, cross-query
caching, a general object-store SDK, or GPU-aware scheduling.

The focused remote suites use generated media and private loopback
storage/provider fixtures only. They are not a live S3/OSS/MinIO account test,
a real-model quality test, or an end-to-end performance benchmark.

The final Debug build passed the **12 native remote contracts in three isolated
rounds**, **10/10 remote SQL contracts** and **9/9 fixture self-tests**. They cover
all four media formats; exact GET targets and signatures; metadata/hash and
4096-byte URLs; default/prefix/privilege denials; no HEAD/proxy/redirect/retry;
untrusted TLS; fixed/chunked/close-delimited lengths and byte caps; held
download/local-worker overlap; shared fetch/encoding admission; cancellation,
deadlines, source-buffer accounting and scheduler shutdown. SQL composition
includes vision MAP -> text AI_AGG -> post-MAP with causal request auditing.

[test_remote_file_shutdown.py](test_remote_file_shutdown.py) additionally passed
one private normal-shutdown contract: the SQL client and four storage GET gates
remain open through SIGUSR1, exit must be zero within ten seconds without forced
termination, every storage socket must close, and no model request may be sent.

Run from the repository root, using the configured Python environment:

```bash
python tools/deploy/mysql_test/test_suite/ai_function/test_remote_file_runtime.py --self-test
python tools/deploy/mysql_test/test_suite/ai_function/test_remote_file_runtime.py \
  --binary "$PWD/build_debug/bin/src/observer/seekdb"
python tools/deploy/mysql_test/test_suite/ai_function/test_file_native.py \
  --suite remote_media --build-dir build_debug --repeat 3 --timeout 180
(
  binary="$PWD/build_debug/bin/src/observer/seekdb"
  cd tools/deploy/mysql_test/test_suite/ai_function
  python -m test_remote_file_shutdown --binary "$binary"
)
```

The native helper links cached build objects rather than rebuilding seekdb.
Explicitly rebuild the production binary after core edits and before native runs.
Remote SQL failures print a bounded set of engine diagnostics before private
database cleanup.

The final build also passed the existing native codec/storage/local media
10/6/29 contracts in three rounds, the standard Bazel FILE codec target, 27 local
fixture checks, 12 selected local FILE SQL contracts including actual PX, and
targeted text MAP/FILTER (6), JOIN (2), AGG (3), and sorted/unordered actual PX (4)
readiness/cancellation regressions. Compile metadata was refreshed to 2977 source
commands. These are targeted results, not a rerun of every historical SQL suite.
Storage download/preparation is still whole-object, SQL slot/Dispatch barriers
remain, and AI_AGG is still text-only and makes its statement serial. This work
does not provide maximum heterogeneous overlap or a query-wide resource DAG.

### FILE persistence, upgrades and verification (2026-10-09)

The normal FILE runner passed **68/68 live SQL contracts** and **27/27 fixture
self-tests** against the final Debug binary. It covers native schema/wire
types, DML and transactions, prepared reexecution, joins/CTEs/INSERT SELECT,
restart, completed MINI/MAJOR compactions, all four media formats, ordered
tuples, model capabilities, short-circuiting, media errors without HTTP,
cross-batch and dependency overlap, cancellation/deadline recovery, and actual
four-task PX execution. Both unordered and compound-key full-sort projections
require four actual task IDs and multiple CPU thread IDs, not just EXPLAIN
DOP=4. The sorted case also requires zero active PX worker slices while all
model replies are held. Ordinary-key sort/group payload preservation has
separate contracts. LIMIT discards missing-file sentinels without loading them;
computed JSON sort errors precede projection FILE demand.

[test_file_shutdown.py](test_file_shutdown.py) separately passed both coordinator
and sorted-PX held-FILE shutdown contracts. Each case owns a new database,
keeps the SQL client and all provider gates open through SIGUSR1, and requires
exit zero within ten seconds without forced termination. Closing the client or
releasing replies first is not accepted as shutdown evidence.

```bash
python tools/deploy/mysql_test/test_suite/ai_function/test_file_shutdown.py \
  --binary "$PWD/build_debug/bin/src/observer/seekdb"
```

The minor persistence check explicitly freezes each FILE tablet with
`ALTER SYSTEM MINOR FREEZE TABLET_ID=...` and waits for a new completed
MINI/MINOR history record before restart. A successful server-wide freeze
request alone is not evidence that this particular tablet was flushed,
especially after a major/restart sequence. Neither compaction check was skipped.

The optional old-catalog gate also passed using a pre-FILE binary to bootstrap
its own directory, followed by two new-binary startups:

```bash
python tools/deploy/mysql_test/test_suite/ai_function/test_file_runtime.py \
  --binary "$PWD/build_debug/bin/src/observer/seekdb" \
  --upgrade-binary /path/to/pre_FILE_seekdb \
  --case test_old_catalog_upgrade_preserves_model_masks_and_is_idempotent
```

Schema fetch supplies logged, safe defaults only for the four media variables,
including the remote root, when their catalog entries are absent; it does not silently
default arbitrary missing variables or replace existing policies. Once system
schemas are published, writable-primary startup persists the missing variables
and their history in one transaction, refreshes, then initializes DDL/max-ID
services and adds the two model columns before readiness. Internal column DDL
uses an initialized UTC time-zone map and does not change table options. The
old-catalog test checks stable column IDs/defaults, old models retaining mask
1, new masks 3/5/7, and preservation of nondefault durable media policy/history
on the second restart. A read-only startup must not run these migrations;
primary write admission additionally requires durable media/model metadata.
End-to-end activation of a fully replayed standby is not included in this
local-only upgrade gate.

The separate [read-only catalog runner](test_file_catalog_roles.py) historically passed two
private contracts: legacy standby startup loads safe defaults without durable
catalog writes, and current standby restart preserves nondefault media policy,
model masks/history and native FILE values. Both reject ordinary writes.
Its pre-FILE contract requires explicit new/old binaries; current-policy and
missing-remote-policy selectors require only the current binary. It never
connects to an existing service:

```bash
python tools/deploy/mysql_test/test_suite/ai_function/test_file_catalog_roles.py \
  --binary "$PWD/build_debug/bin/src/observer/seekdb" \
  --upgrade-binary /path/to/pre_FILE_seekdb
```

The remote-read build passed three current-binary catalog contracts: complete
policy standby preservation, and primary/standby restarts after the fixture
removes only the new remote variable and its history from its own private
catalog. Primary adds the empty deny-by-default policy and one history entry;
standby publishes only a safe in-memory default and leaves the persistent
catalog unchanged. A second restart preserves all policies/history, model masks
and native FILE values. This missing-variable fixture is not a new actual
pre-FILE-binary upgrade test; those historical binary-upgrade checks were not
rerun in this stage.

```bash
(
  binary="$PWD/build_debug/bin/src/observer/seekdb"
  cd tools/deploy/mysql_test/test_suite/ai_function
  python -m test_file_catalog_roles --binary "$binary" \
    --case test_current_standby_restart_preserves_policy_masks_and_file_values \
    --case test_missing_remote_policy_primary_restart_persists_default_and_preserves_catalog \
    --case test_missing_remote_policy_standby_restart_keeps_safe_default_unpersisted
)
```

Native validation passed nine codec/object/compact-cell groups, the four
storage/wire checks plus five encoded microblock formats, 29 asynchronous media
groups, 12 ordered-input/provider groups, 13 model/default groups repeated three
times (39 executions), all twelve type matrices including every pre-FILE entry,
and twelve freshly regenerated parser contracts. The native helper's cached
object checks are distinct from the successful strict-dependency Debug
observer build. Five controlled dispatch scenarios also passed, including
32 held distinct rows with two logical credits, the aggregate row cap, idle-wave
quota failure, timeout cleanup and legacy text behavior. The codec additionally
passes its standard repository Bazel test target:

```bash
python bazel.py --build-dir=build_debug test //unittest/oblib:test_file_ref -c dbg
```

Regeneration of the media system variables preserves all 6,251 pre-FILE
generated field assignments; the larger textual diff is assignment ordering
from the Python 3 generator, not changed old-variable values.

The final binary also passed the existing 39 map/filter, 26 composition,
45 join, nine actual-PX, and 105 legacy AI Function/SOLO SQL contracts; 140
native resource/binding/discovery checks and the native collection runner
passed. The two existing SQL shutdown runners exited normally with SIGUSR1
and zero status. These are deterministic local contracts, not real-model,
full-SemBench, TSan, mixed-version or performance certification.

## Semantic Join Baseline (2026-10-09)

`AI_JOIN(model, rule, left_input, right_input[, options])` is a Boolean pair
predicate for an INNER JOIN ON clause. It is not a general scalar projection,
WHERE predicate, outer/semi/anti join, or approximate-nearest-neighbor interface.
Models and options must be static. The rule is a SQL string expression; each
payload is a SQL string or JSON expression referring only to a nonempty set of
columns on one syntactic join side, with the other payload on the opposite side.
Either argument orientation is allowed and remains unchanged when the optimizer
reorders its candidate join. Constant-only and cross-side payloads are explicitly
unsupported in this first version. Existing semantic SELECT restrictions still
apply.

```sql
SELECT l.id, r.id
FROM left_inputs l
JOIN right_inputs r
  ON AI_JOIN('matcher', 'Do the two inputs describe the same entity?',
             l.description, r.description)
ORDER BY l.id, r.id;
```

The planner creates a dedicated `SEMANTIC JOIN` stage, retaining the two candidate
input table sets and the original model/rule/left/right expression bindings. The
join-elimination rewrite is conservatively disabled for statements containing
AI_JOIN: even a lossless primary-key self-join must retain the candidate-pair
boundary before lowering. Other relational candidate algorithms remain usable.
The first physical strategy is an ordinary binary SQL join producing candidate-pair
batches followed by asynchronous semantic matching. It preserves every
candidate satisfying ordinary SQL qualifications; there is no retrieval pruning
or prompt-text deduplication. Duplicate rows and many-to-many matches retain SQL
bag multiplicity. Without ordinary candidate conditions, the work is worst-case
`N * M` model judgments, not a lower-complexity join algorithm.

The matcher retains bounded, owned pair batches rather than materializing the
complete Cartesian product. Native join fragments, including one-row Cartesian
NLJ output, are coalesced up to the configured semantic batch size before they
consume a pipeline slot. The window therefore does not collapse to two row
requests merely because a native join chose a one-row inner side. A tail, an
upstream wait or query-memory backpressure can finish a partial owned batch.
It uses the existing semantic dependency runtime,
cross-batch pipeline, query-wide task/memory admission, response validation,
deadline, cancellation and rescan cleanup. The native candidate join's own
storage/workarea accounting is separate from the retained semantic buffers.
Rescan restores the native input frame before the framework rescans descendants;
restoring it only in inner_rescan would overwrite freshly reset material/scan
state with a prior match's row values.
An asynchronous join does not imply that every containing SQL/PX shape releases
its physical worker; the existing conservative suspension gates are not widened
to arbitrary binary join trees.

The provider prompt is one JSON object with `rule`, `left`, and `right` fields.
SQL strings are JSON strings, not templates or raw JSON snippets; JSON payloads
retain their JSON type. SQL NULL rule/payloads error when demanded, rather than
silently failing the match. SQL short-circuiting may avoid an undemanded call.
The result uses AI_FILTER's strict Boolean `value` JSON schema, with default
native schema name `ai_join`; a matching caller format/name is preserved.
`Schema` is reserved, and incompatible output formats fail before HTTP.
Options configure the model request, not an unimplemented join strategy.

ANN candidates, blocking, cascades, result caches, outer/semi/anti joins and
semantic cost/quality-based strategy selection are not implemented. Candidate
pruning can change recall and must not be silently enabled as a speed-only
optimization. Exhaustive candidates do not guarantee that the model's judgment
is semantically correct.

The independent [join contracts](test_semantic_join.py) use private databases
and loopback model fixtures:

```bash
python tools/deploy/mysql_test/test_suite/ai_function/test_semantic_join.py \
  --self-test
python tools/deploy/mysql_test/test_suite/ai_function/test_semantic_join.py \
  --binary "$PWD/build_debug/bin/src/observer/seekdb"
```

Repeat `--case` for focused SQL contracts. The existing semantic runtime,
composition and PX suites remain regression gates; native resource checks also
cover join binding integrity, semantic-stage discovery and suspension scopes.

### Validation: 2026-10-09

The final Debug build and the following gates passed on this implementation:

| Gate | Passed |
| --- | --- |
| AI_JOIN SQL contracts | 45/45 |
| AI_JOIN fixture/oracle self-tests | 14/14 |
| Existing AI_MAP / AI_FILTER SQL contracts | 39/39 |
| Existing SQL composition contracts | 26/26 |
| Actual PX contracts, including one new AI_JOIN case | 9/9 |
| Legacy AI Function / SOLO SQL contracts | 105/105 |
| Native semantic resource/binding/discovery checks | 140 (14 cases, 10 repetitions) |
| Native resumable collection scenarios | 78 |
| Separate legacy and semantic-PX normal shutdown checks | Both passed, zero exit status |

The single-worker join gates hold every response, require 32 row requests from
two configured 16-row candidate batches before the first reply, and reject
unbounded refill. They also verify demand-driven AND services, nested map
dependencies, exact duplicate/pair multiplicity, rescan/reexecution, provider
errors, retries, cancel/deadline cleanup and same-connection recovery.
The rescan oracle checks the ancestor path of the semantic stage, not unrelated
materialization inside its native candidate generator. EXPLAIN assertions accept
the engine's type-cast wrappers and Unicode tree connectors while checking the
original argument order and fixed candidate policy.

The new actual-PX join case uses eight partitions and `PARALLEL(4)`. Four task
IDs from one execution and multiple real CPU thread IDs prepare inputs while
all replies are held. It requires the shared 32-row HTTP admission ceiling,
256 exact candidate requests and 128 known matching output pairs, exercising
the serialized SemanticJoinSpec and self-join preservation. This does not add
a join-tree worker-release claim: the existing eight map/filter PX contracts
separately cover checked driver suspension and actual worker reuse.

All SQL checks used temporary private unix-socket databases and loopback model
mocks. No real-model quality, SemBench acceleration, GPU utilization, approximate
recall or actual spill-I/O certification is claimed.

## Asynchronous Semantic Operators (2026-10-08)

`AI_MAP(model, prompt[, options])` returns per-row LONGTEXT.
`AI_FILTER(model, prompt[, options])` returns SQL Boolean 1/0, accepting only a
strict JSON object `{"value": true}` or `{"value": false}` from the provider.
Invalid output is an error, never a false result. Models and options must be
static; prompts can use ordinary SQL expressions, positional `AI_PROMPT`, or a
preceding semantic result. Structured global SOLO input is not supported by
these new interfaces. Existing AI functions retain their existing paths.
Set/window/locking/SELECT-INTO and scalar-subquery SELECTs, semantic equality
join keys, and semantic outer/semi-join ON conditions are explicitly unsupported
in this stage; ordinary inner-join pair filtering and noncorrelated derived
semantic stages are covered.

```sql
SELECT id, AI_MAP('classifier', prompt) AS category
FROM inputs
WHERE AI_FILTER('selector', prompt)
ORDER BY id;
```

The planner creates `SEMANTIC MAP` and `SEMANTIC FILTER` stages, not synchronous
completion aliases. Projecting `AI_FILTER` is a Boolean **map** and preserves
false rows; using its result in WHERE/HAVING consumes it as a row filter.
CASE/AND/OR/NOT retain their SQL demand boundaries. Independent expressions can
submit together; same-row dependencies wait for their inputs, while independent
batches and filter-to-map stages can overlap. Results remain mapped to owned
input rows, including duplicates and partial final batches.

Call ownership does not mean every surviving row has requested that call.
Earlier stages explicitly carry completed semantic values or a private
undemanded tag; subsequent materializers reuse ready values and request only
missing ones at their SQL boundary. Valid semantic outputs are never SQL NULL,
so only declared carried AI expressions use an internal NULL tag. False and
empty-string results remain ready values, not missing values. These tags must
never become SQL-visible fallback results. Projection materialization stays
after LIMIT; sort/group consumers materialize their required expressions before
the corresponding CPU operator.

For an ORDER BY without LIMIT and with only column, already-materialized
aggregate, or literal keys, semantic projection is allocated before ordering
so a final gather does not serialize its model preparation on the coordinator.
Computed keys retain their old evaluation boundary. PX task admission and the
sort operator share the ordinary vectorized full-sort capability predicate.
Prefix/local-merge/partition sorts, prescan, WITH TIES, approximate Top-K,
Top-N filters, and runtime dump/spill restrictions remain protected.

JSON_EXTRACT, JSON_UNQUOTE, and JSON_PRETTY preserve a pending semantic
argument's OB_EAGAIN instead of reclassifying it as invalid JSON/datatype.
JSON_VALID no longer turns pending or failed argument evaluation into a
successful false result. Completed malformed JSON text still returns false;
NULL contents keep their ordinary behavior. Derived JSON MAP/group/HAVING
and held two-wave cases have exact result and request-count regressions.
Four-format FILE-to-MAP-to-JSON projection and sorted JSON projection on
four actual PX tasks are also covered. The complete composition/PX suites
passed 30/15 cases, and the 105 legacy SQL contracts passed again.
This argument-error correction is intentional, including ordinary nested SQL
argument errors; a failed expression is not malformed JSON contents.

CPU recipes derived from partial semantic values, including their conditional
fallback branches, are not advertised as completed relational inputs. Consumers
recompute them inside their semantic runtime, preserving already-completed
child inputs. Per-row availability masks keep ready peers initialized when a
missing peer is requested, avoiding cache replay.
Completing a whole CASE/OR condition does not complete its skipped CPU branches;
those descendants retain deferred classification even when the condition itself
is an exportable stage target.

AI_FILTER uses the existing native `response_format.json_schema` mechanism with
required Boolean `value`, no additional properties, and `strict=true`. The exact
case-sensitive `Schema` key is reserved for AI_FILTER within these two interfaces. A matching caller
native format and its nonempty name are preserved; conflicting formats fail.
AI_MAP retains completion-style option passthrough rather than interpreting
`Schema` as a new shorthand.

### Budgets And Suspension Boundaries

- Each operator retains at most `ai_pipeline_slots` input batches. All stages
  and PX contexts of the same execution share `ai_pipeline_memory_limit`.
- Query-wide active **logical-wave** admission is `ai_pipeline_slots` times the
  number of distinct semantic calls in the plan, not that product times DOP.
  A wave can own multiple HTTP client-batches as subsets become ready; their
  aggregate in-flight rows remain capped at the owned SQL batch size.
  Each completion client-batch contains independent per-row HTTP requests.
  The wave credit bound and the existing scheduler's 64-client limit are not
  themselves counts of HTTP requests.
- Owned inputs/results, prompts, static model/configuration, and HTTP buffers
  participate in logical accounting. Configuration is prepared once per
  submission wave; temporary expression/JSON allocation and allocator overhead
  mean the limit is not a whole-process RSS cap. Existing 4MiB request, 8MiB
  response, and 64MiB client-batch limits remain in effect.
- Outward suspension is default-deny for ordinary parents with unsafe
  stack-local progress, and always disabled during open. Unsupported suspension
  shapes still use the asynchronous semantic batches but do not necessarily
  release their SQL worker. A plan containing a semantic stage is not by itself
  evidence that every ancestor can suspend.
- Resumable PX is narrowly checked: vectorized reduce-transmit, a contiguous
  semantic map/filter region, optional GI, and scan/expr-values. Rescans,
  sampling, shuffle/hybrid modes and unsafe join/receive/blocking ancestors are
  excluded from this gate. The existing PX pool retains at most 64 continuation
  records; memory/context/session/schema/interrupt state survives a wait, and
  completion/admission ownership is released only at true finish.
- WHERE lowering happens before LIMIT/Top-N pushdown. Semantic pair predicates
  are not pushed into mutable NLJ inner parameters. Prefetch preserves the
  entire child datum frame, including live tails retained by prefix sorting.
- The owning SQL worker polls completed tasks across all registered semantic
  stages before preparing its own frame. Polling collects owned buffers and
  releases task credits without evaluating another operator's SQL frame.
  Ready upstream batches therefore cannot pin every credit while a full
  downstream stage waits to submit missing values.
- If the first input cannot fit a query-local memory quota while an upstream
  batch remains pinned, execution reports the quota error rather than awaiting
  memory that the same pull stack cannot release. This is a conservative quota
  policy, not a claim of adaptive concurrency under all memory pressure.

### Test Entrypoints

All SQL runners below create new temporary unix-socket databases with loopback
mocks. They do not connect to the existing debug database or call a real model.

```bash
python tools/deploy/mysql_test/test_suite/ai_function/test_semantic_runtime.py \
  --self-test
python tools/deploy/mysql_test/test_suite/ai_function/test_semantic_runtime.py \
  --binary "$PWD/build_debug/bin/src/observer/seekdb"
python tools/deploy/mysql_test/test_suite/ai_function/test_semantic_composition.py \
  --binary "$PWD/build_debug/bin/src/observer/seekdb"
python tools/deploy/mysql_test/test_suite/ai_function/test_semantic_px.py \
  --binary "$PWD/build_debug/bin/src/observer/seekdb"
python tools/deploy/mysql_test/test_suite/ai_function/test_semantic_px.py \
  --binary "$PWD/build_debug/bin/src/observer/seekdb" --shutdown-test
python tools/deploy/mysql_test/test_suite/ai_function/test_semantic_resources.py \
  --build-dir build_debug --repeat 10
python tools/deploy/mysql_test/test_suite/ai_function/test_semantic_collection.py \
  --build-dir build_debug
```

Repeat `--case` to select SQL contracts. PX shutdown is a separate invocation
because it intentionally stops its own private database.

The composition suite checks joins, aggregate arguments, GROUP BY/HAVING,
derived stages, Boolean NULL logic, and exact ordered LIMIT/OFFSET results.
It also checks partial HAVING/conditional-sort results, nested prompt recipes,
invalid unselected CPU fallbacks, empty strings, 67-row LOB carriers, streaming
credit progress, and exact once-only request counts after LIMIT.
The eight PX contracts use eight partitions and `PARALLEL(4)`, require four
preparing task IDs under one execution ID and multiple actual CPU thread IDs,
and enforce held-request limits of 32 rows for one call or 64 for two calls at
slots=2/batch=16. Unconditional shapes must fill the exact ceiling; guarded
client-batches can have fewer demanded rows, but retain the same hard ceiling.
These are two or four client-batches, not provider batching.
They verify exact rows/call multiplicities, filter pruning, nested dependencies,
cancel/deadline/provider-error recovery, and socket cleanup.

The driver-release case additionally requires no active PX execution slices
while all replies are held, then runs another parallel query on the same
physical driver threads. Normal shutdown must exit with status zero before
the held query deadline. Native budget tests check exact byte/task boundaries,
global rollback, DOP-independent capacity, fatal-error admission, scope/TLS
isolation, concurrent final registry teardown, and completion-credit release
without another child pull. Native collection tests cover waits, errors,
cancel/rescan/close, worker handoff, spill gates and prefix live tails using
cached Debug objects. Actual spill I/O remains uncertified. Native continuation
tests are separate from the real SQL PX proof.

The complete legacy 105 SQL contracts, legacy native resources, and independent
legacy normal-shutdown contract have been rerun successfully. The new 39
semantic SQL contracts, 26 composition contracts, and eight PX contracts pass.
Both independent normal-shutdown contracts, eleven semantic fixture self-tests,
120 repeated semantic budget/scope/registry/completion checks, and 78 native
collection scenarios also pass. The continuation lifecycle suite separately
passed nine cases repeated twenty times and ASan/UBSan; TSan could not initialize
on this host and is not counted as a pass. SQL contract deadlines remain strict;
only metadata bootstrap uses a separate 30-second setup timeout.
These checks do not establish real-model quality, GPU utilization, or a SemBench speedup.
Semantic cost-based optimization and approximate retrieval/cascades are not
implemented here. The newer exact semantic-join and native aggregation paths
are documented separately above; historical validation does not cover them.

## Global SOLO Prompt Reordering (2026-09-29)

A two-argument `AI_PROMPT` whose second argument is a JSON object selects global
SOLO when used directly as the prompt of a supported top-level `AI_COMPLETE`.
No separate field-reordering switch is required. The model and its endpoint must
already be registered:

```sql
SELECT id,
       AI_COMPLETE(
         'my_completion_model',
         AI_PROMPT('Classify the text using the named fields.',
                   JSON_OBJECT('id', id, 'text', prompt))) AS answer
FROM inputs
ORDER BY id;
```

Prefix this query with `EXPLAIN` to check for `AI SOLO GLOBAL`. Existing string
prompts and positional `AI_PROMPT(template, string_args...)` keep their existing
meaning and do not opt into field reordering. Passing a previously stored
structured prompt object as a plain column does not select the new mode.

### Input And Execution Contract

- The instruction, model and optional completion configuration must be static
  constants. The JSON object must be nonempty and have the same field names in
  every row. Field values may include JSON nulls, numbers, strings and nested
  values; a NULL, array or scalar in place of the field object is invalid.
- The operator first drains its entire child, retaining input rows and their
  original row numbers. It then plans one global column order and one global row
  order. Neither collection nor planning sends model requests. Empty input sends
  nothing. An invalid final input row fails without sending earlier rows.
- SOLO uses exact serialized JSON value bytes for dictionary encoding, then
  repeatedly selects the column with the fewest distinct current-prefix/value
  pairs. A final lexicographic sort uses the selected columns and original row
  numbers to break ties. There is no SQL-collation equality, sampling or
  per-network-batch replanning. The greedy column order is not a claim of a
  globally optimal layout across all possible field permutations.
- After planning, requests use a fixed GGR-style task prefix followed by a valid
  JSON object in the selected field order. Names stay attached to their values,
  and JSON escaping is preserved. Each input row remains a separate prompt;
  duplicate rows are not deduplicated. Field reordering preserves the supplied
  data, not a guarantee of identical model answers.
- The existing asynchronous slots consume consecutive portions of the global
  row order. Responses are mapped back to original rows, and output follows the
  child input order. Explicit SQL ordering remains intact; without `ORDER BY`,
  SQL does not promise a particular result order. Network submission order does
  not guarantee the inference service's prefill order or cache hits.
- Retry, deadline, cancellation and worker suspension reuse the existing
  pipeline mechanisms. A retry replays the failed request, not the entire query.
  Collection and planning also check query status. Local failure cannot retract
  requests already accepted by a model service during the execution phase.

### Scope And Memory

This version supports one top-level completion over a single-worker upstream
plan, including ordinary filters, expressions, joins and aggregation. The target
AI expression cannot be evaluated in that upstream plan. Additional AI calls in
retained upstream expressions, including HAVING, GROUP BY and JOIN conditions,
are rejected. Scalar execution, dynamic instruction/model/options, multiple AI
calls, AI-dependent ordering, LIMIT, DISTINCT, window functions, set operations,
subqueries and parallel/exchange plans are not supported. Unsupported paths
reject structured input instead of silently executing it one row at a time.
AI_EMBED and AI_RERANK do not use SOLO.

The full input store, serialized field data and SOLO working allocations share
the existing `ai_pipeline_memory_limit` and instance-wide
`ai_pipeline_total_memory_limit` accounting with network state. Collection and
planning are limited to half the local budget, leaving headroom for execution;
after planning, retained input and results may use the full local budget together
with active requests. The default local limit remains 64MiB, and
`ai_pipeline_slots` still controls in-flight batch slots, not planning windows.
Temporary expression/JSON buffers and other existing exclusions mean this is
not a process RSS cap or a guarantee that every response will fit.

There is no disk spill. A collection/planning budget failure returns size error
4019 before HTTP submission; it does not switch to a windowed layout. Later
request/response/result overflows can still fail an executing query. Existing
4MiB serialized-request, 8MiB response and 64MiB per-client batch bounds remain.

### SOLO Verification

All ten new SQL contracts passed within the complete **105/105** run. They cover
cross-batch global ordering and original-row mapping, invalid-final-row zero
submission, conditional-prefix column selection against an independent
reference, JSON types/escaping/case/duplicates, retry with out-of-order slots,
join/aggregation inputs, unsupported forms and changing fields, local/global
memory failure and quota reuse, cancellation/deadline/provider-error cleanup,
and request-worker release. Native diagnosis and repeated empty-input rescan
checks now also exercise SOLO mode.

The Debug build, native resource suite, five fixture self-tests and independent
normal-shutdown check passed. Tests used private temporary databases and local
HTTP mocks, not a real model or the existing debug database. This does not verify
rescan with retained rows/in-flight requests, every operator allocation failure,
whole-heap sanitizers, or real-model output quality, prefix-cache hits and speedup.

## Suspended Statement Cache Lifetime (2026-09-29)

Larger real-model queries exposed error 4014 after partial results. The session
checker could acquire `query_lock` while an AI statement was suspended and expire
the session's cached schema guard after ten seconds. The retained statement still
borrowed that guard. Short or single-batch tests did not necessarily cross both
the expiry interval and another schema-dependent batch.

The checker now reverts the schema cache and refreshes cached runtime configuration
only for `SESSION_SLEEP`, preserving the pre-suspension assumption that an active
statement owns those caches. Query/session deadlines, cancellation, transaction
timeout checks, and idle-session cache reclamation remain enabled.

The new `test_pipeline_many_batches_preserve_rows_after_repeated_suspension`
uses 2,049 distinct 6KB prompts, the default 256-row vector size, and one/two
pipeline slots. Response gates keep the query suspended across the ten-second
expiry interval and then verify every row, exact response text, call count, tail,
and connection reuse. It reproduced 4014 before the fix in both slot modes and
passed after it. The full 95-contract run and shutdown check also passed.

Run just this regression, or repeat `--case` to select other contracts:

```bash
python tools/deploy/mysql_test/test_suite/ai_function/test_runtime_contracts.py \
  --binary "$PWD/build_debug/bin/src/observer/seekdb" \
  --case test_pipeline_many_batches_preserve_rows_after_repeated_suspension
```

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

## Real provider contracts

[test_semantic_live.py](test_semantic_live.py) is a separate, explicitly opt-in
runner for the current AI_MAP / AI_FILTER / AI_JOIN and native FILE pipeline.
It does not replace the deterministic regressions or the earlier legacy live
runner. There is no default endpoint/model, existing-database mode, or remote
permission: both `--endpoint` and `--model` are mandatory for provider traffic.
Only HTTP(S) numeric loopback addresses and `localhost` are accepted;
`localhost` is pinned to `127.0.0.1`. Credentials in URLs, query strings,
fragments, arbitrary URL paths, redirects and non-loopback hosts are rejected.
HTTP uses direct connections, not ambient proxies. HTTPS keeps certificate
verification and the original TLS hostname.

The parent must already have started an appropriate local OpenAI-compatible
service. Parent-reported deployment on 2026-10-09: offline eager vLLM 0.22.1
with **text-only Qwen3-4B-Instruct-2507**, base origin
`http://127.0.0.1:18081`, served ID `seekdb-file-validation-4b`,
`max_model_len=2048`, `max_num_seqs=8`, 512 MiB KV cache, and prefix cache off.
Use that exact served ID, not an automatically chosen model or an unverified
alias. Service lifecycle is external to this runner; it never starts or stops
a model service. This temporary validation instance was stopped normally
after the checks, with its API/engine processes, socket and GPU allocation
reclaimed. The commands below require a caller-owned compatible service
and do not imply that this port is still available:

```bash
# DB-free self-tests: only in-process fake loopback providers, no model service.
/volume/xicksys/.venv/bin/python -B \
  tools/deploy/mysql_test/test_suite/ai_function/test_semantic_live.py --self-test

# Direct health/model/schema/answer checks, without starting SQL.
/volume/xicksys/.venv/bin/python -B \
  tools/deploy/mysql_test/test_suite/ai_function/test_semantic_live.py \
  --endpoint http://127.0.0.1:18081 --model seekdb-file-validation-4b \
  --preflight-only

# Full text and composed relational contracts in a new private database.
/volume/xicksys/.venv/bin/python -B \
  tools/deploy/mysql_test/test_suite/ai_function/test_semantic_live.py \
  --endpoint http://127.0.0.1:18081 --model seekdb-file-validation-4b \
  --binary "$PWD/build_debug/bin/src/observer/seekdb"
```

The parent subsequently reported a successful unauthenticated direct strict
Boolean probe: required Boolean `value`, `additionalProperties=false`,
`temperature=0`, `max_tokens=32`, content `{ "value": true }`, and
`finish_reason="stop"`. JSON whitespace is valid without stripping or repair.
The runner uses that same Boolean schema and 32-token budget, but never
substitutes the reported response for fresh observations. This positive
probe alone does not establish the runner's real SQL results.

Actual validation on 2026-10-09 subsequently passed all six text SQL cases
against the same Qwen3-4B deployment: MAP, Boolean projection, WHERE filtering,
duplicate-label INNER JOIN, derived filter/MAP/group/HAVING/sort/LIMIT, and
JOIN/filter/MAP composition. Direct preflight and private SQL/relay cleanup
also passed. The DB-free runner suite passed 65 cases. The first real run
exposed the JSON pending-propagation bug described above; the unchanged
composed-group oracle passed after fixing the engine, without casts,
materialization workarounds, substituted responses, or relaxed schemas.
These are small synthetic-label correctness contracts, not SemBench answer
quality, multimodal quality, throughput, or speedup evidence.

The separate local SemBench suite also gained an opt-in `semantic` mode while
retaining its old default modes. Real Qwen3-4B comparisons passed for
`ecomm_q1` and `ecomm_q5` at four input rows each, and `ecomm_q7` at two eligible
products/four ordered candidate pairs. Each legacy/native variant completed;
all three ordered final bag hashes matched. The pairwise query made exactly
four provider calls per variant and retained its two self-pair outputs.
The experiment's 69 DB-free contracts passed in its existing environment.
These comparisons explicitly do not assert stage-answer/token equivalence
or calculate speedups; they are not full SemBench coverage or quality evidence.

The endpoint may be an origin, `/v1` base, or `/v1/chat/completions` URL.
`--list-cases` is offline; repeat `--case NAME` to select SQL cases. Unknown
or repeated cases fail instead of being skipped or generating extra traffic.
Every live invocation preflights `/health`, `/v1/models`, exact response model
identity, two label MAPs, positive/negative FILTERs and positive/negative JOINs
with direct HTTP before database startup. These six inference probes use the
same native `response_format` contracts as SQL, with `strict: true`;
`Schema` is not an AI_MAP shorthand. The fixed inference options are
`temperature=0`, `max_tokens=32`, and `stream=false`. `--timeout` bounds each
HTTP transfer, including progressing headers/body (default 15 seconds, maximum
60); SQL has a 60-second query budget. The text dataset has four left rows
and two right rows: eight logical Cartesian JOIN candidates, including
duplicate left labels and both matching/nonmatching pairs. Prompts are short
synthetic labels, not private data or SemBench inputs; their character bounds
are not a tokenizer/context-length or GPU-concurrency claim.
No schema downgrade, whole-run retry, model substitution, Markdown stripping,
truncation repair, or conversion of invalid output to false is performed.
The [vLLM 0.22.1 message protocol](https://github.com/vllm-project/vllm/blob/v0.22.1/vllm/entrypoints/openai/chat_completion/protocol.py)
defaults `tool_calls` to an empty list. Absent/null/empty-list tool metadata
means no calls and is accepted; nonempty or wrongly typed tool metadata,
refusals and function calls still error. This envelope compatibility does not
relax the content schema or the mandatory `finish_reason="stop"`.

Authentication is optional for an unauthenticated local service. When needed,
pass **only the name** of an already populated credential environment variable
via `--api-key-env NAME`; do not put its value on the command line or in a file.
A synthetic loopback registration key is used inside the private database.
The Python HTTP client uses the actual provider credential; it is not
registered in SQL or written to the temporary database's endpoint metadata.
Named but missing/empty/invalid credentials fail before any traffic.

The runner reuses [test_file_runtime.py](test_file_runtime.py)'s
`PrivateDatabase` UNIX-socket launcher, startup isolation and SIGUSR1 cleanup.
It registers `provider=openai` explicitly. The text model omits
`input_modalities`, verifies the catalog's text-only mask, and leaves the
GLOBAL-only `ai_media_root` at its empty default. SQL covers label MAP,
Boolean projection and filtering, duplicate-label INNER JOIN multiplicity,
filter/MAP/group/HAVING/sort/LIMIT composition, and JOIN/filter/MAP composition.
Oracles require exact ordered rows, types, labels, and Boolean values; they do
not compare two generations or depend on optimizer request counts/order.

### Opt-in image FILE contracts

The 2026-10-09 setup had no usable real image model: Qwen3.5-9B had incomplete
temporary shards, and Qwen3-4B was text-only. On 2026-10-10 a separate,
fully verified Qwen2.5-VL-3B-Instruct checkpoint was used for the real image
experiment below. This does not make the text endpoint multimodal. The
following command requires an explicitly started compatible vision service;
the experiment's temporary service has since been stopped.

Image traffic additionally requires `--image`, an explicit yes/no
`--instruction`, and an independently supplied `--expect-image true|false`.
Use a PNG/JPEG path or `synthetic-red`, a locally generated 128x128 red PNG:

```bash
/volume/xicksys/.venv/bin/python -B \
  tools/deploy/mysql_test/test_suite/ai_function/test_semantic_live.py \
  --endpoint http://127.0.0.1:8000/v1 --model YOUR_SERVED_VISION_MODEL_ID \
  --binary "$PWD/build_debug/bin/src/observer/seekdb" \
  --image synthetic-red --instruction 'Is this image predominantly red?' \
  --expect-image true
```

Custom images must be explicitly selected regular local files of at most
2 MiB, with no symlink components. Only that file's bounded bytes are copied
into a temporary private media root; the source file is not changed and its
path is not a provider prompt. A direct image/schema/expected-answer probe
runs before SQL. Selecting an image alone is not evidence of model capability:
the probe must actually succeed. Only then is a separate completion model
registered with `input_modalities=["text","image"]` and its catalog mask
checked. No audio capability is declared. The root is set with administrator
`SET GLOBAL` exclusively inside the new private database.

Native FILE construction/URI/metadata and both `(text, FILE)` inputs and
direct FILE JOIN inputs are exercised. Image MAP uses a native Boolean
schema; FILTER projection preserves an explicitly guarded NULL row; WHERE
and JOIN have exact expected result sets for either true or false. The JOIN
matches the actual image answer against separate `"true"` / `"false"` rows.
Missing/corrupt/nonboolean/truncated/refused output is always an error, never
a rejected row, false answer, text-path fallback, or skip.

### Real image experiment (2026-10-10)

The local Qwen2.5-VL-3B-Instruct checkpoint at
`/volume/models/data/Qwen2.5-VL-3B-Instruct` is complete. Its pinned official
revision is `66285546d2b821cf421d4f5eb2576359d3770cd3`; all 14 file
hashes/sizes, both safetensors shards and the 824-tensor index were verified,
followed by an offline CPU processor check. The checkpoint's Qwen Research
License permits non-commercial research/evaluation; commercial use requires
separate authorization.

Real inference used one RTX A6000 with existing vLLM 0.22.1, BF16, eager
execution and FlashAttention. The temporary loopback endpoint was
`http://127.0.0.1:18081/v1`, serving `seekdb-file-validation-vl3b`, with
4096 context tokens, two sequences, an explicit 1 GiB KV allocation,
two images/no video per prompt, processor pixel bounds 3136..262144, and
both prefix caching and the multimodal processor cache disabled.
A post-test GPU snapshot was 9,101 MiB used, **not a measured peak or a
performance result**. Inference options remained `temperature=0`,
`max_tokens=32`, `stream=false`; HTTP timeout was explicitly 60 seconds.
The current Debug binary at commit `2ea8cd37c6ae744ffc6f2fad1a38f239219c3068`
built successfully, and all 65 DB-free live-runner self-tests passed.

The independently labeled input was a generated 128x128 all-red PNG.
The question below is followed by the runner's unchanged strict JSON
instruction; each invocation used a fresh private database when preflight
allowed SQL to run.

| Workload | Actual result |
| --- | --- |
| `Is this image predominantly red?`, expected true | Direct strict-schema image preflight and all six text/composition plus four FILE SQL cases passed. |
| `Is this image predominantly blue?`, expected false | Direct preflight returned valid `{"value":true}`; exit 3, `model-quality`. No SQL was started for this workload. |
| Separate `Does the image have a blue background?`, expected false | Direct preflight and FILE MAP/FILTER projection/WHERE passed. FILE-to-text-answer JOIN returned false for right answer `"false"` where a match required true; exit 3, `model-quality`. |
| Separate red/blue image-image same-color comparison | The direct red/red pair passed, but red/blue returned true instead of false. Exit 3 before database startup; the four-pair FILE/FILE SQL oracle was **not exercised**. |

A bounded native Transformers 5.10.2 BF16 greedy SDPA comparison on the
same checkpoint, without vLLM or SQL or guided JSON decoding, reproduced
the incorrect predominantly-blue and red/blue comparison answers.
Freeform red recognition was correct; placing the image before the
unchanged negative question also returned false in both backends.
These are separate diagnostics, not replacements for failed oracles.
They indicate checkpoint/prompt-layout sensitivity rather than evidence of
a database transport bug. SQL's ordered `(text, FILE)` contract was not
changed, expected answers were not relabeled, and strict schemas were not
weakened. Schema validity alone cannot guarantee answer correctness.

This is **partial real-model validation**, not all-positive/negative visual
acceptance. No natural-image dataset, audio inference, complete multimodal
SemBench, speedup, vector-candidate recall, or SOLO/prefix-cache benefit was
measured. Logs and response reports were retained as session artifacts;
generated media/private databases were cleaned up. The task-owned vLLM API
was stopped with SIGTERM and exited zero, its engine exited, port 18081 was
available again, and GPU use returned to 15 MiB with no compute processes.
The downloaded checkpoint remains available offline.

### Failure evidence and limits

A private loopback observation relay forwards each allowed SQL request body
and each actual provider status/body unchanged, while independently checking
its response and answer. It blocks unplanned prompts/media, changed models,
or weakened schemas before forwarding. It never synthesizes a successful
provider response. This also keeps real credentials out of SQL registration
and preserves actual responses when SQL fails.

Failures are labeled separately: `model-quality` (exit 3) for a well-formed
but wrong answer, `provider-protocol` (exit 4) for HTTP/envelope/model/schema
violations, `sql-integration` (exit 5) for SQL or row-shape/binding errors, and
`private-runner-lifecycle` (exit 6) for private startup/cleanup failures.
Invalid opt-in/safety inputs exit 2. An observed provider failure retains its
category alongside a subsequent SQL error; correct upstream answers with
incorrect SQL rows are not attributed to model quality. Failure messages
identify the preflight/SQL phase and synthetic label or left/right pair,
along with the observed and independently expected answers. FILE diagnostics
use a generic question/pair label, not the user's private instruction/path.

Failure diagnostics retain the latest 16 observations plus the first failed
response, at most 2048 redacted characters per body and a 48 KiB observation
output bound. Known credentials are redacted before truncation, including
escaped/encoded forms; raw malformed bodies are shown as bounded escaped
text, not replaced with invented results. Requests are capped at 4 MiB and
responses at 8 MiB. Temporary media, registrations, relay and database are
owned by this invocation and cleaned up; no model service is started/stopped,
no weights are downloaded, and no user environment is modified.

[test_semantic_live_selftest.py](test_semantic_live_selftest.py) tests CLI
safety, direct preflight, strict JSON/Boolean/envelope parsing, transparent
forwarding, credential redaction/bounds, image staging, registration, exact
SQL oracles, failure attribution and cleanup without a database. Fake
provider self-tests are not real-model validation. Passing this runner is a
small integration/answer contract, **not SemBench quality, a performance
benchmark, proof of multimodal support for an unselected model, or proof of
GPU/service concurrency**. seekdb's existing per-request retries and service
queuing can change attempt counts; cancellation cannot retract inference
already accepted by the service.
