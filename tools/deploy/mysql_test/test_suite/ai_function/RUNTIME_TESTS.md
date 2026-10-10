# AI Function Runtime Regression

Legacy baseline validation (2026-09-29): the Debug build, all **105 SQL contracts** (including ten [global SOLO contracts](#global-solo-prompt-reordering-2026-09-29)), **five fixture self-tests**, the [native resource tests](#resource-reliability-tests), and the separate server-shutdown contract passed. The earlier [suspended-statement cache lifetime fix](#suspended-statement-cache-lifetime-2026-09-29) remains covered. Eligible single-statement AI pipeline SELECTs return their worker while waiting and resume from the retained result cursor. The newer semantic-operator validation is recorded below.

For the current architecture, code map, maintenance invariants and handoff checklist, see the [Chinese maintenance handoff](../../../../../docs/developer-guide/zh/ai-function-runtime-handoff.md).

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

Only absolute local paths / `file:///` under the configured root are supported.
Arbitrary HTTP URLs, path escapes, symlinks and nonregular files are not allowed.
Missing, corrupt, oversized, changed or metadata-inconsistent sources error.
JPEG/PNG/WAV/MP3 checks are bounded structural/container validation, not full
pixel or audio decoding; a provider can still reject structurally valid media.
External files do not gain database MVCC; deleting a SQL reference does not
delete media. No remote stage, signed URL, upload, implicit ASR or generic media
decoder is implied. FILE data requires a server supporting the native type;
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

Schema fetch supplies logged, safe defaults only for the three newly introduced
media variables when their catalog entries are absent; it does not silently
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

The separate [read-only catalog runner](test_file_catalog_roles.py) passed two
private contracts: legacy standby startup loads safe defaults without durable
catalog writes, and current standby restart preserves nondefault media policy,
model masks/history and native FILE values. Both reject ordinary writes.
It requires explicit new/old binaries and never connects to an existing service:

```bash
python tools/deploy/mysql_test/test_suite/ai_function/test_file_catalog_roles.py \
  --binary "$PWD/build_debug/bin/src/observer/seekdb" \
  --upgrade-binary /path/to/pre_FILE_seekdb
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
Semantic cost-based optimization, approximate retrieval/cascades, and dedicated
set-level semantic join/aggregation algorithms are not implemented here.

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

**No real image model is currently available in the parent-reported setup.**
Qwen3.5-9B has only incomplete temporary shards; those are not a usable model.
The Qwen3-4B text endpoint above must not be presented as multimodal. The
following example is only for a future, separately available compatible
vision service, not the current text deployment.

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
