# Declared AI model input modalities

`DBMS_AI_SERVICE.CREATE_AI_MODEL(name, JSON)` accepts an optional
`input_modalities` array:

```sql
CALL DBMS_AI_SERVICE.CREATE_AI_MODEL(
  'vision_model',
  '{"model_name":"provider-model","type":"COMPLETION","input_modalities":["text","image"]}'
);
```

The array must contain one to three distinct, lowercase strings chosen from
`text`, `image`, and `audio`. It must include `text`, because every model must
accept textual instructions. Only `COMPLETION` models can declare image or audio
inputs; dense/sparse embedding and rerank models remain text-only.

Omitting the array preserves the existing text-only behavior. An empty array,
non-array value, non-string element, duplicate, unknown spelling, missing
`text`, or media declaration on a non-completion model returns the existing AI
parameter-value error. Capabilities are declarations, not provider probes.

## Metadata and persistence

The layer-neutral `share::AiInputModality` constants are `TEXT=1`, `IMAGE=2`,
`AUDIO=4`, and `ALL=7`. Both `ObAiServiceModelInfo` and `ObAiModelSchema` expose
`get_input_modalities()` and `set_input_modalities(int64_t)`. Reset, omitted
JSON, and older serialized metadata default to `TEXT`.

The v1 serializers append the mask after all existing fields. Older readers
can ignore the tail, and newer readers initialize a missing tail to `TEXT`,
including when reusing a destination previously containing a multimodal model.
A present invalid mask is an error, not a text-only fallback.

The source of truth is
[ob_inner_table_schema_def.py](../../../src/share/inner_table/ob_inner_table_schema_def.py).
The generated `__all_ai_model` and `__all_ai_model_history` tables append an
integer `input_modalities` column with original/current default `1`; its column
IDs are `22` and `24`, respectively. The current table uses `NOT NULL`; the
history table follows the existing nullable-history-column generator policy.
Creation writes the declared mask to both tables, and deletion preserves it in
the history record.

The schema loader defaults only `OB_ERR_COLUMN_NOT_FOUND` to `TEXT` for a
legacy schema. NULL, unreadable values, unknown bits, missing text, and media
bits on embedding/rerank records fail explicitly.

Inspect the numeric capability directly:

```sql
SELECT model_id, name, model_name, input_modalities
FROM oceanbase.__all_ai_model;
```

`DBA_OB_AI_MODELS` retains its existing shape.

## Existing-database upgrade integration

Fresh bootstrap persists the generated table definitions. Restart, however,
loads system-table and column schemas from the persisted schema tables; changing
the generator alone does not upgrade an existing database.

`ObAiModelSqlService::prepare_input_modalities_upgrade(table_schema, arg,
needs_upgrade)` prepares the supported internal `ADD_COLUMN` request for either
model table. It reuses the generated column definition and stable column ID,
rejects an incompatible existing declaration, and returns `needs_upgrade=false`
when the column already matches. It does not run SQL or change server settings.

The three new media system variables need a separate bootstrap compatibility
step. During schema fetch, only absent `ai_media_root`, `ai_media_max_bytes` and
`ai_media_parallel` entries receive their logged generated defaults, with
loading disabled by the empty root. Other missing system variables remain
errors; present entries are never replaced by defaults. After system schemas
are published, writable-primary startup durably inserts only missing media
variables and history in one transaction and refreshes again. Running this SQL
before schema publication fails even for an otherwise current database because
its system tables are not yet resolvable.

The [local management startup](../../../src/rootserver/ob_local_management_service.cpp)
calls this helper for both table IDs after schema refresh, DDL initialization,
and maximum-ID cache initialization, but before marking local services ready.
For each table it obtains a fresh schema guard, prepares the request, and calls
`ddl_service.alter_table(arg, result)` only when `needs_upgrade` is true. It then
refreshes and verifies the resulting schema; every failure prevents readiness.
The caller initializes a UTC time-zone map required by internal column DDL.
The request changes columns only, preserving table options, and uses the
existing internal system-table-check bypass; do not enable the global
system-table DDL setting or execute a migration against a user's running
service. The two additive requests are individually idempotent, so restart can
finish an interrupted upgrade.

Standby/write-fenced startup does not mutate these catalogs. Both normal
promotion and retry of an already committed promotion check catalog readiness
before enabling normal writes. Missing durable media variables or missing
model columns return `OB_SCHEMA_EAGAIN`, leaving writes fenced; this check must
not temporarily enable writes to run a migration.

The native helper tests verify argument construction, not the startup caller's
real persisted-catalog behavior. The separate old-binary to new-binary private
restart gate passed, including stable columns, old text-only models, masks
3/5/7 for new models, preserved media policy/history and second-restart
idempotence. Fully replayed standby activation still requires its own
end-to-end environment; a timeout before readiness checking is not a passing
promotion-fence test.

The two private
[read-only catalog contracts](../../../tools/deploy/mysql_test/test_suite/ai_function/test_file_catalog_roles.py)
also passed: legacy standby startup leaves the missing media/model catalogs
unmodified, while current standby restart preserves media policy, declared
model masks/history and native FILE values. Ordinary writes remain rejected.

## Native validation

[test_ai_model_modalities.cpp](../../../unittest/share/schema/test_ai_model_modalities.cpp)
covers JSON parsing, model-type restrictions, reset/assignment/manager copies,
append-only serialization in both directions, reused legacy destinations,
corrupt stored masks, generated schema defaults/column IDs, idempotent upgrade
arguments, creation/deletion SQL writes, and explicit system-variable defaults
without policy replacement. Thirteen groups repeated three times passed.

Run the standalone
[test_ai_model_modalities.py](../../../tools/deploy/mysql_test/test_suite/ai_function/test_ai_model_modalities.py)
runner with cached Debug link metadata:

```bash
python tools/deploy/mysql_test/test_suite/ai_function/test_ai_model_modalities.py \
  --build-dir build_debug \
  --link-manifest /path/to/cached-seekdb-debug-link-action.json \
  --repeat 3
```

The runner compiles fresh model metadata and generated model-table sources into
a temporary directory and reuses other cached Debug objects. A temporary copy
of the generated-schema archive retains unrelated factories; only the freshly
compiled factory symbols are weakened in that copy so the explicit fresh
definitions take precedence. The original build artifacts are not changed. It does not
rebuild observer, start a database, contact a model, or mutate an existing
service. Temporary generated headers are outside the cached Bazel module map,
so only their standalone compilation omits strict module declaration checking;
normal production builds retain it.
