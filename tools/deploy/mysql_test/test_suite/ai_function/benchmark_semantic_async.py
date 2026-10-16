"""Bounded TEXT-only semantic async benchmark against existing loopback fixtures.

Use --list-cases or --self-test without starting any services. A benchmark needs
--binary and owns exactly one private Unix-socket database for the whole command.
For example: --binary /path/to/seekdb --rows 35 --repeats 3 --output new.json.
JSON publication is atomic and never overwrites an existing path.
--run-label records caller-supplied provenance, not a validation guarantee.

All modes use the same SQL, data, batch size, slots, serial SQL DOP and per-call
configured delays. Native TEXT HTTP parallelism is the batch size per task, not
an independently configurable concurrency knob. The harness records observed
provider concurrency rather than claiming that every request group fills it.
Delays are existing Reply response gates, not a new HTTP provider.

SSCursor fetches, not provider completion or cursor.execute(), measure visible
first-row/first-K-row time. Startup, DDL, EXPLAIN, fixture release and cleanup are
outside the SQL timing window; parsing/execution and client decoding are inside.
No warmup is discarded. Mode order rotates deterministically across repeats.

Unordered ORDER BY combinations are intentionally unsupported and never timed.
FILTER/MAP/LIMIT without ORDER BY can choose different valid rows in different
modes: validate an allowed subset and report descriptive latencies, not an
exact-answer speedup. Exact RANK sorting scores every candidate and uses the
native number schema and SQL DOUBLE, never a string cast or approximate Top-N.

These are configured loopback scheduling observations, not real-model results.
Request counts are HTTP attempts at the mocks. A response timestamp precedes
writing its body, so it is not client receipt or successful model completion.
Outstanding responses at SQL EOF are measurable; per-request cancellation,
runtime byte/credit accounting and token/cost counters are not and remain null
or absent. Releasing outstanding gates for cleanup is not counted as cancellation.
"""

import argparse
from collections import Counter
from contextlib import ExitStack, contextmanager, redirect_stderr, redirect_stdout
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import io
import json
import math
import os
from pathlib import Path
import statistics
import sys
import tempfile
import threading
import time
import traceback
import unittest
from unittest import mock
import uuid

import pymysql
from pymysql.constants import FIELD_TYPE

import test_semantic_cache as cache
import test_semantic_rank as rank
from test_file_runtime import PrivateDatabase
from test_runtime_contracts import Reply, completion, model_socket_fds


SCHEMA = "seekdb.semantic_async.loopback_benchmark"
SCHEMA_VERSION = 1
MAX_ROWS = 128
MAX_REPEATS = 10
MAX_BATCH_ROWS = 32
MAX_SLOTS = 4
MODES = ("default", "ordered", "unordered", "auto")
PROVIDERS = ("loopback-a", "loopback-b")
FUNCTIONS = ("map", "filter", "rank")
ERRORS = (AssertionError, OSError, ValueError, RuntimeError, pymysql.MySQLError)
LIMITATIONS = [
    "Configured TEXT loopback fixtures only; no real-model performance, quality, cost or throughput claim.",
    "SSCursor timestamps are taken after each actual fetch, including SQL work, protocol buffering, "
    "client scheduling and decoding; they are not server-internal row-ready timestamps.",
    "Total SQL elapsed ends at EOF or a reported SQL error, before validation, gate release and cleanup.",
    "Startup, DDL, EXPLAIN and cleanup are excluded; no warmup is discarded, and plan/OS caches "
    "may differ across repeats. Small-sample medians are observations, not guarantees.",
    "A gate waits the configured duration after the existing handler enters gate.wait(); "
    "thread scheduling, HTTP parsing and socket writes add jitter.",
    "Native TEXT max_parallel is the batch size per task. Slots, batch size and PARALLEL(1) "
    "are fixed across modes; there is no independent TEXT HTTP concurrency setting here.",
    "HTTP request counts include attempts, including any late arrivals after SQL EOF. "
    "A response-start timestamp is recorded before its body write, not after delivery.",
    "Outstanding-at-EOF means a received request with no response-start timestamp by SQL EOF. "
    "It does not prove that the SQL client still needs the call or that cancellation failed.",
    "The mocks do not reliably expose individual cancelled requests; cancelled_requests is null. "
    "Post-query database socket counts are separate observations, not cancellation counts.",
    "No runtime byte, credit, token, billing or tenant-wide capacity counters are inferred.",
    "FILTER/MAP/LIMIT has no ORDER BY and validates an allowed subset; its latencies must not "
    "be described as exact-answer speedups even when two observed subsets happen to agree.",
    "Strict unordered plus ORDER BY is intentionally unsupported, is not executed, and "
    "contributes no latency or speedup sample. Exact RANK sort/Top-N still score all candidates.",
]


class BenchmarkError(RuntimeError):
    pass


@dataclass(frozen=True)
class Config:
    rows: int = 35
    repeats: int = 3
    batch_rows: int = 16
    slots: int = 2
    limit: int = 5
    offset: int = 1
    first_k: int = 3
    fast_delay_ms: float = 20.0
    slow_delay_ms: float = 200.0
    query_timeout_s: float = 20.0

    def validate(self):
        for name, low, high in (
            ("rows", 4, MAX_ROWS), ("repeats", 1, MAX_REPEATS),
            ("batch_rows", 2, MAX_BATCH_ROWS), ("slots", 1, MAX_SLOTS),
            ("limit", 1, MAX_ROWS), ("offset", 0, MAX_ROWS),
            ("first_k", 1, MAX_ROWS),
        ):
            value = getattr(self, name)
            if type(value) is not int or not low <= value <= high:
                raise ValueError(f"{name} must be an integer in [{low}, {high}]")
        for name, low, high in (
            ("fast_delay_ms", 0, 2000), ("slow_delay_ms", 0, 2000),
            ("query_timeout_s", 1, 60),
        ):
            value = getattr(self, name)
            if not math.isfinite(value) or not low <= value <= high:
                raise ValueError(f"{name} must be finite and in [{low}, {high}]")
        if self.slow_delay_ms < self.fast_delay_ms:
            raise ValueError("slow_delay_ms must be at least fast_delay_ms")
        if self.slow_delay_ms >= self.query_timeout_s * 1000:
            raise ValueError("query_timeout_s must exceed one configured slow delay")
        if self.offset + self.limit > self.rows:
            raise ValueError("offset + limit must not exceed rows")
        if self.limit > len([index for index in range(self.rows) if accepted(index)]):
            raise ValueError("limit must not exceed the number of FILTER matches (id % 3 = 0)")


@dataclass(frozen=True)
class Case:
    name: str
    description: str
    ordered_sql: bool
    comparison: str
    functions: tuple[str, ...]
    delay_pattern: str


CASES = (
    Case("map_all_rows", "Project AI_MAP for every row without SQL ORDER BY.", False,
         "same_exact_bag", ("map",), "MAP: each input batch head is slow; other rows are fast."),
    Case("map_after_relational_limit", "ORDER BY id, relational LIMIT/OFFSET, then projected MAP.",
         True, "same_exact_ordered_rows", ("map",),
         "MAP: the last selected row is slow; earlier selected rows are fast."),
    Case("filter_map_limit", "Selective FILTER, projected MAP and LIMIT without ORDER BY.", False,
         "allowed_subset_descriptive_only_no_exact_answer_speedup", ("filter", "map"),
         "FILTER and MAP: id=0 is slow; other demanded rows are fast. FILTER accepts id % 3 = 0."),
    Case("rank_exact_sort", "Native numeric RANK over all candidates, then exact SQL sort.", True,
         "same_exact_ordered_rows", ("rank",),
         "RANK: each input batch head is slow; other rows are fast."),
    Case("rank_exact_topn", "Native numeric RANK over all candidates, then exact SQL Top-N.", True,
         "same_exact_ordered_rows", ("rank",),
         "RANK: each input batch head is slow; other rows are fast."),
)
CASE_BY_NAME = {case.name: case for case in CASES}


def prompt(index):
    return f"p3-text-{index:04d}"


def mapped(index):
    return "mapped:" + prompt(index)


def accepted(index):
    return index % 3 == 0


def score(index):
    return float((index * 7) % 19 - 9) + (index % 3) * 0.25


def unsupported_reason(case, mode):
    if mode == "unordered" and case.ordered_sql:
        return "Strict unordered output intentionally does not support ORDER BY (SQL contract 1235)."
    return None


def expected_rows(case, config):
    if case.name == "map_after_relational_limit":
        indices = range(config.offset, config.offset + config.limit)
        return tuple((index, mapped(index)) for index in indices)
    if case.name.startswith("rank_"):
        rows = sorted(((index, score(index)) for index in range(config.rows)),
                      key=lambda row: (-row[1], row[0]))
        return tuple(rows[:config.limit] if case.name == "rank_exact_topn" else rows)
    indices = (index for index in range(config.rows)
               if case.name != "filter_map_limit" or accepted(index))
    return tuple((index, mapped(index)) for index in indices)


def validate_rows(case, config, rows, description):
    expected = expected_rows(case, config)
    if case.name.startswith("rank_"):
        if not description or len(description) != 2 or description[1][1] != FIELD_TYPE.DOUBLE:
            raise BenchmarkError("AI_RANK must return native SQL DOUBLE metadata")
        if any(type(row[1]) is not float or not math.isfinite(row[1]) for row in rows):
            raise BenchmarkError("AI_RANK returned a non-finite or non-DOUBLE score")
    if case.name == "filter_map_limit":
        allowed = Counter(expected)
        observed = Counter(rows)
        if len(rows) != config.limit or observed - allowed:
            raise BenchmarkError("FILTER/MAP/LIMIT returned an invalid allowed bag subset")
    elif Counter(rows) != Counter(expected):
        raise BenchmarkError("result bag values or multiplicities differ from the exact oracle")
    if case.ordered_sql and rows != expected:
        raise BenchmarkError("SQL ORDER BY result order differs from the exact oracle")
    digest = hashlib.sha256(cache.json_text(sorted(rows)).encode("utf-8")).hexdigest()
    return {
        "status": "passed", "comparison": case.comparison, "result_bag_sha256": digest,
        "selected_row_ids": [row[0] for row in rows],
        "exact_answer_comparison_eligible": case.name != "filter_map_limit",
    }


def statement(case, mode, config, models):
    hints = "PARALLEL(1) " + (
        "OPT_PARAM('enable_semantic_row_async','false')" if mode == "default" else
        "OPT_PARAM('enable_semantic_row_async','true') "
        f"OPT_PARAM('semantic_output_order','{mode}')")
    fields = f"a.id,AI_MAP('{models[0]}',a.prompt) AS mapped"
    suffix = ""
    where = None
    if case.name == "map_after_relational_limit":
        suffix = f"ORDER BY a.id LIMIT {config.limit} OFFSET {config.offset}"
    elif case.name == "filter_map_limit":
        where = f"AI_FILTER('{models[1]}',a.prompt)"
        suffix = f"LIMIT {config.limit}"
    elif case.name.startswith("rank_"):
        fields = f"a.id,AI_RANK('{models[0]}',a.prompt) AS score"
        suffix = "ORDER BY score DESC,a.id"
        if case.name == "rank_exact_topn":
            suffix += f" LIMIT {config.limit}"
    return cache.sql_statement(fields, where=where, suffix=suffix, hints=hints,
                               cache="false", enabled_lift="false", batch_rows=config.batch_rows)


class DelayGate(threading.Event):
    """An existing Reply gate whose single handler wait supplies a bounded delay."""

    def __init__(self, delay_ms):
        super().__init__()
        if not math.isfinite(delay_ms) or not 0 <= delay_ms <= 2000:
            raise ValueError("delay_ms must be finite and in [0, 2000]")
        self.delay_s = delay_ms / 1000

    def wait(self, timeout=None):
        if self.is_set():
            return True
        interval = self.delay_s if timeout is None else min(self.delay_s, timeout)
        if super().wait(interval):
            return True
        if timeout is not None and timeout < self.delay_s:
            return False
        self.set()
        return True


@dataclass(frozen=True)
class RequestSpec:
    provider: int
    function: str
    row_id: int
    delay_ms: float

    @property
    def key(self):
        options = {"response_format": rank.RANK_FORMAT} if self.function == "rank" else None
        kind = "filter" if self.function == "filter" else "map"
        return cache.expected_key(self.provider, prompt(self.row_id), kind=kind, options=options)


def request_specs(case, config):
    result = {}
    for function in case.functions:
        provider = int(function == "filter")
        for index in range(config.rows):
            if case.name == "map_after_relational_limit" and not (
                    config.offset <= index < config.offset + config.limit):
                continue
            if case.name == "filter_map_limit" and function == "map" and not accepted(index):
                continue
            if case.name == "map_after_relational_limit":
                slow = index == config.offset + config.limit - 1
            elif case.name == "filter_map_limit":
                slow = index == 0
            else:
                slow = index % config.batch_rows == 0
            spec = RequestSpec(provider, function, index,
                               config.slow_delay_ms if slow else config.fast_delay_ms)
            result[spec.key] = spec
    return result


def configure_replies(fixtures, specs):
    for server in fixtures.mocks:
        with server.condition:
            if server.active or server.fixture_errors:
                raise BenchmarkError("cannot reset an active or failed provider fixture")
            server.reset()
            server.audit.clear()
            server.scenarios = cache.RequestScenarios(server)
    fixtures.gates.clear()
    for spec in specs.values():
        gate = DelayGate(spec.delay_ms)
        fixtures.gates.append(gate)
        if spec.function == "rank":
            key = fixtures.replies(spec.provider, prompt(spec.row_id), [
                Reply(body=completion(cache.json_text({"value": score(spec.row_id)}),
                                      finish_reason="stop"), gate=gate)
            ], options={"response_format": rank.RANK_FORMAT})
        else:
            value = accepted(spec.row_id) if spec.function == "filter" else mapped(spec.row_id)
            key = fixtures.reply(spec.provider, prompt(spec.row_id), value,
                                 kind=spec.function, gate=gate)
        if key != spec.key:
            raise BenchmarkError("configured reply lost its full request identity")


class RowTiming:
    def __init__(self, first_k, clock=time.monotonic):
        self.clock = clock
        self.first_k = first_k
        self.started = clock()
        self.metadata_return_s = None
        self.row_times = []
        self.ended = None

    def metadata(self):
        self.metadata_return_s = self.clock() - self.started

    def row(self):
        self.row_times.append(self.clock() - self.started)

    def finish(self):
        self.ended = self.clock()

    def document(self, complete):
        if self.ended is None:
            raise BenchmarkError("SQL timing was not finished")
        values = [*self.row_times, self.ended - self.started]
        if any(value < 0 for value in values) or values != sorted(values):
            raise BenchmarkError("SQL timing events are not monotonic")
        return {
            "complete_result": complete, "first_k": self.first_k,
            "first_row_s": self.row_times[0] if self.row_times else None,
            "first_k_rows_s": self.row_times[self.first_k - 1]
            if len(self.row_times) >= self.first_k else None,
            "total_elapsed_s": self.ended - self.started,
            "execute_return_s_not_first_row": self.metadata_return_s,
            "row_fetch_elapsed_s": self.row_times, "returned_rows": len(self.row_times),
        }


@dataclass
class Capture:
    rows: tuple
    description: object
    timing: RowTiming
    sql_error: pymysql.MySQLError | None


def capture_sql(connection, sql, first_k, fixtures, *, clock=time.monotonic):
    cursor = connection.cursor(pymysql.cursors.SSCursor)
    timing = RowTiming(first_k, clock)
    rows = []
    description = None
    error = None
    try:
        try:
            cursor.execute(sql)
            timing.metadata()
            description = cursor.description
            while (row := cursor.fetchone()) is not None:
                rows.append(tuple(row))
                timing.row()
        except pymysql.MySQLError as caught:
            error = caught
        finally:
            timing.finish()
    finally:
        fixtures.release()
        cursor.close()
    return Capture(tuple(rows), description, timing, error)


def request_metrics(fixtures, specs, started, ended):
    providers = []
    events = []
    observed = Counter()
    for provider, server in enumerate(fixtures.mocks):
        with server.condition:
            if server.fixture_errors:
                raise BenchmarkError(f"provider fixture failed: {server.fixture_errors!r}")
            requests = tuple(server.requests)
            audit = tuple(dict(record) for record in server.audit)
            peak = server.peak
            observed_at = time.monotonic()
        if len(requests) != len(audit):
            raise BenchmarkError("provider request bodies and audit events disagree")
        functions = {kind: {"http_requests": 0, "received_by_sql_eof": 0,
                            "responses_started_by_sql_eof": 0,
                            "responses_outstanding_at_sql_eof": 0} for kind in FUNCTIONS}
        for body, record in zip(requests, audit):
            key = cache.request_key(body)
            if key not in specs or specs[key].provider != provider:
                raise BenchmarkError(f"unexpected full provider request identity: {key!r}")
            spec = specs[key]
            observed[(spec.function, spec.row_id)] += 1
            if observed[(spec.function, spec.row_id)] != 1:
                raise BenchmarkError("unexpected duplicate HTTP attempt for a deterministic successful call")
            received = record["received"]
            response = record["response"]
            if received < started or (response is not None and response < received):
                raise BenchmarkError("provider audit event is outside its trial or precedes receipt")
            counts = functions[spec.function]
            counts["http_requests"] += 1
            if received <= ended:
                counts["received_by_sql_eof"] += 1
                if response is not None and response <= ended:
                    counts["responses_started_by_sql_eof"] += 1
                else:
                    counts["responses_outstanding_at_sql_eof"] += 1
            events.append({
                "provider": PROVIDERS[provider], "function": spec.function, "row_id": spec.row_id,
                "configured_delay_ms": spec.delay_ms, "received_s": received - started,
                "response_started_s": None if response is None else response - started,
            })
        providers.append({
            "provider": PROVIDERS[provider], "request_model": cache.REQUEST_MODELS[provider],
            "http_requests": len(requests), "peak_active_http_handlers": peak,
            "observed_after_sql_s": observed_at - ended,
            "received_after_sql_eof": sum(record["received"] > ended for record in audit),
            "functions": functions, "cancelled_requests": None,
        })
    return {"providers": providers, "request_events": sorted(
        events, key=lambda event: (event["received_s"], event["provider"], event["row_id"]))}, observed


def validate_requests(case, config, rows, observed):
    if case.name == "filter_map_limit":
        expected = {("filter", index) for index in range(config.rows)}
        expected.update(("map", index) for index in range(config.rows) if accepted(index))
        required = {(kind, row[0]) for row in rows for kind in ("filter", "map")}
        if not required <= set(observed) or not set(observed) <= expected:
            raise BenchmarkError("FILTER/MAP/LIMIT request demand is not a valid accepted-row subset")
    else:
        function = case.functions[0]
        indices = (range(config.offset, config.offset + config.limit)
                   if case.name == "map_after_relational_limit" else range(config.rows))
        expected = Counter((function, index) for index in indices)
        if observed != expected:
            raise BenchmarkError("HTTP demand differs from the exact candidate/relational LIMIT boundary")


def settle(fixtures, database):
    sockets = []
    for provider, server in enumerate(fixtures.mocks):
        with server.condition:
            if not server.condition.wait_for(lambda: server.active == 0, timeout=3):
                raise BenchmarkError("gate release did not settle all provider handlers")
            if server.fixture_errors:
                raise BenchmarkError(f"provider fixture failed: {server.fixture_errors!r}")
        deadline = time.monotonic() + 3
        descriptors = model_socket_fds(database.process.pid, server.server_port)
        while descriptors and time.monotonic() < deadline:
            threading.Event().wait(0.01)
            descriptors = model_socket_fds(database.process.pid, server.server_port)
        sockets.append({"provider": PROVIDERS[provider], "db_socket_fds_after_release": len(descriptors)})
        if descriptors:
            raise BenchmarkError("completed query retained database-owned provider sockets")
    return sockets


@contextmanager
def sql_fixture(database, fixtures, config):
    token = uuid.uuid4().hex[:12]
    schema = "ai_semantic_async_bench_" + token
    models = tuple(f"semantic_async_bench_{token}_{suffix}" for suffix in ("a", "b"))
    with database.connection.cursor() as cursor, ExitStack() as cleanup:
        cursor.execute(f"CREATE DATABASE {schema}")
        cleanup.callback(cursor.execute, f"DROP DATABASE {schema}")
        cursor.execute(f"USE {schema}")
        cursor.execute(f"CREATE TABLE cache_inputs ({cache.TABLES['cache_inputs']})")
        cursor.execute("SET ob_query_timeout=%s,ai_pipeline_slots=%s,ai_pipeline_memory_limit=DEFAULT",
                       (int(config.query_timeout_s * 1000000), config.slots))
        for index, (model, server) in enumerate(zip(models, fixtures.mocks)):
            if server.server_address[0] != "127.0.0.1":
                raise BenchmarkError("provider must be IPv4 loopback")
            cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL(%s,%s)", (
                model, cache.json_text({"type": "completion", "model_name": cache.REQUEST_MODELS[index],
                                       "input_modalities": ["text"]})))
            cleanup.callback(cursor.execute, "CALL DBMS_AI_SERVICE.DROP_AI_MODEL(%s)", (model,))
            endpoint = model + "_endpoint"
            cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT(%s,%s)", (
                endpoint, cache.json_text({
                    "ai_model_name": model, "provider": "openai",
                    "request_model_name": cache.REQUEST_MODELS[index],
                    "access_key": "private-loopback-benchmark-fixture-only",
                    "url": f"http://127.0.0.1:{server.server_port}/v1/chat/completions",
                })))
            cleanup.callback(cursor.execute, "CALL DBMS_AI_SERVICE.DROP_AI_MODEL_ENDPOINT(%s)", (endpoint,))
        cursor.executemany("INSERT INTO cache_inputs VALUES (%s,%s,%s,%s,%s)", [
            (index, prompt(index), 1, models[0], "{}") for index in range(config.rows)])
        cursor.execute("SELECT @@SESSION.ai_pipeline_slots,@@SESSION.ai_pipeline_memory_limit,"
                       "@@SESSION.ob_query_timeout")
        slots, memory_limit, query_timeout = cursor.fetchone()
        if slots != config.slots or query_timeout != int(config.query_timeout_s * 1000000):
            raise BenchmarkError("database did not retain the requested fixed session settings")
        yield cursor, models, {
            "ai_pipeline_slots": slots, "ai_pipeline_memory_limit": memory_limit,
            "ob_query_timeout_us": query_timeout, "sql_dop": 1,
            "http_max_parallel_per_task": config.batch_rows,
            "per_function_http_window_bound": config.batch_rows * config.slots,
            "independent_text_http_concurrency_knob": False,
        }


def validate_plan(cursor, sql, case, mode, fixtures):
    before = tuple(len(server.requests) for server in fixtures.mocks)
    cursor.execute("EXPLAIN " + sql)
    plan = cache.lift.ExplainPlan(cache.lift.render_explain(cursor.fetchall()))
    if before != tuple(len(server.requests) for server in fixtures.mocks):
        raise BenchmarkError("EXPLAIN unexpectedly made provider requests")
    for function in case.functions:
        node = plan.one("SEMANTIC " + function.upper())
        annotation = node.annotation.upper()
        if mode == "default":
            if "ROW_ASYNC=" in annotation:
                raise BenchmarkError("batch/default unexpectedly enabled row async")
        else:
            effective = ("ORDERED_PREFIX" if mode == "ordered" or
                         (mode == "auto" and case.name == "map_after_relational_limit")
                         else "UNORDERED_READY")
            if "ROW_ASYNC=" + effective not in annotation:
                raise BenchmarkError(f"EXPLAIN did not apply {mode}/{effective} to {function}")
            if mode == "auto" and "OUTPUT_POLICY=AUTO" not in annotation:
                raise BenchmarkError("EXPLAIN does not identify the explicit AUTO policy")
    if case.name.startswith("rank_"):
        rank_node = plan.one("SEMANTIC RANK")
        if not any("SORT" in node.operator and plan.is_above(node, rank_node)
                   for node in plan.nodes.values()):
            raise BenchmarkError("exact numeric RANK sort must consume all scored candidates")
    return plan.text


def error_document(error):
    details = {"type": type(error).__name__, "message": str(error)}
    if isinstance(error, pymysql.MySQLError) and error.args:
        details["sql_code"] = error.args[0]
    contexts = []
    current = error.__cause__ or error.__context__
    while current is not None and len(contexts) < 8:
        contexts.append({"type": type(current).__name__, "message": str(current)})
        current = current.__cause__ or current.__context__
    if contexts:
        details["exception_chain"] = contexts
    return details


def measure(group, case, config, fixtures, database, repeat):
    sample = {"repeat": repeat + 1, "status": "running"}
    group["samples"].append(sample)
    specs = request_specs(case, config)
    try:
        configure_replies(fixtures, specs)
        capture = capture_sql(database.connection, group["sql"], config.first_k, fixtures)
        sample["timing"] = capture.timing.document(capture.sql_error is None)
        sample["rows"] = capture.rows
        sample["cleanup_observation"] = settle(fixtures, database)
        sample["provider_observation_after_sql_s"] = time.monotonic() - capture.timing.ended
        sample["requests"], observed = request_metrics(
            fixtures, specs, capture.timing.started, capture.timing.ended)
        if capture.sql_error is not None:
            raise capture.sql_error
        sample["validation"] = validate_rows(case, config, capture.rows, capture.description)
        validate_requests(case, config, capture.rows, observed)
        sample["validation"]["request_demand"] = "passed"
        sample["status"] = "passed"
    except ERRORS as error:
        sample["status"] = "failed"
        sample["error"] = error_document(error)
        raise


def groups_for(cases, modes):
    groups = []
    for case in cases:
        for mode in modes:
            reason = unsupported_reason(case, mode)
            group = {
                "case": case.name, "mode": mode, "comparison": case.comparison,
                "status": "intentionally_unsupported" if reason else "pending",
                "samples": [], "summary": None,
            }
            if reason:
                group.update(reason=reason, execution="not_attempted_by_design",
                             latency_sample_eligible=False)
            groups.append(group)
    return groups


def summarize(report, config):
    completed = failed = unsupported = 0
    for group in report["groups"]:
        if group["status"] == "intentionally_unsupported":
            unsupported += 1
            continue
        passed = [sample for sample in group["samples"] if sample["status"] == "passed"]
        failed += sum(sample["status"] == "failed" for sample in group["samples"])
        completed += len(passed)
        if passed:
            summary = {"passed_samples": len(passed), "returned_rows": [
                sample["timing"]["returned_rows"] for sample in passed]}
            for field in ("first_row_s", "first_k_rows_s", "total_elapsed_s"):
                values = [sample["timing"][field] for sample in passed
                          if sample["timing"][field] is not None]
                summary[field] = ({"samples": len(values), "min": min(values),
                                   "median": statistics.median(values), "max": max(values)}
                                  if values else None)
            group["summary"] = summary
        group["status"] = ("failed" if any(sample["status"] == "failed" for sample in group["samples"])
                           else "passed" if len(passed) == config.repeats else "not_completed")
    expected = (len(report["groups"]) - unsupported) * config.repeats
    report["counts"] = {
        "case_mode_combinations": len(report["groups"]),
        "intentionally_unsupported_combinations": unsupported,
        "expected_sql_trials": expected, "passed_sql_trials": completed,
        "failed_sql_trials": failed, "not_run_sql_trials": expected - completed - failed,
        "returned_rows_across_passed_trials": sum(
            sample["timing"]["returned_rows"] for group in report["groups"]
            for sample in group["samples"] if sample["status"] == "passed"),
    }


def base_document(kind, config, cases, modes):
    data = [(index, prompt(index), mapped(index), accepted(index), score(index))
            for index in range(config.rows)]
    return {
        "schema": SCHEMA, "schema_version": SCHEMA_VERSION, "kind": kind,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "fixture_only_not_real_model_performance": True, "configuration": asdict(config),
        "case_order": [case.name for case in cases], "requested_mode_order": list(modes),
        "repeat_mode_order": [list(modes[index % len(modes):] + modes[:index % len(modes)])
                              for index in range(config.repeats)],
        "cases": [asdict(case) for case in cases],
        "dataset_sha256": hashlib.sha256(cache.json_text(data).encode("utf-8")).hexdigest(),
        "measurement_limitations": LIMITATIONS,
    }


def close_fixtures(fixtures, report):
    fixtures.close()
    report["cleanup"]["providers_closed"] = True


def close_database(database, log_dir, report):
    try:
        try:
            database.stop()
        finally:
            if log_dir is not None:
                with redirect_stdout(sys.stderr):
                    copied = cache.retain_deployment_logs(database, log_dir)
                report["retained_logs"] = [str(path) for path in copied]
    finally:
        database.close()
        report["cleanup"]["private_database_closed"] = True


def run_benchmark(options):
    config = options.config
    cases = [CASE_BY_NAME[name] for name in options.case]
    modes = tuple(options.mode)
    report = base_document("benchmark", config, cases, modes)
    report.update(status="running", run_label=options.run_label, groups=groups_for(cases, modes), errors=[],
                  cleanup={"private_database_closed": False, "providers_closed": False})
    binary = options.binary
    with binary.open("rb") as source:
        digest = hashlib.file_digest(source, "sha256").hexdigest()
    report["runtime"] = {
        "python": sys.version, "python_executable": sys.executable,
        "bytecode_disabled": sys.dont_write_bytecode,
        "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "binary": str(binary), "binary_sha256": digest,
        "binary_size": binary.stat().st_size, "binary_mtime_ns": binary.stat().st_mtime_ns,
        "provider_credentials": "hard-coded loopback fixture credentials only",
    }
    try:
        with ExitStack() as cleanup:
            database = PrivateDatabase(binary)
            cleanup.callback(close_database, database, options.log_dir, report)
            database.start()
            with Path(f"/proc/{database.process.pid}/exe").open("rb") as executable:
                running_digest = hashlib.file_digest(executable, "sha256").hexdigest()
            report["runtime"]["running_binary_sha256"] = running_digest
            if running_digest != digest:
                raise BenchmarkError("running executable differs from the binary hashed before startup")
            fixtures = cache.CacheFixtures()
            cleanup.callback(close_fixtures, fixtures, report)
            with sql_fixture(database, fixtures, config) as (cursor, models, settings):
                report["runtime"]["effective_settings"] = settings
                for group in report["groups"]:
                    case = CASE_BY_NAME[group["case"]]
                    group["sql"] = statement(case, group["mode"], config, models)
                    if group["status"] != "intentionally_unsupported":
                        group["explain"] = validate_plan(cursor, group["sql"], case, group["mode"], fixtures)
                lookup = {(group["case"], group["mode"]): group for group in report["groups"]}
                for repeat in range(config.repeats):
                    order = modes[repeat % len(modes):] + modes[:repeat % len(modes)]
                    for case in cases:
                        for mode in order:
                            group = lookup[(case.name, mode)]
                            if group["status"] != "intentionally_unsupported":
                                measure(group, case, config, fixtures, database, repeat)
    except ERRORS as error:
        report["errors"].append(error_document(error))
        traceback.print_exception(error, file=sys.stderr)
    summarize(report, config)
    report["status"] = ("passed" if not report["errors"] and
                        report["counts"]["passed_sql_trials"] == report["counts"]["expected_sql_trials"]
                        and all(report["cleanup"].values()) else "failed")
    return report


def publish_json(document, output):
    text = json.dumps(document, indent=2, sort_keys=True, allow_nan=False) + "\n"
    if output is None:
        sys.stdout.write(text)
        return
    temporary_file = tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", prefix=f".{output.name}.", suffix=".tmp",
        dir=output.parent, delete=False)
    temporary = Path(temporary_file.name)
    try:
        with temporary_file as stream:
            if stream.write(text) != len(text):
                raise OSError("short JSON output write")
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, output)
        temporary.unlink()
        directory_fd = os.open(output.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)


def parse_options(arguments=None):
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--binary", type=Path, help="Executable for one owned private database")
    source.add_argument("--self-test", action="store_true", help="Service-free fixture/measurement tests")
    source.add_argument("--list-cases", action="store_true", help="Service-free versioned JSON inventory")
    parser.add_argument("--case", action="append", choices=tuple(CASE_BY_NAME), default=[])
    parser.add_argument("--mode", action="append", choices=MODES, default=[])
    parser.add_argument("--output", type=Path, help="Atomic JSON destination that must not exist")
    parser.add_argument("--log-dir", type=Path, help="New directory for owned deployment logs")
    parser.add_argument("--run-label", default="unlabelled",
                        help="Caller provenance label (for example baseline_old_binary), not validation proof")
    for name in ("rows", "repeats", "batch_rows", "slots", "limit", "offset", "first_k"):
        parser.add_argument("--" + name.replace("_", "-"), type=int, default=getattr(Config(), name))
    for name in ("fast_delay_ms", "slow_delay_ms", "query_timeout_s"):
        parser.add_argument("--" + name.replace("_", "-"), type=float, default=getattr(Config(), name))
    options = parser.parse_args(arguments)
    try:
        options.config = Config(**{name: getattr(options, name) for name in Config.__dataclass_fields__})
        options.config.validate()
    except ValueError as error:
        parser.error(str(error))
    if options.binary is None and not (options.self_test or options.list_cases):
        parser.error("--binary is required unless --list-cases or --self-test is selected")
    if options.log_dir is not None and options.binary is None:
        parser.error("--log-dir requires --binary")
    if options.self_test and (options.case or options.mode):
        parser.error("--self-test cannot select SQL --case or --mode")
    if not 1 <= len(options.run_label) <= 80 or any(
            not 32 <= ord(character) < 127 for character in options.run_label):
        parser.error("--run-label must contain 1-80 printable ASCII characters")
    options.case = list(dict.fromkeys(options.case)) or list(CASE_BY_NAME)
    options.mode = list(dict.fromkeys(options.mode)) or list(MODES)
    if options.binary is not None:
        options.binary = options.binary.expanduser().resolve()
        if not options.binary.is_file() or not os.access(options.binary, os.X_OK):
            parser.error("--binary must be an executable file")
    for name in ("output", "log_dir"):
        path = getattr(options, name)
        if path is not None:
            path = path.expanduser().absolute()
            setattr(options, name, path)
            if os.path.lexists(path):
                parser.error(f"--{name.replace('_', '-')} already exists; refusing to overwrite: {path}")
            if not path.parent.is_dir():
                parser.error(f"--{name.replace('_', '-')} parent directory must exist: {path.parent}")
    return options


def self_test_document():
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(BenchmarkSelfTests)
    result = unittest.TextTestRunner(stream=sys.stderr, verbosity=2).run(suite)
    return {
        "schema": SCHEMA, "schema_version": SCHEMA_VERSION, "kind": "fixture_self_test",
        "status": "passed" if result.wasSuccessful() else "failed", "service_free": True,
        "tests_run": result.testsRun, "failures": len(result.failures), "errors": len(result.errors),
    }


def main(arguments=None):
    options = parse_options(arguments)
    if options.self_test:
        document = self_test_document()
    elif options.list_cases:
        cases = [CASE_BY_NAME[name] for name in options.case]
        document = base_document("case_inventory", options.config, cases, tuple(options.mode))
        document.update(status="not_run", service_free=True,
                        groups=groups_for(cases, options.mode))
    else:
        document = run_benchmark(options)
    document["run_label"] = options.run_label
    try:
        publish_json(document, options.output)
    except OSError as error:
        print(f"JSON publication failed (no overwrite/fallback): {error}", file=sys.stderr)
        return 1
    return 0 if document["status"] in ("passed", "not_run") else 1


class BenchmarkSelfTests(unittest.TestCase):
    def test_sscursor_fetch_not_execute_supplies_visible_row_times(self):
        now = [5.0]

        class Cursor:
            description = (("id", FIELD_TYPE.LONG), ("mapped", FIELD_TYPE.VAR_STRING))

            def execute(self, _sql):
                now[0] = 5.01
                self.rows = iter(((5.07, (0, "a")), (5.11, (1, "b")), (5.2, None)))

            def fetchone(self):
                now[0], row = next(self.rows)
                return row

            def close(self):
                now[0] = 6.0

        connection = mock.Mock()
        connection.cursor.return_value = Cursor()
        fixtures = mock.Mock()
        captured = capture_sql(connection, "SELECT fixture", 2, fixtures, clock=lambda: now[0])
        connection.cursor.assert_called_once_with(pymysql.cursors.SSCursor)
        fixtures.release.assert_called_once_with()
        timing = captured.timing.document(True)
        self.assertAlmostEqual(timing["first_row_s"], 0.07)
        self.assertAlmostEqual(timing["first_k_rows_s"], 0.11)
        self.assertAlmostEqual(timing["total_elapsed_s"], 0.2)
        self.assertAlmostEqual(timing["execute_return_s_not_first_row"], 0.01)
        self.assertEqual(captured.rows, ((0, "a"), (1, "b")))

    def test_first_k_is_null_for_fewer_rows_and_first_row_is_null_for_empty(self):
        for count in (0, 2):
            times = iter(range(10))
            timing = RowTiming(3, lambda: float(next(times)))
            for _ in range(count):
                timing.row()
            timing.finish()
            document = timing.document(True)
            self.assertIsNone(document["first_k_rows_s"])
            self.assertEqual(document["first_row_s"], 1.0 if count else None)

    def test_sql_error_keeps_partial_fetches_and_releases_gates(self):
        cursor = mock.Mock(description=(("id", FIELD_TYPE.LONG),))
        cursor.fetchone.side_effect = [(1,), pymysql.OperationalError(1317, "fixture interruption")]
        connection = mock.Mock()
        connection.cursor.return_value = cursor
        fixtures = mock.Mock()
        times = iter((1.0, 1.01, 1.04, 1.1))
        captured = capture_sql(connection, "SELECT fixture", 2, fixtures,
                               clock=lambda: next(times))
        self.assertEqual(captured.rows, ((1,),))
        self.assertEqual(captured.sql_error.args[0], 1317)
        self.assertFalse(captured.timing.document(False)["complete_result"])
        self.assertIsNone(captured.timing.document(False)["first_k_rows_s"])
        fixtures.release.assert_called_once_with()
        cursor.close.assert_called_once_with()

    def test_delay_gate_uses_bounded_existing_handler_wait(self):
        gate = DelayGate(25)
        with mock.patch.object(threading.Event, "wait", return_value=False) as wait:
            self.assertTrue(gate.wait(timeout=10))
            wait.assert_called_once_with(0.025)
        self.assertTrue(gate.is_set())
        limited = DelayGate(100)
        with mock.patch.object(threading.Event, "wait", return_value=False) as wait:
            self.assertFalse(limited.wait(timeout=0.01))
            wait.assert_called_once_with(0.01)
        limited.set()
        self.assertTrue(limited.wait(timeout=10))

    def test_invalid_cli_values_are_rejected_without_services(self):
        invalid = [
            [], ["--rows", "0"], ["--rows", str(MAX_ROWS + 1)], ["--rows", "1.5"],
            ["--repeats", "0"], ["--repeats", str(MAX_REPEATS + 1)],
            ["--batch-rows", "1"], ["--batch-rows", str(MAX_BATCH_ROWS + 1)],
            ["--slots", "0"], ["--slots", str(MAX_SLOTS + 1)], ["--first-k", "0"],
            ["--limit", "0"], ["--limit", "13"], ["--offset", "33"],
            ["--fast-delay-ms", "nan"], ["--slow-delay-ms", "inf"],
            ["--fast-delay-ms", "-1"], ["--slow-delay-ms", "2001"],
            ["--fast-delay-ms", "201"], ["--query-timeout-s", "0"],
            ["--query-timeout-s", "1", "--slow-delay-ms", "1000"],
            ["--case", "missing"], ["--mode", "missing"],
            ["--run-label", ""], ["--run-label", "x" * 81], ["--run-label", "bad\nlabel"],
        ]
        for arguments in invalid:
            command = arguments if not arguments else ["--list-cases", *arguments]
            with self.subTest(arguments=arguments), redirect_stderr(io.StringIO()), \
                    self.assertRaises(SystemExit) as caught:
                parse_options(command)
            self.assertEqual(caught.exception.code, 2)

    def test_unsupported_order_by_is_explicit_and_never_has_samples(self):
        groups = groups_for(CASES, MODES)
        unsupported = [group for group in groups if group["status"] == "intentionally_unsupported"]
        self.assertEqual({group["case"] for group in unsupported},
                         {"map_after_relational_limit", "rank_exact_sort", "rank_exact_topn"})
        self.assertTrue(all(group["mode"] == "unordered" and not group["samples"]
                            and group["summary"] is None and not group["latency_sample_eligible"]
                            for group in unsupported))

    def test_mode_sql_changes_only_hints_not_selected_semantics(self):
        config = Config()
        for case in CASES:
            bodies = []
            for mode in MODES:
                sql = statement(case, mode, config, ("mock_a", "mock_b"))
                bodies.append(sql.split("*/", 1)[1])
                self.assertIn("OPT_PARAM('enable_semantic_cache', 'false')", sql)
                self.assertIn("OPT_PARAM('rowsets_max_rows', 16)", sql)
                self.assertIn("PARALLEL(1)", sql)
                if mode == "default":
                    self.assertIn("'enable_semantic_row_async','false'", sql)
            self.assertEqual(len(set(bodies)), 1)

    def test_rank_uses_native_number_schema_and_exact_double_order(self):
        config = Config()
        case = CASE_BY_NAME["rank_exact_topn"]
        specs = request_specs(case, config)
        self.assertEqual(len(specs), config.rows)
        for spec in specs.values():
            options = json.loads(spec.key.options)
            self.assertEqual(options["response_format"], rank.RANK_FORMAT)
            self.assertEqual(options["response_format"]["json_schema"]["schema"]
                             ["properties"]["value"], {"type": "number"})
        sql = statement(case, "auto", config, ("mock_a", "mock_b"))
        self.assertIn("AI_RANK(", sql)
        self.assertNotIn("CAST(", sql)
        rows = expected_rows(case, config)
        description = (("id", FIELD_TYPE.LONG), ("score", FIELD_TYPE.DOUBLE))
        self.assertEqual(validate_rows(case, config, rows, description)["status"], "passed")
        with self.assertRaises(BenchmarkError):
            validate_rows(case, config, rows, (("id", FIELD_TYPE.LONG),
                                              ("score", FIELD_TYPE.VAR_STRING)))
        with self.assertRaises(BenchmarkError):
            validate_rows(case, config, rows[::-1], description)

    def test_exact_bags_and_ordered_limit_have_different_oracles(self):
        config = Config()
        case = CASE_BY_NAME["map_all_rows"]
        rows = expected_rows(case, config)
        self.assertEqual(validate_rows(case, config, rows[::-1], None)["status"], "passed")
        with self.assertRaises(BenchmarkError):
            validate_rows(case, config, rows[:-1] + rows[:1], None)
        case = CASE_BY_NAME["map_after_relational_limit"]
        rows = expected_rows(case, config)
        with self.assertRaises(BenchmarkError):
            validate_rows(case, config, rows[::-1], None)
        observed = Counter(("map", index) for index in range(config.offset, config.offset + config.limit))
        validate_requests(case, config, rows, observed)
        with self.assertRaises(BenchmarkError):
            validate_requests(case, config, rows, observed + Counter({("map", 0): 1}))

    def test_filter_limit_allows_different_valid_subsets_without_exact_speedup(self):
        config = Config()
        case = CASE_BY_NAME["filter_map_limit"]
        allowed = expected_rows(case, config)
        first = validate_rows(case, config, allowed[:config.limit], None)
        last = validate_rows(case, config, allowed[-config.limit:], None)
        self.assertNotEqual(first["result_bag_sha256"], last["result_bag_sha256"])
        self.assertFalse(first["exact_answer_comparison_eligible"])
        self.assertIn("no_exact_answer_speedup", first["comparison"])
        with self.assertRaises(BenchmarkError):
            validate_rows(case, config, (allowed[0],) * config.limit, None)
        with self.assertRaises(BenchmarkError):
            validate_rows(case, config, allowed[:config.limit - 1] + ((1, mapped(1)),), None)

    def test_request_metrics_measure_outstanding_not_invented_cancellation(self):
        spec = RequestSpec(0, "map", 0, 200)
        other = RequestSpec(1, "filter", 3, 20)
        servers = []
        for body, received, response in (
            (cache.request_body(0, prompt(0)), 10.1, 11.0),
            (cache.request_body(1, prompt(3), kind="filter"), 10.2, 10.3),
        ):
            server = mock.Mock()
            server.condition = threading.Condition()
            server.requests = [body]
            server.audit = [{"received": received, "response": response}]
            server.peak = 1
            server.fixture_errors = []
            servers.append(server)
        metrics, observed = request_metrics(mock.Mock(mocks=servers),
                                            {spec.key: spec, other.key: other}, 10.0, 10.5)
        self.assertEqual(observed, Counter({("map", 0): 1, ("filter", 3): 1}))
        first, second = metrics["providers"]
        self.assertEqual(first["functions"]["map"]["responses_outstanding_at_sql_eof"], 1)
        self.assertEqual(second["functions"]["filter"]["responses_started_by_sql_eof"], 1)
        self.assertTrue(all(provider["cancelled_requests"] is None for provider in (first, second)))
        self.assertNotIn("byte", json.dumps(metrics))
        self.assertNotIn("credit", json.dumps(metrics))
        servers[0].requests *= 2
        servers[0].audit *= 2
        with self.assertRaises(BenchmarkError):
            request_metrics(mock.Mock(mocks=servers), {spec.key: spec, other.key: other}, 10, 10.5)

    def test_atomic_publication_does_not_overwrite_or_leave_temporary_files(self):
        with tempfile.TemporaryDirectory(prefix="semantic-async-bench-selftest-") as directory:
            root = Path(directory)
            output = root / "result.json"
            document = {"schema": SCHEMA, "schema_version": SCHEMA_VERSION, "value": 1}
            publish_json(document, output)
            self.assertEqual(json.loads(output.read_text()), document)
            with self.assertRaises(FileExistsError):
                publish_json({"value": 2}, output)
            self.assertEqual(json.loads(output.read_text()), document)
            self.assertEqual(list(root.iterdir()), [output])
            with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                parse_options(["--list-cases", "--output", str(output)])

    def test_atomic_link_race_and_invalid_json_never_publish_partial_results(self):
        with tempfile.TemporaryDirectory(prefix="semantic-async-bench-selftest-") as directory:
            root = Path(directory)
            output = root / "result.json"
            original_link = os.link

            def racing_link(source, target):
                Path(target).write_text("existing artifact")
                return original_link(source, target)

            with mock.patch.object(os, "link", side_effect=racing_link), self.assertRaises(FileExistsError):
                publish_json({"value": 1}, output)
            self.assertEqual(output.read_text(), "existing artifact")
            with self.assertRaises(ValueError):
                publish_json({"value": math.nan}, root / "invalid.json")
            self.assertEqual(list(root.iterdir()), [output])

    def test_list_cases_is_service_free_versioned_json(self):
        output = io.StringIO()
        with mock.patch.object(sys.modules[__name__], "PrivateDatabase",
                               side_effect=AssertionError("must not start DB")), \
                mock.patch.object(cache, "CacheFixtures",
                                  side_effect=AssertionError("must not start HTTP")), \
                redirect_stdout(output):
            self.assertEqual(main(["--list-cases", "--run-label", "baseline_old_binary"]), 0)
        document = json.loads(output.getvalue())
        self.assertEqual(document["schema_version"], SCHEMA_VERSION)
        self.assertEqual(document["run_label"], "baseline_old_binary")
        self.assertTrue(document["service_free"])
        self.assertEqual(len(document["groups"]), 20)
        self.assertEqual(sum(group["status"] == "intentionally_unsupported"
                             for group in document["groups"]), 3)

    def test_fixture_and_database_cleanup_are_attempted_after_failure(self):
        report = {"cleanup": {"private_database_closed": False, "providers_closed": False}}
        database = mock.Mock()
        fixtures = mock.Mock()
        fixtures.close.side_effect = BenchmarkError("fixture cleanup diagnostic")
        with self.assertRaisesRegex(BenchmarkError, "fixture cleanup diagnostic"):
            with ExitStack() as cleanup:
                cleanup.callback(close_database, database, None, report)
                cleanup.callback(close_fixtures, fixtures, report)
                raise BenchmarkError("primary query failure")
        database.stop.assert_called_once_with()
        database.close.assert_called_once_with()
        self.assertTrue(report["cleanup"]["private_database_closed"])
        self.assertFalse(report["cleanup"]["providers_closed"])

    def test_summary_excludes_unsupported_and_failed_samples(self):
        config = Config(repeats=2)
        groups = groups_for((CASE_BY_NAME["rank_exact_sort"],), ("default", "unordered"))
        groups[0]["samples"] = [
            {"status": "passed", "timing": {
                "returned_rows": 2, "first_row_s": 0.2, "first_k_rows_s": None, "total_elapsed_s": 0.3}},
            {"status": "failed", "timing": {
                "returned_rows": 1, "first_row_s": 0.01, "first_k_rows_s": None, "total_elapsed_s": 0.02}},
        ]
        report = {"groups": groups}
        summarize(report, config)
        self.assertEqual(report["counts"]["expected_sql_trials"], 2)
        self.assertEqual(report["counts"]["passed_sql_trials"], 1)
        self.assertEqual(report["counts"]["failed_sql_trials"], 1)
        self.assertEqual(report["counts"]["returned_rows_across_passed_trials"], 2)
        self.assertEqual(groups[0]["summary"]["first_row_s"]["median"], 0.2)
        self.assertIsNone(groups[0]["summary"]["first_k_rows_s"])
        self.assertIsNone(groups[1]["summary"])


if __name__ == "__main__":
    raise SystemExit(main())
