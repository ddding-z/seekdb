"""Bounded TEXT semantic-cache SQL/mock contracts.

Run --self-test for fixtures only, or --list-cases without starting services.
Repeat --case to select methods. SQL requires either --binary /path/to/seekdb
(the existing private Unix-socket launcher) or --socket /path/to/private.sock.
The socket mode closes only its own connections and drops only its uniquely
named database, models and endpoints. It never stops the supplied database.
The local PX case also needs --server-log in socket mode.
Use --fail-fast to stop on the first selected-case failure. With --binary,
--log-dir copies owned deployment logs to a new directory before cleanup.

These sixteen cases require the approved implementation, not an old-binary
baseline: cache defaults/off retain duplicate demanded work; cache-only freezes
and coalesces calls without changing placement; both enable_semantic_cache and
enable_semantic_lift permit greedy nonunique HASH/MERGE INNER JOIN lifting.
The MAP lifting case exercises an AI_MAP recipe in a SEMANTIC FILTER; it does
not independently force a projected SEMANTIC MAP to start below a join.
Exact bag results, full provider-request identities, HTTP attempts, operator
topology and operator-owned movement markers are the oracles. No estimate,
operator ID, retry deduplication or model-proxy behavior substitutes for them.

Equal wire bodies from different function kinds still require separate work.
AI_JOIN normalizes only JSON serialization whitespace, never rule/left/right
roles, and its SQL candidate boundary must not move. Held replies provide
cross-batch evidence; actual preparation records provide local PX task/thread
evidence. Cancellation and a 1 MiB query budget must fail explicitly and release
query-local state. The memory case first proves the uncached streaming query
fits that same budget and one cached result fits. Seventeen distinct 32 KiB
answers total 544 KiB: below the query budget but above its 512 KiB persistent
cache quota. Cached execution must fail, not evict or recompute. Metadata can
cause an earlier failure; an exact metadata-dependent failure ordinal is not
assumed. No separate cache-size hint is used.

No AGG caching, FILE-dependent lifting, distributed/remote PX, tenant-wide
capacity exhaustion, shutdown or native cache-container limit is claimed.
Fixture self-tests are not proof of SQL integration. No build is performed.
"""

import argparse
from collections import Counter
from contextlib import ExitStack, redirect_stderr, redirect_stdout
from dataclasses import dataclass
from http.client import HTTPConnection, RemoteDisconnected
import io
import json
import os
from pathlib import Path
import reprlib
import shutil
import socket
import struct
import tempfile
import threading
import time
import unittest
from unittest import mock
import uuid

import pymysql

import test_file_runtime as files
import test_semantic_join as joins
import test_semantic_lift as lift
import test_semantic_runtime as semantic
from test_semantic_px import PREPARE
from test_runtime_contracts import Reply, completion, model_socket_fds, sql_error


BATCH_ROWS = semantic.BATCH_ROWS
DUPLICATE_ROWS = 2 * BATCH_ROWS + 3
SQL_CASE_COUNT = 16
REQUEST_MODELS = ("mock-semantic-cache-a", "mock-semantic-cache-b")
JOIN_RULE = "Respect the ordered left and right roles."
SESSION_SETTINGS = ("ob_query_timeout", "ai_pipeline_slots", "ai_pipeline_memory_limit")
TABLES = {
    "cache_inputs": (
        "id INT PRIMARY KEY, prompt LONGTEXT, guard_flag INT NOT NULL, "
        "model VARCHAR(64), opts LONGTEXT"
    ),
    "cache_right": (
        "row_id INT PRIMARY KEY, join_id INT NOT NULL, keep_row INT NOT NULL, "
        "prompt LONGTEXT, category VARCHAR(64)"
    ),
    "cache_chain": (
        "row_id INT PRIMARY KEY, join_id INT NOT NULL, keep_row INT NOT NULL, "
        "category VARCHAR(64)"
    ),
    "cache_px_inputs": "id INT PRIMARY KEY, prompt LONGTEXT",
}
INVALID_HINT_VALUES = ("", "not-a-boolean", "true,false")
QUERY_MEMORY_BYTES = 1048576
CACHE_PERSISTENT_BYTES = QUERY_MEMORY_BYTES // 2
CACHE_RESULT_BYTES = 32768
CACHE_OVERFLOW_ROWS = CACHE_PERSISTENT_BYTES // CACHE_RESULT_BYTES + 1


def json_text(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


@dataclass(frozen=True)
class RequestKey:
    model: str
    content: str
    options: str


def request_key(body):
    if not isinstance(body, dict) or not isinstance(body.get("model"), str):
        raise AssertionError("completion request must declare a string model")
    messages = body.get("messages")
    if not isinstance(messages, list) or len(messages) != 1:
        raise AssertionError("TEXT fixtures require exactly one user message")
    message = messages[0]
    if not isinstance(message, dict) or set(message) != {"role", "content"}:
        raise AssertionError("TEXT user message must contain only role and content")
    if message["role"] != "user" or not isinstance(message["content"], str):
        raise AssertionError("TEXT prompt must remain a user string")
    content = message["content"]
    response_format = body.get("response_format")
    native = response_format.get("json_schema") if isinstance(response_format, dict) else None
    if isinstance(native, dict) and native.get("name") == "ai_join":
        content = "pair:" + joins.pair_key(joins.parse_join_prompt(content))
    else:
        content = "text:" + content
    options = {name: value for name, value in body.items() if name not in ("model", "messages")}
    return RequestKey(body["model"], content, json_text(options))


def request_body(provider, prompt, *, kind="map", options=None):
    if provider not in (0, 1) or type(provider) is not int:
        raise ValueError("provider must be 0 or 1")
    if kind not in ("map", "filter", "join"):
        raise ValueError("fixture kind must be map, filter or join")
    if options is not None and not isinstance(options, dict):
        raise ValueError("fixture options must be an object")
    options = dict(options or {})
    if kind == "join":
        prompt = joins.prompt_text(prompt) if isinstance(prompt, dict) else prompt
        joins.parse_join_prompt(prompt)
        options.setdefault("response_format", joins.JOIN_FORMAT)
    elif kind == "filter":
        options.setdefault("response_format", semantic.MATCHING_FILTER_FORMAT)
    return {
        "model": REQUEST_MODELS[provider],
        "messages": [{"role": "user", "content": prompt}],
        **options,
    }


def expected_key(provider, prompt, *, kind="map", options=None):
    return request_key(request_body(provider, prompt, kind=kind, options=options))


class RequestScenarios(dict):
    """Route the existing handler by its just-recorded full request."""

    def __init__(self, server):
        super().__init__()
        self.server = server
        self.starts = {}

    def occurrences(self, key):
        return sum(request_key(body) == key for body in self.server.requests)

    def __setitem__(self, key, replies):
        if not isinstance(key, RequestKey):
            raise ValueError("scenario keys must be complete RequestKeys")
        replies = tuple(replies)
        if not replies or not all(isinstance(reply, Reply) for reply in replies):
            raise ValueError("scenario replies must be a nonempty Reply sequence")
        with self.server.condition:
            self.starts[key] = self.occurrences(key)
            super().__setitem__(key, replies)

    def get(self, prompt, default=None):
        with self.server.condition:
            if not self.server.requests:
                raise AssertionError("scenario lookup preceded request recording")
            body = self.server.requests[-1]
            if body["messages"][-1]["content"] != prompt:
                raise AssertionError("scenario lookup lost its recorded request")
            key = request_key(body)
            replies = super().get(key)
            if replies is None:
                raise AssertionError(f"unconfigured semantic request: {reprlib.repr(key)}")
            index = self.occurrences(key) - self.starts[key] - 1
            if index < 0:
                raise AssertionError("scenario request history was reset")
            # ContractHandler evaluates its nested get() default twice.
            return (replies[min(index, len(replies) - 1)],)


class CacheFixtures(semantic.MockFixtures):
    def __init__(self):
        super().__init__()
        for server in self.mocks:
            server.scenarios = RequestScenarios(server)

    def __enter__(self):
        return self

    def __exit__(self, *_error):
        self.close()

    def replies(self, provider, prompt, replies, *, kind="map", options=None):
        key = expected_key(provider, prompt, kind=kind, options=options)
        self.mocks[provider].scenarios[key] = replies
        return key

    def reply(self, provider, prompt, value, *, kind="map", options=None, gate=None):
        if kind == "map":
            if not isinstance(value, str):
                raise ValueError("MAP fixture answers must be strings")
            content = value
        else:
            if type(value) is not bool:
                raise ValueError("FILTER/JOIN fixture answers must be actual booleans")
            content = json_text({"value": value})
        return self.replies(provider, prompt, [
            Reply(body=completion(content, finish_reason="stop"), gate=gate)
        ], kind=kind, options=options)


@dataclass(frozen=True)
class Snapshot:
    counts: Counter
    requests: tuple


def snapshot(server, *, settled=True):
    if settled:
        requests = lift.provider_snapshot(server).requests
    else:
        with server.condition:
            if server.fixture_errors:
                raise AssertionError(f"provider fixture failed: {server.fixture_errors!r}")
            requests = tuple(server.requests)
            if sum(server.counts.values()) != len(requests):
                raise AssertionError("provider request bodies and HTTP attempts disagree")
    return Snapshot(Counter(request_key(body) for body in requests), requests)


def assert_window(test, before, after, expected):
    test.assertEqual(after.requests[:len(before.requests)], before.requests,
                     "provider request history changed during a measurement window")
    lift.assert_clean_work(test, before.counts, after.counts, Counter(expected))


def assert_cache_quota_window(test, before, after, planned_keys):
    attempts = lift.counter_delta(before.counts, after.counts)
    assert_window(test, before, after, attempts)
    test.assertTrue(attempts, "cache quota fixture submitted no request")
    test.assertLessEqual(sum(attempts.values()), CACHE_OVERFLOW_ROWS,
                         "cache exceeded its half-budget quota or recomputed a demanded input")
    test.assertTrue(all(count == 1 for count in attempts.values()),
                    "cache resource failure evicted/recomputed or retried successful work")
    test.assertTrue(set(attempts) <= set(planned_keys), "cache emitted an unplanned request")


def assert_held_window(test, server, before, gate):
    test.assertFalse(gate.is_set(), "the response gate was already released")
    records = semantic.audit(server)[len(before.requests):]
    test.assertTrue(records, "no request arrived in the held measurement window")
    test.assertTrue(all(record["response"] is None for record in records),
                    "a response was sent before the window's gate was released")


def assert_explicit_sql_error(test, codes, operation):
    error = sql_error(test, codes, operation)
    test.assertGreaterEqual(len(error.args), 2, "SQL error lacks a diagnostic")
    test.assertTrue(error.args[1], "SQL error must have an explicit diagnostic")
    return error


def sql_statement(fields, *, source="cache_inputs a", cache=None, enabled_lift=None,
                  where=None, suffix="ORDER BY a.id", hints="", batch_rows=BATCH_ROWS):
    parameters = [f"OPT_PARAM('rowsets_max_rows', {batch_rows})"]
    for name, value in (("enable_semantic_cache", cache), ("enable_semantic_lift", enabled_lift)):
        if value not in (None, "true", "false"):
            raise ValueError(f"{name} must be omitted or a true/false varchar hint")
        if value is not None:
            parameters.append(f"OPT_PARAM('{name}', '{value}')")
    if type(batch_rows) is not int or batch_rows <= 0:
        raise ValueError("batch_rows must be a positive integer")
    hint = " ".join([hints, *parameters]).strip()
    predicate = "" if where is None else " WHERE " + where
    return f"SELECT /*+ {hint} */ {fields} FROM {source}{predicate} {suffix}".rstrip()


def assert_kept(test, plan, *, join_label="HASH JOIN"):
    stage = lift.assert_below_join(
        test, plan, join_label=join_label, require_stop=True)
    test.assertRegex(stage.annotation, r"(?i)\bsemantic_placement\s*=\s*KEPT\b")
    return stage


def assert_candidate_boundary(test, plan):
    candidate = plan.one("SEMANTIC JOIN")
    test.assertRegex(candidate.annotation, r"(?i)\bcandidate_policy\s*=\s*ALL_SQL_CANDIDATES\b")
    test.assertIn("pair_predicates", candidate.annotation.lower())
    test.assertTrue(plan.is_above(candidate, plan.one("HASH JOIN")), plan.text)
    lift.assert_no_lift(test, plan)
    return candidate


def assert_px_preparations(test, records):
    test.assertTrue(records, "no native semantic preparation records were observed")
    test.assertEqual(len({record[0] for record in records}), 1,
                     "PX evidence must belong to one query execution")
    test.assertEqual(len({record[1] for record in records}), 1,
                     "PX fixture requires one semantic operator")
    workers = [record for record in records if record[2] >= 0 and record[4] > 0]
    test.assertEqual(len({record[2] for record in workers}), 4,
                     "four actual PX tasks with nonempty input are required")
    test.assertGreaterEqual(len({record[3] for record in workers}), 2,
                            "task IDs alone do not prove multiple worker threads")


class SocketDatabase:
    def __init__(self, sql_socket, server_log=None):
        self.sql_socket = str(sql_socket)
        self.server_log = server_log
        self.connection = None

    def start(self):
        self.connection = pymysql.connect(
            unix_socket=self.sql_socket, user="root", autocommit=True,
            read_timeout=100, write_timeout=100, charset="utf8mb4")

    def close(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None


class SemanticCacheContracts(unittest.TestCase):
    database = None

    @classmethod
    def setUpClass(cls):
        token = uuid.uuid4().hex[:12]
        cls.schema = "ai_semantic_cache_contract_" + token
        cls.models = tuple(f"semantic_cache_{token}_{suffix}" for suffix in ("a", "b"))
        cls.created_models = []
        cls.created_endpoints = []
        cls.schema_created = False
        cls.addClassCleanup(cls.drop_owned_sql_objects)
        with cls.database.connection.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE {cls.schema}")
            cls.schema_created = True
            cursor.execute(f"USE {cls.schema}")
            for table, definition in TABLES.items():
                partition = " PARTITION BY HASH(id) PARTITIONS 8" if table == "cache_px_inputs" else ""
                cursor.execute(f"CREATE TABLE {table} ({definition}){partition}")

    @classmethod
    def drop_owned_sql_objects(cls):
        with cls.database.connection.cursor() as cursor, ExitStack() as cleanup:
            if cls.schema_created:
                cleanup.callback(cursor.execute, f"DROP DATABASE {cls.schema}")
            for model in cls.created_models:
                cleanup.callback(cursor.execute, "CALL DBMS_AI_SERVICE.DROP_AI_MODEL(%s)", (model,))
            for endpoint in cls.created_endpoints:
                cleanup.callback(cursor.execute, "CALL DBMS_AI_SERVICE.DROP_AI_MODEL_ENDPOINT(%s)",
                                 (endpoint,))

    def setUp(self):
        self.cursor = self.database.connection.cursor()
        self.addCleanup(self.cursor.close)
        self.cursor.execute(f"USE {self.schema}")
        self.original_session = self.execute(
            "SELECT " + ", ".join("@@SESSION." + name for name in SESSION_SETTINGS))[0]
        self.addCleanup(self.restore_settings)
        self.cursor.execute("SET ob_query_timeout=30000000, ai_pipeline_slots=2, "
                            "ai_pipeline_memory_limit=DEFAULT")
        for table in TABLES:
            self.cursor.execute(f"TRUNCATE TABLE {table}")
        self.fixtures = CacheFixtures()
        self.first, self.second = self.fixtures.mocks
        self.addCleanup(self.close_case)
        self.prepare_models()
        self.process_id = struct.unpack(
            "3i", self.database.connection._sock.getsockopt(
                socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i")))[0]

    def restore_settings(self):
        self.cursor.execute(
            "SET " + ", ".join(name + "=%s" for name in SESSION_SETTINGS), self.original_session)

    def close_case(self):
        try:
            self.fixtures.release()
            for pending in self.fixtures.pending:
                pending.thread.join(5)
            if any(pending.thread.is_alive() for pending in self.fixtures.pending):
                self.kill_query()
        finally:
            self.fixtures.close()

    def prepare_models(self):
        for index, (model, server) in enumerate(zip(self.models, self.fixtures.mocks)):
            endpoint = model + "_endpoint"
            if model not in self.created_models:
                self.cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL(%s,%s)", (
                    model, json_text({
                        "type": "completion", "model_name": REQUEST_MODELS[index],
                        "input_modalities": ["text"],
                    }),
                ))
                self.created_models.append(model)
            if endpoint not in self.created_endpoints:
                self.cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT(%s,%s)", (
                    endpoint, json_text({
                        "ai_model_name": model, "provider": "openai",
                        "request_model_name": REQUEST_MODELS[index],
                        "access_key": "private-semantic-cache-fixture-only",
                        "url": f"http://127.0.0.1:{server.server_port}/v1/chat/completions",
                    }),
                ))
                self.created_endpoints.append(endpoint)
            else:
                self.cursor.execute("CALL DBMS_AI_SERVICE.ALTER_AI_MODEL_ENDPOINT(%s,%s)", (
                    endpoint, json_text({
                        "provider": "openai",
                        "url": f"http://127.0.0.1:{server.server_port}/v1/chat/completions",
                    }),
                ))

    def execute(self, statement, parameters=None):
        self.last_statement = statement
        self.last_parameters = parameters
        self.last_rows = None
        self.last_request_offsets = []
        if hasattr(self, "fixtures"):
            for server in self.fixtures.mocks:
                with server.condition:
                    self.last_request_offsets.append(len(server.requests))
        self.cursor.execute(statement, parameters)
        self.last_rows = self.cursor.fetchall()
        return self.last_rows

    def call(self, kind, prompt="a.prompt", *, provider=0, options=None):
        arguments = [f"'{self.models[provider]}'", prompt]
        if options is not None:
            arguments.append(options)
        return f"AI_{kind.upper()}(" + ",".join(arguments) + ")"

    def join_call(self, *, left="a.prompt", right="b.prompt", provider=1):
        return f"AI_JOIN('{self.models[provider]}','{JOIN_RULE}',{left},{right})"

    def load_inputs(self, prompts, *, flags=None):
        if flags is not None and len(flags) != len(prompts):
            raise ValueError("guard flags must match source rows")
        self.cursor.execute("TRUNCATE TABLE cache_inputs")
        self.cursor.executemany("INSERT INTO cache_inputs VALUES (%s,%s,%s,%s,%s)", [
            (index, prompt, 1 if flags is None else flags[index], self.models[0],
             '{"temperature":0.25}') for index, prompt in enumerate(prompts)
        ])

    def load_right(self, count, *, keep=lambda index: index % 4 == 0, copies=2,
                   prompt=lambda index: f"right-{index % 6}"):
        rows = [
            (copies * index + copy, index, int(keep(index)), prompt(index),
             f"category-{index}-{copy}") for index in range(count) for copy in range(copies)
        ]
        self.cursor.execute("TRUNCATE TABLE cache_right")
        self.cursor.executemany("INSERT INTO cache_right VALUES (%s,%s,%s,%s,%s)", rows)
        return rows

    def load_multiplying(self, count=12, **right_options):
        prompts = [f"shared-{index % 6}" for index in range(count)]
        self.load_inputs(prompts)
        return prompts, self.load_right(count, **right_options)

    def snapshots(self, *, settled=True):
        return tuple(snapshot(server, settled=settled) for server in self.fixtures.mocks)

    def assert_windows(self, before, *, first=(), second=(), settled=True):
        for old, new, expected in zip(before, self.snapshots(settled=settled), (first, second)):
            assert_window(self, old, new, expected)

    def explain(self, statement, parameters=None):
        self.last_plan = None
        before = self.snapshots()
        plan = lift.ExplainPlan(lift.render_explain(
            self.execute("EXPLAIN " + statement, parameters)))
        self.last_plan = plan
        self.assert_windows(before)
        return plan

    def check_query(self, statement, expected_rows, *, first=(), second=(), parameters=None,
                    ordered=True):
        before = self.snapshots()
        rows = self.execute(statement, parameters)
        self.assert_windows(before, first=first, second=second)
        self.assertEqual(Counter(rows), Counter(expected_rows),
                         "SQL bag values or multiplicity changed")
        if ordered:
            self.assertEqual(rows, tuple(expected_rows), "SQL ORDER BY was not preserved")
        return rows

    def filter_join(self, *, cache=None, enabled_lift=None, algorithm="HASH", predicate=None,
                    fields="a.id,b.category", source=None):
        return sql_statement(
            fields, cache=cache, enabled_lift=enabled_lift,
            source=source or "cache_inputs a JOIN cache_right b ON a.id=b.join_id",
            where="b.keep_row=1 AND " + (predicate or self.call("filter")),
            hints=f"LEADING(a b) USE_{algorithm}(b)", suffix="ORDER BY a.id,b.row_id")

    def control_connection(self):
        return pymysql.connect(
            unix_socket=self.database.sql_socket, user="root", autocommit=True,
            read_timeout=10, write_timeout=10, charset="utf8mb4")

    def kill_query(self):
        with self.control_connection() as control, control.cursor() as cursor:
            cursor.execute(f"KILL QUERY {self.database.connection.thread_id()}")

    def assert_no_model_sockets(self):
        for server in self.fixtures.mocks:
            self.assertFalse(model_socket_fds(self.process_id, server.server_port),
                             "finished error left a database-owned model-service socket")

    def test_default_and_off_retain_duplicate_demanded_work(self):
        prompt = "uncached-duplicate"
        self.load_inputs([prompt] * DUPLICATE_ROWS)
        filter_key = self.fixtures.reply(1, prompt, True, kind="filter")
        map_key = expected_key(0, prompt)
        for cache in (None, "false"):
            with self.subTest(cache=cache):
                answers = [f"uncached-{index}" for index in range(DUPLICATE_ROWS)]
                self.fixtures.replies(0, prompt, [
                    Reply(body=completion(value, finish_reason="stop")) for value in answers
                ])
                statement = sql_statement(
                    "a.id," + self.call("map") + "," + self.call("filter", provider=1), cache=cache)
                lift.assert_no_lift(self, self.explain(statement))
                before = self.snapshots()
                rows = self.execute(statement)
                self.assertEqual([row[0] for row in rows], list(range(DUPLICATE_ROWS)))
                self.assertEqual(Counter(row[1] for row in rows), Counter(answers),
                                 "default/off unexpectedly froze a duplicate MAP answer")
                self.assertTrue(all(row[2] == 1 for row in rows))
                self.assert_windows(before, first=[map_key] * DUPLICATE_ROWS,
                                    second=[filter_key] * DUPLICATE_ROWS)

    def test_cache_only_shares_effective_filter_calls_across_operators(self):
        prompts = ["operator-duplicate"] * DUPLICATE_ROWS
        self.load_inputs(prompts)
        key = self.fixtures.reply(0, prompts[0], True, kind="filter")
        for cache in ("false", "true"):
            with self.subTest(cache=cache):
                statement = sql_statement(
                    "a.id," + self.call("filter", "CONCAT(a.prompt,'')"), cache=cache,
                    where=self.call("filter"))
                plan = self.explain(statement)
                lift.assert_no_lift(self, plan)
                self.assertTrue(plan.is_above(plan.one("SEMANTIC MAP"),
                                              plan.one("SEMANTIC FILTER")), plan.text)
                self.check_query(
                    statement, [(index, 1) for index in range(DUPLICATE_ROWS)],
                    first=[key] * (2 * DUPLICATE_ROWS if cache == "false" else 1))

    def test_nonunique_hash_filter_requires_both_flags_and_preserves_bag(self):
        prompts, right = self.load_multiplying()
        keys = {
            prompt: self.fixtures.reply(0, prompt, prompt != "shared-2", kind="filter")
            for prompt in dict.fromkeys(prompts)
        }
        expected = [(index, category) for _row, index, keep, _prompt, category in right
                    if keep and prompts[index] != "shared-2"]
        combinations = ((None, None), ("false", None), (None, "true"), ("false", "true"),
                        ("true", None), ("true", "false"), ("true", "true"))
        for cache, enabled_lift in combinations:
            with self.subTest(cache=cache, enabled_lift=enabled_lift):
                statement = self.filter_join(cache=cache, enabled_lift=enabled_lift)
                plan = self.explain(statement)
                if cache == enabled_lift == "true":
                    lift.assert_lifted(self, plan)
                    demanded = list(dict.fromkeys(prompts[::4]))
                else:
                    if enabled_lift == "true":
                        assert_kept(self, plan)
                    else:
                        lift.assert_below_join(self, plan)
                    demanded = list(dict.fromkeys(prompts)) if cache == "true" else prompts
                self.check_query(statement, expected, first=[keys[prompt] for prompt in demanded])

    def test_nonunique_hash_map_recipe_lifts_only_with_cache(self):
        prompts, right = self.load_multiplying()
        keys = {
            prompt: self.fixtures.reply(0, prompt, "drop" if prompt == "shared-2" else "keep")
            for prompt in dict.fromkeys(prompts)
        }
        expected = [(index, category) for _row, index, keep, _prompt, category in right
                    if keep and prompts[index] != "shared-2"]
        for cache in ("false", "true"):
            with self.subTest(cache=cache):
                statement = self.filter_join(
                    cache=cache, enabled_lift="true", predicate=self.call("map") + "='keep'")
                plan = self.explain(statement)
                if cache == "true":
                    lift.assert_lifted(self, plan)
                    demanded = list(dict.fromkeys(prompts[::4]))
                else:
                    assert_kept(self, plan)
                    demanded = prompts
                self.check_query(statement, expected, first=[keys[prompt] for prompt in demanded])

    def test_nonunique_merge_join_kept_cache_only_and_lifted_with_both(self):
        prompts, right = self.load_multiplying(keep=lambda index: index in (0, 11))
        keys = {prompt: self.fixtures.reply(0, prompt, True, kind="filter")
                for prompt in dict.fromkeys(prompts)}
        expected = [(index, category) for _row, index, keep, _prompt, category in right if keep]
        for cache, enabled_lift in (("false", "true"), ("true", "false"), ("true", "true")):
            with self.subTest(cache=cache, enabled_lift=enabled_lift):
                statement = self.filter_join(
                    cache=cache, enabled_lift=enabled_lift, algorithm="MERGE")
                plan = self.explain(statement)
                if cache == enabled_lift == "true":
                    lift.assert_lifted(self, plan, join_labels=("MERGE JOIN",))
                    demanded = [prompts[0], prompts[11]]
                else:
                    if enabled_lift == "true":
                        assert_kept(self, plan, join_label="MERGE JOIN")
                    else:
                        lift.assert_below_join(self, plan, join_label="MERGE JOIN")
                    demanded = list(dict.fromkeys(prompts)) if cache == "true" else prompts
                self.check_query(statement, expected, first=[keys[prompt] for prompt in demanded])

    def test_consecutive_nonunique_hash_joins_lift_greedily_twice(self):
        prompts = [f"chain-input-{index % 8}" for index in range(24)]
        self.load_inputs(prompts)
        self.load_right(24, keep=lambda index: index % 2 == 0)
        self.cursor.executemany("INSERT INTO cache_chain VALUES (%s,%s,%s,%s)", [
            (3 * index + copy, index, int(index % 3 == 0), f"chain-{index}-{copy}")
            for index in range(24) for copy in range(3)
        ])
        keys = {prompt: self.fixtures.reply(0, prompt, True, kind="filter")
                for prompt in dict.fromkeys(prompts)}
        expected = [(index, f"category-{index}-{left}", f"chain-{index}-{right}")
                    for index in range(0, 24, 6) for left in range(2) for right in range(3)]
        for cache in ("false", "true"):
            with self.subTest(cache=cache):
                statement = sql_statement(
                    "a.id,b.category,c.category", cache=cache, enabled_lift="true",
                    source="cache_inputs a JOIN cache_right b ON a.id=b.join_id "
                           "JOIN cache_chain c ON b.join_id=c.join_id",
                    where="b.keep_row=1 AND c.keep_row=1 AND " + self.call("filter"),
                    hints="LEADING(a b c) USE_HASH(b) USE_HASH(c)",
                    suffix="ORDER BY a.id,b.row_id,c.row_id")
                plan = self.explain(statement)
                if cache == "true":
                    lift.assert_lifted(self, plan, crossed=2)
                    demanded = list(dict.fromkeys(prompts[::6]))
                else:
                    lift.assert_no_lift(self, plan)
                    stage = plan.one("SEMANTIC FILTER")
                    lift.assert_lift_stop(self, stage)
                    self.assertRegex(stage.annotation, r"(?i)\bsemantic_placement\s*=\s*KEPT\b")
                    self.assertEqual(len(plan.find("HASH JOIN")), 2, plan.text)
                    self.assertTrue(all(plan.is_above(join, stage)
                                        for join in plan.find("HASH JOIN")), plan.text)
                    demanded = prompts
                self.check_query(statement, expected, first=[keys[prompt] for prompt in demanded])

    def test_same_prompt_different_models_options_and_function_kinds_remain_distinct(self):
        prompt = "identical-prompt"
        self.load_inputs([prompt] * DUPLICATE_ROWS)
        zero, one = {"temperature": 0}, {"temperature": 1}
        native = {"response_format": semantic.MATCHING_FILTER_FORMAT}
        a_zero = self.fixtures.reply(0, prompt, "a-zero", options=zero)
        b_zero = self.fixtures.reply(1, prompt, "b-zero", options=zero)
        a_one = self.fixtures.reply(0, prompt, "a-one", options=one)
        shared_wire = self.fixtures.reply(0, prompt, True, kind="filter", options=native)
        self.assertEqual(shared_wire, expected_key(0, prompt, options=native),
                         "kind isolation fixture must use exactly equal MAP/FILTER wire bodies")
        statement = sql_statement(
            "a.id," + ",".join((
                self.call("map", options="%s"), self.call("map", provider=1, options="%s"),
                self.call("map", options="%s"), self.call("map", options="%s"),
                self.call("filter", options="%s"),
            )), cache="true")
        parameters = tuple(map(json_text, (zero, zero, one, native, native)))
        lift.assert_no_lift(self, self.explain(statement, parameters))
        self.check_query(
            statement, [(index, "a-zero", "b-zero", "a-one", '{"value":true}', 1)
                        for index in range(DUPLICATE_ROWS)],
            first=[a_zero, a_one, shared_wire, shared_wire], second=[b_zero], parameters=parameters)

    def test_ai_join_duplicate_tuples_coalesce_but_swapped_roles_stay_distinct(self):
        def role_query(cache):
            return sql_statement(
                "a.id,b.row_id", cache=cache, enabled_lift="true",
                source="cache_inputs a JOIN cache_right b ON a.id=b.join_id AND NOT "
                       + self.join_call() + " AND "
                       + self.join_call(left="b.prompt", right="a.prompt"),
                hints="LEADING(a b) USE_HASH(b)", suffix="ORDER BY a.id,b.row_id")

        self.load_inputs(["left-value"] * 6)
        right = self.load_right(6, keep=lambda _index: True, prompt=lambda _index: "right-value")
        forward = joins.pair_payload(JOIN_RULE, "left-value", "right-value")
        reverse = joins.pair_payload(JOIN_RULE, "right-value", "left-value")
        forward_key = self.fixtures.reply(1, forward, False, kind="join")
        reverse_key = self.fixtures.reply(1, reverse, True, kind="join")
        expected = [(index, row) for row, index, _keep, _prompt, _category in right]
        for cache in ("false", "true"):
            with self.subTest(cache=cache):
                statement = role_query(cache)
                assert_candidate_boundary(self, self.explain(statement))
                repetitions = len(right) if cache == "false" else 1
                self.check_query(statement, expected,
                                 second=[forward_key, reverse_key] * repetitions)
        boundary_pairs = (("ab", "c"), ("a", "bc"), ("a|b", "c"), ("a", "b|c"))
        self.load_inputs([left for left, _right in boundary_pairs for _copy in range(2)])
        right = self.load_right(
            2 * len(boundary_pairs), keep=lambda _index: True,
            prompt=lambda index: boundary_pairs[index // 2][1])
        boundary_keys = []
        for left, text in boundary_pairs:
            boundary_keys.append(self.fixtures.reply(
                1, joins.pair_payload(JOIN_RULE, left, text), False, kind="join"))
            boundary_keys.append(self.fixtures.reply(
                1, joins.pair_payload(JOIN_RULE, text, left), True, kind="join"))
        self.assertEqual(len(set(boundary_keys)), 8,
                         "tuple-boundary fixtures must contain eight distinct ordered calls")
        statement = role_query("true")
        assert_candidate_boundary(self, self.explain(statement))
        self.check_query(
            statement, [(index, row) for row, index, _keep, _text, _category in right],
            second=boundary_keys)

    def test_ai_join_sql_candidate_boundary_and_input_filter_stay_protected(self):
        prompts, right = self.load_multiplying(
            prompt=lambda index: f"right-{index % 6}" if index % 4 == 0 else None)
        self.cursor.execute("INSERT INTO cache_right VALUES (99,99,1,NULL,'unmatched-invalid')")
        filter_keys = {
            prompt: self.fixtures.reply(0, prompt, prompt != "shared-2", kind="filter")
            for prompt in dict.fromkeys(prompts)
        }
        payloads = [joins.pair_payload(JOIN_RULE, prompts[index], text)
                    for _row, index, keep, text, _category in right
                    if keep and prompts[index] != "shared-2"]
        pair_keys = {
            joins.pair_key(payload): self.fixtures.reply(
                1, payload, payload["left"] == "shared-0", kind="join")
            for payload in payloads
        }
        expected = [(index, category) for _row, index, keep, _text, category in right
                    if keep and prompts[index] == "shared-0"]
        for cache in ("false", "true"):
            with self.subTest(cache=cache):
                statement = self.filter_join(
                    cache=cache, enabled_lift="true",
                    source="cache_inputs a JOIN cache_right b ON a.id=b.join_id AND "
                           + self.join_call())
                plan = self.explain(statement)
                stage = assert_kept(self, plan)
                candidate = assert_candidate_boundary(self, plan)
                self.assertTrue(plan.is_above(candidate, stage), plan.text)
                demanded = list(dict.fromkeys(prompts)) if cache == "true" else prompts
                join_keys = [pair_keys[joins.pair_key(payload)] for payload in payloads]
                if cache == "true":
                    join_keys = list(dict.fromkeys(join_keys))
                self.check_query(statement, expected,
                                 first=[filter_keys[prompt] for prompt in demanded], second=join_keys)

    def test_held_duplicates_coalesce_across_batches_before_any_reply(self):
        shared, witness = "held-shared", "second-batch-witness"
        prompts = [shared] * DUPLICATE_ROWS
        prompts[BATCH_ROWS] = witness
        self.load_inputs(prompts)
        gate = self.fixtures.gate()
        shared_key = self.fixtures.reply(0, shared, "frozen-shared", gate=gate)
        witness_key = self.fixtures.reply(0, witness, "witness-answer", gate=gate)
        statement = sql_statement("a.id," + self.call("map"), cache="true")
        self.explain(statement).one("SEMANTIC MAP")
        before = self.snapshots()
        pending = self.fixtures.start(lambda: self.execute(statement))
        semantic.wait_evidence(
            self, self.first, lambda: self.first.counts[witness] >= 1, pending,
            message="a demanded row from batch two must arrive while batch one is held")
        semantic.assert_held(self, self.first, [gate])
        self.assertFalse(pending.done.is_set(), repr(pending.error))
        self.assert_windows(before, first=[shared_key, witness_key], settled=False)
        gate.set()
        self.assertEqual(pending.result(timeout=10), tuple(
            (index, "witness-answer" if prompt == witness else "frozen-shared")
            for index, prompt in enumerate(prompts)))
        self.assert_windows(before, first=[shared_key, witness_key])

    def test_next_query_never_reuses_a_frozen_answer(self):
        prompt = "query-local-only"
        self.load_inputs([prompt] * DUPLICATE_ROWS)
        key = self.fixtures.replies(0, prompt, [
            Reply(body=completion(value, finish_reason="stop"))
            for value in ("query-one", "query-two")
        ])
        statement = sql_statement("a.id," + self.call("map"), cache="true")
        for value in ("query-one", "query-two"):
            with self.subTest(answer=value):
                self.check_query(statement, [(index, value) for index in range(DUPLICATE_ROWS)],
                                 first=[key])

    def test_strict_boolean_provider_and_dynamic_model_errors_are_not_cached_successes(self):
        for kind in ("filter", "join"):
            for invalid in ('{"value":"true"}', '{"value":true,"value":false}'):
                with self.subTest(kind=kind, invalid=invalid):
                    prompt = "strict-" + kind
                    self.load_inputs([prompt] * DUPLICATE_ROWS)
                    if kind == "join":
                        self.load_right(DUPLICATE_ROWS, keep=lambda _index: True,
                                        prompt=lambda _index: "strict-right")
                        content = joins.pair_payload(JOIN_RULE, prompt, "strict-right")
                        statement = sql_statement(
                            "a.id,b.row_id", cache="true", enabled_lift="true",
                            source="cache_inputs a JOIN cache_right b ON a.id=b.join_id AND "
                                   + self.join_call(provider=0),
                            hints="LEADING(a b) USE_HASH(b)", suffix="ORDER BY a.id,b.row_id")
                        rows = [(index, 2 * index + copy) for index in range(DUPLICATE_ROWS)
                                for copy in range(2)]
                    else:
                        content = prompt
                        statement = sql_statement(
                            "a.id," + self.call("filter"), cache="true", enabled_lift="true")
                        rows = [(index, 1) for index in range(DUPLICATE_ROWS)]
                    key = self.fixtures.replies(0, content, [
                        Reply(body=completion(invalid, finish_reason="stop"))
                    ], kind=kind)
                    before = self.snapshots()
                    assert_explicit_sql_error(self, 4070, lambda: self.execute(statement))
                    self.assert_windows(before, first=[key])
                    self.assert_no_model_sockets()
                    self.fixtures.reply(0, content, True, kind=kind)
                    self.check_query(statement, rows, first=[key])
        prompt = "provider-hard-error"
        self.load_inputs([prompt] * DUPLICATE_ROWS)
        key = self.fixtures.replies(0, prompt, [Reply(status=400, body=b'{"error":"bad model"}')])
        statement = sql_statement("a.id," + self.call("map"), cache="true")
        before = self.snapshots()
        assert_explicit_sql_error(self, 4216, lambda: self.execute(statement))
        self.assert_windows(before, first=[key])
        self.assert_no_model_sockets()
        self.fixtures.reply(0, prompt, "recovered")
        self.check_query(statement, [(index, "recovered") for index in range(DUPLICATE_ROWS)],
                         first=[key])
        for expression in (f"AI_MAP(a.model,a.prompt)", self.call("map", options="a.opts")):
            with self.subTest(dynamic=expression):
                before = self.snapshots()
                invalid = sql_statement("a.id," + expression, cache="true", enabled_lift="true")
                assert_explicit_sql_error(self, (1210, 1235), lambda: self.execute(invalid))
                self.assert_windows(before)

    def test_invalid_cache_hint_values_fail_explain_and_select_without_model_work(self):
        self.load_inputs(["invalid-hint-must-not-run"])
        valid = sql_statement("a.id," + self.call("map"), cache="true", enabled_lift="true")
        for value in INVALID_HINT_VALUES:
            with self.subTest(value=value):
                statement = valid.replace(
                    "OPT_PARAM('enable_semantic_cache', 'true')",
                    f"OPT_PARAM('enable_semantic_cache', '{value}')")
                before = self.snapshots()
                for prefix in ("EXPLAIN ", ""):
                    assert_explicit_sql_error(self, 1210, lambda: self.execute(prefix + statement))
                self.assert_windows(before)
                self.assertEqual(self.execute("SELECT 1"), ((1,),))

    def test_guards_and_undemanded_calls_preserve_invalid_skipped_inputs(self):
        prompts = [None, "", "guard-safe", "guard-safe", None, ""]
        self.load_inputs(prompts, flags=[0, 0, 1, 1, 0, 0])
        right = self.load_right(6, keep=lambda index: index % 3 == 0)
        filter_key = self.fixtures.reply(0, "guard-safe", True, kind="filter")
        map_key = self.fixtures.reply(1, "guard-safe", "guard-map")
        predicate = "CASE WHEN a.guard_flag=1 THEN " + self.call("filter") + " ELSE TRUE END"
        fields = "a.id,b.category,CASE WHEN a.guard_flag=1 THEN " + self.call(
            "map", provider=1) + " ELSE 'skip' END"
        expected = [(index, category, "guard-map" if index == 3 else "skip")
                    for _row, index, keep, _text, category in right if keep]
        for cache in ("false", "true"):
            with self.subTest(cache=cache):
                statement = self.filter_join(
                    cache=cache, enabled_lift="true", predicate=predicate, fields=fields)
                assert_kept(self, self.explain(statement))
                repetitions = 2 if cache == "false" else 1
                self.check_query(statement, expected, first=[filter_key] * repetitions,
                                 second=[map_key] * repetitions)
        self.check_query(statement + " LIMIT 0", ())
        empty = self.filter_join(
            cache="true", enabled_lift="true", predicate="a.id<0 AND " + predicate, fields=fields)
        self.check_query(empty, ())
        for index in (0, 1):
            with self.subTest(demanded_invalid_row=index):
                before = self.snapshots()
                demanded = sql_statement(
                    "a.id," + self.call("map"), cache="true", where=f"a.id={index}")
                assert_explicit_sql_error(self, 1210, lambda: self.execute(demanded))
                self.assert_windows(before)

    def wait_px_preparations(self, start):
        records = []
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            with self.database.server_log.open(errors="replace") as log:
                log.seek(start)
                records = [tuple(map(int, match.groups())) for line in log
                           if (match := PREPARE.search(line))]
            if len({record[2] for record in records if record[2] >= 0 and record[4] > 0}) == 4:
                assert_px_preparations(self, records)
                return records
            threading.Event().wait(0.01)
        self.fail(f"four actual PX preparation tasks were not observed: {records!r}")

    def wait_px_quiescence(self):
        with self.control_connection() as control, control.cursor() as cursor:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                cursor.execute("SELECT COUNT(*) FROM oceanbase.__all_virtual_px_worker_stat")
                if cursor.fetchone() == (0,):
                    return
                threading.Event().wait(0.01)
        self.fail("local PX workers did not suspend while their shared reply was held")

    def start_held_px(self, statement, key, gate):
        start = self.database.server_log.stat().st_size
        before = self.snapshots()
        pending = self.fixtures.start(lambda: self.execute(statement))
        semantic.wait_evidence(
            self, self.first, lambda: snapshot(self.first, settled=False).counts[key]
            > before[0].counts[key], pending, message="the shared PX request did not arrive")
        self.wait_px_preparations(start)
        self.wait_px_quiescence()
        assert_held_window(self, self.first, before[0], gate)
        self.assertFalse(pending.done.is_set(), repr(pending.error))
        self.assert_windows(before, first=[key], settled=False)
        return pending, before

    def test_local_px_shares_pending_calls_cancels_waiters_and_recovers(self):
        if self.database.server_log is None:
            self.skipTest("socket mode needs --server-log for actual local PX task/thread evidence")
        self.assertTrue(self.database.server_log.is_file(),
                        f"server log is unavailable: {self.database.server_log}")
        prompt = "shared-across-local-px"
        row_count = 256
        self.cursor.executemany("INSERT INTO cache_px_inputs VALUES (%s,%s)",
                                [(index, prompt) for index in range(row_count)])
        statement = sql_statement(
            "a.id," + self.call("map"), source="cache_px_inputs a", suffix="",
            hints="PARALLEL(4)", cache="true")
        plan = self.explain(statement)
        plan.one("PX COORDINATOR")
        plan.one("SEMANTIC MAP")
        self.assertIn("DOP=4", plan.text.upper())
        gate = self.fixtures.gate()
        key = self.fixtures.reply(0, prompt, "px-frozen", gate=gate)
        pending, before = self.start_held_px(statement, key, gate)
        gate.set()
        self.assertEqual(Counter(pending.result(timeout=20)),
                         Counter((index, "px-frozen") for index in range(row_count)))
        self.assert_windows(before, first=[key])
        abandoned = self.fixtures.gate()
        self.fixtures.reply(0, prompt, "must-not-reach-sql", gate=abandoned)
        pending, before = self.start_held_px(statement, key, abandoned)
        self.kill_query()
        error = pending.expect_sql_error(self, 1317, timeout=5)
        self.assertTrue(error.args[1], "cancellation lacks an explicit SQL diagnostic")
        self.assertFalse(abandoned.is_set(), "cancellation required releasing the model response")
        self.assert_no_model_sockets()
        self.assert_windows(before, first=[key], settled=False)
        self.fixtures.reply(0, prompt, "px-recovered")
        before = self.snapshots(settled=False)
        recovery = self.fixtures.start(lambda: self.execute(statement))
        self.assertEqual(Counter(recovery.result(timeout=10)),
                         Counter((index, "px-recovered") for index in range(row_count)))
        self.assert_windows(before, first=[key], settled=False)
        self.assertFalse(abandoned.is_set(), "recovery depended on the canceled response")

    def test_bounded_non_evicting_cache_reports_quota_failure_and_releases_memory(self):
        unique_count = CACHE_OVERFLOW_ROWS
        prompts = [f"cache-memory-{index}" for index in range(unique_count)]
        prompts.extend([prompts[0], prompts[0]])
        self.load_inputs(prompts)
        result = "r" * CACHE_RESULT_BYTES
        keys = {prompt: self.fixtures.reply(0, prompt, result) for prompt in dict.fromkeys(prompts)}
        self.cursor.execute("SET ai_pipeline_slots=1, ai_pipeline_memory_limit=%s, "
                            "ob_query_timeout=20000000", (QUERY_MEMORY_BYTES,))
        statement = sql_statement(
            "a.id," + self.call("map"), cache="false", batch_rows=1, suffix="", hints="PARALLEL(1)")
        self.explain(statement).one("SEMANTIC MAP")
        self.check_query(statement, [(index, result) for index in range(len(prompts))],
                         first=[keys[prompt] for prompt in prompts], ordered=False)
        statement = statement.replace(
            "OPT_PARAM('enable_semantic_cache', 'false')",
            "OPT_PARAM('enable_semantic_cache', 'true')")
        self.check_query(sql_statement(
            "a.id," + self.call("map"), cache="true", batch_rows=1, where="a.id=0"),
            [(0, result)], first=[keys[prompts[0]]])
        self.cursor.execute("SET ob_query_timeout=3000000")
        before = self.snapshots()
        assert_explicit_sql_error(self, 4019, lambda: self.execute(statement))
        after = self.snapshots()
        assert_cache_quota_window(self, before[0], after[0], keys.values())
        assert_window(self, before[1], after[1], ())
        self.assert_no_model_sockets()
        self.cursor.execute("SET ob_query_timeout=20000000")
        self.fixtures.reply(0, prompts[0], "quota-recovered")
        self.check_query(sql_statement(
            "a.id," + self.call("map"), cache="true", batch_rows=1, where="a.id=0"),
            [(0, "quota-recovered")], first=[keys[prompts[0]]])


class SQLDiagnosticResult(unittest.TextTestResult):
    def write_sql_diagnostics(self, test):
        if not isinstance(test, SemanticCacheContracts):
            return
        self.stream.writeln("\nPrivate semantic-cache SQL diagnostics:")
        statement = getattr(test, "last_statement", None)
        if statement is not None:
            self.stream.writeln(statement)
            parameters = getattr(test, "last_parameters", None)
            if parameters is not None:
                self.stream.writeln(f"parameters={parameters!r}")
        plan = getattr(test, "last_plan", None)
        if plan is not None:
            self.stream.writeln(plan.text)
        rows = getattr(test, "last_rows", None)
        if rows is not None:
            self.stream.writeln(f"returned_rows={len(rows)}, sample={rows[:5]!r}")
        fixtures = getattr(test, "fixtures", None)
        if fixtures is not None:
            offsets = getattr(test, "last_request_offsets", ())
            for index, server in enumerate(fixtures.mocks):
                with server.condition:
                    start = offsets[index] if index < len(offsets) else 0
                    requests = tuple(server.requests[start:])
                    errors = tuple(server.fixture_errors)
                counts = Counter(repr(body) for body in requests)
                self.stream.writeln(
                    f"provider={index}, HTTP_attempts={len(requests)}, "
                    f"request_counts={dict(counts)!r}, fixture_errors={errors!r}")

    def addFailure(self, test, error):
        self.write_sql_diagnostics(test)
        super().addFailure(test, error)

    def addError(self, test, error):
        self.write_sql_diagnostics(test)
        super().addError(test, error)

    def addSubTest(self, test, subtest, error):
        if error is not None:
            self.write_sql_diagnostics(test)
        super().addSubTest(test, subtest, error)


def retain_deployment_logs(database, directory):
    directory.mkdir(parents=True, mode=0o700)
    sources = [*database.base.glob("startup-*.log"),
               *(database.base / "log").glob("seekdb.log*")]
    retained = []
    for source in sorted(sources):
        if source.is_file() and not source.is_symlink():
            target = directory / source.name
            shutil.copy2(source, target)
            retained.append(target)
    if not retained:
        raise AssertionError("private deployment produced no logs to retain")
    print(f"Retained private deployment logs: {directory}", flush=True)
    return retained


def post_request(server, body):
    client = HTTPConnection("127.0.0.1", server.server_port, timeout=8)
    try:
        client.request("POST", "/v1/chat/completions", json_text(body),
                       {"Content-Type": "application/json"})
        response = client.getresponse()
        return response.status, response.read()
    finally:
        client.close()


def response_content(response):
    status, body = response
    if status != 200:
        raise AssertionError(f"fixture completion failed: {status}, {body!r}")
    return json.loads(body)["choices"][0]["message"]["content"]


class SemanticCacheHarnessTests(unittest.TestCase):
    def test_request_identity_retains_model_options_and_equal_kind_wire_work(self):
        body = request_body(0, "same")
        key = request_key(body)
        self.assertNotEqual(key, expected_key(1, "same"))
        self.assertNotEqual(key, expected_key(0, "same", options={"temperature": 0}))
        self.assertNotEqual(expected_key(0, "same", options={"temperature": 0}),
                            expected_key(0, "same", options={"temperature": 1}))
        self.assertNotEqual(key, expected_key(0, "same", kind="filter"))
        native = {"response_format": semantic.MATCHING_FILTER_FORMAT}
        self.assertEqual(expected_key(0, "same", options=native),
                         expected_key(0, "same", kind="filter", options=native))
        self.assertEqual(Counter([key, key])[key], 2,
                         "wire equality is not a license to deduplicate HTTP evidence")

    def test_ai_join_identity_normalizes_whitespace_not_rule_or_roles(self):
        forward = joins.pair_payload(JOIN_RULE, "left", "right")
        body = request_body(0, forward, kind="join")
        whitespace = request_body(0, joins.prompt_text(forward, indent=2), kind="join")
        self.assertEqual(request_key(body), request_key(whitespace))
        self.assertNotEqual(request_key(body), expected_key(
            0, joins.pair_payload(JOIN_RULE, "right", "left"), kind="join"))
        self.assertNotEqual(request_key(body), expected_key(
            0, joins.pair_payload("another rule", "left", "right"), kind="join"))
        for left, right, other_left, other_right in (
                ("ab", "c", "a", "bc"), ("a|b", "c", "a", "b|c")):
            self.assertNotEqual(expected_key(
                0, joins.pair_payload(JOIN_RULE, left, right), kind="join"), expected_key(
                0, joins.pair_payload(JOIN_RULE, other_left, other_right), kind="join"))

    def test_request_identity_rejects_nontext_and_missing_provider_evidence(self):
        for body in ({}, {"model": 1, "messages": []},
                     {"model": "m", "messages": []},
                     {"model": "m", "messages": [{"role": "user", "content": None}]},
                     {"model": "m", "messages": [{"role": "system", "content": "text"}]},
                     {"model": "m", "messages": [
                         {"role": "user", "content": "text"}, {"role": "user", "content": "text"}]}):
            with self.subTest(body=body), self.assertRaises(AssertionError):
                request_key(body)

    def test_full_request_scenarios_preserve_options_and_independent_sequences(self):
        with CacheFixtures() as fixtures:
            zero, one = {"temperature": 0}, {"temperature": 1}
            zero_key = fixtures.replies(0, "same", [
                Reply(body=completion("zero-1")), Reply(body=completion("zero-2"))
            ], options=zero)
            one_key = fixtures.reply(0, "same", "one", options=one)
            before = snapshot(fixtures.mocks[0])
            for options, wanted in ((zero, "zero-1"), (one, "one"), (zero, "zero-2")):
                self.assertEqual(response_content(post_request(
                    fixtures.mocks[0], request_body(0, "same", options=options))), wanted)
            assert_window(self, before, snapshot(fixtures.mocks[0]), [zero_key, one_key, zero_key])
            fixtures.reply(0, "same", "new-sequence", options=zero)
            self.assertEqual(response_content(post_request(
                fixtures.mocks[0], request_body(0, "same", options=zero))), "new-sequence")

    def test_filter_and_join_fixtures_return_strict_native_boolean_completions(self):
        with CacheFixtures() as fixtures:
            payload = joins.pair_payload(JOIN_RULE, "left", "right")
            filter_key = fixtures.reply(0, "filter", False, kind="filter")
            pair_key = fixtures.reply(1, payload, True, kind="join")
            for provider, prompt, kind, wanted in (
                    (0, "filter", "filter", '{"value":false}'),
                    (1, payload, "join", '{"value":true}')):
                response = post_request(
                    fixtures.mocks[provider], request_body(provider, prompt, kind=kind))
                self.assertEqual(response_content(response), wanted)
                envelope = json.loads(response[1])
                self.assertEqual(envelope["choices"][0]["finish_reason"], "stop")
            self.assertEqual(snapshot(fixtures.mocks[0]).counts, Counter([filter_key]))
            self.assertEqual(snapshot(fixtures.mocks[1]).counts, Counter([pair_key]))
            semantic.assert_filter_request(self, fixtures.mocks[0].requests[0])
            self.assertEqual(joins.assert_join_request(
                self, fixtures.mocks[1].requests[0]), payload)
            for value in ("true", 1, None):
                with self.subTest(value=value), self.assertRaises(ValueError):
                    fixtures.reply(0, "filter", value, kind="filter")

    def test_http_fixture_never_coalesces_held_duplicate_requests_itself(self):
        with CacheFixtures() as fixtures:
            gate = fixtures.gate()
            key = fixtures.reply(0, "held", "reply", gate=gate)
            server = fixtures.mocks[0]
            before = snapshot(server)
            pending = [fixtures.start(lambda: post_request(server, request_body(0, "held")))
                       for _ in range(2)]
            semantic.wait_evidence(
                self, server, lambda: len(server.requests) == 2, pending[0],
                message="the fixture must record both equal HTTP attempts")
            semantic.assert_held(self, server, [gate])
            after = snapshot(server, settled=False)
            assert_window(self, before, after, [key, key])
            with self.assertRaises(AssertionError):
                assert_window(self, before, after, [key])
            self.assertTrue(all(not query.done.is_set() for query in pending))
            gate.set()
            self.assertEqual([response_content(query.result()) for query in pending], ["reply"] * 2)

    def test_unconfigured_http_request_is_a_fixture_failure_not_a_default_answer(self):
        with self.assertRaisesRegex(AssertionError, "unconfigured semantic request"):
            with CacheFixtures() as fixtures:
                with self.assertRaises(RemoteDisconnected):
                    post_request(fixtures.mocks[0], request_body(0, "unplanned"))

    def test_request_window_is_exact_append_only_and_counts_all_attempts(self):
        body = request_body(0, "same")
        key = request_key(body)
        before = Snapshot(Counter([key]), (body,))
        after = Snapshot(Counter([key, key, key]), (body, body, body))
        assert_window(self, before, after, [key, key])
        for expected in ([], [key], [key, key, key]):
            with self.subTest(expected=expected), self.assertRaises(AssertionError):
                assert_window(self, before, after, expected)
        with self.assertRaises(AssertionError):
            assert_window(self, after, before, [])
        changed = request_body(1, "same")
        with self.assertRaises(AssertionError):
            assert_window(self, before, Snapshot(Counter([key]), (changed,)), [])

    def test_cache_quota_shape_and_oracle_enforce_half_budget_without_recomputation(self):
        self.assertEqual(CACHE_PERSISTENT_BYTES, QUERY_MEMORY_BYTES // 2)
        self.assertEqual(CACHE_OVERFLOW_ROWS, 17)
        self.assertGreater(CACHE_OVERFLOW_ROWS * CACHE_RESULT_BYTES, CACHE_PERSISTENT_BYTES)
        self.assertLess(CACHE_OVERFLOW_ROWS * CACHE_RESULT_BYTES, QUERY_MEMORY_BYTES)
        bodies = [request_body(0, f"quota-{index}") for index in range(CACHE_OVERFLOW_ROWS + 1)]
        keys = [request_key(body) for body in bodies]
        before = Snapshot(Counter(), ())
        for count in (1, CACHE_OVERFLOW_ROWS - 1, CACHE_OVERFLOW_ROWS):
            assert_cache_quota_window(
                self, before, Snapshot(Counter(keys[:count]), tuple(bodies[:count])), keys)
        unexpected = request_body(1, "unplanned")
        invalid = (
            Snapshot(Counter(), ()),
            Snapshot(Counter([keys[0], keys[0]]), (bodies[0], bodies[0])),
            Snapshot(Counter(keys), tuple(bodies)),
            Snapshot(Counter([request_key(unexpected)]), (unexpected,)),
        )
        for evidence in invalid:
            with self.subTest(evidence=evidence), self.assertRaises(AssertionError):
                assert_cache_quota_window(self, before, evidence, keys)

    def test_snapshot_rejects_counter_mismatch_and_fixture_errors(self):
        server = mock.Mock()
        server.condition = threading.Condition()
        server.requests = [request_body(0, "p")]
        server.counts = Counter()
        server.fixture_errors = []
        with self.assertRaisesRegex(AssertionError, "HTTP attempts"):
            snapshot(server, settled=False)
        server.counts["p"] = 1
        self.assertEqual(sum(snapshot(server, settled=False).counts.values()), 1)
        server.fixture_errors = ["bad fixture"]
        with self.assertRaisesRegex(AssertionError, "bad fixture"):
            snapshot(server, settled=False)

    def test_held_window_excludes_finished_prior_queries_but_never_released_new_work(self):
        server = mock.Mock()
        server.condition = threading.Condition()
        body = request_body(0, "p")
        before = Snapshot(Counter([request_key(body)]), (body,))
        server.audit = [{"response": 1.0}, {"response": None}]
        gate = threading.Event()
        assert_held_window(self, server, before, gate)
        server.audit[-1]["response"] = 2.0
        with self.assertRaises(AssertionError):
            assert_held_window(self, server, before, gate)
        server.audit[-1]["response"] = None
        gate.set()
        with self.assertRaises(AssertionError):
            assert_held_window(self, server, before, gate)

    def test_kept_and_lifted_oracles_require_owned_markers_and_actual_topology(self):
        kept = lift.ExplainPlan(lift.fixture_plan([
            (0, 0, "HASH JOIN", ""), (1, 1, "SEMANTIC FILTER", ""),
            (2, 2, "TABLE FULL SCAN", "a"), (3, 1, "TABLE FULL SCAN", "b"),
        ], [(1, "semantic_placement=KEPT, crossed_inner_joins=0, lift_stop_reason=nonunique")]))
        assert_kept(self, kept)
        lifted = lift.ExplainPlan(lift.fixture_plan([
            (0, 0, "SEMANTIC FILTER", ""), (1, 1, "HASH JOIN", ""),
            (2, 2, "TABLE FULL SCAN", "a"), (3, 2, "TABLE FULL SCAN", "b"),
        ], [(0, "semantic_placement=LIFTED, crossed_inner_joins=1")]))
        lift.assert_lifted(self, lifted)
        with self.assertRaises(AssertionError):
            lift.assert_lifted(self, kept)
        with self.assertRaises(AssertionError):
            assert_kept(self, lifted)
        wrong_owner = lift.ExplainPlan(lift.fixture_plan([
            (0, 0, "SEMANTIC FILTER", ""), (1, 1, "HASH JOIN", ""),
            (2, 2, "TABLE FULL SCAN", "a"), (3, 2, "TABLE FULL SCAN", "b"),
        ], [(1, "semantic_placement=LIFTED, crossed_inner_joins=1")]))
        with self.assertRaises(AssertionError):
            lift.assert_lifted(self, wrong_owner)

    def test_ai_join_boundary_oracle_rejects_movement_and_missing_candidate_policy(self):
        rows = [
            (0, 0, "SEMANTIC JOIN", ""), (1, 1, "HASH JOIN", ""),
            (2, 2, "TABLE FULL SCAN", "a"), (3, 2, "TABLE FULL SCAN", "b"),
        ]
        annotation = "candidate_policy=ALL_SQL_CANDIDATES, pair_predicates([AI_JOIN(...)])"
        assert_candidate_boundary(self, lift.ExplainPlan(lift.fixture_plan(rows, [(0, annotation)])))
        for details in ([], [(1, annotation)], [(0, annotation +
                        ", semantic_placement=LIFTED, crossed_inner_joins=1")]):
            with self.subTest(details=details), self.assertRaises(AssertionError):
                assert_candidate_boundary(self, lift.ExplainPlan(lift.fixture_plan(rows, details)))
        moved = [(0, 0, "HASH JOIN", ""), (1, 1, "SEMANTIC JOIN", ""),
                 (2, 2, "TABLE FULL SCAN", "a"), (3, 1, "TABLE FULL SCAN", "b")]
        with self.assertRaises(AssertionError):
            assert_candidate_boundary(self, lift.ExplainPlan(lift.fixture_plan(moved, [(1, annotation)])))

    def test_px_evidence_requires_one_execution_four_nonempty_tasks_and_multiple_threads(self):
        records = [(1, 7, task, 100 + task % 2, 16) for task in range(4)]
        assert_px_preparations(self, records)
        invalid = (
            records[:3], [(1, 7, task, 100, 16) for task in range(4)],
            records + [(2, 7, 0, 101, 16)], records + [(1, 8, 0, 101, 16)],
            [(1, 7, task, 100 + task % 2, 0) for task in range(4)],
            [(1, 7, -1, 100 + task, 16) for task in range(4)],
        )
        for evidence in invalid:
            with self.subTest(evidence=evidence), self.assertRaises(AssertionError):
                assert_px_preparations(self, evidence)

    def test_sql_failure_oracle_rejects_success_wrong_error_and_empty_diagnostic(self):
        def fail(code, diagnostic):
            raise pymysql.OperationalError(code, diagnostic)
        assert_explicit_sql_error(self, 4019, lambda: fail(4019, "quota exhausted"))
        for operation in (lambda: None, lambda: fail(4012, "deadline"), lambda: fail(4019, "")):
            with self.subTest(operation=operation), self.assertRaises(AssertionError):
                assert_explicit_sql_error(self, 4019, operation)

    def test_pending_failure_is_never_a_successful_result(self):
        with CacheFixtures() as fixtures:
            def fail():
                raise pymysql.OperationalError(1317, "canceled")
            pending = fixtures.start(fail)
            pending.expect_sql_error(self, 1317)
            with self.assertRaises(pymysql.MySQLError):
                pending.result()

    def test_hint_builder_preserves_default_and_varchar_boolean_conventions(self):
        for cache in (None, "true", "false"):
            for enabled_lift in (None, "true", "false"):
                statement = sql_statement("a.id", cache=cache, enabled_lift=enabled_lift)
                for name, value in (("enable_semantic_cache", cache),
                                    ("enable_semantic_lift", enabled_lift)):
                    if value is None:
                        self.assertNotIn(name, statement)
                    else:
                        self.assertIn(f"OPT_PARAM('{name}', '{value}')", statement)
        for value in (True, False, 0, 1, "TRUE", "yes", ""):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    sql_statement("a.id", cache=value)
                with self.assertRaises(ValueError):
                    sql_statement("a.id", enabled_lift=value)
        for rows in (0, -1, True, "16"):
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                sql_statement("a.id", batch_rows=rows)

    def test_case_inventory_is_bounded_and_never_inherits_old_suite_discovery(self):
        self.assertEqual(SemanticCacheContracts.__bases__, (unittest.TestCase,))
        names = case_names(SemanticCacheContracts)
        self.assertEqual(len(names), SQL_CASE_COUNT)
        for old_class in (lift.SemanticLiftContracts, semantic.SemanticContracts,
                          joins.SemanticJoinContracts, files.FileContracts):
            self.assertFalse(set(names) & set(unittest.defaultTestLoader.getTestCaseNames(old_class)))
        self.assertEqual([case._testMethodName for case in build_suite(
            SemanticCacheContracts, names)], names)

    def test_cli_target_selection_is_validated_deduplicated_and_suite_local(self):
        first, second = case_names(SemanticCacheContracts)[:2]
        options = parse_options(["--binary", "/private/seekdb", "--case", first,
                                 "--case", second, "--case", first])
        self.assertEqual(selected_cases(SemanticCacheContracts, options.case), [first, second])
        self.assertEqual(build_suite(SemanticCacheContracts, [first]).countTestCases(), 1)
        own = "test_hint_builder_preserves_default_and_varchar_boolean_conventions"
        options = parse_options(["--self-test", "--case", own])
        self.assertEqual(selected_cases(SemanticCacheHarnessTests, options.case), [own])
        self.assertEqual(parse_options(["--socket", "/private/sql.sock"]).socket,
                         Path("/private/sql.sock"))

    def test_cli_rejects_unknown_conflicting_missing_and_wrong_mode_options(self):
        first = case_names(SemanticCacheContracts)[0]
        arguments = (
            [], ["--self-test", "--binary", "/private/seekdb"],
            ["--self-test", "--socket", "/private/sql.sock"],
            ["--binary", "/private/seekdb", "--socket", "/private/sql.sock"],
            ["--self-test", "--case", first], ["--list-cases", "--case", "test_old_suite"],
            ["--socket", "/private/sql.sock", "--case", "test_old_suite"],
            ["--server-log", "/private/seekdb.log", "--binary", "/private/seekdb"],
        )
        for command in arguments:
            with self.subTest(command=command), redirect_stderr(io.StringIO()), \
                    self.assertRaises(SystemExit) as caught:
                parse_options(command)
            self.assertEqual(caught.exception.code, 2)

    def test_cli_list_modes_start_no_database_or_http_fixtures(self):
        with mock.patch.object(files, "PrivateDatabase") as database, \
                mock.patch(__name__ + ".SocketDatabase") as socket_database, \
                mock.patch(__name__ + ".CacheFixtures") as fixtures, \
                mock.patch.object(pymysql, "connect") as connect:
            for arguments, test_class in ((["--list-cases"], SemanticCacheContracts),
                                           (["--self-test", "--list-cases"], SemanticCacheHarnessTests)):
                output = io.StringIO()
                with redirect_stdout(output):
                    main(arguments)
                self.assertEqual(output.getvalue().splitlines(), case_names(test_class))
            database.assert_not_called()
            socket_database.assert_not_called()
            fixtures.assert_not_called()
            connect.assert_not_called()

    def test_cli_invalid_source_paths_fail_before_allocating_resources(self):
        with mock.patch.object(files, "PrivateDatabase") as database, \
                mock.patch(__name__ + ".SocketDatabase") as socket_database, \
                mock.patch(__name__ + ".CacheFixtures") as fixtures:
            for arguments in (["--binary", str(Path(__file__).resolve())],
                              ["--socket", str(Path(__file__).resolve())]):
                with self.subTest(arguments=arguments), self.assertRaises(SystemExit):
                    main(arguments)
            database.assert_not_called()
            socket_database.assert_not_called()
            fixtures.assert_not_called()

    def test_socket_database_closes_only_its_owned_connection(self):
        with mock.patch.object(pymysql, "connect") as connect:
            database = SocketDatabase("/private/sql.sock")
            database.start()
            connection = connect.return_value
            self.assertEqual(connect.call_args.kwargs["unix_socket"], "/private/sql.sock")
            database.close()
            database.close()
            connection.close.assert_called_once_with()
            self.assertIsNone(database.connection)

    def test_sql_cleanup_targets_only_successfully_created_owned_objects(self):
        cursor = mock.MagicMock()
        connection = mock.Mock()
        connection.cursor.return_value.__enter__ = mock.Mock(return_value=cursor)
        connection.cursor.return_value.__exit__ = mock.Mock(return_value=False)
        with mock.patch.object(SemanticCacheContracts, "database", mock.Mock(connection=connection)), \
                mock.patch.object(SemanticCacheContracts, "schema", "owned_schema", create=True), \
                mock.patch.object(SemanticCacheContracts, "schema_created", True, create=True), \
                mock.patch.object(SemanticCacheContracts, "created_models", ["owned_model"], create=True), \
                mock.patch.object(SemanticCacheContracts, "created_endpoints",
                                  ["owned_endpoint"], create=True):
            SemanticCacheContracts.drop_owned_sql_objects()
        self.assertEqual(cursor.execute.call_args_list, [
            mock.call("CALL DBMS_AI_SERVICE.DROP_AI_MODEL_ENDPOINT(%s)", ("owned_endpoint",)),
            mock.call("CALL DBMS_AI_SERVICE.DROP_AI_MODEL(%s)", ("owned_model",)),
            mock.call("DROP DATABASE owned_schema"),
        ])

    def test_sql_failure_diagnostics_show_only_current_query_attempts(self):
        contract = SemanticCacheContracts(
            "test_default_and_off_retain_duplicate_demanded_work")
        contract.last_statement = "SELECT diagnostic"
        contract.last_parameters = None
        contract.last_rows = ((0, '"quoted"', 1),)
        contract.last_plan = lift.ExplainPlan(lift.fixture_plan([
            (0, 0, "SEMANTIC MAP", ""), (1, 1, "TABLE FULL SCAN", "cache_inputs"),
        ]))
        server = mock.Mock()
        server.condition = threading.Condition()
        server.requests = [request_body(0, "previous"), request_body(0, "current"),
                           request_body(0, "current")]
        server.fixture_errors = []
        contract.fixtures = mock.Mock(mocks=[server])
        contract.last_request_offsets = [1]
        output = io.StringIO()
        result = SQLDiagnosticResult(unittest.runner._WritelnDecorator(output), True, 2)
        result.write_sql_diagnostics(contract)
        text = output.getvalue()
        self.assertIn("SELECT diagnostic", text)
        self.assertIn("SEMANTIC MAP", text)
        self.assertIn("HTTP_attempts=2", text)
        self.assertIn("current", text)
        self.assertNotIn("previous", text)
        self.assertIn("returned_rows=1", text)

    def test_retained_logs_never_copy_storage_or_overwrite_existing_artifacts(self):
        with tempfile.TemporaryDirectory(prefix="semantic-cache-log-fixture-") as temporary:
            root = Path(temporary)
            base = root / "deployment"
            (base / "log").mkdir(parents=True)
            (base / "startup-0.log").write_text("startup")
            (base / "log/seekdb.log").write_text("database")
            (base / "log/seekdb.log.wf").write_text("warnings")
            (base / "datafile").write_text("not a log")
            destination = root / "retained"
            with redirect_stdout(io.StringIO()):
                copied = retain_deployment_logs(mock.Mock(base=base), destination)
            self.assertEqual({path.name for path in copied},
                             {"startup-0.log", "seekdb.log", "seekdb.log.wf"})
            self.assertFalse((destination / "datafile").exists())
            with self.assertRaises(FileExistsError):
                retain_deployment_logs(mock.Mock(base=base), destination)

    def test_cli_fail_fast_and_log_retention_are_explicit_and_binary_owned(self):
        options = parse_options(["--binary", "/private/seekdb", "--fail-fast",
                                 "--log-dir", "/private/new-logs"])
        self.assertTrue(options.fail_fast)
        self.assertEqual(options.log_dir, Path("/private/new-logs"))
        for arguments in (["--self-test", "--log-dir", "/private/new-logs"],
                          ["--socket", "/private/sql.sock", "--log-dir", "/private/new-logs"]):
            with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as caught:
                parse_options(arguments)
            self.assertEqual(caught.exception.code, 2)


def case_names(test_class):
    return sorted(name for name, method in test_class.__dict__.items()
                  if name.startswith("test_") and callable(method))


def selected_cases(test_class, requested):
    return list(dict.fromkeys(requested)) if requested else case_names(test_class)


def build_suite(test_class, selected):
    unknown = set(selected) - set(case_names(test_class))
    if unknown:
        raise ValueError("unknown suite-local case(s): " + ", ".join(sorted(unknown)))
    return unittest.TestSuite(test_class(name) for name in selected)


def parse_options(arguments=None):
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--binary", type=Path, help="Executable seekdb for a new private Unix-socket DB")
    source.add_argument("--socket", type=Path, help="Existing private Unix socket; never stop its server")
    parser.add_argument("--server-log", type=Path, help="Native seekdb.log for local PX in socket mode")
    parser.add_argument("--case", action="append", default=[], help="Suite-local method; repeat to select")
    parser.add_argument("--self-test", action="store_true", help="Run fixtures only, without SQL")
    parser.add_argument("--list-cases", action="store_true", help="List names without starting services")
    parser.add_argument("--fail-fast", action="store_true", help="Stop on the first selected-case failure")
    parser.add_argument("--log-dir", type=Path,
                        help="New destination for owned deployment logs; requires --binary")
    options = parser.parse_args(arguments)
    test_class = SemanticCacheHarnessTests if options.self_test else SemanticCacheContracts
    unknown = set(options.case) - set(case_names(test_class))
    if unknown:
        parser.error("unknown case(s): " + ", ".join(sorted(unknown)))
    if options.self_test and (options.binary is not None or options.socket is not None):
        parser.error("--self-test cannot be combined with --binary or --socket")
    if options.server_log is not None and options.socket is None:
        parser.error("--server-log requires --socket")
    if options.log_dir is not None and options.binary is None:
        parser.error("--log-dir requires --binary")
    if not options.self_test and not options.list_cases and options.binary is None and options.socket is None:
        parser.error("--binary or --socket is required unless --self-test or --list-cases is selected")
    return options


def run_suite(test_class, requested, *, failfast=False):
    suite = build_suite(test_class, selected_cases(test_class, requested))
    result_class = SQLDiagnosticResult if test_class is SemanticCacheContracts else unittest.TextTestResult
    if not unittest.TextTestRunner(
            verbosity=2, failfast=failfast, resultclass=result_class).run(suite).wasSuccessful():
        raise SystemExit(1)


def main(arguments=None):
    options = parse_options(arguments)
    test_class = SemanticCacheHarnessTests if options.self_test else SemanticCacheContracts
    if options.list_cases:
        for name in selected_cases(test_class, options.case):
            print(name)
    elif options.self_test:
        run_suite(test_class, options.case, failfast=options.fail_fast)
    else:
        if options.binary is not None:
            binary = options.binary.resolve()
            if not binary.is_file() or not os.access(binary, os.X_OK):
                raise SystemExit(f"--binary is not an executable file: {binary}")
            if options.log_dir is not None:
                options.log_dir = options.log_dir.resolve()
                if options.log_dir.exists():
                    raise SystemExit(f"--log-dir already exists; refusing to overwrite: {options.log_dir}")
            database = files.PrivateDatabase(binary)
            database.server_log = database.base / "log/seekdb.log"
        else:
            sql_socket = options.socket.resolve()
            if not sql_socket.is_socket():
                raise SystemExit(f"--socket is not a Unix socket: {sql_socket}")
            server_log = options.server_log.resolve() if options.server_log is not None else None
            if server_log is not None and not server_log.is_file():
                raise SystemExit(f"--server-log is not a file: {server_log}")
            database = SocketDatabase(sql_socket, server_log)
        try:
            print(f"Private semantic-cache deployment socket: {database.sql_socket}", flush=True)
            database.start()
            SemanticCacheContracts.database = database
            run_suite(SemanticCacheContracts, options.case, failfast=options.fail_fast)
        finally:
            try:
                try:
                    if options.log_dir is not None:
                        try:
                            database.stop()
                        finally:
                            retain_deployment_logs(database, options.log_dir)
                finally:
                    database.close()
            finally:
                SemanticCacheContracts.database = None


if __name__ == "__main__":
    main()
