"""Focused private SQL/mock contracts for opt-in semantic placement.

Run --self-test for fixtures only; --list-cases starts no services. SQL requires
--binary and always starts test_file_runtime.PrivateDatabase on its own Unix
socket. Repeat --case to select contracts. No existing database, real model,
GPU, external storage, or cloud credentials are used. Shutdown is the launcher's
normal SIGUSR1 path, with explicit cleanup of owned mocks, media and storage.

The positive baseline is SORT -> HASH JOIN -> SEMANTIC FILTER on the input:
100 inputs, ten matching unique keys, ten exact rows, 100 successful requests.
Omitted/'false' preserve it; 'true' must move the filter above HASH JOIN, annotate
that operator with semantic_placement=LIFTED and crossed_inner_joins=1, and make
exactly ten requests. Another case measures 1000 -> 100. Estimates and operator
IDs are not contracts. Every measured HTTP attempt must equal a logical
row/candidate evaluation: retries are not silently deduplicated.

Public proof uses EXPLAIN topology/markers and exact request counters. Internal
OPT_TRACE is a separate file tracer; information_schema.OPTIMIZER_TRACE JSON is
not a movement oracle and is never queried here.

Assumptions: current semops native FILE and AI_JOIN support; ordinary HASH/MERGE
INNER JOIN with complete, same-type, direct '=' keys against a nonnullable
primary key is the positive path. Opposite-side category columns are retained
to prevent join elimination. Both parameterized and materialized NLJ are blocked.
Raw semantic equality join keys remain unsupported (1235); NO_MERGE derived
model keys exercise an accepted result dependency. Text-only AI_JOIN payloads
are strict rule/left/right JSON strings, compared structurally, not by whitespace.
Nullable UNIQUE with duplicate NULLs and '<=>' is deliberately unsafe. Scalar,
ordinary CAST/JSON, guarded AND/OR/CASE, shared/carried and semantic sibling/source
fixtures require conservative no-lift. Attempted blocked stages must own a
nonempty lift_stop_reason and zero crossings; the reason's exact spelling is not
a contract. Volatile semantic inputs retain the resolver's existing 1235 error;
correlated inputs retain their already-late placement after decorrelation.
Sibling prompts are LONGTEXT to avoid old VARCHAR selectivity-time model calls.

Every SQL case runs by default, including the strict two-join chain requiring
crossed_inner_joins=2 and 24 -> 4 requests. MERGE consumes a matching last key,
so the old plan must demand all twelve rows and the lifted plan exactly two.
Invalid varchar option values must fail both EXPLAIN and SELECT with
OB_INVALID_ARGUMENT (MySQL 1210), without model work. Fixture tests and old-binary
baseline probes are not proof of enabled integration; rebuild first.
"""

import argparse
from collections import Counter
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass, field
from decimal import Decimal
import io
import os
from pathlib import Path
import re
import reprlib
import unittest
from unittest import mock

import pymysql

import test_file_runtime as files
import test_remote_file_runtime as remote
import test_semantic_join as joins
import test_semantic_runtime as semantic
from test_runtime_contracts import completion, sql_error


DATABASE = "ai_semantic_lift_contract_test"
MODEL_A = "semantic_lift_a"
MODEL_B = "semantic_lift_b"
BATCH_ROWS = semantic.BATCH_ROWS
CHAIN_CASE = "test_enabled_two_join_chain_crosses_twice"
INVALID_HINT_CASE = "test_invalid_semantic_lift_hint_values_are_errors"
INVALID_HINT_VALUES = ("", "not-a-boolean", "true,false")
JOIN_RULE = "Keep the declared left and right roles."
TABLES = {
    "lift_left": "id INT PRIMARY KEY, prompt LONGTEXT",
    "lift_right": "id INT PRIMARY KEY, keep_row INT, category VARCHAR(32)",
    "lift_big_keys": "id BIGINT PRIMARY KEY, keep_row INT, category VARCHAR(32)",
    "lift_semantic_right": (
        "id INT PRIMARY KEY, keep_row INT, category VARCHAR(32), prompt LONGTEXT"
    ),
    "lift_chain": "id INT PRIMARY KEY, keep_row INT, category VARCHAR(32)",
    "lift_keys": "id INT PRIMARY KEY, k1 INT NOT NULL, k2 INT NOT NULL, prompt LONGTEXT",
    "lift_composite": (
        "k1 INT NOT NULL, k2 INT NOT NULL, keep_row INT, category VARCHAR(32), "
        "PRIMARY KEY(k1,k2)"
    ),
    "lift_dupes": (
        "row_id INT PRIMARY KEY, join_id INT NOT NULL, keep_row INT, category VARCHAR(32)"
    ),
    "lift_nullable_left": "id INT PRIMARY KEY, join_id INT NULL, prompt LONGTEXT",
    "lift_nullable_right": (
        "row_id INT PRIMARY KEY, join_id INT NULL, keep_row INT, category VARCHAR(32), "
        "UNIQUE KEY nullable_key(join_id)"
    ),
    "lift_guards": "id INT PRIMARY KEY, prompt LONGTEXT, guard_flag INT NULL",
    "lift_files": "id INT PRIMARY KEY, prompt LONGTEXT, asset FILE",
}
SESSION_SETTINGS = (
    "ob_query_timeout", "ai_pipeline_slots", "ai_pipeline_memory_limit",
    "ai_media_parallel", "ai_media_max_bytes",
)


@dataclass
class PlanNode:
    operator_id: int
    operator: str
    name: str
    depth: int
    parent_id: int | None
    details: list[str] = field(default_factory=list)

    def matches(self, label):
        return self.operator == label or self.operator.startswith(label + " ")

    @property
    def annotation(self):
        return "\n".join(self.details)


class ExplainPlan:
    """Read only the tree and per-operator annotations from ordinary EXPLAIN."""

    def __init__(self, text):
        self.text = text
        self.nodes = {}
        ancestors = []
        for line in text.splitlines():
            row = re.match(r"^\s*\|?\s*(\d+)\s*\|([^|]+)\|([^|]*)\|", line)
            if row is None:
                continue
            cell = re.fullmatch(r"([^A-Za-z]*)([A-Za-z].*?)\s*", row[2])
            if cell is None:
                raise ValueError(f"unreadable EXPLAIN operator cell: {row[2]!r}")
            operator_id = int(row[1])
            if operator_id in self.nodes:
                raise ValueError(f"duplicate EXPLAIN operator ID: {operator_id}")
            depth = len(cell[1])
            while ancestors and ancestors[-1].depth >= depth:
                ancestors.pop()
            node = PlanNode(
                operator_id, cell[2].upper(), row[3].strip(), depth,
                ancestors[-1].operator_id if ancestors else None,
            )
            self.nodes[operator_id] = node
            ancestors.append(node)
        if not self.nodes:
            raise ValueError("EXPLAIN has no operator-tree rows")
        if sum(node.parent_id is None for node in self.nodes.values()) != 1:
            raise ValueError("EXPLAIN must contain one rooted operator tree")
        current = None
        for line in text.splitlines():
            detail = re.match(r"^\s*(\d+)\s*-\s*(.*)$", line)
            if detail is not None:
                operator_id = int(detail[1])
                if operator_id not in self.nodes:
                    raise ValueError(f"EXPLAIN details reference unknown operator {operator_id}")
                current = self.nodes[operator_id]
                current.details.append(detail[2])
            elif current is not None and line.strip():
                current.details.append(line.strip())

    def find(self, label):
        return [node for node in self.nodes.values() if node.matches(label)]

    def one(self, label):
        nodes = self.find(label)
        if len(nodes) != 1:
            raise AssertionError(f"expected one {label}, found {len(nodes)}:\n{self.text}")
        return nodes[0]

    def ancestors(self, node):
        result = []
        while node.parent_id is not None:
            node = self.nodes[node.parent_id]
            result.append(node)
        return result

    def is_above(self, ancestor, descendant):
        return ancestor in self.ancestors(descendant)


def render_explain(rows):
    return "\n".join(" ".join(str(value) for value in row if value is not None) for row in rows)


def assert_no_lift(test, plan):
    for node in plan.nodes.values():
        test.assertNotRegex(
            node.annotation, r"(?i)\bsemantic_placement\s*=\s*LIFTED\b",
            f"unexpected placement movement on {node.operator}:\n{plan.text}",
        )


def assert_lift_stop(test, stage):
    reasons = re.findall(r"(?i)\blift_stop_reason\s*=\s*([A-Za-z_][A-Za-z_0-9-]*)",
                         stage.annotation)
    test.assertEqual(len(reasons), 1,
                     f"attempted blocked stage lacks its own unique reason: {stage.annotation}")
    test.assertNotIn(reasons[0].lower(), ("none", "nil", "null"),
                     f"blocked stage has an empty/sentinel reason: {stage.annotation}")
    test.assertEqual(re.findall(r"(?i)\bcrossed_inner_joins\s*=\s*(\d+)\b", stage.annotation),
                     ["0"], f"blocked stage unexpectedly reports a crossing: {stage.annotation}")
    test.assertNotRegex(stage.annotation, r"(?i)\bsemantic_placement\s*=\s*LIFTED\b")


def assert_below_join(test, plan, *, label="SEMANTIC FILTER", join_label="HASH JOIN",
                      require_stop=False):
    stage = plan.one(label)
    join = plan.one(join_label)
    test.assertTrue(plan.is_above(join, stage),
                    f"{label} must remain below {join_label}:\n{plan.text}")
    assert_no_lift(test, plan)
    if require_stop:
        assert_lift_stop(test, stage)
    return stage


def assert_lifted(test, plan, *, crossed=1, label="SEMANTIC FILTER", join_labels=("HASH JOIN",)):
    stage = plan.one(label)
    test.assertRegex(stage.annotation, r"(?i)\bsemantic_placement\s*=\s*LIFTED\b",
                     f"the semantic operator lacks its own movement marker:\n{plan.text}")
    markers = re.findall(r"(?i)\bcrossed_inner_joins\s*=\s*(\d+)\b", stage.annotation)
    test.assertEqual(markers, [str(crossed)],
                     f"wrong/missing semantic crossing annotation:\n{plan.text}")
    inner_joins = [node for node in plan.nodes.values()
                   if node.matches("HASH JOIN") or node.matches("MERGE JOIN")]
    test.assertEqual(len(inner_joins), crossed, plan.text)
    for join in inner_joins:
        test.assertTrue(any(join.matches(join_label) for join_label in join_labels),
                        f"positive fixture changed its required join algorithm:\n{plan.text}")
        test.assertTrue(plan.is_above(stage, join),
                        f"marker alone is not evidence of movement above INNER JOIN:\n{plan.text}")
    test.assertFalse(any(node.matches("HASH JOIN") or node.matches("MERGE JOIN")
                         for node in plan.ancestors(stage)),
                     f"a positive case still has an uncrossed INNER JOIN:\n{plan.text}")
    return stage


def counter_delta(before, after):
    keys = before.keys() | after.keys()
    delta = Counter({key: after[key] - before[key] for key in keys if after[key] != before[key]})
    if any(count < 0 for count in delta.values()):
        raise AssertionError("request counters were reset during a measurement window")
    return delta


def assert_clean_work(test, before, after, expected, *, normalize=None):
    actual = counter_delta(before, after)
    if normalize is not None:
        normalized = Counter()
        for key, count in actual.items():
            normalized[normalize(key)] += count
        actual = normalized
    test.assertTrue(
        actual == expected,
        "clean-success HTTP attempts must equal logical evaluations, including multiplicity; "
        f"HTTP attempts={sum(actual.values())}, logical evaluations={sum(expected.values())}; "
        f"missing={reprlib.repr(dict(expected - actual))}, "
        f"extra/retries={reprlib.repr(dict(actual - expected))}",
    )


def pair_counter_key(encoded_content):
    prompt = files.strict_json(encoded_content)
    return joins.pair_key(joins.parse_join_prompt(prompt))


class PairScenarios(dict):
    def __setitem__(self, encoded_content, replies):
        super().__setitem__(pair_counter_key(encoded_content), replies)

    def get(self, encoded_content, default=None):
        return super().get(pair_counter_key(encoded_content), default)


@dataclass(frozen=True)
class ProviderSnapshot:
    counts: Counter
    requests: tuple


def provider_snapshot(server):
    with server.condition:
        if not server.condition.wait_for(lambda: server.active == 0, timeout=5):
            raise AssertionError("owned provider handlers did not finish")
        if server.fixture_errors:
            raise AssertionError(f"provider fixture failed: {server.fixture_errors!r}")
        counts = Counter(server.counts)
        requests = tuple(server.requests)
    if sum(counts.values()) != len(requests):
        raise AssertionError("provider request bodies and HTTP attempt counters disagree")
    return ProviderSnapshot(counts, requests)


def sql_statement(fields, *, enabled=None, from_sql="lift_left a JOIN lift_right b",
                  on="a.id=b.id", where=None, suffix="ORDER BY a.id",
                  hints="LEADING(a b) USE_HASH(b)"):
    if enabled not in (None, "true", "false"):
        raise ValueError("enable_semantic_lift must be omitted or a true/false varchar hint")
    option = "" if enabled is None else f" OPT_PARAM('enable_semantic_lift', '{enabled}')"
    predicates = "" if where is None else " WHERE " + where
    return (
        f"SELECT /*+ {hints} OPT_PARAM('rowsets_max_rows', {BATCH_ROWS}){option} */ "
        f"{fields} FROM {from_sql} ON {on}{predicates} {suffix}"
    )


class SemanticLiftContracts(unittest.TestCase):
    database = None
    models_created = False

    @classmethod
    def setUpClass(cls):
        with cls.database.connection.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE {DATABASE}")
            cursor.execute(f"USE {DATABASE}")
            for table, definition in TABLES.items():
                cursor.execute(f"CREATE TABLE {table} ({definition})")

    def setUp(self):
        self.cursor = self.database.connection.cursor()
        self.addCleanup(self.cursor.close)
        self.cursor.execute(f"USE {DATABASE}")
        self.original_roots = self.execute(
            "SELECT @@GLOBAL.ai_media_root, @@GLOBAL.ai_media_remote_root")[0]
        self.original_session = self.execute(
            "SELECT " + ", ".join("@@SESSION." + name for name in SESSION_SETTINGS))[0]
        self.addCleanup(self.restore_settings)
        self.cursor.execute("SET GLOBAL ai_media_root=''")
        self.cursor.execute("SET GLOBAL ai_media_remote_root=''")
        self.cursor.execute("SET ob_query_timeout=60000000, ai_pipeline_slots=2, "
                            "ai_pipeline_memory_limit=DEFAULT, ai_media_parallel=4, "
                            "ai_media_max_bytes=DEFAULT")
        for table in TABLES:
            self.cursor.execute(f"TRUNCATE TABLE {table}")
        self.fixtures = files.FileFixtures()
        self.addCleanup(self.fixtures.close)
        self.first, self.second = self.fixtures.mocks
        self.expected = {server: {} for server in self.fixtures.mocks}
        self.join_servers = set()
        self.media = None
        self.storage = None
        self.prepare_models()

    def restore_settings(self):
        try:
            self.cursor.execute("SET GLOBAL ai_media_root=%s", (self.original_roots[0],))
        finally:
            try:
                self.cursor.execute("SET GLOBAL ai_media_remote_root=%s", (self.original_roots[1],))
            finally:
                assignments = ", ".join(name + "=%s" for name in SESSION_SETTINGS)
                self.cursor.execute("SET " + assignments, self.original_session)

    def execute(self, statement, parameters=None):
        self.cursor.execute(statement, parameters)
        return self.cursor.fetchall()

    def prepare_models(self):
        if not type(self).models_created:
            for model, server in ((MODEL_A, self.first), (MODEL_B, self.second)):
                self.cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL(%s,%s)", (
                    model, files.descriptor_text({
                        "type": "completion", "model_name": "mock-semantic-lift",
                        "input_modalities": ["text", "image"],
                    }),
                ))
                self.cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT(%s,%s)", (
                    model + "_endpoint", files.descriptor_text({
                        "ai_model_name": model, "provider": "openai",
                        "request_model_name": "mock-semantic-lift",
                        "access_key": "private-semantic-lift-fixture-only",
                        "url": f"http://127.0.0.1:{server.server_port}/v1/chat/completions",
                    }),
                ))
            type(self).models_created = True
        for model, server in ((MODEL_A, self.first), (MODEL_B, self.second)):
            self.cursor.execute("CALL DBMS_AI_SERVICE.ALTER_AI_MODEL_ENDPOINT(%s,%s)", (
                model + "_endpoint", files.descriptor_text({
                    "provider": "openai",
                    "url": f"http://127.0.0.1:{server.server_port}/v1/chat/completions",
                }),
            ))

    def seed(self, content, result=True, *, second=False, boolean=True, assets=()):
        server = self.second if second else self.first
        self.expected[server][files.content_key(content)] = (content, tuple(assets), boolean)
        return self.fixtures.reply(server, content, result, boolean=boolean)

    def seed_pair(self, payload, result):
        self.expected[self.second][joins.pair_key(payload)] = (payload, (), True)
        return self.fixtures.reply(
            self.second, files.descriptor_text(payload), result, boolean=True)

    def load_basic(self, count=100, *, stride=10, invalid_after=None):
        prompts = [f"lift-{index}" for index in range(count)]
        self.cursor.executemany("INSERT INTO lift_left VALUES (%s,%s)", [
            (index, prompt if invalid_after is None or index < invalid_after else None)
            for index, prompt in enumerate(prompts)
        ])
        self.load_right(count, stride=stride)
        return prompts

    def load_right(self, count, *, stride=4):
        self.cursor.executemany("INSERT INTO lift_right VALUES (%s,%s,%s)", [
            (index, int(index % stride == 0), f"category-{index}") for index in range(count)
        ])

    def snapshots(self):
        return {server: provider_snapshot(server) for server in self.fixtures.mocks}

    def explain(self, statement, parameters=None):
        before = self.snapshots()
        storage_before = self.storage.counts() if self.storage is not None else None
        plan = ExplainPlan(render_explain(self.execute("EXPLAIN " + statement, parameters)))
        for server, snapshot in self.snapshots().items():
            self.assertEqual(snapshot.counts, before[server].counts,
                             "EXPLAIN unexpectedly evaluated a semantic expression")
        if self.storage is not None:
            self.assertEqual(self.storage.counts(), storage_before,
                             "EXPLAIN unexpectedly fetched a remote FILE")
        return plan

    def assert_provider_window(self, server, before, contents):
        after = provider_snapshot(server)
        is_join = server in self.join_servers
        expected = Counter(
            joins.pair_key(content) if is_join else files.content_key(content) for content in contents)
        assert_clean_work(self, before.counts, after.counts, expected,
                          normalize=pair_counter_key if is_join else None)
        for body in after.requests[len(before.requests):]:
            if is_join:
                content = joins.assert_join_request(self, body)
                key = joins.pair_key(content)
            else:
                content = body["messages"][-1]["content"]
                key = files.content_key(content)
            self.assertIn(key, self.expected[server], "provider received an unplanned row/candidate")
            wanted, assets, boolean = self.expected[server][key]
            if is_join:
                self.assertEqual(content, wanted, "AI_JOIN rule or left/right role changed")
            else:
                files.assert_content(self, body, wanted, assets=assets)
                if boolean:
                    semantic.assert_filter_request(self, body)
            wire = files.descriptor_text(body)
            for asset in assets:
                if isinstance(asset, remote.RemoteAsset):
                    self.assertNotIn(asset.uri, wire, "remote object URL leaked to the model")
                else:
                    self.assertNotIn(str(asset.path), wire, "local FILE path leaked to the model")
                    self.assertNotIn(asset.path.as_uri(), wire, "local FILE URI leaked to the model")

    def check_query(self, statement, expected_rows, *, first=(), second=(), parameters=None,
                    file_columns=(), gets=()):
        before = self.snapshots()
        storage_before = self.storage.counts() if self.storage is not None else None
        rows = self.execute(statement, parameters)
        self.assert_provider_window(self.first, before[self.first], first)
        self.assert_provider_window(self.second, before[self.second], second)
        if self.storage is not None:
            assert_clean_work(self, storage_before, self.storage.counts(),
                              Counter(("GET", target) for target in gets))
            self.assertFalse(self.storage.fixture_errors, self.storage.fixture_errors)
        if file_columns:
            normalized = []
            for row in rows:
                values = list(row)
                for column in file_columns:
                    self.assertIn(self.cursor.description[column][1], files.TEXT_WIRE_TYPES)
                    values[column] = None if row[column] is None else files.decode_descriptor(row[column])
                normalized.append(tuple(values))
            rows = tuple(normalized)
        self.assertEqual(rows, expected_rows, "semantic placement changed exact SQL values/multiplicity")
        return rows

    def reject_input(self, statement, *, gets=(), code=None):
        before = self.snapshots()
        storage_before = self.storage.counts() if self.storage is not None else None
        with self.assertRaises(pymysql.MySQLError) as caught:
            self.execute(statement)
        error = caught.exception
        self.assertIsInstance(error.args[0], int)
        self.assertTrue(error.args[1], "surviving invalid input must have an explicit diagnostic")
        self.assertNotIn(error.args[0], (1064, 1235, 1317, 4012),
                         "input validation failed only through syntax/unsupported/cancel/timeout")
        if code is not None:
            self.assertEqual(error.args[0], code, error.args)
        for server in self.fixtures.mocks:
            self.assert_provider_window(server, before[server], ())
        if self.storage is not None:
            assert_clean_work(self, storage_before, self.storage.counts(),
                              Counter(("GET", target) for target in gets))
            self.assertFalse(self.storage.fixture_errors, self.storage.fixture_errors)
        self.assertEqual(self.execute("SELECT 1"), ((1,),), "input error poisoned the private connection")

    def filter_query(self, *, enabled=None, **overrides):
        arguments = {
            "fields": "a.id,b.category", "enabled": enabled,
            "where": f"b.keep_row=1 AND AI_FILTER('{MODEL_A}',a.prompt)",
        }
        arguments.update(overrides)
        return sql_statement(**arguments)

    def test_default_and_false_preserve_100_requests(self):
        prompts = self.load_basic()
        for prompt in prompts:
            self.seed(prompt)
        expected = tuple((index, f"category-{index}") for index in range(0, 100, 10))
        for enabled in (None, "false"):
            with self.subTest(enabled=enabled):
                statement = self.filter_query(enabled=enabled)
                assert_below_join(self, self.explain(statement))
                self.check_query(statement, expected, first=prompts)

    def test_invalid_semantic_lift_hint_values_are_errors(self):
        for prompt in self.load_basic(12, stride=4):
            self.seed(prompt)
        valid = self.filter_query(enabled="true")
        for value in INVALID_HINT_VALUES:
            with self.subTest(value=value):
                statement = valid.replace(
                    "OPT_PARAM('enable_semantic_lift', 'true')",
                    f"OPT_PARAM('enable_semantic_lift', '{value}')")
                before = self.snapshots()
                for prefix in ("EXPLAIN ", ""):
                    error = sql_error(self, 1210, lambda: self.execute(prefix + statement))
                    self.assertTrue(error.args[1], "invalid hint must have an explicit diagnostic")
                for server in self.fixtures.mocks:
                    self.assert_provider_window(server, before[server], ())
                self.assertEqual(self.execute("SELECT 1"), ((1,),),
                                 "invalid hint poisoned the private connection")

    def test_enabled_unique_hash_join_either_operand_100_to_10(self):
        prompts = self.load_basic()
        for prompt in prompts:
            self.seed(prompt)
        retained = prompts[::10]
        expected = tuple((index, f"category-{index}") for index in range(0, 100, 10))
        for hints in ("LEADING(a b) USE_HASH(b)", "LEADING(b a) USE_HASH(a)"):
            with self.subTest(hints=hints):
                baseline = self.filter_query(hints=hints)
                assert_below_join(self, self.explain(baseline))
                self.check_query(baseline, expected, first=prompts)
                enabled = self.filter_query(enabled="true", hints=hints)
                assert_lifted(self, self.explain(enabled))
                self.check_query(enabled, expected, first=retained)

    def test_enabled_unique_hash_join_1000_to_100(self):
        prompts = self.load_basic(1000)
        for prompt in prompts:
            self.seed(prompt)
        expected = tuple((index, f"category-{index}") for index in range(0, 1000, 10))
        baseline = self.filter_query()
        assert_below_join(self, self.explain(baseline))
        self.check_query(baseline, expected, first=prompts)
        enabled = self.filter_query(enabled="true")
        assert_lifted(self, self.explain(enabled))
        self.check_query(enabled, expected, first=prompts[::10])

    def test_enabled_group_and_distinct_refresh_selected_plan_ancestors(self):
        prompts = self.load_basic()
        self.cursor.execute("UPDATE lift_right SET category=CONCAT('bucket-',MOD(id,3))")
        for index, prompt in enumerate(prompts):
            self.seed(prompt, index != 40)
        queries = (
            ("group", "b.category,COUNT(*) AS n,SUM(a.id) AS id_sum", "GROUP BY b.category",
             (("bucket-0", 4, Decimal(180)), ("bucket-1", 2, Decimal(80)),
              ("bucket-2", 3, Decimal(150)))),
            ("distinct", "DISTINCT b.category", "",
             (("bucket-0",), ("bucket-1",), ("bucket-2",))),
        )
        for kind, fields, suffix, expected in queries:
            for enabled in (None, "true"):
                with self.subTest(kind=kind, enabled=enabled):
                    statement = self.filter_query(enabled=enabled, fields=fields, suffix=suffix)
                    plan = self.explain(statement)
                    before = self.snapshots()
                    rows = self.execute(statement)
                    after = self.snapshots()
                    attempts = sum(counter_delta(
                        before[self.first].counts, after[self.first].counts).values())
                    print(f"{kind} enabled={enabled or 'default'}: model_requests={attempts}, "
                          f"rows={rows!r}, operators="
                          f"{[node.operator for node in plan.nodes.values()]!r}")
                    stage = (assert_below_join(self, plan) if enabled is None
                             else assert_lifted(self, plan))
                    boundaries = [
                        node for node in plan.nodes.values()
                        if "GROUP BY" in node.operator or "DISTINCT" in node.operator
                    ]
                    self.assertEqual(len(boundaries), 1, plan.text)
                    self.assertTrue(plan.is_above(boundaries[0], stage),
                                    f"semantic demand crossed the {kind} boundary:\n{plan.text}")
                    self.assert_provider_window(
                        self.first, before[self.first], prompts if enabled is None else prompts[::10])
                    self.assert_provider_window(self.second, before[self.second], ())
                    self.assertEqual(Counter(rows), Counter(expected),
                                     f"{kind} changed exact groups/values/multiplicity")

    def test_enabled_unique_merge_join_12_to_2(self):
        prompts = self.load_basic(12, stride=4)
        self.cursor.execute("UPDATE lift_right SET keep_row=(id=0 OR id=11)")
        for prompt in prompts:
            self.seed(prompt)
        expected = ((0, "category-0"), (11, "category-11"))
        for enabled in (None, "true"):
            with self.subTest(enabled=enabled):
                statement = self.filter_query(enabled=enabled, hints="LEADING(a b) USE_MERGE(b)")
                plan = self.explain(statement)
                if enabled is None:
                    assert_below_join(self, plan, join_label="MERGE JOIN")
                else:
                    assert_lifted(self, plan, join_labels=("MERGE JOIN",))
                self.check_query(statement, expected,
                                 first=prompts if enabled is None else [prompts[0], prompts[11]])

    def test_all_nested_loop_joins_stay_below(self):
        prompts = self.load_basic(12, stride=4)
        for prompt in prompts:
            self.seed(prompt)
        expected = tuple((index, f"category-{index}") for index in range(0, 12, 4))
        for materialized in (False, True):
            materialization_hint = (
                "USE_NL_MATERIALIZATION(b)" if materialized else "NO_USE_NL_MATERIALIZATION(b)")
            for enabled in (None, "true"):
                with self.subTest(materialized=materialized, enabled=enabled):
                    statement = self.filter_query(
                        enabled=enabled, hints="LEADING(a b) USE_NL(b) " + materialization_hint)
                    plan = self.explain(statement)
                    assert_below_join(self, plan, join_label="NESTED-LOOP JOIN",
                                      require_stop=enabled == "true")
                    join = plan.one("NESTED-LOOP JOIN")
                    if materialized:
                        self.assertEqual(len(plan.find("MATERIAL")), 1, plan.text)
                        self.assertRegex(join.annotation, r"(?i)\bnl_params_\s*\(\s*nil\s*\)")
                    else:
                        self.assertRegex(join.annotation, r"(?i)\bnl_params_\s*\(\s*\[")
                    self.check_query(statement, expected, first=prompts)

    def test_mismatched_equality_key_types_stay_below(self):
        prompts = self.load_basic(12, stride=4)
        self.cursor.executemany("INSERT INTO lift_big_keys VALUES (%s,%s,%s)", [
            (index, int(index % 4 == 0), f"category-{index}") for index in range(12)
        ])
        for prompt in prompts:
            self.seed(prompt)
        expected = tuple((index, f"category-{index}") for index in range(0, 12, 4))
        for enabled in (None, "true"):
            with self.subTest(enabled=enabled):
                statement = self.filter_query(enabled=enabled, from_sql="lift_left a JOIN lift_big_keys b")
                assert_below_join(self, self.explain(statement), require_stop=enabled == "true")
                self.check_query(statement, expected, first=prompts)

    def test_scalar_cast_and_json_recipes_stay_below(self):
        prompts = self.load_basic(12, stride=4)
        dates = [f"2026-10-13 00:00:{index:02d}" for index in range(12)]
        recipes = (
            ("UPPER(a.prompt)", prompts, [prompt.upper() for prompt in prompts]),
            ("CAST(CAST(a.prompt AS DATETIME) AS CHAR)", dates, dates),
            ("JSON_UNQUOTE(JSON_EXTRACT(a.prompt,'$.value'))",
             [files.descriptor_text({"value": prompt}) for prompt in prompts], prompts),
        )
        expected = tuple((index, f"category-{index}") for index in range(0, 12, 4))
        for recipe, inputs, demanded in recipes:
            self.cursor.executemany("UPDATE lift_left SET prompt=%s WHERE id=%s",
                                    [(prompt, index) for index, prompt in enumerate(inputs)])
            for prompt in demanded:
                self.seed(prompt)
            for enabled in (None, "true"):
                with self.subTest(recipe=recipe, enabled=enabled):
                    statement = self.filter_query(
                        enabled=enabled,
                        where=f"b.keep_row=1 AND AI_FILTER('{MODEL_A}',{recipe})")
                    assert_below_join(self, self.explain(statement), require_stop=enabled == "true")
                    self.check_query(statement, expected, first=demanded)

    def test_volatile_rejection_and_correlated_late_placement_preserved(self):
        prompts = self.load_basic(12, stride=4)
        self.cursor.executemany("INSERT INTO lift_chain VALUES (%s,%s,%s)",
                                [(index, 1, prompt) for index, prompt in enumerate(prompts)])
        for prompt in prompts:
            self.seed(prompt)
        expected = tuple((index, f"category-{index}") for index in range(0, 12, 4))
        for enabled in (None, "true"):
            with self.subTest(enabled=enabled, recipe="correlated"):
                statement = self.filter_query(
                    enabled=enabled, where=f"b.keep_row=1 AND AI_FILTER('{MODEL_A}',"
                    "(SELECT c.category FROM lift_chain c WHERE c.id=a.id))")
                plan = self.explain(statement)
                assert_no_lift(self, plan)
                self.assertTrue(plan.is_above(plan.one("SEMANTIC FILTER"), plan.one("HASH JOIN")),
                                plan.text)
                self.check_query(statement, expected, first=prompts[::4])
            with self.subTest(enabled=enabled, recipe="volatile resolver rejection"):
                statement = self.filter_query(
                    enabled=enabled, where=f"b.keep_row=1 AND AI_FILTER('{MODEL_A}',"
                    "IF(RAND()<0,'not-reached',a.prompt))")
                before = self.snapshots()
                for prefix in ("EXPLAIN ", ""):
                    error = sql_error(self, 1235, lambda: self.execute(prefix + statement))
                    self.assertIn("ordinary deterministic sql expressions", str(error).lower())
                for server in self.fixtures.mocks:
                    self.assert_provider_window(server, before[server], ())
                self.assertEqual(self.execute("SELECT 1"), ((1,),))

    def test_and_guard_inside_semantic_recipe_stays_below(self):
        prompts = self.load_basic(12, stride=3)
        for index, prompt in enumerate(prompts):
            self.seed(prompt, index % 2 == 0)
            self.seed(prompt, second=True)
        expected = ((3, "category-3"), (9, "category-9"))
        for enabled in (None, "true"):
            with self.subTest(enabled=enabled):
                # Keep the guard in one recipe; top-level WHERE AND is split into predicates.
                statement = self.filter_query(
                    enabled=enabled, where=f"b.keep_row=1 AND NOT (AI_FILTER('{MODEL_A}',a.prompt)"
                    f" AND AI_FILTER('{MODEL_B}',a.prompt))")
                assert_below_join(self, self.explain(statement), require_stop=enabled == "true")
                self.check_query(statement, expected, first=prompts, second=prompts[::2])

    def test_shared_carried_and_semantic_subtrees_stay_below(self):
        prompts = self.load_basic(12, stride=4)
        self.cursor.executemany("INSERT INTO lift_semantic_right VALUES (%s,%s,%s,%s)", [
            (index, int(index % 4 == 0), f"category-{index}", f"right-{index}")
            for index in range(12)
        ])
        for index, prompt in enumerate(prompts):
            self.seed(prompt)
            self.seed(prompt, "mapped:" + prompt, second=True, boolean=False)
            self.seed(f"right-{index}", second=True)
        retained = list(range(0, 12, 4))
        expected = tuple((index, f"category-{index}") for index in retained)
        for enabled in (None, "true"):
            with self.subTest(enabled=enabled, subtree="semantic sibling"):
                statement = self.filter_query(
                    enabled=enabled, from_sql="lift_left a JOIN lift_semantic_right b",
                    where=f"b.keep_row=1 AND AI_FILTER('{MODEL_A}',a.prompt)"
                    f" AND AI_FILTER('{MODEL_B}',b.prompt)")
                plan = self.explain(statement)
                assert_no_lift(self, plan)
                stages = plan.find("SEMANTIC FILTER")
                self.assertEqual(len(stages), 2, plan.text)
                for stage in stages:
                    self.assertTrue(plan.is_above(plan.one("HASH JOIN"), stage), plan.text)
                    if enabled == "true":
                        assert_lift_stop(self, stage)
                self.check_query(statement, expected, first=prompts,
                                 second=[f"right-{index}" for index in retained])
            with self.subTest(enabled=enabled, subtree="shared/carried alias"):
                statement = sql_statement(
                    f"a.id,b.category,AI_FILTER('{MODEL_A}',a.prompt) AS verdict",
                    enabled=enabled, where="b.keep_row=1", suffix="HAVING verdict ORDER BY a.id")
                plan = self.explain(statement)
                assert_below_join(self, plan, require_stop=enabled == "true")
                self.assertTrue(plan.is_above(plan.one("SEMANTIC MAP"), plan.one("HASH JOIN")),
                                plan.text)
                self.check_query(statement, tuple((*row, 1) for row in expected), first=prompts)
            with self.subTest(enabled=enabled, subtree="semantic source"):
                statement = self.filter_query(
                    enabled=enabled, fields="a.id,b.category,a.semantic_value",
                    from_sql=f"(SELECT id,prompt,AI_MAP('{MODEL_B}',prompt) AS semantic_value "
                    "FROM lift_left) a JOIN lift_right b",
                    hints="LEADING(a b) USE_HASH(b) NO_MERGE(a)")
                plan = self.explain(statement)
                stage = assert_below_join(self, plan, require_stop=enabled == "true")
                self.assertTrue(plan.is_above(stage, plan.one("SEMANTIC MAP")), plan.text)
                self.check_query(
                    statement, tuple((*row, "mapped:" + prompts[row[0]]) for row in expected),
                    first=prompts, second=prompts)

    def test_shared_projected_filter_having_without_order_or_limit_stays_below(self):
        prompts = self.load_basic(12, stride=4)
        for index, prompt in enumerate(prompts):
            self.seed(prompt, index != 8)
        expected = ((0, "category-0", 1), (4, "category-4", 1))
        for enabled in (None, "true"):
            with self.subTest(enabled=enabled):
                statement = sql_statement(
                    f"a.id,b.category,AI_FILTER('{MODEL_A}',a.prompt) AS verdict",
                    enabled=enabled, where="b.keep_row=1", suffix="HAVING verdict")
                self.assertNotIn("ORDER BY", statement.upper())
                self.assertNotIn("LIMIT", statement.upper())
                plan = self.explain(statement)
                self.assertFalse(any("SORT" in node.operator or node.matches("LIMIT")
                                     for node in plan.nodes.values()), plan.text)
                before = self.snapshots()
                rows = self.execute(statement)
                after = self.snapshots()
                attempts = sum(counter_delta(
                    before[self.first].counts, after[self.first].counts).values())
                print(f"shared HAVING without ORDER/LIMIT enabled={enabled or 'default'}: "
                      f"model_requests={attempts}, rows={rows!r}, operators="
                      f"{[node.operator for node in plan.nodes.values()]!r}")
                stage = assert_below_join(self, plan, require_stop=enabled == "true")
                self.assertTrue(plan.is_above(plan.one("SEMANTIC MAP"), stage), plan.text)
                self.assert_provider_window(self.first, before[self.first], prompts)
                self.assert_provider_window(self.second, before[self.second], ())
                self.assertEqual(Counter(rows), Counter(expected),
                                 "shared HAVING changed unordered SQL values/multiplicity")

    def load_composite(self):
        prompts = [f"composite-{index}" for index in range(20)]
        self.cursor.executemany("INSERT INTO lift_keys VALUES (%s,%s,%s,%s)", [
            (index, index // 2, index % 2, prompt) for index, prompt in enumerate(prompts)
        ])
        self.cursor.executemany("INSERT INTO lift_composite VALUES (%s,%s,%s,%s)", [
            (key, part, int(key % 3 == 0), f"composite-category-{key}-{part}")
            for key in range(10) for part in range(2)
        ])
        for prompt in prompts:
            self.seed(prompt)
        return prompts

    def test_complete_composite_unique_keys_lift(self):
        prompts = self.load_composite()
        retained = [index for index in range(20) if (index // 2) % 3 == 0]
        expected = tuple((index, f"composite-category-{index // 2}-{index % 2}")
                         for index in retained)
        for enabled in (None, "true"):
            with self.subTest(enabled=enabled):
                statement = self.filter_query(
                    enabled=enabled, from_sql="lift_keys a JOIN lift_composite b",
                    on="a.k1=b.k1 AND a.k2=b.k2")
                plan = self.explain(statement)
                if enabled is None:
                    assert_below_join(self, plan)
                else:
                    assert_lifted(self, plan)
                contents = prompts if enabled is None else [prompts[index] for index in retained]
                self.check_query(statement, expected, first=contents)

    def test_incomplete_composite_unique_keys_stay_below(self):
        prompts = self.load_composite()
        expected = tuple(
            (index, f"composite-category-{index // 2}-{part}")
            for index in range(20) if (index // 2) % 3 == 0 for part in range(2)
        )
        for enabled in (None, "true"):
            with self.subTest(enabled=enabled):
                statement = self.filter_query(
                    enabled=enabled, from_sql="lift_keys a JOIN lift_composite b", on="a.k1=b.k1",
                    suffix="ORDER BY a.id,b.k2")
                assert_below_join(self, self.explain(statement), require_stop=enabled == "true")
                self.check_query(statement, expected, first=prompts)

    def test_nonunique_row_multiplying_join_stays_below(self):
        prompts = self.load_basic(12, stride=4)
        self.cursor.executemany("INSERT INTO lift_dupes VALUES (%s,%s,%s,%s)", [
            (2 * index + copy, index, int(index % 4 == 0), f"duplicate-{index}-{copy}")
            for index in range(12) for copy in range(2)
        ])
        for prompt in prompts:
            self.seed(prompt)
        expected = tuple((index, f"duplicate-{index}-{copy}")
                         for index in range(0, 12, 4) for copy in range(2))
        for enabled in (None, "true"):
            with self.subTest(enabled=enabled):
                statement = self.filter_query(
                    enabled=enabled, from_sql="lift_left a JOIN lift_dupes b", on="a.id=b.join_id",
                    suffix="ORDER BY a.id,b.row_id")
                assert_below_join(self, self.explain(statement), require_stop=enabled == "true")
                self.check_query(statement, expected, first=prompts)

    def test_null_safe_and_nullable_unique_hazards_stay_below(self):
        prompts = self.load_basic(12, stride=4)
        for prompt in prompts:
            self.seed(prompt)
        expected = tuple((index, f"category-{index}") for index in range(0, 12, 4))
        for enabled in (None, "true"):
            with self.subTest(key="nonnullable primary key", enabled=enabled):
                statement = self.filter_query(enabled=enabled, on="a.id <=> b.id")
                assert_below_join(self, self.explain(statement), require_stop=enabled == "true")
                self.check_query(statement, expected, first=prompts)
        nullable_prompts = [f"nullable-{index}" for index in range(6)]
        left_keys = (None, None, 1, 2, 3, 4)
        self.cursor.executemany("INSERT INTO lift_nullable_left VALUES (%s,%s,%s)", [
            (index, key, nullable_prompts[index]) for index, key in enumerate(left_keys)
        ])
        right_rows = (
            (0, None, 1, "null-first"), (1, None, 1, "null-second"),
            (2, 1, 1, "key-one"), (3, 2, 0, "key-two"),
            (4, 3, 0, "key-three"), (5, 4, 0, "key-four"),
        )
        self.cursor.executemany("INSERT INTO lift_nullable_right VALUES (%s,%s,%s,%s)", right_rows)
        for prompt in nullable_prompts:
            self.seed(prompt)
        expected = (
            (0, "null-first"), (0, "null-second"), (1, "null-first"),
            (1, "null-second"), (2, "key-one"),
        )
        for enabled in (None, "true"):
            with self.subTest(key="nullable unique with duplicate NULL", enabled=enabled):
                statement = self.filter_query(
                    enabled=enabled, from_sql="lift_nullable_left a JOIN lift_nullable_right b",
                    on="a.join_id <=> b.join_id", suffix="ORDER BY a.id,b.row_id")
                assert_below_join(self, self.explain(statement), require_stop=enabled == "true")
                self.check_query(statement, expected, first=nullable_prompts)

    def test_case_and_or_guards_preserve_skipped_invalid_inputs(self):
        self.load_right(12, stride=3)
        self.cursor.executemany("INSERT INTO lift_guards VALUES (%s,%s,%s)", [
            (index, None if index % 2 == 0 else f"guard-{index}", index % 2) for index in range(12)
        ])
        demanded = [f"guard-{index}" for index in range(1, 12, 2)]
        for index in range(1, 12, 2):
            self.seed(f"guard-{index}", index % 4 != 3)
        expected = ((0, "category-0"), (6, "category-6"), (9, "category-9"))
        expressions = (
            f"CASE WHEN a.guard_flag=1 THEN AI_FILTER('{MODEL_A}',a.prompt) ELSE TRUE END",
            f"(a.guard_flag=0 OR AI_FILTER('{MODEL_A}',a.prompt))",
        )
        for expression in expressions:
            for enabled in (None, "true"):
                with self.subTest(expression=expression, enabled=enabled):
                    statement = self.filter_query(
                        enabled=enabled, from_sql="lift_guards a JOIN lift_right b",
                        where="b.keep_row=1 AND " + expression)
                    assert_below_join(self, self.explain(statement), require_stop=enabled == "true")
                    self.check_query(statement, expected, first=demanded)

    def test_three_valued_projection_preserves_results_and_multiplicity(self):
        prompts = self.load_basic(12, stride=3)
        retained = list(range(0, 12, 3))
        for index in retained:
            self.seed(prompts[index], index % 6 == 0)
        expected = tuple(
            (index, f"category-{index}", 1 if index % 6 == 0 else None,
             None if index % 6 == 0 else 0) for index in retained
        )
        for enabled in (None, "true"):
            with self.subTest(enabled=enabled):
                statement = sql_statement(
                    f"a.id,b.category,NULL OR AI_FILTER('{MODEL_A}',a.prompt),"
                    f"NULL AND AI_FILTER('{MODEL_A}',a.prompt)",
                    enabled=enabled, where="b.keep_row=1")
                plan = self.explain(statement)
                assert_no_lift(self, plan)
                self.assertTrue(plan.is_above(plan.one("SEMANTIC MAP"), plan.one("HASH JOIN")),
                                plan.text)
                self.check_query(statement, expected, first=[prompts[index] for index in retained] * 2)

    def test_model_result_join_key_is_not_bypassed(self):
        prompts = self.load_basic(12, stride=4)
        for index, prompt in enumerate(prompts):
            self.seed(prompt, str(11 - index), second=True, boolean=False)
        expected = ((3, "category-8"), (7, "category-4"), (11, "category-0"))
        derived = (
            f"(SELECT id,CAST(AI_MAP('{MODEL_B}',prompt) AS UNSIGNED) AS join_key "
            "FROM lift_left) a JOIN lift_right b"
        )
        for enabled in (None, "true"):
            with self.subTest(enabled=enabled):
                statement = sql_statement(
                    "a.id,b.category", enabled=enabled, from_sql=derived, on="a.join_key=b.id",
                    where="b.keep_row=1", hints="LEADING(a b) USE_HASH(b) NO_MERGE(a)")
                plan = self.explain(statement)
                stage = assert_below_join(self, plan, label="SEMANTIC MAP")
                self.assertTrue(plan.is_above(plan.one("SUBPLAN SCAN"), stage), plan.text)
                self.check_query(statement, expected, second=prompts)
                unsupported = sql_statement(
                    "a.id,b.category", enabled=enabled,
                    on=f"CAST(AI_MAP('{MODEL_B}',a.prompt) AS UNSIGNED)=b.id",
                    where="b.keep_row=1")
                before = self.snapshots()
                for prefix in ("EXPLAIN ", ""):
                    error = sql_error(self, 1235, lambda: self.execute(prefix + unsupported))
                    self.assertIn("semantic calls in equality join keys", str(error).lower())
                for server in self.fixtures.mocks:
                    self.assert_provider_window(server, before[server], ())
                self.assertEqual(self.execute("SELECT 1"), ((1,),))

    def test_limit_filter_order_and_projection_map_late_demand(self):
        prompts = self.load_basic()
        for index, prompt in enumerate(prompts):
            self.seed(prompt, index % 30 != 0)
            self.seed(prompt, "mapped:" + prompt, second=True, boolean=False)
        accepted = [index for index in reversed(range(0, 100, 10)) if index % 30 != 0]
        expected_filter = tuple((index, f"category-{index}") for index in accepted[1:4])
        retained_map = list(reversed(range(0, 100, 10)))[2:5]
        expected_map = tuple((index, f"category-{index}", "mapped:" + prompts[index])
                             for index in retained_map)
        for enabled in (None, "true"):
            with self.subTest(enabled=enabled, demand="WHERE filter before LIMIT"):
                statement = self.filter_query(
                    enabled=enabled, suffix="ORDER BY a.id DESC LIMIT 3 OFFSET 1")
                plan = self.explain(statement)
                stage = (assert_below_join(self, plan) if enabled is None
                         else assert_lifted(self, plan))
                self.assertTrue(plan.is_above(plan.one("LIMIT"), stage), plan.text)
                self.check_query(statement, expected_filter,
                                 first=prompts if enabled is None else prompts[::10])
            with self.subTest(enabled=enabled, demand="ordinary projection MAP after LIMIT"):
                statement = sql_statement(
                    f"a.id,b.category,AI_MAP('{MODEL_B}',a.prompt)", enabled=enabled,
                    where="b.keep_row=1", suffix="ORDER BY a.id DESC LIMIT 3 OFFSET 2")
                plan = self.explain(statement)
                assert_no_lift(self, plan)
                self.assertTrue(plan.is_above(plan.one("SEMANTIC MAP"), plan.one("LIMIT")), plan.text)
                self.check_query(statement, expected_map,
                                 second=[prompts[index] for index in retained_map])

    def test_aggregate_boundary_retains_group_level_demand(self):
        self.load_basic(12, stride=2)
        self.cursor.execute("UPDATE lift_right SET category=CONCAT('group-',MOD(id,3))")
        group_prompts = [f"group:group-{index}:2" for index in range(3)]
        for index, prompt in enumerate(group_prompts):
            self.seed(prompt, index != 1)
            self.seed(prompt, "summary:" + prompt, second=True, boolean=False)
        expected = (("group-0", 2, "summary:group:group-0:2"),
                    ("group-2", 2, "summary:group:group-2:2"))
        for enabled in (None, "true"):
            with self.subTest(enabled=enabled):
                statement = sql_statement(
                    f"b.category,COUNT(*) AS n,AI_MAP('{MODEL_B}',"
                    "CONCAT('group:',b.category,':',COUNT(*)))",
                    enabled=enabled, where="b.keep_row=1",
                    suffix=f"GROUP BY b.category HAVING AI_FILTER('{MODEL_A}',"
                    "CONCAT('group:',b.category,':',COUNT(*))) ORDER BY b.category")
                plan = self.explain(statement)
                assert_no_lift(self, plan)
                groups = [node for node in plan.nodes.values() if "GROUP BY" in node.operator]
                self.assertEqual(len(groups), 1, plan.text)
                self.assertTrue(plan.is_above(plan.one("SEMANTIC FILTER"), groups[0]), plan.text)
                self.assertTrue(plan.is_above(plan.one("SEMANTIC MAP"), groups[0]), plan.text)
                self.check_query(statement, expected, first=group_prompts,
                                 second=[group_prompts[index] for index in (0, 2)])

    def test_ai_join_roles_and_candidate_boundary_retained(self):
        prompts = self.load_basic(12, stride=4)
        for prompt in prompts:
            self.seed(prompt)
        self.second.scenarios = PairScenarios()
        self.join_servers.add(self.second)
        expected = ((0, "category-0"), (8, "category-8"))
        for reverse in (False, True):
            payloads = []
            for index in range(0, 12, 4):
                left, right = prompts[index], f"category-{index}"
                if reverse:
                    left, right = right, left
                payload = joins.pair_payload(JOIN_RULE, left, right)
                payloads.append(payload)
                self.seed_pair(payload, index != 4)
            operands = "b.category,a.prompt" if reverse else "a.prompt,b.category"
            for enabled in (None, "true"):
                with self.subTest(reverse=reverse, enabled=enabled):
                    statement = self.filter_query(
                        enabled=enabled,
                        on=f"a.id=b.id AND AI_JOIN('{MODEL_B}','{JOIN_RULE}',{operands})")
                    plan = self.explain(statement)
                    stage = assert_below_join(self, plan, require_stop=enabled == "true")
                    candidate = plan.one("SEMANTIC JOIN")
                    self.assertTrue(plan.is_above(candidate, stage), plan.text)
                    self.assertRegex(candidate.annotation,
                                     r"(?i)\bcandidate_policy\s*=\s*ALL_SQL_CANDIDATES\b")
                    self.assertIn("pair_predicates", candidate.annotation.lower())
                    self.check_query(statement, expected, first=prompts, second=payloads)

    def test_ordinary_prefilters_remain_first(self):
        prompts = self.load_basic(invalid_after=50)
        for prompt in prompts[:50]:
            self.seed(prompt)
        expected = tuple((index, f"category-{index}") for index in range(0, 50, 10))
        for enabled in (None, "true"):
            with self.subTest(enabled=enabled):
                statement = self.filter_query(
                    enabled=enabled, where=f"a.id<50 AND b.keep_row=1 AND "
                    f"AI_FILTER('{MODEL_A}',a.prompt)")
                plan = self.explain(statement)
                if enabled is None:
                    assert_below_join(self, plan)
                else:
                    assert_lifted(self, plan)
                self.check_query(statement, expected,
                                 first=prompts[:50] if enabled is None else prompts[:50:10])
        self.cursor.execute("UPDATE lift_right SET keep_row=(id=50)")
        invalid = self.filter_query(
            enabled="true", where=f"a.id<=50 AND b.keep_row=1 AND AI_FILTER('{MODEL_A}',a.prompt)")
        assert_lifted(self, self.explain(invalid))
        self.reject_input(invalid, code=1210)

    def prepare_media(self, *, remote_storage=False):
        self.media = files.MediaFiles()
        self.addCleanup(self.media.close)
        self.cursor.execute("SET GLOBAL ai_media_root=%s", (str(self.media.root),))
        if remote_storage:
            self.storage = remote.StorageServer()
            self.addCleanup(self.storage.close)
            self.cursor.execute("SET GLOBAL ai_media_remote_root=%s", (self.storage.root,))

    def load_file_values(self, assets):
        self.load_right(len(assets), stride=4)
        self.cursor.executemany("INSERT INTO lift_files VALUES (%s,%s,TO_FILE(%s))", [
            (index, f"file-{index}", files.descriptor_text(asset.descriptor()))
            for index, asset in enumerate(assets)
        ])
        contents = []
        for index, asset in enumerate(assets):
            content = [files.text_block(f"file-{index}"), asset.block()]
            contents.append(content)
            self.seed(content, assets=[asset])
        return contents

    def file_query(self, *, enabled=None):
        return self.filter_query(
            enabled=enabled, fields="a.id,b.category,a.asset",
            from_sql="lift_files a JOIN lift_right b",
            where=f"b.keep_row=1 AND AI_FILTER('{MODEL_A}',(a.prompt,a.asset))")

    def test_local_file_pruning_and_surviving_input_validation(self):
        self.prepare_media()
        assets = [self.media.asset("png", 100 + index) for index in range(12)]
        contents = self.load_file_values(assets)
        retained = list(range(0, 12, 4))
        expected = tuple((index, f"category-{index}", assets[index].descriptor()) for index in retained)
        for enabled in (None, "true"):
            with self.subTest(enabled=enabled):
                statement = self.file_query(enabled=enabled)
                plan = self.explain(statement)
                if enabled is None:
                    assert_below_join(self, plan)
                else:
                    assert_lifted(self, plan)
                self.check_query(statement, expected, file_columns=(2,),
                                 first=contents if enabled is None else [contents[i] for i in retained])
        missing = files.descriptor(self.media.root / "rejected-missing.png", mime_type="image/png")
        corrupt = self.media.corrupt("png").descriptor()
        for index in range(12):
            if index not in retained:
                value = missing if index % 2 else corrupt
                self.cursor.execute("UPDATE lift_files SET asset=TO_FILE(%s) WHERE id=%s",
                                    (files.descriptor_text(value), index))
        statement = self.file_query(enabled="true")
        assert_lifted(self, self.explain(statement))
        self.check_query(statement, expected, first=[contents[index] for index in retained],
                         file_columns=(2,))
        self.cursor.execute("UPDATE lift_right SET keep_row=(id=1)")
        assert_lifted(self, self.explain(statement))
        self.reject_input(statement)
        self.cursor.execute("UPDATE lift_right SET keep_row=(id=2)")
        assert_lifted(self, self.explain(statement))
        self.reject_input(statement)

    def test_remote_file_prunes_gets_and_validates_surviving_bytes(self):
        self.prepare_media(remote_storage=True)
        assets = [self.storage.asset("png", 200 + index, target=(
            f"/media/lift%20{index}.png?signature=local%2B%2f&repeat=a+b&repeat=%252F"
        )) for index in range(12)]
        contents = self.load_file_values(assets)
        retained = list(range(0, 12, 4))
        expected = tuple((index, f"category-{index}", assets[index].descriptor()) for index in retained)
        for enabled in (None, "true"):
            with self.subTest(enabled=enabled):
                statement = self.file_query(enabled=enabled)
                plan = self.explain(statement)
                if enabled is None:
                    assert_below_join(self, plan)
                else:
                    assert_lifted(self, plan)
                demanded = list(range(12)) if enabled is None else retained
                self.check_query(statement, expected, file_columns=(2,),
                                 first=[contents[index] for index in demanded],
                                 gets=[assets[index].target for index in demanded])
        corrupt = self.storage.asset("png", 300, target="/media/rejected-corrupt.png?sig=local",
                                     raw=files.png_bytes(300)[:8])
        self.cursor.execute("UPDATE lift_files SET asset=TO_FILE(%s) WHERE id=1",
                            (files.descriptor_text(corrupt.descriptor()),))
        statement = self.file_query(enabled="true")
        assert_lifted(self, self.explain(statement))
        self.check_query(statement, expected, first=[contents[index] for index in retained],
                         gets=[assets[index].target for index in retained], file_columns=(2,))
        self.cursor.execute("UPDATE lift_right SET keep_row=(id=1)")
        assert_lifted(self, self.explain(statement))
        self.reject_input(statement, gets=[corrupt.target])

    def test_subquery_limit_boundary_retained(self):
        prompts = self.load_basic(12, stride=3)
        for prompt in prompts:
            self.seed(prompt)
        derived = (
            f"(SELECT id,prompt FROM lift_left WHERE AI_FILTER('{MODEL_A}',prompt) "
            "ORDER BY id LIMIT 8) a JOIN lift_right b"
        )
        expected = ((0, "category-0"), (3, "category-3"), (6, "category-6"))
        for enabled in (None, "true"):
            with self.subTest(enabled=enabled):
                statement = sql_statement(
                    "a.id,b.category", enabled=enabled, from_sql=derived, where="b.keep_row=1",
                    hints="LEADING(a b) USE_HASH(b) NO_MERGE(a)")
                plan = self.explain(statement)
                stage = assert_below_join(self, plan)
                self.assertTrue(plan.is_above(plan.one("SUBPLAN SCAN"), stage), plan.text)
                self.assertTrue(plan.is_above(plan.one("LIMIT"), stage), plan.text)
                self.check_query(statement, expected, first=prompts)

    def test_outer_join_boundary_retained(self):
        prompts = self.load_basic(12, stride=4)
        for prompt in prompts:
            self.seed(prompt)
        expected = tuple((index, f"category-{index}" if index % 4 == 0 else None)
                         for index in range(12))
        for enabled in (None, "true"):
            with self.subTest(enabled=enabled):
                statement = self.filter_query(
                    enabled=enabled, from_sql="lift_left a LEFT JOIN lift_right b",
                    on="a.id=b.id AND b.keep_row=1", where=f"AI_FILTER('{MODEL_A}',a.prompt)")
                assert_below_join(self, self.explain(statement), join_label="HASH OUTER JOIN",
                                  require_stop=enabled == "true")
                self.check_query(statement, expected, first=prompts)

    def test_enabled_two_join_chain_crosses_twice(self):
        prompts = self.load_basic(24, stride=2)
        self.cursor.executemany("INSERT INTO lift_chain VALUES (%s,%s,%s)", [
            (index, int(index % 3 == 0), f"chain-category-{index}") for index in range(24)
        ])
        for prompt in prompts:
            self.seed(prompt)
        expected = tuple((index, f"category-{index}", f"chain-category-{index}")
                         for index in range(0, 24, 6))
        for enabled in (None, "true"):
            with self.subTest(enabled=enabled):
                statement = self.filter_query(
                    enabled=enabled, fields="a.id,b.category,c.category",
                    from_sql="lift_left a JOIN lift_right b ON a.id=b.id JOIN lift_chain c",
                    on="b.id=c.id",
                    where=f"b.keep_row=1 AND c.keep_row=1 AND AI_FILTER('{MODEL_A}',a.prompt)",
                    hints="LEADING(a b c) USE_HASH(b) USE_HASH(c)")
                plan = self.explain(statement)
                if enabled is None:
                    assert_no_lift(self, plan)
                    stage = plan.one("SEMANTIC FILTER")
                    self.assertEqual(len(plan.find("HASH JOIN")), 2, plan.text)
                    self.assertTrue(all(plan.is_above(join, stage) for join in plan.find("HASH JOIN")),
                                    plan.text)
                else:
                    assert_lifted(self, plan, crossed=2)
                self.check_query(statement, expected,
                                 first=prompts if enabled is None else prompts[::6])


def fixture_plan(rows, details=(), *, unicode_tree=False, leading_pipe=True):
    lines = []
    for operator_id, depth, operator, name in rows:
        prefix = " " * (depth * 2)
        if unicode_tree and depth:
            prefix = " " * (2 * depth - 2) + "\u2514\u2500"
        pipe = "|" if leading_pipe else ""
        lines.append(f"{pipe}{operator_id}|{prefix}{operator}|{name}|999|888|")
    lines.extend(f"  {operator_id} - {annotation}" for operator_id, annotation in details)
    return "\n".join(lines)


class SemanticLiftHarnessTests(unittest.TestCase):
    def test_blocked_stage_oracle_requires_owned_reason_and_zero_crossings(self):
        rows = (
            (3, 0, "HASH JOIN", ""), (11, 1, "SEMANTIC FILTER", ""),
            (2, 2, "TABLE FULL SCAN", "a"), (8, 1, "TABLE FULL SCAN", "b"),
        )
        marker = "semantic_placement=KEPT, crossed_inner_joins=0, lift_stop_reason=unsafe_recipe"
        assert_below_join(self, ExplainPlan(fixture_plan(rows, ((11, marker),))), require_stop=True)
        invalid_details = (
            (), ((3, marker),),
            ((11, marker.replace("unsafe_recipe", "nil")),),
            ((11, marker.replace("unsafe_recipe", "")),),
            ((11, marker.replace("crossed_inner_joins=0", "crossed_inner_joins=1")),),
            ((11, marker + ", lift_stop_reason=another_reason"),),
        )
        for details in invalid_details:
            with self.subTest(details=details), self.assertRaises(AssertionError):
                assert_below_join(self, ExplainPlan(fixture_plan(rows, details)), require_stop=True)

    def test_merge_movement_oracle_never_relaxes_hash_positive_expectations(self):
        rows = (
            (3, 0, "SEMANTIC FILTER", ""), (11, 1, "MERGE JOIN", ""),
            (2, 2, "TABLE FULL SCAN", "a"), (8, 2, "TABLE FULL SCAN", "b"),
        )
        marker = "semantic_placement=LIFTED, crossed_inner_joins=1"
        plan = ExplainPlan(fixture_plan(rows, ((3, marker),)))
        assert_lifted(self, plan, join_labels=("MERGE JOIN",))
        with self.assertRaises(AssertionError):
            assert_lifted(self, plan)
        below = (
            (3, 0, "MERGE JOIN", ""), (11, 1, "SEMANTIC FILTER", ""),
            (2, 2, "TABLE FULL SCAN", "a"), (8, 1, "TABLE FULL SCAN", "b"),
        )
        with self.assertRaises(AssertionError):
            assert_lifted(self, ExplainPlan(fixture_plan(below, ((11, marker),))),
                          join_labels=("MERGE JOIN",))

    def test_invalid_hint_sql_contract_never_accepts_ignored_values_or_wrong_errors(self):
        contract = SemanticLiftContracts(INVALID_HINT_CASE)
        servers = (object(), object())
        contract.fixtures = mock.Mock(mocks=servers)
        contract.load_basic = mock.Mock(return_value=[])
        contract.snapshots = mock.Mock(return_value={
            server: ProviderSnapshot(Counter(), ()) for server in servers
        })
        contract.assert_provider_window = mock.Mock()

        def reject_hint(statement):
            if statement == "SELECT 1":
                return ((1,),)
            raise pymysql.OperationalError(1210, "invalid enable_semantic_lift")

        with mock.patch.object(contract, "execute", side_effect=reject_hint) as execute:
            contract.test_invalid_semantic_lift_hint_values_are_errors()
            rejected = [call.args[0] for call in execute.call_args_list if call.args[0] != "SELECT 1"]
            self.assertEqual(len(rejected), 2 * len(INVALID_HINT_VALUES))
            for value in INVALID_HINT_VALUES:
                matching = [statement for statement in rejected
                            if f"OPT_PARAM('enable_semantic_lift', '{value}')" in statement]
                self.assertEqual(len(matching), 2)
                self.assertEqual(sum(statement.startswith("EXPLAIN ") for statement in matching), 1)
        self.assertEqual(contract.assert_provider_window.call_count, 2 * len(INVALID_HINT_VALUES))
        for side_effect in (
                lambda _statement: ((1,),),
                pymysql.OperationalError(1235, "wrong rejection")):
            with self.subTest(side_effect=side_effect), \
                    mock.patch.object(contract, "execute", side_effect=side_effect):
                with self.assertRaises(AssertionError):
                    contract.test_invalid_semantic_lift_hint_values_are_errors()

    def test_plan_tree_parses_ascii_unicode_and_arbitrary_operator_ids(self):
        rows = (
            (91, 0, "SORT", ""), (24, 1, "HASH JOIN", ""),
            (700, 2, "SEMANTIC FILTER", ""), (8, 3, "TABLE FULL SCAN", "a"),
            (5, 2, "TABLE FULL SCAN", "b"),
        )
        for unicode_tree in (False, True):
            for leading_pipe in (False, True):
                with self.subTest(unicode_tree=unicode_tree, leading_pipe=leading_pipe):
                    plan = ExplainPlan(fixture_plan(
                        rows, unicode_tree=unicode_tree, leading_pipe=leading_pipe))
                    assert_below_join(self, plan)
                    self.assertEqual(plan.one("SEMANTIC FILTER").parent_id, 24)
                    self.assertEqual([node.name for node in plan.find("TABLE FULL SCAN")], ["a", "b"])
                    self.assertFalse(plan.is_above(plan.one("SEMANTIC FILTER"), plan.nodes[5]))

    def test_plan_oracle_requires_shape_and_markers_on_the_semantic_operator(self):
        lifted = (
            (13, 0, "SORT", ""), (27, 1, "SEMANTIC FILTER", ""),
            (89, 2, "HASH JOIN", ""), (4, 3, "TABLE FULL SCAN", "a"),
            (99, 3, "TABLE FULL SCAN", "b"),
        )
        marker = "semantic_placement=LIFTED, crossed_inner_joins=1"
        assert_lifted(self, ExplainPlan(fixture_plan(lifted, ((27, marker),))))
        invalid_details = (
            (), ((89, marker),), ((13, marker),),
            ((27, "semantic_placement=LIFTED"),),
            ((27, "semantic_placement=LIFTED, crossed_inner_joins=0"),),
            ((27, "semantic_placement=ORIGINAL, crossed_inner_joins=1"),),
            ((27, marker + ", crossed_inner_joins=1"),),
        )
        for details in invalid_details:
            with self.subTest(details=details), self.assertRaises(AssertionError):
                assert_lifted(self, ExplainPlan(fixture_plan(lifted, details)))
        unmoved = (
            (13, 0, "SORT", ""), (89, 1, "HASH JOIN", ""),
            (27, 2, "SEMANTIC FILTER", ""), (4, 3, "TABLE FULL SCAN", "a"),
            (99, 2, "TABLE FULL SCAN", "b"),
        )
        with self.assertRaises(AssertionError):
            assert_lifted(self, ExplainPlan(fixture_plan(unmoved, ((27, marker),))))
        with self.assertRaises(AssertionError):
            assert_below_join(self, ExplainPlan(fixture_plan(lifted, ((27, marker),))))

    def test_plan_markers_can_span_continuation_lines_but_not_operator_boundaries(self):
        rows = ((0, 0, "SEMANTIC FILTER", ""), (1, 1, "HASH JOIN", ""),
                (2, 2, "TABLE FULL SCAN", "a"), (3, 2, "TABLE FULL SCAN", "b"))
        text = fixture_plan(rows) + (
            "\n  0 - output([a.id]), semantic_placement=LIFTED,\n"
            "      crossed_inner_joins=1\n  1 - equal_conds([a.id=b.id])"
        )
        assert_lifted(self, ExplainPlan(text))
        with self.assertRaises(AssertionError):
            assert_lifted(self, ExplainPlan(text.replace(
                "      crossed_inner_joins=1", "  1 - crossed_inner_joins=1")))

    def test_two_join_oracle_requires_both_crossings(self):
        rows = (
            (40, 0, "SEMANTIC FILTER", ""), (3, 1, "HASH JOIN", ""),
            (91, 2, "HASH JOIN", ""), (8, 3, "TABLE FULL SCAN", "a"),
            (5, 3, "TABLE FULL SCAN", "b"), (2, 2, "TABLE FULL SCAN", "c"),
        )
        marker = "semantic_placement=LIFTED, crossed_inner_joins=2"
        plan = ExplainPlan(fixture_plan(rows, ((40, marker),)))
        assert_lifted(self, plan, crossed=2)
        with self.assertRaises(AssertionError):
            assert_lifted(self, plan, crossed=1)
        partial = (
            (3, 0, "HASH JOIN", ""), (40, 1, "SEMANTIC FILTER", ""),
            (91, 2, "HASH JOIN", ""), (8, 3, "TABLE FULL SCAN", "a"),
            (5, 3, "TABLE FULL SCAN", "b"), (2, 1, "TABLE FULL SCAN", "c"),
        )
        with self.assertRaises(AssertionError):
            assert_lifted(self, ExplainPlan(fixture_plan(partial, ((40, marker),))), crossed=2)

    def test_plan_parser_rejects_missing_duplicate_or_unknown_operator_evidence(self):
        for text in ("SEMANTIC FILTER above HASH JOIN", "0|SORT||1|1|\n0|HASH JOIN||1|1|",
                     "0|SORT||1|1|\n1|HASH JOIN||1|1|",
                     "0|SORT||1|1|\n  2 - semantic_placement=LIFTED"):
            with self.subTest(text=text), self.assertRaises(ValueError):
                ExplainPlan(text)
        plan = ExplainPlan("0|SORT||1|1|")
        with self.assertRaises(AssertionError):
            plan.one("SEMANTIC FILTER")
        self.assertEqual(render_explain((("0|SORT||1|1|",), (None, "1| HASH JOIN||1|1|"))),
                         "0|SORT||1|1|\n1| HASH JOIN||1|1|")

    def test_counter_oracle_is_windowed_exact_and_preserves_duplicate_work(self):
        first, second = files.content_key("duplicate"), files.content_key("other")
        before = Counter({first: 7, second: 1})
        expected = Counter({first: 2, second: 1})
        assert_clean_work(self, before, before + expected, expected)
        self.assertEqual(counter_delta(before, before + expected), expected)
        for actual in (Counter({first: 3}), Counter({first: 1, second: 2}),
                       Counter({first: 2}), Counter({first: 3, second: 1})):
            with self.subTest(actual=actual), self.assertRaises(AssertionError):
                assert_clean_work(self, before, before + actual, expected)
        with self.assertRaises(AssertionError):
            counter_delta(before, Counter({first: 6, second: 1}))
        assert_clean_work(self, before, before, Counter())

    def test_pair_oracle_normalizes_only_whitespace_and_never_roles_or_retries(self):
        payload = joins.pair_payload(JOIN_RULE, "left", "right")
        compact = files.content_key(files.descriptor_text(payload))
        pretty = files.content_key(joins.prompt_text(payload, indent=2))
        expected = Counter({joins.pair_key(payload): 1})
        assert_clean_work(self, Counter(), Counter({pretty: 1}), expected, normalize=pair_counter_key)
        with self.assertRaises(AssertionError):
            assert_clean_work(self, Counter(), Counter({compact: 1, pretty: 1}), expected,
                              normalize=pair_counter_key)
        swapped = files.content_key(files.descriptor_text(
            joins.pair_payload(JOIN_RULE, "right", "left")))
        with self.assertRaises(AssertionError):
            assert_clean_work(self, Counter(), Counter({swapped: 1}), expected,
                              normalize=pair_counter_key)
        with self.assertRaises(AssertionError):
            pair_counter_key(files.content_key('{"left":"left","rule":"rule","right":"right"}'))
        scenarios = PairScenarios()
        scenarios[compact] = ["fixture-response"]
        self.assertEqual(scenarios.get(pretty), ["fixture-response"])
        self.assertIsNone(scenarios.get(swapped))

    def test_provider_filter_fixture_returns_native_boolean_schema(self):
        fixtures = files.FileFixtures()
        self.addCleanup(fixtures.close)
        server = fixtures.mocks[0]
        fixtures.reply(server, "lift-fixture", True, boolean=True)
        before = provider_snapshot(server)
        status, raw = files.post_body(server, {
            "model": "mock-semantic-lift",
            "messages": [{"role": "user", "content": "lift-fixture"}],
        })
        self.assertEqual(status, 200)
        body = files.strict_json(raw)
        self.assertEqual(files.strict_json(body["choices"][0]["message"]["content"]), {"value": True})
        after = provider_snapshot(server)
        assert_clean_work(self, before.counts, after.counts,
                          Counter({files.content_key("lift-fixture"): 1}))
        self.assertEqual(len(after.requests), 1)
        self.assertEqual(raw, completion('{"value":true}', finish_reason="stop"))

    def test_provider_snapshot_rejects_fixture_errors_and_counter_mismatches(self):
        import threading

        server = mock.Mock(condition=threading.Condition(), active=0,
                           fixture_errors=["unplanned request"], counts=Counter(), requests=[])
        with self.assertRaises(AssertionError):
            provider_snapshot(server)
        server.fixture_errors = []
        server.counts = Counter({"encoded": 1})
        with self.assertRaises(AssertionError):
            provider_snapshot(server)

    def test_generated_local_and_remote_media_oracles_require_no_descriptor_io(self):
        media = files.MediaFiles()
        self.addCleanup(media.close)
        asset = media.asset("png", 401)
        self.assertEqual(asset.raw, files.png_bytes(401))
        self.assertEqual(files.decode_descriptor(files.descriptor_text(asset.descriptor())),
                         asset.descriptor())
        with remote.StorageServer() as storage:
            target = "/media/lift%20fixture.png?signature=local%2B%2f&repeat=a+b"
            remote_asset = storage.asset("png", 402, target=target)
            self.assertEqual(storage.counts(), Counter())
            self.assertEqual(files.decode_descriptor(files.descriptor_text(remote_asset.descriptor())),
                             remote_asset.descriptor())
            before = storage.counts()
            self.assertEqual(remote.storage_get(storage, target)[2], remote_asset.raw)
            expected = Counter({("GET", target): 1})
            assert_clean_work(self, before, storage.counts(), expected)
            with self.assertRaises(AssertionError):
                assert_clean_work(self, before, Counter({("HEAD", target): 1}), expected)
            with self.assertRaises(AssertionError):
                assert_clean_work(self, before, Counter({("GET", target): 2}), expected)

    def test_hint_builder_uses_only_repository_varchar_boolean_conventions(self):
        for enabled in (None, "true", "false"):
            statement = sql_statement("a.id,b.category", enabled=enabled)
            if enabled is None:
                self.assertNotIn("enable_semantic_lift", statement)
            else:
                self.assertIn(f"OPT_PARAM('enable_semantic_lift', '{enabled}')", statement)
            self.assertIn("USE_HASH(b)", statement)
            self.assertIn("b.category", statement)
            self.assertIn("OPT_PARAM('rowsets_max_rows', 16)", statement)
        for enabled in (True, False, 0, 1, "yes", "TRUE"):
            with self.subTest(enabled=enabled), self.assertRaises(ValueError):
                sql_statement("a.id", enabled=enabled)

    def test_cli_repeated_selection_is_deduplicated_without_inherited_discovery(self):
        names = case_names(SemanticLiftContracts)
        self.assertEqual(SemanticLiftContracts.__bases__, (unittest.TestCase,))
        self.assertIn(CHAIN_CASE, names)
        self.assertEqual(selected_cases(SemanticLiftContracts, []), names)
        self.assertIn(INVALID_HINT_CASE, names)
        self.assertEqual(selected_cases(SemanticLiftContracts, [CHAIN_CASE]), [CHAIN_CASE])
        self.assertEqual(parse_options(["--binary", "/private/seekdb", "--case", INVALID_HINT_CASE]).case,
                         [INVALID_HINT_CASE])
        first, second = names[:2]
        options = parse_options(["--binary", "/private/seekdb", "--case", first,
                                 "--case", second, "--case", first])
        selected = selected_cases(SemanticLiftContracts, options.case)
        self.assertEqual(selected, [first, second])
        self.assertEqual([test._testMethodName for test in build_suite(SemanticLiftContracts, selected)],
                         selected)
        self.assertNotIn("test_remote_map_exact_get_query_and_all_media_formats", names)
        self.assertFalse(set(names) & set(case_names(files.FileContracts)))
        self.assertFalse(set(names) & set(case_names(remote.RemoteFileContracts)))

    def test_cli_rejects_unknown_conflicting_and_missing_options(self):
        sql_case = case_names(SemanticLiftContracts)[0]
        fixture_case = case_names(SemanticLiftHarnessTests)[0]
        invalid = (
            [], ["--self-test", "--binary", "/private/seekdb"],
            ["--list-cases", "--case", "not_a_case"],
            ["--self-test", "--case", sql_case],
            ["--binary", "/private/seekdb", "--case", fixture_case],
        )
        for arguments in invalid:
            with self.subTest(arguments=arguments), redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as caught:
                    parse_options(arguments)
                self.assertEqual(caught.exception.code, 2)
        self.assertIsNone(parse_options(["--list-cases"]).binary)
        self.assertTrue(parse_options(["--self-test", "--case", fixture_case]).self_test)

    def test_cli_list_modes_start_no_database_or_http_fixtures(self):
        with mock.patch.object(files, "PrivateDatabase") as database, \
                mock.patch.object(files, "FileFixtures") as fixtures, \
                mock.patch.object(remote, "StorageServer") as storage:
            for self_test in (False, True):
                arguments = ["--list-cases"] + (["--self-test"] if self_test else [])
                stream = io.StringIO()
                with redirect_stdout(stream):
                    main(arguments)
                test_class = SemanticLiftHarnessTests if self_test else SemanticLiftContracts
                self.assertEqual(stream.getvalue().splitlines(), case_names(test_class))
            database.assert_not_called()
            fixtures.assert_not_called()
            storage.assert_not_called()

    def test_cli_rejects_a_missing_binary_before_allocating_resources(self):
        with mock.patch.object(Path, "is_file", return_value=False), \
                mock.patch.object(files, "PrivateDatabase") as database:
            with self.assertRaisesRegex(SystemExit, "not an executable file"):
                main(["--binary", "/private/nonexistent-seekdb"])
            database.assert_not_called()


def case_names(test_class):
    return unittest.defaultTestLoader.getTestCaseNames(test_class)


def selected_cases(test_class, requested):
    if requested:
        return list(dict.fromkeys(requested))
    return case_names(test_class)


def parse_options(arguments=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, help="Executable seekdb for a new private Unix-socket DB")
    parser.add_argument("--case", action="append", default=[], help="Test method; repeat to select")
    parser.add_argument("--self-test", action="store_true", help="Run deterministic fixtures without SQL")
    parser.add_argument("--list-cases", action="store_true", help="List without starting any services")
    options = parser.parse_args(arguments)
    test_class = SemanticLiftHarnessTests if options.self_test else SemanticLiftContracts
    unknown = [name for name in options.case if name not in case_names(test_class)]
    if unknown:
        parser.error("unknown case(s): " + ", ".join(unknown))
    if options.self_test and options.binary is not None:
        parser.error("--self-test cannot be combined with --binary")
    if not options.self_test and not options.list_cases and options.binary is None:
        parser.error("--binary is required unless --self-test or --list-cases is selected")
    return options


def build_suite(test_class, selected):
    return unittest.TestSuite(test_class(name) for name in selected)


def run_suite(test_class, requested):
    suite = build_suite(test_class, selected_cases(test_class, requested))
    if not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful():
        raise SystemExit(1)


def main(arguments=None):
    options = parse_options(arguments)
    test_class = SemanticLiftHarnessTests if options.self_test else SemanticLiftContracts
    if options.list_cases:
        for name in selected_cases(test_class, options.case):
            print(name)
    elif options.self_test:
        run_suite(test_class, options.case)
    else:
        binary = options.binary.resolve()
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise SystemExit(f"--binary is not an executable file: {binary}")
        database = files.PrivateDatabase(binary)
        try:
            database.start()
            SemanticLiftContracts.database = database
            SemanticLiftContracts.models_created = False
            run_suite(SemanticLiftContracts, options.case)
        finally:
            try:
                database.close()
            finally:
                SemanticLiftContracts.database = None
                SemanticLiftContracts.models_created = False


if __name__ == "__main__":
    main()
