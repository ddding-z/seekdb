"""Independent, gated AI_MAP / AI_FILTER execution contracts.

--self-test exercises only the loopback HTTP fixtures and assertion helpers.
--binary /path/to/seekdb starts a new private unix-socket database using the
existing runtime launcher. Repeat --case test_name to select contracts.
--list-cases lists names without starting a database.

The expected physical labels are SEMANTIC MAP and SEMANTIC FILTER. HTTP overlap
is evidence of asynchronous execution, not evidence of local multiworker input
splitting. No multiworker or shared-budget claim is made by this suite.
Boolean projection is a semantic map; WHERE/HAVING consume the complete SQL
condition tree in a semantic filter, rather than dropping every false AI result.
Positional AI_PROMPT is ordinary semantic input; structured SOLO is unsupported.
Expression evaluation must use the installed operator runtime, not a synchronous
completion fallback.
AI_FILTER reserves the case-sensitive Schema option. A native response_format matching
the fixed schema is preserved; conflicting native formats are rejected.
Generated schema names need only be nonempty; caller names are preserved.
"""

import argparse
from collections import Counter
from contextlib import redirect_stderr
from http.client import HTTPConnection
import io
import json
import os
from pathlib import Path
import reprlib
import socket
import struct
import sys
import threading
import unittest

import pymysql
from pymysql.constants import FIELD_TYPE

import test_runtime as runtime
from test_runtime_contracts import (
    Reply,
    answer,
    completion,
    configure_mock,
    constrained_options,
    model_socket_fds,
    sql_error,
)


BATCH_ROWS = 16
ROW_COUNT = 2 * BATCH_ROWS + 3
FILTER_SCHEMA = {
    "type": "object",
    "properties": {"value": {"type": "boolean"}},
    "required": ["value"],
    "additionalProperties": False,
}
MATCHING_FILTER_FORMAT = {
    "type": "json_schema",
    "json_schema": {"name": "ai_filter", "strict": True, "schema": FILTER_SCHEMA},
}
PAIR_FIELDS = (
    "id, AI_MAP('semantic_a', CONCAT('M:', prompt)), "
    "AI_FILTER('semantic_b', CONCAT('F:', prompt))"
)


class PendingQuery:
    """Carry a background result without turning an exception into a result."""

    def __init__(self, operation, mocks):
        self.value = None
        self.error = None
        self.done = threading.Event()

        def execute():
            try:
                self.value = operation()
            except Exception as error:
                self.error = error
            finally:
                self.done.set()
                for mock in mocks:
                    with mock.condition:
                        mock.condition.notify_all()

        self.thread = threading.Thread(target=execute, daemon=True)
        self.thread.start()

    def wait(self, timeout=5):
        if not self.done.wait(timeout):
            raise AssertionError("operation did not finish while response gates remained controlled")
        self.thread.join(timeout)
        if self.thread.is_alive():
            raise AssertionError("operation signalled completion but its worker did not exit")

    def result(self, timeout=5):
        self.wait(timeout)
        if self.error is not None:
            raise self.error
        return self.value

    def expect_sql_error(self, test, code, timeout=5):
        self.wait(timeout)
        test.assertIsInstance(self.error, pymysql.MySQLError,
                              f"expected SQL error, got value={self.value!r}, error={self.error!r}")
        codes = (code,) if isinstance(code, int) else code
        test.assertIn(self.error.args[0], codes, self.error.args)
        return self.error


class MockFixtures:
    def __init__(self):
        self.mocks = []
        self.threads = []
        self.gates = []
        self.pending = []
        for _ in range(2):
            mock = runtime.MockServer()
            mock.daemon_threads = False
            configure_mock(mock)
            thread = threading.Thread(target=mock.serve_forever, daemon=True)
            thread.start()
            self.mocks.append(mock)
            self.threads.append(thread)

    def gate(self):
        gate = threading.Event()
        self.gates.append(gate)
        return gate

    def start(self, operation):
        pending = PendingQuery(operation, self.mocks)
        self.pending.append(pending)
        return pending

    def release(self):
        for gate in self.gates:
            gate.set()

    def close(self):
        self.release()
        for pending in self.pending:
            pending.thread.join(5)
        for mock, thread in zip(self.mocks, self.threads):
            mock.shutdown()
            mock.server_close()
            thread.join(5)
        if any(pending.thread.is_alive() for pending in self.pending):
            raise AssertionError("fixture cleanup left a query worker alive")
        if any(thread.is_alive() for thread in self.threads):
            raise AssertionError("fixture cleanup left an HTTP server alive")
        for mock in self.mocks:
            if mock.active or mock.fixture_errors:
                raise AssertionError(
                    f"HTTP fixture failed: active={mock.active}, errors={mock.fixture_errors!r}")


def audit(mock):
    with mock.condition:
        return [dict(record) for record in mock.audit]


def wait_evidence(test, mock, predicate, pending, *, timeout=5, message):
    with mock.condition:
        mock.condition.wait_for(
            lambda: predicate() or pending.done.is_set() or mock.fixture_errors,
            timeout=timeout)
        test.assertFalse(mock.fixture_errors, mock.fixture_errors)
        test.assertTrue(predicate(),
                        f"{message}; requests={reprlib.repr(dict(mock.counts))}, error={pending.error!r}, "
                        f"finished={pending.done.is_set()}")


def assert_held(test, mock, gates, prompts=None):
    test.assertTrue(gates, "no response gate was supplied")
    test.assertTrue(all(not gate.is_set() for gate in gates), "a response gate was already released")
    records = audit(mock)
    if prompts is not None:
        records = [record for record in records if record["prompt"] in prompts]
    test.assertTrue(records, "no held request reached the mock")
    test.assertTrue(all(record["response"] is None for record in records),
                    "a response was sent before the test released its gate")


def assert_dependencies(test, inner_records, outer_records, dependencies):
    inner = {record["prompt"]: record for record in inner_records}
    test.assertEqual(len(inner), len(inner_records), "dependency evidence requires unique inner prompts")
    test.assertTrue(outer_records, "no outer requests were observed")
    for outer in outer_records:
        test.assertIn(outer["prompt"], dependencies, "unexpected dependent prompt")
        inner_prompt = dependencies[outer["prompt"]]
        test.assertIn(inner_prompt, inner, "outer request has no matching inner request")
        response = inner[inner_prompt]["response"]
        test.assertIsNotNone(response, "outer request was submitted while its inner response was held")
        test.assertLessEqual(response, outer["received"],
                             "outer request was submitted before its own inner response")


def assert_filter_request(test, body, caller_format=None):
    test.assertNotIn("Schema", body, "reserved Schema leaked into the filter provider request")
    test.assertIn("response_format", body, "AI_FILTER did not supply its own output schema")
    response_format = body["response_format"]
    test.assertIsInstance(response_format, dict)
    test.assertTrue({"type", "json_schema"} <= response_format.keys(),
                    "native output format is missing required fields")
    test.assertEqual(response_format["type"], "json_schema")
    native = response_format["json_schema"]
    test.assertIsInstance(native, dict)
    test.assertTrue({"name", "strict", "schema"} <= native.keys(),
                    "native output schema is missing required fields")
    test.assertIsInstance(native["name"], str)
    test.assertTrue(native["name"], "output-schema name must be nonempty")
    test.assertIs(native["strict"], True)
    test.assertEqual(native["schema"], FILTER_SCHEMA)
    test.assertIs(native["schema"]["additionalProperties"], False)
    if caller_format is not None:
        test.assertEqual(response_format, caller_format, "caller native format was changed")


def post_completion(mock, prompt):
    client = HTTPConnection("127.0.0.1", mock.server_port, timeout=8)
    try:
        client.request("POST", "/v1/chat/completions",
                       json.dumps({"messages": [{"role": "user", "content": prompt}]}),
                       {"Content-Type": "application/json"})
        response = client.getresponse()
        return response.status, response.read()
    finally:
        client.close()


class SemanticHarnessTests(unittest.TestCase):
    def setUp(self):
        self.fixtures = MockFixtures()
        self.addCleanup(self.fixtures.close)
        self.first, self.second = self.fixtures.mocks

    def test_loopback_mocks_hold_both_then_release_independently(self):
        for mock in self.fixtures.mocks:
            self.assertEqual(mock.server_address[0], "127.0.0.1")
        first_gate, second_gate = self.fixtures.gate(), self.fixtures.gate()
        first_body = completion('literal:"a"\n')
        second_body = completion('literal:"b"\n')
        self.first.scenarios["A"] = [Reply(body=first_body, gate=first_gate)]
        self.second.scenarios["B"] = [Reply(body=second_body, gate=second_gate)]
        first = self.fixtures.start(lambda: post_completion(self.first, "A"))
        second = self.fixtures.start(lambda: post_completion(self.second, "B"))
        for mock, pending in ((self.first, first), (self.second, second)):
            wait_evidence(self, mock, lambda mock=mock: len(mock.audit) == 1, pending,
                          message="fixture request never arrived")
        assert_held(self, self.first, [first_gate])
        assert_held(self, self.second, [second_gate])
        self.assertFalse(first.done.is_set())
        self.assertFalse(second.done.is_set())
        second_gate.set()
        self.assertEqual(second.result(), (200, second_body))
        self.assertFalse(first.done.is_set())
        assert_held(self, self.first, [first_gate])
        first_gate.set()
        self.assertEqual(first.result(), (200, first_body))

    def test_held_assertion_rejects_empty_released_and_responded_evidence(self):
        gate = self.fixtures.gate()
        with self.assertRaisesRegex(AssertionError, "no held request"):
            assert_held(self, self.first, [gate])
        with self.first.condition:
            self.first.audit.append({"prompt": "probe", "received": 1, "response": 2})
        with self.assertRaisesRegex(AssertionError, "response was sent"):
            assert_held(self, self.first, [gate])
        gate.set()
        with self.assertRaisesRegex(AssertionError, "already released"):
            assert_held(self, self.first, [gate])
        with self.assertRaisesRegex(AssertionError, "no response gate"):
            assert_held(self, self.first, [])

    def test_mock_assigns_constant_prompt_ordinals_before_response_release(self):
        gate = self.fixtures.gate()
        bodies = [completion(f"ordinal-{index}") for index in range(5)]
        self.first.scenarios["constant"] = [
            Reply(body=body, gate=gate if index == 0 else None)
            for index, body in enumerate(bodies)]
        first = self.fixtures.start(lambda: post_completion(self.first, "constant"))
        wait_evidence(self, self.first, lambda: self.first.counts["constant"] == 1, first,
                      message="first constant-prompt request did not arrive")
        assert_held(self, self.first, [gate])
        for body in bodies[1:]:
            self.assertEqual(post_completion(self.first, "constant"), (200, body))
        self.assertFalse(first.done.is_set())
        self.assertFalse(gate.is_set())
        self.assertIsNone(audit(self.first)[0]["response"])
        gate.set()
        self.assertEqual(first.result(), (200, bodies[0]))
        self.assertEqual(self.first.counts, Counter({"constant": len(bodies)}))

    def test_dependency_oracle_rejects_unready_missing_and_reversed_inputs(self):
        outer = [{"prompt": "outer", "received": 5, "response": None}]
        dependencies = {"outer": "inner"}
        for inner in ([], [{"prompt": "inner", "received": 1, "response": None}],
                      [{"prompt": "inner", "received": 1, "response": 6}]):
            with self.subTest(inner=inner), self.assertRaises(AssertionError):
                assert_dependencies(self, inner, outer, dependencies)
        assert_dependencies(self, [{"prompt": "inner", "received": 1, "response": 4}],
                            outer, dependencies)
        with self.assertRaisesRegex(AssertionError, "no outer requests"):
            assert_dependencies(self, [], [], {})

    def test_background_error_is_not_success_or_evidence(self):
        def fail():
            raise pymysql.OperationalError(1210, "fixture argument error")

        pending = self.fixtures.start(fail)
        with self.assertRaises(pymysql.OperationalError):
            pending.result()
        pending.expect_sql_error(self, 1210)
        with self.assertRaises(AssertionError):
            pending.expect_sql_error(self, 4012)
        with self.assertRaisesRegex(AssertionError, "requests=.*finished=True"):
            wait_evidence(self, self.first, lambda: bool(self.first.audit), pending,
                          message="SQL failure is not asynchronous progress")
        success = self.fixtures.start(lambda: ())
        with self.assertRaises(AssertionError):
            success.expect_sql_error(self, 1210)

    def test_large_prompt_failure_diagnostics_are_bounded(self):
        with self.first.condition:
            self.first.counts["large-" + "x" * 131073] = 1
        pending = self.fixtures.start(lambda: ())
        pending.result()
        with self.assertRaises(AssertionError) as caught:
            wait_evidence(self, self.first, lambda: False, pending,
                          message="large-prompt negative control")
        self.assertLess(len(str(caught.exception)), 512)

    def test_mock_preserves_false_and_malformed_content_without_interpreting_it(self):
        texts = ('{"value":false}', '{"value":"false"}', '{"value":true,"extra":1}',
                 '```json\n{"value":true}\n```', '{"value":')
        for index, text in enumerate(texts):
            prompt = f"fixture-content-{index}"
            body = completion(text, finish_reason="stop")
            self.first.scenarios[prompt] = [Reply(body=body)]
            self.assertEqual(post_completion(self.first, prompt), (200, body))
        self.assertEqual(self.first.counts,
                         Counter(f"fixture-content-{index}" for index in range(len(texts))))

    def test_mock_records_native_schema_and_options_without_transforming_them(self):
        body = {
            "messages": [{"role": "user", "content": "schema-fixture"}],
            "response_format": MATCHING_FILTER_FORMAT,
            "temperature": 0.75,
            "max_tokens": 32,
        }
        response_body = completion('{"value":false}', finish_reason="stop")
        self.first.scenarios["schema-fixture"] = [Reply(body=response_body)]
        client = HTTPConnection("127.0.0.1", self.first.server_port, timeout=5)
        try:
            client.request("POST", "/", json.dumps(body), {"Content-Type": "application/json"})
            response = client.getresponse()
            self.assertEqual((response.status, response.read()), (200, response_body))
        finally:
            client.close()
        self.assertEqual(self.first.requests, [body])
        self.assertNotIn("Schema", self.first.requests[0])

    def test_filter_request_oracle_allows_generated_names_but_locks_shape_and_caller_format(self):
        for name in ("generated-name-a", "generated-name-b"):
            response_format = {
                **MATCHING_FILTER_FORMAT,
                "json_schema": {**MATCHING_FILTER_FORMAT["json_schema"], "name": name},
            }
            body = {"response_format": response_format, "schema": {"type": "string"}}
            assert_filter_request(self, body)
            assert_filter_request(self, body, response_format)
            with self.assertRaises(AssertionError):
                assert_filter_request(self, body, MATCHING_FILTER_FORMAT)
        native = MATCHING_FILTER_FORMAT["json_schema"]
        invalid = [
            {"strict": True, "schema": FILTER_SCHEMA},
            {**native, "name": ""}, {**native, "name": None}, {**native, "name": 7},
            {**native, "strict": 1},
            {**native, "schema": {**FILTER_SCHEMA, "additionalProperties": 0}},
            {**native, "schema": {**FILTER_SCHEMA, "required": []}},
        ]
        for native in invalid:
            with self.subTest(native=native), self.assertRaises(AssertionError):
                assert_filter_request(self, {"response_format": {
                    "type": "json_schema", "json_schema": native}})
        with self.assertRaises(AssertionError):
            assert_filter_request(self, {"Schema": None, "response_format": MATCHING_FILTER_FORMAT})

    def test_cleanup_releases_pending_responses_and_joins_handlers(self):
        gate = self.fixtures.gate()
        self.first.scenarios["cleanup"] = [Reply(gate=gate)]
        pending = self.fixtures.start(lambda: post_completion(self.first, "cleanup"))
        wait_evidence(self, self.first, lambda: self.first.counts["cleanup"] == 1, pending,
                      message="cleanup probe never arrived")
        assert_held(self, self.first, [gate])
        self.fixtures.release()
        self.assertEqual(pending.result(), (200, completion(answer("cleanup"))))
        with self.first.condition:
            self.assertTrue(self.first.condition.wait_for(lambda: self.first.active == 0, timeout=5))

    def test_cli_repeats_cases_and_never_accepts_an_existing_database(self):
        options = parse_options(["--binary", "/not-executed/seekdb",
                                 "--case", "test_independent_maps_overlap_before_any_reply",
                                 "--case", "test_second_batch_arrives_before_first_batch_replies"])
        self.assertEqual(options.case, [
            "test_independent_maps_overlap_before_any_reply",
            "test_second_batch_arrives_before_first_batch_replies",
        ])
        options = parse_options(["--self-test", "--case",
                                 "test_background_error_is_not_success_or_evidence"])
        self.assertTrue(options.self_test)
        for arguments in (["--self-test", "--binary", "/not-executed/seekdb"],
                          ["--self-test", "--socket", "/existing/sql.sock"],
                          ["--self-test", "--case", "test_does_not_exist"],
                          ["--case", "test_map_longtext_and_filter_sql_boolean"]):
            with self.subTest(arguments=arguments), redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as caught:
                    parse_options(arguments)
                self.assertEqual(caught.exception.code, 2)


class SemanticContracts(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with cls.connection.cursor() as cursor:
            cursor.execute("SET ob_query_timeout = 30000000")
            cursor.execute("CREATE DATABASE ai_semantic_contract_test")
            cursor.execute("USE ai_semantic_contract_test")
            for suffix in ("a", "b"):
                model = f"semantic_{suffix}"
                cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL(%s, %s)",
                               (model, json.dumps({"type": "completion", "model_name": f"mock-{suffix}"})))
                endpoint = {
                    "ai_model_name": model,
                    "url": f"http://127.0.0.1:{cls.server.server_port}/v1/chat/completions",
                    "access_key": "semantic-fixture-only",
                    "provider": "openai",
                    "request_model_name": f"mock-{suffix}",
                }
                cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT(%s, %s)",
                               (f"{model}_endpoint", json.dumps(endpoint)))
            cursor.execute("CREATE TABLE semantic_inputs (id INT PRIMARY KEY, prompt LONGTEXT, "
                           "model VARCHAR(64), opts LONGTEXT, guard INT)")

    def setUp(self):
        self.fixtures = MockFixtures()
        self.mock_a, self.mock_b = self.fixtures.mocks
        self.filter_prompts = {self.mock_a: set(), self.mock_b: set()}
        self.caller_filter_formats = {self.mock_a: {}, self.mock_b: {}}
        self.cursor = self.connection.cursor()
        self.addCleanup(self.close_case)
        self.cursor.execute("SET ob_query_timeout = 20000000")
        self.cursor.execute("SET ai_pipeline_slots = 2, ai_pipeline_memory_limit = DEFAULT")
        for suffix, mock in zip(("a", "b"), self.fixtures.mocks):
            self.cursor.execute("CALL DBMS_AI_SERVICE.ALTER_AI_MODEL_ENDPOINT(%s, %s)",
                                (f"semantic_{suffix}_endpoint", json.dumps({
                                    "url": f"http://127.0.0.1:{mock.server_port}/v1/chat/completions",
                                    "provider": "openai",
                                })))
        self.cursor.execute("TRUNCATE TABLE semantic_inputs")
        self.process_id = struct.unpack(
            "3i", self.connection._sock.getsockopt(
                socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i")))[0]

    def close_case(self):
        try:
            self.fixtures.release()
            for pending in self.fixtures.pending:
                pending.thread.join(5)
            if any(pending.thread.is_alive() for pending in self.fixtures.pending):
                self.kill_query()
        finally:
            try:
                self.fixtures.close()
            finally:
                self.cursor.close()
        for mock in self.fixtures.mocks:
            for body in mock.requests:
                prompt = body["messages"][-1]["content"]
                if prompt in self.filter_prompts[mock]:
                    assert_filter_request(self, body, self.caller_filter_formats[mock][prompt])

    def load(self, prompts, *, models=None, flags=None):
        self.cursor.execute("TRUNCATE TABLE semantic_inputs")
        if models is not None:
            self.assertEqual(len(models), len(prompts))
        if flags is not None:
            self.assertEqual(len(flags), len(prompts))
        self.cursor.executemany(
            "INSERT INTO semantic_inputs VALUES (%s, %s, %s, %s, %s)",
            [(index, prompt, models[index] if models is not None else "semantic_a",
              '{"temperature":0.25}', flags[index] if flags is not None else index % 2)
             for index, prompt in enumerate(prompts)])

    def statement(self, fields, suffix="ORDER BY id"):
        return (f"SELECT /*+ OPT_PARAM('rowsets_max_rows', {BATCH_ROWS}) */ "
                f"{fields} FROM semantic_inputs {suffix}")

    def select(self, fields, suffix="ORDER BY id", parameters=None):
        self.cursor.execute(self.statement(fields, suffix), parameters)
        return self.cursor.fetchall()

    def assert_plan_labels(self, statement, labels):
        self.cursor.execute("EXPLAIN " + statement)
        plan = "\n".join(" ".join(str(value) for value in row)
                         for row in self.cursor.fetchall()).upper()
        for label in labels:
            self.assertIn(label, plan)
        return plan

    def filter_content(self, mock, prompt, content, *, gate=None):
        mock.scenarios[prompt] = [Reply(body=completion(content, finish_reason="stop"), gate=gate)]
        self.filter_prompts[mock].add(prompt)
        self.caller_filter_formats[mock][prompt] = None

    def filter_reply(self, mock, prompt, value, *, gate=None):
        self.assertIs(type(value), bool)
        self.filter_content(mock, prompt, json.dumps({"value": value}), gate=gate)

    def assert_counts(self, mock, prompts):
        with mock.condition:
            self.assertEqual(mock.counts, Counter(prompts))

    def assert_no_model_sockets(self):
        for mock in self.fixtures.mocks:
            self.assertFalse(model_socket_fds(self.process_id, mock.server_port),
                             "completed error left a model-service socket owned by the database")

    def kill_query(self):
        with pymysql.connect(unix_socket=self.sql_socket, user="root", autocommit=True,
                             read_timeout=5, write_timeout=5) as control:
            with control.cursor() as cursor:
                cursor.execute(f"KILL QUERY {self.connection.thread_id()}")

    def held_pair(self, prefix):
        prompts = [f"{prefix}-{index}" for index in range(ROW_COUNT)]
        self.load(prompts)
        map_gate, filter_gate = self.fixtures.gate(), self.fixtures.gate()
        self.mock_a.default_reply = Reply(gate=map_gate)
        map_prompts = ["M:" + prompt for prompt in prompts]
        filter_prompts = ["F:" + prompt for prompt in prompts]
        for prompt in filter_prompts:
            self.filter_reply(self.mock_b, prompt, True, gate=filter_gate)
        pending = self.fixtures.start(lambda: self.select(PAIR_FIELDS))
        return pending, (map_gate, filter_gate), map_prompts, filter_prompts

    def reuse_pair_while_old_responses_are_held(self, gates):
        self.assertTrue(all(not gate.is_set() for gate in gates))
        self.cursor.execute("SET ob_query_timeout = 20000000")
        self.load(["recovered"])
        self.mock_a.scenarios["M:recovered"] = [Reply(body=completion('reused:"map"\n'))]
        self.filter_reply(self.mock_b, "F:recovered", False)
        pending = self.fixtures.start(lambda: self.select(PAIR_FIELDS))
        self.assertEqual(pending.result(timeout=3), ((0, 'reused:"map"\n', 0),))
        self.assertTrue(all(not gate.is_set() for gate in gates))

    def test_plan_labels_are_semantic_operators(self):
        self.load(["plan-a", "plan-b"])
        statements = [
            (self.statement("id, AI_MAP('semantic_a', CONCAT('cpu:', prompt))"), ("SEMANTIC MAP",)),
            (self.statement("id, AI_FILTER('semantic_a', prompt)"), ("SEMANTIC MAP",)),
            (self.statement("id, prompt", "WHERE AI_FILTER('semantic_a', prompt) ORDER BY id"),
             ("SEMANTIC FILTER",)),
            (self.statement("id, AI_MAP('semantic_b', CONCAT('M:', prompt))",
                            "WHERE AI_FILTER('semantic_a', CONCAT('F:', prompt)) ORDER BY id"),
             ("SEMANTIC MAP", "SEMANTIC FILTER")),
        ]
        for statement, labels in statements:
            with self.subTest(labels=labels):
                plan = self.assert_plan_labels(statement, labels)
                if labels == ("SEMANTIC MAP",):
                    self.assertNotIn("SEMANTIC FILTER", plan)
        self.assertFalse(self.mock_a.requests)
        self.assertFalse(self.mock_b.requests)

    def test_map_longtext_and_filter_sql_boolean(self):
        prompts = ["longtext", "json-as-text", "escaped"]
        texts = ["x" * 131073, '{"value":false}', 'quote" slash\\ line\n nul\0 \u4e2d\U0001f600']
        self.load(prompts)
        for prompt, text in zip(prompts, texts):
            self.mock_a.scenarios[prompt] = [Reply(body=completion(text))]
        rows = self.select("id, AI_MAP('semantic_a', prompt)")
        self.assertEqual(rows, tuple(enumerate(texts)))
        self.assertEqual(self.cursor.description[1][1], FIELD_TYPE.BLOB)
        self.assert_counts(self.mock_a, prompts)
        self.cursor.execute("DROP TABLE IF EXISTS semantic_map_type")
        self.cursor.execute("CREATE TABLE semantic_map_type AS " +
                            self.statement("AI_MAP('semantic_a', prompt) AS mapped"))
        self.cursor.execute("SHOW COLUMNS FROM semantic_map_type")
        self.assertEqual(self.cursor.fetchone()[1].lower(), "longtext")
        self.cursor.execute("DROP TABLE semantic_map_type")
        self.assert_counts(self.mock_a, prompts * 2)
        values = [True, False, True]
        for prompt, value in zip(prompts, values):
            self.filter_reply(self.mock_b, prompt, value)
        rows = self.select("id, AI_FILTER('semantic_b', prompt)")
        self.assertEqual(rows, ((0, 1), (1, 0), (2, 1)))
        for _, value in rows:
            self.assertIs(type(value), int, "AI_FILTER returned JSON/text instead of a SQL boolean")
        self.assert_counts(self.mock_b, prompts)

    def test_independent_maps_overlap_before_any_reply(self):
        self.load(["seed"])
        gate_a, gate_b = self.fixtures.gate(), self.fixtures.gate()
        self.mock_a.default_reply = Reply(gate=gate_a)
        self.mock_b.default_reply = Reply(gate=gate_b)
        fields = ("id, AI_MAP('semantic_a', CONCAT('A:', prompt)), "
                  "AI_MAP('semantic_b', CONCAT('B:', prompt))")
        pending = self.fixtures.start(lambda: self.select(fields))
        for mock, prompt in ((self.mock_a, "A:seed"), (self.mock_b, "B:seed")):
            wait_evidence(self, mock, lambda mock=mock, prompt=prompt: mock.counts[prompt] == 1,
                          pending, message="independent A and B must both reach their services")
        assert_held(self, self.mock_a, [gate_a])
        assert_held(self, self.mock_b, [gate_b])
        self.assertFalse(pending.done.is_set())
        gate_b.set()
        wait_evidence(self, self.mock_b, lambda: "B:seed" in self.mock_b.finished, pending,
                      message="B response was not sent independently")
        assert_held(self, self.mock_a, [gate_a])
        self.assertFalse(pending.done.is_set())
        gate_a.set()
        self.assertEqual(pending.result(), ((0, answer("A:seed"), answer("B:seed")),))
        self.assert_counts(self.mock_a, ["A:seed"])
        self.assert_counts(self.mock_b, ["B:seed"])

    def test_constant_maps_are_row_local_not_query_broadcasts(self):
        source_prompts = [f"constant-map-source-{index}" for index in range(ROW_COUNT)]
        self.load(source_prompts)
        prompt = "constant-map"
        gates = [self.fixtures.gate(), self.fixtures.gate()]
        replies = [[f"{prefix}-ordinal-{index:02d}" for index in range(ROW_COUNT)]
                   for prefix in ("A", "B")]
        for mock, texts, gate in zip(self.fixtures.mocks, replies, gates):
            mock.scenarios[prompt] = [Reply(body=completion(text), gate=gate) for text in texts]
        fields = (
            "id, prompt, AI_MAP('semantic_a', 'constant-map', '{\"temperature\":0.25}'), "
            "AI_MAP('semantic_b', 'constant-map', '{\"temperature\":0.75}')"
        )
        self.assert_plan_labels(self.statement(fields), ("SEMANTIC MAP",))
        pending = self.fixtures.start(lambda: self.select(fields))
        for mock, gate in zip(self.fixtures.mocks, gates):
            wait_evidence(self, mock, lambda mock=mock: mock.counts[prompt] > BATCH_ROWS, pending,
                          message="constant map must demand distinct rows in the next batch")
            assert_held(self, mock, [gate])
        gates[1].set()
        wait_evidence(self, self.mock_b, lambda: len(self.mock_b.finished) > BATCH_ROWS, pending,
                      message="B constant results must complete while A remains pending")
        assert_held(self, self.mock_a, [gates[0]])
        gates[0].set()
        rows = pending.result()
        self.assertEqual([(row[0], row[1]) for row in rows], list(enumerate(source_prompts)))
        # Request ordinals identify calls, not row IDs: concurrent arrivals can be reordered.
        for column, mock, texts in zip((2, 3), self.fixtures.mocks, replies):
            self.assertEqual(Counter(row[column] for row in rows), Counter(texts))
            self.assert_counts(mock, [prompt] * ROW_COUNT)

    def test_constant_filters_evaluate_each_active_row(self):
        source_prompts = [f"constant-filter-source-{index}" for index in range(ROW_COUNT)]
        self.load(source_prompts)
        prompt = "constant-filter"
        gates = [self.fixtures.gate(), self.fixtures.gate()]
        values = [[index % 3 != 1 for index in range(ROW_COUNT)],
                  [index % 2 == 0 for index in range(ROW_COUNT)]]
        for mock, results, gate in zip(self.fixtures.mocks, values, gates):
            self.filter_reply(mock, prompt, results[0], gate=gate)
            mock.scenarios[prompt] = [
                Reply(body=completion(json.dumps({"value": value}), finish_reason="stop"), gate=gate)
                for value in results]
        fields = ("id, prompt, AI_FILTER('semantic_a', 'constant-filter'), "
                  "AI_FILTER('semantic_b', 'constant-filter')")
        pending = self.fixtures.start(lambda: self.select(fields))
        for mock, gate in zip(self.fixtures.mocks, gates):
            wait_evidence(self, mock, lambda mock=mock: mock.counts[prompt] > BATCH_ROWS, pending,
                          message="constant filter must demand distinct rows in the next batch")
            assert_held(self, mock, [gate])
        for gate in gates:
            gate.set()
        rows = pending.result()
        self.assertEqual([(row[0], row[1]) for row in rows], list(enumerate(source_prompts)))
        for column, mock, results in zip((2, 3), self.fixtures.mocks, values):
            self.assertEqual(Counter(row[column] for row in rows),
                             Counter(int(value) for value in results))
            for row in rows:
                self.assertIs(type(row[column]), int)
            self.assert_counts(mock, [prompt] * ROW_COUNT)
        self.assert_plan_labels(self.statement(fields), ("SEMANTIC MAP",))

    def test_constant_calls_respect_case_and_boolean_guards(self):
        source_prompts = [f"constant-guard-source-{index}" for index in range(ROW_COUNT)]
        self.load(source_prompts)
        active_maps = [index for index in range(ROW_COUNT) if index % 3 == 0]
        map_replies = [f"guard-ordinal-{index:02d}" for index in range(len(active_maps))]
        self.mock_a.scenarios["constant-case"] = [
            Reply(body=completion(text)) for text in map_replies]
        self.filter_reply(self.mock_b, "constant-and", True)
        self.filter_reply(self.mock_b, "constant-or", False)
        self.filter_reply(self.mock_b, "constant-not", True)
        fields = (
            "id, prompt, CASE WHEN MOD(id, 3) = 0 THEN "
            "AI_MAP('semantic_a', 'constant-case') ELSE 'skip' END, "
            "(MOD(id, 3) = 1 AND AI_FILTER('semantic_b', 'constant-and')), "
            "(MOD(id, 3) != 2 OR AI_FILTER('semantic_b', 'constant-or')), "
            "NOT (MOD(id, 5) != 0 AND AI_FILTER('semantic_b', 'constant-not'))"
        )
        rows = self.select(fields)
        self.assertEqual([(row[0], row[1], row[3], row[4], row[5]) for row in rows],
                         [(index, prompt, int(index % 3 == 1), int(index % 3 != 2),
                           int(index % 5 == 0)) for index, prompt in enumerate(source_prompts)])
        self.assertEqual(Counter(row[2] for row in rows if row[0] in active_maps), Counter(map_replies))
        self.assertTrue(all(row[2] == "skip" for row in rows if row[0] not in active_maps))
        self.assert_counts(self.mock_a, ["constant-case"] * len(active_maps))
        self.assert_counts(self.mock_b, (
            ["constant-and"] * sum(index % 3 == 1 for index in range(ROW_COUNT)) +
            ["constant-or"] * sum(index % 3 == 2 for index in range(ROW_COUNT)) +
            ["constant-not"] * sum(index % 5 != 0 for index in range(ROW_COUNT))))
        self.assert_plan_labels(self.statement(fields), ("SEMANTIC MAP",))

    def test_second_batch_arrives_before_first_batch_replies(self):
        prompts = [f"cross-batch-{index}" for index in range(ROW_COUNT)]
        self.load(prompts)
        gate = self.fixtures.gate()
        self.mock_a.default_reply = Reply(gate=gate)
        pending = self.fixtures.start(lambda: self.select("id, AI_MAP('semantic_a', prompt)"))
        wait_evidence(self, self.mock_a,
                      lambda: all(self.mock_a.counts[prompt] == 1 for prompt in prompts[:BATCH_ROWS + 1]),
                      pending, message="batch 2 must arrive before any response from batch 1")
        assert_held(self, self.mock_a, [gate])
        self.assertGreater(len(audit(self.mock_a)), BATCH_ROWS)
        gate.set()
        self.assertEqual(pending.result(), tuple((index, answer(prompt))
                                                 for index, prompt in enumerate(prompts)))
        self.assert_counts(self.mock_a, prompts)

    def test_filter_prunes_maps_and_overlaps_a2_with_b1(self):
        prompts = [f"chain-{index}" for index in range(ROW_COUNT)]
        self.load(prompts)
        first_gate, later_gate, map_gate = (self.fixtures.gate() for _ in range(3))
        accepted = [index for index in range(ROW_COUNT) if index % 3 != 1]
        filter_prompts = ["F:" + prompt for prompt in prompts]
        for index, prompt in enumerate(filter_prompts):
            self.filter_reply(self.mock_a, prompt, index in accepted,
                              gate=first_gate if index < BATCH_ROWS else later_gate)
        self.mock_b.default_reply = Reply(gate=map_gate)
        fields = "id, AI_MAP('semantic_b', CONCAT('M:', prompt))"
        suffix = "WHERE AI_FILTER('semantic_a', CONCAT('F:', prompt)) ORDER BY id"
        pending = self.fixtures.start(lambda: self.select(fields, suffix))
        wait_evidence(self, self.mock_a, lambda: self.mock_a.counts[filter_prompts[BATCH_ROWS]] == 1,
                      pending, message="next filter batch did not reach its held service")
        assert_held(self, self.mock_a, [first_gate, later_gate])
        self.assertFalse(self.mock_b.requests, "map ran before any filter result")
        first_gate.set()
        first_maps = ["M:" + prompts[index] for index in accepted if index < BATCH_ROWS]
        wait_evidence(self, self.mock_b,
                      lambda: all(self.mock_b.counts[prompt] == 1 for prompt in first_maps), pending,
                      message="maps for accepted batch 1 must start while filter batch 2 is held")
        assert_held(self, self.mock_a, [later_gate], set(filter_prompts[BATCH_ROWS:]))
        assert_held(self, self.mock_b, [map_gate])
        self.assert_counts(self.mock_b, first_maps)
        self.assertFalse(pending.done.is_set())
        later_gate.set()
        map_gate.set()
        self.assertEqual(pending.result(),
                         tuple((index, answer("M:" + prompts[index])) for index in accepted))
        self.assert_counts(self.mock_a, filter_prompts)
        self.assert_counts(self.mock_b, ["M:" + prompts[index] for index in accepted])
        self.assertTrue(set(self.mock_b.counts).isdisjoint(
            {"M:" + prompts[index] for index in range(ROW_COUNT) if index not in accepted}))
        assert_dependencies(self, audit(self.mock_a), audit(self.mock_b),
                            {"M:" + prompts[index]: filter_prompts[index] for index in accepted})

    def test_all_false_filter_drains_tail_without_downstream_calls(self):
        prompts = [f"all-false-{index}" for index in range(ROW_COUNT)]
        prompts[BATCH_ROWS] = prompts[0]
        self.load(prompts)
        filter_prompts = ["F:" + prompt for prompt in prompts]
        for prompt in filter_prompts:
            self.filter_reply(self.mock_a, prompt, False)
        fields = "id, AI_MAP('semantic_b', CONCAT('M:', prompt))"
        suffix = "WHERE AI_FILTER('semantic_a', CONCAT('F:', prompt)) ORDER BY id"
        self.assertEqual(self.select(fields, suffix), ())
        self.assert_counts(self.mock_a, filter_prompts)
        self.assertFalse(self.mock_b.requests, "all-false filter submitted downstream work")

    def test_nested_maps_obey_same_row_dependencies_and_overlap_batches(self):
        prompts = [f"nested-{index}" for index in range(ROW_COUNT)]
        self.load(prompts)
        first_gate, later_gate, outer_gate = (self.fixtures.gate() for _ in range(3))
        inner_prompts = ["A:" + prompt for prompt in prompts]
        outer_prompts = [f"B:intermediate-{index}" for index in range(ROW_COUNT)]
        for index, prompt in enumerate(inner_prompts):
            self.mock_a.scenarios[prompt] = [Reply(
                body=completion(f"intermediate-{index}"),
                gate=first_gate if index < BATCH_ROWS else later_gate)]
        self.mock_b.default_reply = Reply(gate=outer_gate)
        fields = ("id, AI_MAP('semantic_b', CONCAT('B:', "
                  "AI_MAP('semantic_a', CONCAT('A:', prompt))))")
        pending = self.fixtures.start(lambda: self.select(fields))
        wait_evidence(self, self.mock_a, lambda: self.mock_a.counts[inner_prompts[BATCH_ROWS]] == 1,
                      pending, message="nested map did not prefetch its next inner batch")
        assert_held(self, self.mock_a, [first_gate, later_gate])
        self.assertFalse(self.mock_b.requests, "outer map ran without its own inner result")
        first_gate.set()
        wait_evidence(self, self.mock_b,
                      lambda: all(self.mock_b.counts[prompt] == 1 for prompt in outer_prompts[:BATCH_ROWS]),
                      pending, message="outer batch 1 must overlap held inner batch 2")
        assert_held(self, self.mock_a, [later_gate], set(inner_prompts[BATCH_ROWS:]))
        assert_held(self, self.mock_b, [outer_gate])
        self.assert_counts(self.mock_b, outer_prompts[:BATCH_ROWS])
        dependencies = dict(zip(outer_prompts, inner_prompts))
        assert_dependencies(self, audit(self.mock_a), audit(self.mock_b), dependencies)
        later_gate.set()
        outer_gate.set()
        self.assertEqual(pending.result(), tuple((index, answer(prompt))
                                                 for index, prompt in enumerate(outer_prompts)))
        self.assert_counts(self.mock_a, inner_prompts)
        self.assert_counts(self.mock_b, outer_prompts)
        assert_dependencies(self, audit(self.mock_a), audit(self.mock_b), dependencies)

    def test_positional_ai_prompt_is_semantic_input(self):
        self.load(["alpha", "beta"])
        self.filter_reply(self.mock_b, "check alpha", True)
        self.filter_reply(self.mock_b, "check beta", False)
        fields = ("id, AI_MAP('semantic_a', AI_PROMPT('hello {0}', CAST(prompt AS CHAR))), "
                  "AI_FILTER('semantic_b', AI_PROMPT('check {0}', CAST(prompt AS CHAR)))")
        self.assert_plan_labels(self.statement(fields), ("SEMANTIC MAP",))
        self.assertEqual(self.select(fields),
                         ((0, answer("hello alpha"), 1), (1, answer("hello beta"), 0)))
        self.assert_counts(self.mock_a, ["hello alpha", "hello beta"])
        self.assert_counts(self.mock_b, ["check alpha", "check beta"])

    def test_cpu_prompt_preserves_values_and_descending_order(self):
        prompts = [f"cpu-{index % 7}:quote\"\\\n" for index in range(ROW_COUNT)]
        models = [f"unchanged-{index}" for index in range(ROW_COUNT)]
        self.load(prompts, models=models)
        request_prompts = [f"cpu|{index * 7}|{prompt.upper()}|{'x' * (index % 3)}"
                           for index, prompt in enumerate(prompts)]
        fields = ("id, prompt, model, AI_MAP('semantic_a', "
                  "CONCAT('cpu|', CAST(id * 7 AS CHAR), '|', UPPER(prompt), '|', "
                  "REPEAT('x', MOD(id, 3)))), LENGTH(prompt)")
        self.assert_plan_labels(self.statement(fields, "ORDER BY id DESC"), ("SEMANTIC MAP",))
        self.assertEqual(self.select(fields, "ORDER BY id DESC"),
                         tuple((index, prompts[index], models[index], answer(request_prompts[index]),
                                len(prompts[index].encode("utf-8")))
                               for index in reversed(range(ROW_COUNT))))
        self.assert_counts(self.mock_a, request_prompts)

    def test_duplicates_tail_and_out_of_order_map_results_are_exact(self):
        prompts = [f"tail-{index}" for index in range(ROW_COUNT)]
        for index in (2, BATCH_ROWS + 1, ROW_COUNT - 1):
            prompts[index] = "duplicate"
        self.load(prompts)
        gate = self.fixtures.gate()
        self.mock_a.scenarios[prompts[0]] = [Reply(gate=gate)]
        pending = self.fixtures.start(
            lambda: self.select("id, prompt, AI_MAP('semantic_a', prompt)"))
        wait_evidence(self, self.mock_a,
                      lambda: self.mock_a.counts[prompts[0]] == 1 and
                      any(record["response"] is not None for record in self.mock_a.audit
                          if record["prompt"] != prompts[0]),
                      pending, message="a later response must overtake the held first row")
        assert_held(self, self.mock_a, [gate], {prompts[0]})
        gate.set()
        self.assertEqual(pending.result(), tuple((index, prompt, answer(prompt))
                                                 for index, prompt in enumerate(prompts)))
        self.assert_counts(self.mock_a, prompts)
        self.assertEqual(self.mock_a.counts["duplicate"], 3)

    def test_lob_prompts_survive_async_batches(self):
        prompts = [f"lob-{index}" for index in range(ROW_COUNT)]
        for index in (0, BATCH_ROWS + 1, ROW_COUNT - 1):
            prompts[index] += ":" + "x" * 131073 + 'quote"\0\\\n\u4e2d\U0001f600'
        self.load(prompts)
        gate = self.fixtures.gate()
        self.mock_a.scenarios[prompts[0]] = [Reply(gate=gate)]
        pending = self.fixtures.start(
            lambda: self.select("id, prompt, AI_MAP('semantic_a', prompt)"))
        wait_evidence(
            self, self.mock_a,
            lambda: self.mock_a.counts[prompts[0]] == 1 and
            prompts[BATCH_ROWS + 1] in self.mock_a.finished,
            pending, message="later LOB input must complete while the first LOB response is held")
        assert_held(self, self.mock_a, [gate], {prompts[0]})
        gate.set()
        self.assertEqual(pending.result(),
                         tuple((index, prompt, answer(prompt)) for index, prompt in enumerate(prompts)))
        self.assert_counts(self.mock_a, prompts)

    def test_filter_projection_duplicates_and_tail_are_exact(self):
        prompts = [f"bool-{index}" for index in range(ROW_COUNT)]
        for index in (1, BATCH_ROWS, ROW_COUNT - 1):
            prompts[index] = "same-false"
        self.load(prompts)
        values = [prompt != "same-false" and index % 3 == 0
                  for index, prompt in enumerate(prompts)]
        for prompt, value in zip(prompts, values):
            self.filter_reply(self.mock_a, prompt, value)
        self.assertEqual(self.select("id, prompt, AI_FILTER('semantic_a', prompt)"),
                         tuple((index, prompt, int(value))
                               for index, (prompt, value) in enumerate(zip(prompts, values))))
        self.assert_counts(self.mock_a, prompts)
        self.assertEqual(self.mock_a.counts["same-false"], 3)

    def test_empty_and_limit_zero_issue_no_requests(self):
        fields = ("id, AI_MAP('semantic_a', prompt), AI_FILTER('semantic_b', prompt)")
        self.assertEqual(self.select(fields), ())
        self.load([None, "", "not-needed"] * BATCH_ROWS)
        self.assertEqual(self.select(fields, "WHERE 1 = 0 ORDER BY id"), ())
        self.assertEqual(self.select(fields, "LIMIT 0"), ())
        self.assertFalse(self.mock_a.requests)
        self.assertFalse(self.mock_b.requests)

    def test_case_guards_skip_invalid_maps_and_filters(self):
        self.load([None, "", "selected", None, ""])
        self.filter_reply(self.mock_a, "selected", True)
        fields = ("id, CASE WHEN id = 2 THEN AI_MAP('semantic_b', prompt) ELSE 'skip' END, "
                  "CASE WHEN id = 2 THEN AI_FILTER('semantic_a', prompt) ELSE FALSE END")
        self.assertEqual(self.select(fields),
                         ((0, "skip", 0), (1, "skip", 0), (2, answer("selected"), 1),
                          (3, "skip", 0), (4, "skip", 0)))
        self.assert_counts(self.mock_a, ["selected"])
        self.assert_counts(self.mock_b, ["selected"])
        self.assert_plan_labels(self.statement(fields), ("SEMANTIC MAP",))

    def test_and_or_not_guards_skip_invalid_filters(self):
        self.load([None, "", "yes", "no", None, ""])
        for mock in (self.mock_a, self.mock_b):
            self.filter_reply(mock, "yes", True)
            self.filter_reply(mock, "no", False)
        self.filter_reply(self.mock_a, "not:yes", True)
        self.filter_reply(self.mock_a, "not:no", False)
        fields = (
            "id, (id IN (2, 3) AND AI_FILTER('semantic_a', prompt)), "
            "(id NOT IN (2, 3) OR AI_FILTER('semantic_b', prompt)), "
            "NOT (id IN (2, 3) AND AI_FILTER('semantic_a', CONCAT('not:', prompt)))"
        )
        self.assertEqual(self.select(fields),
                         ((0, 0, 1, 1), (1, 0, 1, 1), (2, 1, 1, 0),
                          (3, 0, 0, 1), (4, 0, 1, 1), (5, 0, 1, 1)))
        self.assert_counts(self.mock_a, ["yes", "no", "not:yes", "not:no"])
        self.assert_counts(self.mock_b, ["yes", "no"])
        self.assert_plan_labels(self.statement(fields), ("SEMANTIC MAP",))

    def test_where_guard_never_submits_invalid_skipped_prompts(self):
        self.load([None, "", "accepted", "rejected", None])
        self.filter_reply(self.mock_a, "accepted", True)
        self.filter_reply(self.mock_a, "rejected", False)
        suffix = "WHERE id IN (2, 3) AND AI_FILTER('semantic_a', prompt) ORDER BY id"
        self.assert_plan_labels(self.statement("id, prompt", suffix), ("SEMANTIC FILTER",))
        self.assertEqual(self.select("id, prompt", suffix), ((2, "accepted"),))
        self.assert_counts(self.mock_a, ["accepted", "rejected"])
        self.assertFalse(self.mock_b.requests)

    def test_having_not_consumes_complete_condition_tree(self):
        self.load([f"having-source-{index}" for index in range(ROW_COUNT)])
        prompts = ["having:0:12", "having:1:12", "having:2:11"]
        for prompt, value in zip(prompts, (False, True, False)):
            self.filter_reply(self.mock_a, prompt, value)
        fields = "MOD(id, 3) AS bucket, COUNT(*) AS rows_count"
        suffix = (
            "GROUP BY MOD(id, 3) HAVING NOT AI_FILTER('semantic_a', "
            "CONCAT('having:', CAST(bucket AS CHAR), ':', CAST(rows_count AS CHAR))) "
            "ORDER BY bucket"
        )
        self.assertEqual(self.select(fields, suffix), ((0, 12), (2, 11)))
        self.assert_counts(self.mock_a, prompts)
        self.assertFalse(self.mock_b.requests)
        self.assert_plan_labels(self.statement(fields, suffix), ("SEMANTIC FILTER",))

    def test_static_options_are_preserved_and_filter_schema_is_injected(self):
        prompts = ["options-a", "options-b"]
        self.load(prompts)
        map_options = {"temperature": 0.25, "max_tokens": 64}
        filter_options = {"temperature": 0.75, "max_tokens": 32, "schema": {"type": "string"}}
        for prompt, value in zip(prompts, (True, False)):
            self.filter_reply(self.mock_b, prompt, value)
        fields = ("id, AI_MAP('semantic_a', prompt, %s), "
                  "AI_FILTER('semantic_b', prompt, %s)")
        self.assertEqual(self.select(fields, parameters=(
            json.dumps(map_options), json.dumps(filter_options))),
            ((0, answer("options-a"), 1), (1, answer("options-b"), 0)))
        for mock, options, model in ((self.mock_a, map_options, "mock-a"),
                                     (self.mock_b, filter_options, "mock-b")):
            self.assert_counts(mock, prompts)
            for body in mock.requests:
                self.assertEqual(body["model"], model)
                self.assertEqual({key: body[key] for key in options}, options)
        self.assertTrue(all("response_format" not in body for body in self.mock_a.requests))

    def test_dynamic_model_or_options_are_rejected_before_requests(self):
        self.load(["dynamic-must-not-run"])
        for function in ("AI_MAP", "AI_FILTER"):
            for expression in (f"{function}(model, prompt)",
                               f"{function}('semantic_a', prompt, opts)"):
                with self.subTest(expression=expression):
                    sql_error(self, (1210, 1235), lambda: self.select("id, " + expression))
                    self.assertFalse(self.mock_a.requests)
                    self.assertFalse(self.mock_b.requests)

    def test_structured_solo_prompt_is_rejected_before_http(self):
        self.load(["one", "two"])
        for function in ("AI_MAP", "AI_FILTER"):
            with self.subTest(function=function):
                expression = (f"{function}('semantic_a', AI_PROMPT('classify record', "
                              "JSON_OBJECT('row', id, 'text', prompt)))")
                sql_error(self, 1235, lambda: self.select("id, " + expression))
                self.assertFalse(self.mock_a.requests)
                self.assertFalse(self.mock_b.requests)

    def test_nonstring_inputs_are_rejected_during_typing(self):
        self.load(["typing-control"])
        self.filter_reply(self.mock_b, "typing-control", True)
        self.assertEqual(
            self.select("id, AI_MAP('semantic_a', prompt), AI_FILTER('semantic_b', prompt)"),
            ((0, answer("typing-control"), 1),))
        for function in ("AI_MAP", "AI_FILTER"):
            for arguments in ("'semantic_a', id", "'semantic_a', TRUE",
                              "'semantic_a', CAST('2026-01-01' AS DATE)"):
                with self.subTest(function=function, arguments=arguments):
                    with self.assertRaises(pymysql.MySQLError) as caught:
                        self.cursor.execute("EXPLAIN " + self.statement(
                            f"id, {function}({arguments})"))
                    self.assertEqual(caught.exception.args[0], 5083)
                    self.assertRegex(str(caught.exception), "(?i)invalid data type")
                    self.cursor.execute("SELECT 42")
                    self.assertEqual(self.cursor.fetchall(), ((42,),))
        self.assert_counts(self.mock_a, ["typing-control"])
        self.assert_counts(self.mock_b, ["typing-control"])

    def test_null_empty_and_invalid_arguments_are_hard_errors(self):
        cases = [
            ("'semantic_a', NULL", (1210,)),
            ("'semantic_a', ''", (1210,)),
            ("NULL, 'must-not-run'", (1210,)),
            ("'', 'must-not-run'", (1210,)),
            ("'semantic_a', 'must-not-run', NULL", (1210,)),
            ("'semantic_a', 'must-not-run', 1", (3146,)),
            ("'semantic_a', 'must-not-run', 'not-json'", (3140, 3141, 5447)),
            ("'semantic_a', 'must-not-run', '[]'", (1210,)),
            ("'semantic_a', 'must-not-run', 'null'", (1210,)),
            ("'semantic_a', 'must-not-run', 'true'", (1210,)),
            ("'semantic_a', 'must-not-run', '1'", (1210,)),
            ("'semantic_a', 'must-not-run', '\"scalar\"'", (1210,)),
        ]
        for function in ("AI_MAP", "AI_FILTER"):
            for arguments, codes in cases:
                with self.subTest(function=function, arguments=arguments):
                    sql_error(self, codes, lambda: self.cursor.execute(
                        f"SELECT {function}({arguments})"))
                    self.assertFalse(self.mock_a.requests)
                    self.assertFalse(self.mock_b.requests)
        self.cursor.execute("SELECT 42")
        self.assertEqual(self.cursor.fetchall(), ((42,),))

    def test_filter_rejects_caller_schema_before_requests(self):
        options = [
            {"Schema": None},
            {"Schema": FILTER_SCHEMA},
            {"Schema": {}},
            {"Schema": "reserved-even-if-unused"},
        ]
        for config in options:
            with self.subTest(config=config):
                sql_error(self, 1210, lambda: self.cursor.execute(
                    "SELECT AI_FILTER('semantic_a', 'must-not-run', %s)", (json.dumps(config),)))
                self.assertFalse(self.mock_a.requests)
                self.assertFalse(self.mock_b.requests)

    def test_filter_preserves_matching_native_response_format_and_options(self):
        formats = [
            MATCHING_FILTER_FORMAT,
            constrained_options(FILTER_SCHEMA)["response_format"],
            {"type": "json_schema", "json_schema": {
                "name": "filter_contract_custom", "strict": True,
                "schema": {"additionalProperties": False, "required": ["value"],
                           "properties": {"value": {"type": "boolean"}}, "type": "object"},
            }},
        ]
        all_prompts = []
        for index, response_format in enumerate(formats):
            prompts = [f"matching-{index}-true", f"matching-{index}-false"]
            all_prompts.extend(prompts)
            self.load(prompts)
            for prompt, value in zip(prompts, (True, False)):
                self.filter_reply(self.mock_a, prompt, value)
                self.caller_filter_formats[self.mock_a][prompt] = response_format
            options = {"response_format": response_format, "temperature": 0.25, "max_tokens": 32}
            self.assertEqual(
                self.select("id, AI_FILTER('semantic_a', prompt, %s)",
                            parameters=(json.dumps(options),)),
                ((0, 1), (1, 0)))
            for body in self.mock_a.requests:
                if body["messages"][-1]["content"] in prompts:
                    self.assertEqual({key: body[key] for key in options}, options)
        self.assert_counts(self.mock_a, all_prompts)

    def test_map_schema_option_is_provider_passthrough_not_shorthand(self):
        options = {"Schema": FILTER_SCHEMA, "temperature": 0.25}
        self.load(["map-schema-pass"])
        self.mock_a.scenarios["map-schema-pass"] = [
            Reply(body=completion('not-json:"unconstrained map"\n'))]
        fields = "id, AI_MAP('semantic_a', prompt, %s)"
        self.assertEqual(self.select(fields, parameters=(json.dumps(options),)),
                         ((0, 'not-json:"unconstrained map"\n'),))
        self.load(["map-schema-provider-reject"])
        self.mock_a.scenarios["map-schema-provider-reject"] = [
            Reply(400, b'{"error":"unknown provider option Schema"}')]
        sql_error(self, 4216, lambda: self.select(fields, parameters=(json.dumps(options),)))
        self.assert_counts(self.mock_a, ["map-schema-pass", "map-schema-provider-reject"])
        self.assertFalse(self.mock_b.requests)
        for body in self.mock_a.requests:
            self.assertEqual({key: body[key] for key in options}, options)
            self.assertNotIn("response_format", body, "AI_MAP treated Schema as a SQL shorthand")

    def test_filter_rejects_conflicting_native_response_formats(self):
        json_schema = {"name": "conflict", "strict": True, "schema": FILTER_SCHEMA}
        formats = [
            None, [], {"type": "text"}, {"type": "json_object"},
            {"type": "json_schema"},
            {"type": "json_schema", "json_schema": {"strict": True, "schema": FILTER_SCHEMA}},
            {"type": "json_schema", "json_schema": {"name": "conflict", "schema": FILTER_SCHEMA}},
            {"type": "json_schema", "json_schema": {**json_schema, "name": ""}},
            {"type": "json_schema", "json_schema": {**json_schema, "name": None}},
            {"type": "json_schema", "json_schema": {**json_schema, "name": 7}},
            {"type": "json_schema", "json_schema": {**json_schema, "strict": False}},
            {"type": "json_schema", "json_schema": {**json_schema, "strict": 1}},
            {"type": "json_schema", "json_schema": {**json_schema, "schema": {"type": "boolean"}}},
            {"type": "json_schema", "json_schema": {
                **json_schema, "schema": {**FILTER_SCHEMA, "additionalProperties": True}}},
            {"type": "json_schema", "json_schema": {
                **json_schema, "schema": {**FILTER_SCHEMA, "additionalProperties": 0}}},
            {"type": "json_schema", "json_schema": {
                **json_schema, "schema": {**FILTER_SCHEMA, "required": []}}},
            {"type": "json_schema", "json_schema": {
                **json_schema, "schema": {**FILTER_SCHEMA, "properties": {"value": {"type": "string"}}}}},
        ]
        for response_format in formats:
            with self.subTest(response_format=response_format):
                sql_error(self, 1210, lambda: self.cursor.execute(
                    "SELECT AI_FILTER('semantic_a', 'must-not-run', %s)",
                    (json.dumps({"response_format": response_format}),)))
                self.assertFalse(self.mock_a.requests)
                self.assertFalse(self.mock_b.requests)

    def test_filter_provider_envelope_is_strict_and_not_retried(self):
        content = '{"value":true}'
        choice = {"finish_reason": "stop", "message": {"content": content}}
        payloads = [
            {"choices": []},
            {"choices": [choice, choice]},
            {"choices": [{"message": {"content": content}}]},
            {"choices": [{**choice, "finish_reason": None}]},
            {"choices": [{**choice, "finish_reason": "length"}]},
            {"choices": [{**choice, "finish_reason": "tool_calls"}]},
            {"choices": [{**choice, "message": {"content": {"value": True}}}]},
            {"choices": [{**choice, "message": {"content": True}}]},
            {"choices": [{**choice, "message": {"content": None}}]},
            {"choices": [{**choice, "message": {"content": content, "refusal": "refused"}}]},
            {"choices": [{**choice, "message": {"content": content, "refusal": ""}}]},
        ]
        prompts = []
        for index, payload in enumerate(payloads):
            prompt = f"bad-filter-envelope-{index}"
            prompts.append(prompt)
            with self.subTest(payload=payload):
                self.load([prompt])
                self.filter_reply(self.mock_a, prompt, True)
                self.mock_a.scenarios[prompt] = [Reply(body=json.dumps(payload).encode("utf-8"))]
                sql_error(self, 4070, lambda: self.select("id, AI_FILTER('semantic_a', prompt)"))
                self.assertEqual(self.mock_a.counts[prompt], 1, "invalid provider envelope was retried")
        self.assert_counts(self.mock_a, prompts)
        self.assertFalse(self.mock_b.requests)

    def test_filter_accepts_absent_or_null_refusal(self):
        prompts = ["absent-refusal", "null-refusal"]
        self.load(prompts)
        self.filter_reply(self.mock_a, prompts[0], True)
        self.filter_reply(self.mock_a, prompts[1], False)
        self.mock_a.scenarios[prompts[1]] = [Reply(body=json.dumps({
            "choices": [{"finish_reason": "stop", "message": {
                "content": '{"value":false}', "refusal": None,
            }}],
        }).encode("utf-8"))]
        self.assertEqual(self.select("id, AI_FILTER('semantic_a', prompt)"), ((0, 1), (1, 0)))
        self.assert_counts(self.mock_a, prompts)

    def test_filter_rejects_nonboolean_missing_extra_and_malformed_content(self):
        invalid_data = [
            "true", "false", '"true"', '"false"', "0", "1", "null", "[]",
            '[{"value":true}]', "{}",
            '{"value":1}', '{"value":0}', '{"value":"true"}', '{"value":"false"}',
            '{"value":null}', '{"Value":true}', '{"value":true,"extra":0}',
            '{"value":true,"value":false}', '{"value":true,"value":true}',
        ]
        invalid_syntax = [
            '{"value":', '{"value":true,}', '{"value":true} trailing',
            '```json\n{"value":true}\n```', "",
        ]
        cases = ([(text, 4070) for text in invalid_data] +
                 [(text, (4070, 3140, 3141, 5447)) for text in invalid_syntax])
        prompts = []
        for index, (text, codes) in enumerate(cases):
            prompt = f"invalid-filter-{index}"
            prompts.append(prompt)
            with self.subTest(content=text):
                self.load([prompt])
                self.filter_content(self.mock_a, prompt, text)
                sql_error(self, codes,
                          lambda: self.select("id, AI_FILTER('semantic_a', prompt)"))
                self.assertEqual(self.mock_a.counts[prompt], 1, "invalid content was retried")
                self.assertFalse(self.mock_b.requests)
        self.assert_counts(self.mock_a, prompts)
        self.load(["valid-false-after-errors"])
        self.filter_content(self.mock_a, "valid-false-after-errors", ' \n {"value": false}\t')
        self.assertEqual(self.select("id, AI_FILTER('semantic_a', prompt)"), ((0, 0),))

    def test_filter_error_fails_fast_cancels_held_peers_and_reuses_connection(self):
        prompts = [f"fail-fast-{index}" for index in range(ROW_COUNT)]
        self.load(prompts)
        held_gate, bad_gate = self.fixtures.gate(), self.fixtures.gate()
        filter_prompts = ["F:" + prompt for prompt in prompts]
        for prompt in filter_prompts:
            self.filter_reply(self.mock_a, prompt, True, gate=held_gate)
        bad_prompt = filter_prompts[BATCH_ROWS]
        self.filter_content(self.mock_a, bad_prompt, '{"value":"true"}', gate=bad_gate)
        fields = "id, AI_MAP('semantic_b', CONCAT('M:', prompt))"
        suffix = "WHERE AI_FILTER('semantic_a', CONCAT('F:', prompt)) ORDER BY id"
        pending = self.fixtures.start(lambda: self.select(fields, suffix))
        wait_evidence(self, self.mock_a,
                      lambda: all(self.mock_a.counts[prompt] == 1
                                  for prompt in filter_prompts[:BATCH_ROWS + 1]),
                      pending, message="held first batch and malformed second batch must both arrive")
        assert_held(self, self.mock_a, [held_gate, bad_gate])
        bad_gate.set()
        pending.expect_sql_error(self, 4070, timeout=3)
        assert_held(self, self.mock_a, [held_gate], set(filter_prompts) - {bad_prompt})
        self.assertFalse(self.mock_b.requests, "a malformed filter result leaked into downstream map")
        self.assertEqual(self.mock_a.counts[bad_prompt], 1)
        self.assertTrue(all(count == 1 for count in self.mock_a.counts.values()))
        self.assert_no_model_sockets()
        self.load(["filter-recovery"])
        self.filter_reply(self.mock_a, "F:filter-recovery", True)
        recovery = self.fixtures.start(lambda: self.select(fields, suffix))
        self.assertEqual(recovery.result(timeout=3), ((0, answer("M:filter-recovery")),))
        self.assertFalse(held_gate.is_set(), "recovery required releasing the abandoned requests")

    def test_retries_preserve_requests_without_resending_successful_rows(self):
        prompts = [f"retry-{index}" for index in range(ROW_COUNT)]
        self.load(prompts)
        map_prompts = ["M:" + prompt for prompt in prompts]
        filter_prompts = ["F:" + prompt for prompt in prompts]
        gate = self.fixtures.gate()
        self.mock_a.scenarios[map_prompts[0]] = [Reply(gate=gate)]
        map_retry = map_prompts[BATCH_ROWS + 1]
        filter_retry = filter_prompts[BATCH_ROWS + 2]
        self.mock_a.scenarios[map_retry] = [
            Reply(429, b'{"error":"retry-map"}', {"Retry-After": "0"}), Reply()]
        values = [index % 2 == 0 for index in range(ROW_COUNT)]
        for prompt, value in zip(filter_prompts, values):
            self.filter_reply(self.mock_b, prompt, value)
        self.mock_b.scenarios[filter_retry].insert(
            0, Reply(503, b'{"error":"retry-filter"}', {"Retry-After": "0"}))
        pending = self.fixtures.start(lambda: self.select(PAIR_FIELDS))
        for mock, prompt in ((self.mock_a, map_retry), (self.mock_b, filter_retry)):
            wait_evidence(self, mock, lambda mock=mock, prompt=prompt: mock.counts[prompt] == 2,
                          pending, timeout=6,
                          message="later batches must retry while the first map response is held")
        assert_held(self, self.mock_a, [gate], {map_prompts[0]})
        gate.set()
        self.assertEqual(pending.result(), tuple((index, answer(map_prompts[index]), int(values[index]))
                                                 for index in range(ROW_COUNT)))
        self.assert_counts(self.mock_a, map_prompts + [map_retry])
        self.assert_counts(self.mock_b, filter_prompts + [filter_retry])
        for mock, prompt in ((self.mock_a, map_retry), (self.mock_b, filter_retry)):
            bodies = [body for body in mock.requests if body["messages"][-1]["content"] == prompt]
            self.assertEqual(len(bodies), 2)
            self.assertEqual(bodies[0], bodies[1], "retry changed the prompt/options/schema")

    def test_cancel_held_map_and_filter_then_reuse_connection(self):
        pending, gates, map_prompts, filter_prompts = self.held_pair("cancel")
        for mock, prompts in ((self.mock_a, map_prompts), (self.mock_b, filter_prompts)):
            wait_evidence(self, mock,
                          lambda mock=mock, prompts=prompts: mock.counts[prompts[BATCH_ROWS]] == 1,
                          pending, message="cancellation must cover both operators and later batches")
        for mock, gate in zip(self.fixtures.mocks, gates):
            assert_held(self, mock, [gate])
        self.kill_query()
        pending.expect_sql_error(self, 1317, timeout=3)
        for mock, gate in zip(self.fixtures.mocks, gates):
            assert_held(self, mock, [gate])
        self.assert_no_model_sockets()
        self.reuse_pair_while_old_responses_are_held(gates)

    def test_deadline_held_map_and_filter_then_reuse_connection(self):
        self.cursor.execute("SET ob_query_timeout = 3000000")
        pending, gates, map_prompts, filter_prompts = self.held_pair("deadline")
        for mock, prompts in ((self.mock_a, map_prompts), (self.mock_b, filter_prompts)):
            wait_evidence(self, mock, lambda mock=mock, prompts=prompts: mock.counts[prompts[0]] == 1,
                          pending, message="both map and filter must have pending requests at deadline")
        pending.expect_sql_error(self, 4012, timeout=5)
        for mock, gate in zip(self.fixtures.mocks, gates):
            assert_held(self, mock, [gate])
        self.assert_no_model_sockets()
        self.reuse_pair_while_old_responses_are_held(gates)

    def exercise_retry_backoff_abort(self, *, cancel):
        self.load(["backoff"])
        gate = self.fixtures.gate()
        self.mock_a.default_reply = Reply(gate=gate)
        self.filter_reply(self.mock_b, "F:backoff", True)
        self.mock_b.scenarios["F:backoff"].insert(
            0, Reply(429, b'{"error":"retry-later"}', {"Retry-After": "60"}))
        if not cancel:
            self.cursor.execute("SET ob_query_timeout = 3000000")
        pending = self.fixtures.start(lambda: self.select(PAIR_FIELDS))
        wait_evidence(self, self.mock_a, lambda: self.mock_a.counts["M:backoff"] == 1, pending,
                      message="map must remain independently active during filter retry backoff")
        wait_evidence(self, self.mock_b, lambda: "F:backoff" in self.mock_b.finished, pending,
                      message="rate-limit response did not enter the retry queue")
        assert_held(self, self.mock_a, [gate])
        if cancel:
            self.kill_query()
        pending.expect_sql_error(self, 1317 if cancel else 4012, timeout=5)
        self.assert_counts(self.mock_b, ["F:backoff"])
        self.assert_no_model_sockets()
        self.reuse_pair_while_old_responses_are_held([gate])
        self.assertEqual(self.mock_b.counts["F:backoff"], 1, "abandoned retry was submitted")

    def test_cancel_retry_backoff_and_reuse_connection(self):
        self.exercise_retry_backoff_abort(cancel=True)

    def test_deadline_retry_backoff_and_reuse_connection(self):
        self.exercise_retry_backoff_abort(cancel=False)

    def test_reused_expression_nodes_are_not_requested_twice(self):
        prompts = [f"reuse-{index}" for index in range(ROW_COUNT)]
        self.load(prompts)
        filter_prompts = ["F:" + prompt for prompt in prompts]
        values = [index % 2 == 0 for index in range(ROW_COUNT)]
        for prompt, value in zip(filter_prompts, values):
            self.filter_reply(self.mock_b, prompt, value)
        self.cursor.execute(
            "SELECT id, mapped, mapped, accepted, NOT accepted FROM (" +
            self.statement("id, AI_MAP('semantic_a', prompt) AS mapped, "
                           "AI_FILTER('semantic_b', CONCAT('F:', prompt)) AS accepted", "") +
            ") derived ORDER BY id")
        self.assertEqual(self.cursor.fetchall(),
                         tuple((index, answer(prompt), answer(prompt), int(values[index]),
                                int(not values[index])) for index, prompt in enumerate(prompts)))
        self.assert_counts(self.mock_a, prompts)
        self.assert_counts(self.mock_b, filter_prompts)


def case_names(test_class):
    return unittest.defaultTestLoader.getTestCaseNames(test_class)


def parse_options(arguments=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, help="Built seekdb binary for a new private test database")
    parser.add_argument("--case", action="append", default=[], help="Test method name; repeat to select cases")
    parser.add_argument("--self-test", action="store_true", help="Run fixtures only; never start a database")
    parser.add_argument("--list-cases", action="store_true", help="List selected-suite cases without running")
    options = parser.parse_args(arguments)
    test_class = SemanticHarnessTests if options.self_test else SemanticContracts
    unknown = [name for name in options.case if name not in case_names(test_class)]
    if unknown:
        parser.error(f"unknown {test_class.__name__} case(s): {', '.join(unknown)}")
    if options.self_test and options.binary is not None:
        parser.error("--self-test cannot be combined with --binary")
    if not options.self_test and not options.list_cases and options.binary is None:
        parser.error("--binary is required unless --self-test or --list-cases is selected")
    return options


def run_suite(test_class, selected):
    names = selected or case_names(test_class)
    result = unittest.TextTestRunner(verbosity=2).run(
        unittest.TestSuite(test_class(name) for name in names))
    if not result.wasSuccessful():
        raise SystemExit(1)


def run_contracts(connection, server, sql_socket, selected):
    SemanticContracts.connection = connection
    SemanticContracts.server = server
    SemanticContracts.sql_socket = sql_socket
    run_suite(SemanticContracts, selected)


def main(arguments=None):
    options = parse_options(arguments)
    test_class = SemanticHarnessTests if options.self_test else SemanticContracts
    if options.list_cases:
        for name in options.case or case_names(test_class):
            print(name)
    elif options.self_test:
        run_suite(SemanticHarnessTests, options.case)
    else:
        binary = options.binary.resolve()
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise SystemExit(f"--binary is not an executable file: {binary}")
        previous = sys.argv
        try:
            sys.argv = [previous[0], "--binary", str(binary)]
            runtime.main(lambda *args: run_contracts(*args, options.case),
                         sql_timeout=75, description=__doc__)
        finally:
            sys.argv = previous


if __name__ == "__main__":
    main()
