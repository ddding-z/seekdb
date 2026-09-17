"""Opt-in real-model SQL tests using the seekdb-debug notebook API settings."""

from collections import Counter
import json
import os
import subprocess
import sys
import time
import unittest
from urllib.parse import urlsplit
import uuid

import pymysql

import test_runtime as runtime


API_BASE = os.environ.get("SEEKDB_AI_API_BASE", "http://copilot-api:4141/v1")
MODEL = os.environ.get("SEEKDB_AI_MODEL", "gemini-3.8-flash")
API_KEY = os.environ.get("SEEKDB_AI_API_KEY", "dummy")
QUERY_TIMEOUT_SECONDS = 180
OPTIONS = {"temperature": 0, "max_tokens": 256}
CASES = {
    "scalar": ("test_scalar_notebook_prompt", 1),
    "batch": ("test_batch_row_mapping_and_case_skip", 3),
    "fallback": ("test_dynamic_model_scalar_fallback", 3),
    "json": ("test_json_prompt", 1),
}


def request_for(row_id, left, right):
    return ("Return exactly one JSON object with keys row_id and answer. "
            f"row_id must be {json.dumps(row_id)}. "
            f"answer must be the integer result of {left} + {right}. "
            "No markdown, explanation, or additional keys.")


def validate_answer(value, row_id, expected):
    if not isinstance(value, str) or not value.strip():
        raise AssertionError("Model did not return nonempty text")
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as error:
        raise AssertionError("Model did not return the requested JSON; response omitted") from error
    if (not isinstance(parsed, dict) or set(parsed) != {"row_id", "answer"} or
            parsed["row_id"] != row_id or type(parsed["answer"]) is not int or
            parsed["answer"] != expected):
        raise AssertionError("Model answer or SQL row mapping differs from the independent expected value")


def process_proxy_settings():
    entries = list(filter(None, os.environ.get("no_proxy", os.environ.get("NO_PROXY", "")).split(",")))
    entries.extend(["localhost", "127.0.0.1"])
    if urlsplit(API_BASE).hostname == "copilot-api":
        entries.append("copilot-api")
    value = ",".join(dict.fromkeys(entries))
    return {"no_proxy": value, "NO_PROXY": value}


class OracleTests(unittest.TestCase):
    def test_live_execution_requires_explicit_opt_in(self):
        result = subprocess.run([sys.executable, __file__, "--binary", "/not-a-test-binary"],
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 2)
        self.assertIn("--allow-live", result.stderr)

    def test_expected_json_is_accepted(self):
        validate_answer('{"answer":8,"row_id":"row-a"}', "row-a", 8)

    def test_wrong_row_answer_type_and_empty_output_are_rejected(self):
        values = ["", "not json", '{"row_id":"row-b","answer":8}',
                  '{"row_id":"row-a","answer":9}', '{"row_id":"row-a","answer":"8"}',
                  '{"row_id":"row-a","answer":true}', '{"row_id":"row-a","answer":8,"extra":0}']
        for value in values:
            with self.subTest(value=value):
                with self.assertRaises(AssertionError):
                    validate_answer(value, "row-a", 8)

    def test_prompt_is_bound_to_independent_input(self):
        prompt = request_for('row-"quoted', 3, 5)
        self.assertIn(json.dumps('row-"quoted'), prompt)
        self.assertIn("3 + 5", prompt)


class LiveSQLTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not MODEL or not API_KEY:
            raise ValueError("SEEKDB_AI_MODEL and SEEKDB_AI_API_KEY must not be empty")
        parsed = urlsplit(API_BASE)
        if (parsed.scheme not in ("http", "https") or not parsed.hostname or
                parsed.username is not None or parsed.password is not None or parsed.query or parsed.fragment):
            raise ValueError("SEEKDB_AI_API_BASE must be an HTTP(S) base URL without credentials, query or fragment")
        cls.model_name = "live_" + uuid.uuid4().hex[:12]
        cls.endpoint_name = cls.model_name + "_endpoint"
        with cls.connection.cursor() as cursor:
            cursor.execute("SET ob_query_timeout = %s", (QUERY_TIMEOUT_SECONDS * 1000000,))
            cursor.execute("SET ob_trx_timeout = %s", (QUERY_TIMEOUT_SECONDS * 1000000,))
            cursor.execute("CREATE DATABASE ai_live_test")
            cursor.execute("USE ai_live_test")
            cursor.execute("CREATE TABLE inputs (id INT PRIMARY KEY, prompt LONGTEXT, model VARCHAR(64))")
            cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL(%s, %s)",
                           (cls.model_name, json.dumps({"type": "completion", "model_name": MODEL})))
            cls.addClassCleanup(cls.drop_registration, "DROP_AI_MODEL", cls.model_name)
            cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT(%s, %s)",
                           (cls.endpoint_name, json.dumps({"ai_model_name": cls.model_name,
                            "url": API_BASE.rstrip("/") + "/chat/completions", "access_key": API_KEY,
                            "request_model_name": MODEL, "provider": "openai"})))
            cls.addClassCleanup(cls.drop_registration, "DROP_AI_MODEL_ENDPOINT", cls.endpoint_name)

    @classmethod
    def drop_registration(cls, operation, name):
        with cls.connection.cursor() as cursor:
            cursor.execute(f"CALL DBMS_AI_SERVICE.{operation}(%s)", (name,))

    def setUp(self):
        self.cursor = self.connection.cursor()
        self.addCleanup(self.cursor.close)
        self.cursor.execute("TRUNCATE TABLE inputs")

    def execute(self, statement, parameters, logical_requests):
        start = time.monotonic()
        status = "error"
        try:
            self.cursor.execute(statement, parameters)
            rows = self.cursor.fetchall()
            self.assertTrue(rows, "SQL returned no rows")
            status = "returned"
            return rows
        finally:
            elapsed = time.monotonic() - start
            print(f"LIVE {self._testMethodName}: logical_requests={logical_requests} "
                  f"sql_status={status} sql_wall={elapsed:.3f}s", flush=True)

    def test_scalar_notebook_prompt(self):
        rows = self.execute("SELECT AI_COMPLETE(%s, %s)",
                            (self.model_name, "Reply with exactly SEEKDB_AI_OK and no other text."), 1)
        self.assertEqual(len(rows), 1)
        self.assertIsInstance(rows[0][0], str)
        self.assertEqual(rows[0][0].strip(), "SEEKDB_AI_OK", "Model instruction compliance failed")

    def load_arithmetic_inputs(self):
        expected = {}
        values = []
        for index, (left, right) in enumerate(((3, 5), (12, 7), (21, 4))):
            row_id = uuid.uuid4().hex
            expected[index] = (row_id, left + right)
            values.append((index, request_for(row_id, left, right), self.model_name))
        self.cursor.executemany("INSERT INTO inputs VALUES (%s, %s, %s)", values)
        return expected

    def check_rows(self, rows, expected):
        self.assertEqual(Counter(row[0] for row in rows), Counter(expected.keys()))
        for index, value in rows:
            row_id, answer = expected[index]
            validate_answer(value, row_id, answer)

    def test_batch_row_mapping_and_case_skip(self):
        expected = self.load_arithmetic_inputs()
        self.cursor.execute("INSERT INTO inputs VALUES (3, NULL, %s)", (self.model_name,))
        rows = self.execute("SELECT id, CASE WHEN id < 3 THEN AI_COMPLETE(%s, prompt, %s) "
                            "ELSE 'skipped' END FROM inputs ORDER BY id",
                            (self.model_name, json.dumps(OPTIONS)), 3)
        self.assertEqual(rows[-1], (3, "skipped"))
        self.check_rows(rows[:-1], expected)

    def test_dynamic_model_scalar_fallback(self):
        expected = self.load_arithmetic_inputs()
        rows = self.execute("SELECT id, AI_COMPLETE(model, prompt, %s) FROM inputs ORDER BY id",
                            (json.dumps(OPTIONS),), 3)
        self.check_rows(rows, expected)

    def test_json_prompt(self):
        row_id = uuid.uuid4().hex
        prompt = request_for(row_id, 8, 9)
        rows = self.execute("SELECT AI_COMPLETE(%s, AI_PROMPT('{0}', %s), %s)",
                            (self.model_name, prompt, json.dumps(OPTIONS)), 1)
        self.assertEqual(len(rows), 1)
        validate_answer(rows[0][0], row_id, 17)


def run_live_checks(connection, server, sql_socket):
    if server is not None:
        raise AssertionError("Live tests must not start a mock service")
    LiveSQLTests.connection = connection
    selected = os.environ.get("SEEKDB_AI_LIVE_CASE", "all")
    if selected != "all" and selected not in CASES:
        raise ValueError("SEEKDB_AI_LIVE_CASE must be all, scalar, batch, fallback or json")
    cases = list(CASES.values()) if selected == "all" else [CASES[selected]]
    logical_requests = sum(count for _, count in cases)
    print(f"LIVE provider=openai model={MODEL} host={urlsplit(API_BASE).hostname}; "
          f"case={selected} logical_requests={logical_requests} on success; "
          "client retries may increase upstream calls.", flush=True)
    suite = unittest.TestSuite(LiveSQLTests(method) for method, _ in cases)
    result = unittest.TextTestRunner(verbosity=2, failfast=True).run(suite)
    if not result.wasSuccessful():
        raise SystemExit(1)


if __name__ == "__main__":
    if "--self-test" in sys.argv:
        unittest.main(argv=[sys.argv[0]], defaultTest="OracleTests", verbosity=2)
    else:
        runtime.main(run_live_checks, live=True, server_env=process_proxy_settings(),
                     sql_timeout=QUERY_TIMEOUT_SECONDS + 30, description=__doc__)