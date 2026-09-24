"""Black-box runtime contracts; use --self-test for the test harness only."""

from collections import Counter
from dataclasses import dataclass, field
from email.utils import formatdate, parsedate_to_datetime
from http.client import HTTPConnection
import json
from pathlib import Path
import socket
import struct
import sys
import threading
import time
import unittest

import pymysql

import test_runtime as runtime


REQUEST_LIMIT = 4 * 1024 * 1024
RESPONSE_LIMIT = 8 * 1024 * 1024


def answer(prompt):
    return 'result:"' + prompt + '"\n'


def completion(content, *, finish_reason=None, refusal=None):
    choice = {"message": {"content": content}}
    if finish_reason is not None:
        choice["finish_reason"] = finish_reason
    if refusal is not None:
        choice["message"]["refusal"] = refusal
    return json.dumps({"choices": [choice]}, ensure_ascii=False).encode("utf-8")


def embedding_response(inputs, vectors, response_format="openai"):
    index_key = "text_index" if response_format == "dashscope" else "index"
    entries = [{index_key: index, "embedding": vectors[text]}
               for index, text in reversed(list(enumerate(inputs)))]
    response = {"output": {"embeddings": entries}} if response_format == "dashscope" else {"data": entries}
    return json.dumps(response).encode("utf-8")


def constrained_options(schema):
    return {"response_format": {"type": "json_schema", "json_schema": {
        "name": "seekdb_output", "strict": True, "schema": schema}}}


def sized_completion(size):
    prefix = b'{"choices":[{"message":{"content":"'
    suffix = b'"}}]}'
    return prefix + b"x" * (size - len(prefix) - len(suffix)) + suffix


def sql_error(test, code, operation):
    with test.assertRaises(pymysql.MySQLError) as caught:
        operation()
    expected = (code,) if isinstance(code, int) else code
    test.assertIn(caught.exception.args[0], expected, caught.exception.args)
    return caught.exception


def model_socket_fds(process_id, port):
    process = Path("/proc") / str(process_id)
    inodes = {f"socket:[{fields[9]}]"
              for line in (process / "net/tcp").read_text().splitlines()[1:]
              if (fields := line.split()) and int(fields[2].rsplit(":", 1)[1], 16) == port}
    descriptors = set()
    for descriptor in (process / "fd").iterdir():
        try:
            if str(descriptor.readlink()) in inodes:
                descriptors.add(descriptor.name)
        except FileNotFoundError:
            continue
    return descriptors


@dataclass
class Reply:
    status: int = 200
    body: bytes | None = None
    headers: dict = field(default_factory=dict)
    gate: threading.Event | None = None
    peers: int = 0
    drop: bool = False
    truncate: bool = False


class ContractHandler(runtime.MockHandler):
    def handle(self):
        try:
            super().handle()
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_POST(self):
        server = self.server
        record = None
        try:
            raw_body = self.rfile.read(int(self.headers["Content-Length"]))
            body = json.loads(raw_body)
            inputs = None
            if "messages" in body:
                prompt = body["messages"][-1]["content"]
            else:
                inputs = body["input"]
                if isinstance(inputs, dict):
                    inputs = inputs["texts"]
                if not isinstance(inputs, list) or not inputs or not all(isinstance(text, str) for text in inputs):
                    raise AssertionError("embedding input must be a nonempty array of strings")
                prompt = inputs[0] if len(inputs) == 1 else tuple(inputs)
            with server.condition:
                server.active += 1
                server.peak = max(server.peak, server.active)
                server.counts[prompt] += 1
                server.requests.append(body)
                record = {"prompt": prompt, "received": time.monotonic(),
                          "wall_received": time.time(), "response": None,
                          "request_bytes": len(raw_body),
                          "headers": {name.lower(): value for name, value in self.headers.items()}}
                server.audit.append(record)
                sequence = server.scenarios.get(prompt, server.scenarios.get(
                    inputs[0] if inputs else prompt, [server.default_reply]))
                reply = sequence[min(server.counts[prompt] - 1, len(sequence) - 1)]
                server.condition.notify_all()
                if reply.peers:
                    if not server.condition.wait_for(lambda: len(server.audit) >= reply.peers, timeout=3):
                        raise AssertionError("peer request did not reach mock")
            if reply.gate is not None and not reply.gate.wait(timeout=10):
                raise AssertionError("test did not release response gate")
            if reply.drop:
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
                return
            raw = reply.body
            if raw is None:
                raw = (completion(answer(prompt)) if inputs is None else
                       embedding_response(inputs, server.embedding_vectors, server.embedding_format))
            self.send_response(reply.status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw) + (100 if reply.truncate else 0)))
            if reply.truncate:
                self.send_header("Connection", "close")
                self.close_connection = True
            for name, value in reply.headers.items():
                self.send_header(name, value)
            self.end_headers()
            with server.condition:
                record["response"] = time.monotonic()
                server.condition.notify_all()
            self.wfile.write(raw)
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as error:
            with server.condition:
                server.fixture_errors.append(repr(error))
            self.close_connection = True
        finally:
            if record is not None:
                with server.condition:
                    server.active -= 1
                    server.finished.append(record["prompt"])
                    server.condition.notify_all()


def configure_mock(server):
    server.RequestHandlerClass = ContractHandler
    server.audit = []
    server.scenarios = {}
    server.default_reply = Reply()
    server.fixture_errors = []
    server.embedding_vectors = {}
    server.embedding_format = "openai"


class HarnessTests(unittest.TestCase):
    def test_wrong_error_code_is_rejected(self):
        def wrong_error():
            raise pymysql.OperationalError(1210, "not a timeout")
        with self.assertRaises(AssertionError):
            sql_error(self, 4012, wrong_error)

    def test_success_is_not_an_expected_failure(self):
        with self.assertRaises(AssertionError):
            sql_error(self, 4012, lambda: None)

    def test_response_boundary_fixture_is_valid_json(self):
        for size in (RESPONSE_LIMIT - 1, RESPONSE_LIMIT, RESPONSE_LIMIT + 1):
            raw = sized_completion(size)
            self.assertEqual(len(raw), size)
            value = json.loads(raw)["choices"][0]["message"]["content"]
            self.assertTrue(value and set(value) == {"x"})

    def test_scripted_http_status_and_body(self):
        server = runtime.MockServer()
        configure_mock(server)
        server.scenarios["probe"] = [Reply(429, b"not json", {"Retry-After": "4"}), Reply()]
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            client = HTTPConnection("127.0.0.1", server.server_port, timeout=5)
            try:
                body = json.dumps({"messages": [{"content": "probe"}]})
                for expected in (429, 200):
                    client.request("POST", "/", body, {"Content-Type": "application/json",
                                                      "aUtHoRiZaTiOn": "Bearer fixture-only"})
                    response = client.getresponse()
                    self.assertEqual(response.status, expected)
                    if expected == 429:
                        self.assertEqual(response.getheader("Retry-After"), "4")
                        self.assertEqual(response.read(), b"not json")
                    else:
                        self.assertEqual(json.loads(response.read())["choices"][0]["message"]["content"],
                                         answer("probe"))
            finally:
                client.close()
            self.assertEqual(server.counts, Counter({"probe": 2}))
            self.assertEqual(server.audit[0]["headers"]["authorization"], "Bearer fixture-only")
            self.assertFalse(server.fixture_errors)
        finally:
            server.shutdown()
            server.server_close()
            worker.join()

    def test_native_embedding_fixture_preserves_indices_and_distinct_vectors(self):
        server = runtime.MockServer()
        configure_mock(server)
        server.embedding_vectors = {"left": [0.25, -1], "right": [0.5, 2]}
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            client = HTTPConnection("127.0.0.1", server.server_port, timeout=5)
            try:
                body = json.dumps({"model": "fixture-embed", "input": ["left", "right", "left"]})
                client.request("POST", "/", body, {"Content-Type": "application/json"})
                response = client.getresponse()
                self.assertEqual(response.status, 200)
                self.assertEqual(json.loads(response.read()), {"data": [
                    {"index": 2, "embedding": [0.25, -1]},
                    {"index": 1, "embedding": [0.5, 2]},
                    {"index": 0, "embedding": [0.25, -1]}]})
            finally:
                client.close()
            self.assertEqual(server.counts, Counter({("left", "right", "left"): 1}))
            self.assertFalse(server.fixture_errors)
        finally:
            server.shutdown()
            server.server_close()
            worker.join()


class RuntimeContracts(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        configure_mock(cls.server)
        with cls.connection.cursor() as cursor:
            cursor.execute("CREATE DATABASE ai_contract_test")
            cursor.execute("USE ai_contract_test")
            cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL(%s, %s)",
                           ("contract_model", json.dumps({"type": "completion", "model_name": "mock"})))
            endpoint = {"ai_model_name": "contract_model", "url":
                        f"http://127.0.0.1:{cls.server.server_port}/v1/chat/completions",
                        "access_key": "contract-test-only", "provider": "openai",
                        "request_model_name": "mock"}
            cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT(%s, %s)",
                           ("contract_endpoint", json.dumps(endpoint)))
            cursor.execute("CREATE TABLE inputs (id INT PRIMARY KEY, prompt LONGTEXT, model VARCHAR(64))")
            cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL(%s, %s)",
                           ("contract_embed", json.dumps({"type": "dense_embedding", "model_name": "mock-embed"})))
            endpoint.update(ai_model_name="contract_embed", request_model_name="mock-embed")
            cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT(%s, %s)",
                           ("contract_embed_endpoint", json.dumps(endpoint)))

    def setUp(self):
        self.gates = []
        self.server = runtime.MockServer()
        self.server.daemon_threads = False
        configure_mock(self.server)
        self.mock_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.mock_thread.start()
        self.addCleanup(self.close_mock)
        self.cursor = self.connection.cursor()
        self.addCleanup(self.cursor.close)
        self.cursor.execute("SET ob_query_timeout = 20000000")
        for endpoint in ("contract_endpoint", "contract_embed_endpoint"):
            self.cursor.execute("CALL DBMS_AI_SERVICE.ALTER_AI_MODEL_ENDPOINT(%s, %s)",
                                (endpoint, json.dumps({"url": f"http://127.0.0.1:{self.server.server_port}/",
                                                       "provider": "openai"})))
        self.cursor.execute("TRUNCATE TABLE inputs")

    def close_mock(self):
        for gate in self.gates:
            gate.set()
        self.server.shutdown()
        self.server.server_close()
        self.mock_thread.join()
        self.assertEqual(self.server.active, 0)
        self.assertFalse(self.server.fixture_errors, self.server.fixture_errors)

    def load(self, prompts):
        self.cursor.executemany("INSERT INTO inputs VALUES (%s, %s, 'contract_model')",
                                list(enumerate(prompts)))

    def load_embeddings(self, prompts):
        self.load(prompts)
        self.server.embedding_vectors = {text: [index + 0.125, -index - 0.25, 0.5]
                                         for index, text in enumerate(dict.fromkeys(prompts))}
        return tuple((index, self.server.embedding_vectors[text]) for index, text in enumerate(prompts))

    def query_embeddings(self, expression="AI_EMBED('contract_embed', prompt, 3)"):
        self.cursor.execute("SELECT /*+ OPT_PARAM('rowsets_max_rows', 128) */ "
                            f"id, {expression} FROM inputs ORDER BY id")
        return tuple((row, json.loads(value)) for row, value in self.cursor.fetchall())

    def query(self, expression="AI_COMPLETE('contract_model', prompt)", suffix="ORDER BY id"):
        self.cursor.execute(f"SELECT id, {expression} FROM inputs {suffix}")
        return self.cursor.fetchall()

    def query_pipeline(self, expression="AI_COMPLETE('contract_model', prompt)", suffix="ORDER BY id"):
        self.cursor.execute("SELECT /*+ OPT_PARAM('rowsets_max_rows', 16) */ "
                            f"id, {expression} FROM inputs {suffix}")
        return self.cursor.fetchall()

    def query_constrained(self, schema, model="'contract_model'", **options):
        config = constrained_options(schema) | options
        self.cursor.execute(f"SELECT id, AI_COMPLETE({model}, prompt, %s) FROM inputs ORDER BY id",
                            (json.dumps(config),))
        return self.cursor.fetchall()

    def gate(self):
        gate = threading.Event()
        self.gates.append(gate)
        return gate

    def test_completion_pipeline_submits_next_batch_before_first_response(self):
        prompts = [f"completion-pipeline-{index}" for index in range(50)]
        self.load(prompts)
        gate = self.gate()
        self.server.default_reply = Reply(gate=gate)
        worker, outcome = self.start_embedding_query(self.query_pipeline)
        try:
            with self.server.condition:
                self.assertTrue(self.server.condition.wait_for(lambda: len(self.server.audit) > 16, timeout=3),
                                f"only {len(self.server.audit)} requests submitted before the first response")
                self.assertLessEqual(len(self.server.audit), 32, "at most two SQL batches may be pending")
                self.assertTrue(all(record["response"] is None for record in self.server.audit))
        finally:
            gate.set()
            worker.join(5)
        self.assertFalse(worker.is_alive(), "completion pipeline did not drain its final batch")
        self.assertEqual(outcome, [tuple((index, answer(prompt)) for index, prompt in enumerate(prompts))])
        self.assertEqual(self.server.counts, Counter(prompts))

    def test_completion_pipeline_refills_after_first_batch_finishes(self):
        prompts = [f"completion-refill-{index}" for index in range(50)]
        self.load(prompts)
        first_gate, other_gate = self.gate(), self.gate()
        self.server.default_reply = Reply(gate=other_gate)
        for prompt in prompts[:16]:
            self.server.scenarios[prompt] = [Reply(gate=first_gate)]
        worker, outcome = self.start_embedding_query(self.query_pipeline)
        try:
            with self.server.condition:
                self.assertTrue(self.server.condition.wait_for(lambda: len(self.server.audit) == 32, timeout=3))
            first_gate.set()
            with self.server.condition:
                self.assertTrue(self.server.condition.wait_for(lambda: len(self.server.audit) >= 48, timeout=3),
                                "the completed first batch must free a slot while the second is unfinished")
                self.assertEqual(len(self.server.audit), 48)
                self.assertEqual(sum(record["response"] is None for record in self.server.audit), 32)
        finally:
            first_gate.set()
            other_gate.set()
            worker.join(5)
        self.assertFalse(worker.is_alive())
        self.assertEqual(outcome, [tuple((index, answer(prompt)) for index, prompt in enumerate(prompts))])
        self.assertEqual(self.server.counts, Counter(prompts))

    def test_completion_pipeline_later_error_cancels_held_first_batch(self):
        prompts = [f"completion-later-error-{index}" for index in range(50)]
        self.load(prompts)
        process_id = struct.unpack("3i", self.connection._sock.getsockopt(
            socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i")))[0]
        config = self.connection.escape(json.dumps(constrained_options({"type": "boolean"})))
        cases = [(Reply(body=b'{"unexpected":true}'), 4070, None),
                 (Reply(400, b"{}"), 4216, None),
                 (Reply(body=completion('"not-a-boolean"', finish_reason="stop")), 4070, config)]
        for reply, code, options in cases:
            with self.subTest(code=code, constrained=options is not None):
                gate = self.gate()
                before = len(self.server.audit)
                valid_body = completion("true", finish_reason="stop") if options else None
                self.server.default_reply = Reply(body=valid_body)
                for prompt in prompts[:16]:
                    self.server.scenarios[prompt] = [Reply(body=valid_body, gate=gate)]
                reply.peers = before + 17
                self.server.scenarios[prompts[16]] = [reply]
                expression = (f"AI_COMPLETE('contract_model', prompt, {options})" if options else
                              "AI_COMPLETE('contract_model', prompt)")
                self.cursor.execute("SET ob_query_timeout = 3000000")
                try:
                    start = time.monotonic()
                    sql_error(self, code, lambda: self.query_pipeline(expression))
                    self.assertLess(time.monotonic() - start, 2.5)
                    self.assertGreater(len(self.server.audit) - before, 16)
                    self.assertLessEqual(len(self.server.audit) - before, 32)
                    self.assertTrue(any(record["response"] is None for record in self.server.audit[before:]))
                    self.assertFalse(model_socket_fds(process_id, self.server.server_port))
                finally:
                    gate.set()
                self.server.scenarios.clear()
                self.server.default_reply = Reply()
                self.cursor.execute("SET ob_query_timeout = 20000000")
                self.assertEqual(self.query_pipeline(),
                                 tuple((index, answer(prompt)) for index, prompt in enumerate(prompts)))

    def test_completion_pipeline_cancel_releases_both_batches_and_recovers(self):
        prompts = [f"completion-cancel-{index}" for index in range(50)]
        self.load(prompts)
        gate = self.gate()
        self.server.default_reply = Reply(gate=gate)
        worker, outcome = self.start_embedding_query(self.query_pipeline)
        try:
            with self.server.condition:
                self.assertTrue(self.server.condition.wait_for(lambda: len(self.server.audit) == 32, timeout=3))
            process_id = struct.unpack("3i", self.connection._sock.getsockopt(
                socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i")))[0]
            self.assertEqual(len(model_socket_fds(process_id, self.server.server_port)), 32)
            with pymysql.connect(unix_socket=self.sql_socket, user="root", autocommit=True) as control:
                with control.cursor() as cursor:
                    cursor.execute(f"KILL QUERY {self.connection.thread_id()}")
            worker.join(3)
            self.assertFalse(worker.is_alive())
            self.assertEqual(len(outcome), 1)
            self.assertIsInstance(outcome[0], pymysql.MySQLError)
            self.assertEqual(outcome[0].args[0], 1317)
            self.assertEqual(len(self.server.audit), 32)
            self.assertFalse(model_socket_fds(process_id, self.server.server_port))
        finally:
            gate.set()
            worker.join(5)
        self.server.default_reply = Reply()
        self.assertEqual(self.query_pipeline(), tuple((index, answer(prompt)) for index, prompt in enumerate(prompts)))

    def test_completion_pipeline_deadline_releases_both_batches_and_recovers(self):
        prompts = [f"completion-deadline-{index}" for index in range(50)]
        self.load(prompts)
        gate = self.gate()
        self.server.default_reply = Reply(gate=gate)
        self.cursor.execute("SET ob_query_timeout = 1000000")
        try:
            sql_error(self, 4012, self.query_pipeline)
            self.assertEqual(len(self.server.audit), 32)
            process_id = struct.unpack("3i", self.connection._sock.getsockopt(
                socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i")))[0]
            self.assertFalse(model_socket_fds(process_id, self.server.server_port))
        finally:
            gate.set()
        self.server.default_reply = Reply()
        self.cursor.execute("SET ob_query_timeout = 20000000")
        self.assertEqual(self.query_pipeline(), tuple((index, answer(prompt)) for index, prompt in enumerate(prompts)))

    def test_completion_pipeline_out_of_order_preserves_other_columns(self):
        prompts = [f"completion-owned-{index}:" + "\u4e2d\n\x00" * (2048 + index % 13) for index in range(50)]
        prompts[10] = prompts[8]
        self.load(prompts)
        self.cursor.execute("UPDATE inputs SET model = CONCAT('row-model-', id)")
        gate = self.gate()
        self.server.scenarios[prompts[-1]] = [Reply(gate=gate)]

        def query():
            self.cursor.execute("SELECT /*+ OPT_PARAM('rowsets_max_rows', 16) */ "
                                "id, prompt, model, AI_COMPLETE('contract_model', prompt) FROM inputs ORDER BY id DESC")
            return self.cursor.fetchall()

        worker, outcome = self.start_embedding_query(query)
        try:
            with self.server.condition:
                self.assertTrue(self.server.condition.wait_for(
                    lambda: len(self.server.audit) >= 32 and len(self.server.finished) >= 31, timeout=3))
                self.assertEqual(len(self.server.audit), 32, "ready results behind the first batch must stay bounded")
                self.assertTrue(any(record["response"] is None for record in self.server.audit))
        finally:
            gate.set()
            worker.join(5)
        self.assertFalse(worker.is_alive())
        expected = tuple((index, prompts[index], f"row-model-{index}", answer(prompts[index]))
                         for index in reversed(range(len(prompts))))
        self.assertEqual(outcome, [expected])
        self.assertEqual(self.server.counts, Counter(prompts))

    def test_completion_pipeline_schema_and_retry_preserve_requests(self):
        prompts = [f"completion-schema-{index}" for index in range(50)]
        self.load(prompts)
        expected = tuple((index, json.dumps({"row": index})) for index in range(len(prompts)))
        schema = {"type": "object", "properties": {"row": {"type": "integer"}},
                  "required": ["row"], "additionalProperties": False}
        options = constrained_options(schema) | {"temperature": 0}
        config = self.connection.escape(json.dumps(options))
        for index, prompt in enumerate(prompts):
            self.server.scenarios[prompt] = [Reply(body=completion(expected[index][1], finish_reason="stop"))]
        self.server.scenarios[prompts[19]].insert(0, Reply(429, b"{}", {"Retry-After": "0"}))
        gate = self.gate()
        self.server.scenarios[prompts[0]][0].gate = gate
        worker, outcome = self.start_embedding_query(
            lambda: self.query_pipeline(f"AI_COMPLETE('contract_model', prompt, {config})"))
        try:
            with self.server.condition:
                self.assertTrue(self.server.condition.wait_for(lambda: self.server.counts[prompts[19]] == 2, timeout=4),
                                "the second batch must retry while the first response is held")
                self.assertFalse(gate.is_set())
        finally:
            gate.set()
            worker.join(5)
        self.assertFalse(worker.is_alive())
        self.assertEqual(outcome, [expected])
        self.assertEqual(self.server.counts, Counter(prompts) + Counter({prompts[19]: 1}))
        for body in self.server.requests:
            self.assertEqual(body["response_format"], options["response_format"])
            self.assertEqual(body["temperature"], 0)

    def test_completion_pipeline_plan_selection_and_fallback(self):
        base = "SELECT id, AI_COMPLETE('contract_model', prompt) FROM inputs"
        cases = [(base + " ORDER BY id", True), (base + " WHERE id > 0 ORDER BY id DESC", True),
                 ("SELECT id, AI_COMPLETE('contract_model', prompt, '{\"temperature\":0}') FROM inputs", True),
                 (base + " ORDER BY id LIMIT 3", False), (base + " ORDER BY 2", False),
                 ("SELECT id, CASE WHEN id > 0 THEN AI_COMPLETE('contract_model', prompt) END FROM inputs", False),
                 ("SELECT id, AI_COMPLETE(model, prompt) FROM inputs", False),
                 ("SELECT id, AI_COMPLETE('contract_model', prompt, JSON_OBJECT('temperature', id)) FROM inputs", False),
                 ("SELECT id, AI_COMPLETE('contract_model', CONCAT(prompt, 'suffix')) FROM inputs", False),
                 ("SELECT AI_COMPLETE('contract_model', prompt), AI_EMBED('contract_embed', prompt) FROM inputs", False),
                 ("SELECT AI_COMPLETE('contract_model', 'constant')", False)]
        for query, expected in cases:
            with self.subTest(query=query):
                self.cursor.execute("EXPLAIN " + query)
                plan = "\n".join(str(row[0]) for row in self.cursor.fetchall())
                self.assertEqual("AI FUNCTION PIPELINE" in plan, expected, plan)
        self.assertFalse(self.server.requests)

    def test_completion_pipeline_json_column_preserves_prompt_contract(self):
        prompts = [f'json-column-{index}:\u4e2d\n"\x00' for index in range(50)]
        self.load(prompts)
        self.cursor.execute("CREATE TABLE pipeline_json_inputs (id INT PRIMARY KEY, prompt JSON)")
        self.addCleanup(lambda: self.cursor.execute("DROP TABLE pipeline_json_inputs"))
        self.cursor.execute("INSERT INTO pipeline_json_inputs "
                            "SELECT id, AI_PROMPT('hello {0}', CAST(prompt AS CHAR)) FROM inputs")
        query_sql = ("SELECT /*+ OPT_PARAM('rowsets_max_rows', 16) */ "
                     "id, AI_COMPLETE('contract_model', prompt) FROM pipeline_json_inputs ORDER BY id")
        self.cursor.execute("EXPLAIN " + query_sql)
        self.assertIn("AI FUNCTION PIPELINE", "\n".join(str(row[0]) for row in self.cursor.fetchall()))

        def query():
            self.cursor.execute(query_sql)
            return self.cursor.fetchall()

        gate = self.gate()
        self.server.default_reply = Reply(gate=gate)
        worker, outcome = self.start_embedding_query(query)
        try:
            with self.server.condition:
                self.assertTrue(self.server.condition.wait_for(lambda: len(self.server.audit) == 32, timeout=3))
        finally:
            gate.set()
            worker.join(5)
        self.assertFalse(worker.is_alive())
        self.assertEqual(outcome, [tuple((index, answer("hello " + prompt)) for index, prompt in enumerate(prompts))])
        self.assertEqual(self.server.counts, Counter("hello " + prompt for prompt in prompts))
        before = len(self.server.requests)
        self.cursor.execute("UPDATE pipeline_json_inputs SET prompt = JSON_OBJECT('unexpected', TRUE) WHERE id = 0")
        sql_error(self, 1210, query)
        self.assertEqual(len(self.server.requests), before, "invalid first-batch input must fail before HTTP")

    def test_completion_pipeline_scalar_engine_fallback(self):
        prompts = ["completion-scalar-left", "completion-scalar-middle", "completion-scalar-right"]
        self.load(prompts)
        expected = tuple((index, answer(prompt)) for index, prompt in enumerate(prompts))
        for batch_size in (0, 1):
            with self.subTest(batch_size=batch_size):
                query = (f"SELECT /*+ OPT_PARAM('rowsets_max_rows', {batch_size}) */ "
                         "id, AI_COMPLETE('contract_model', prompt) FROM inputs ORDER BY id")
                self.cursor.execute("EXPLAIN " + query)
                self.assertIn("AI FUNCTION PIPELINE", "\n".join(str(row[0]) for row in self.cursor.fetchall()))
                before = len(self.server.requests)
                self.cursor.execute(query)
                self.assertEqual(self.cursor.fetchall(), expected)
                self.assertEqual(Counter(body["messages"][-1]["content"] for body in self.server.requests[before:]),
                                 Counter(prompts))

    def test_completion_pipeline_filtered_rows_and_limit_do_not_add_calls(self):
        prompts = [f"completion-filter-{index}" for index in range(100)]
        self.load(prompts)
        expected = tuple((index, answer(prompt)) for index, prompt in enumerate(prompts))
        self.assertEqual(self.query_pipeline(suffix="WHERE id % 3 = 0 ORDER BY id"), expected[::3])
        self.assertEqual(self.server.counts, Counter(prompts[::3]))
        before = len(self.server.requests)
        self.assertEqual(self.query_pipeline(suffix="ORDER BY id LIMIT 3"), expected[:3])
        self.assertEqual([body["messages"][-1]["content"] for body in self.server.requests[before:]], prompts[:3])

    def test_default_parallelism_matches_batch_size(self):
        prompts = [f"parallel-{index}" for index in range(72)]
        self.load(prompts)
        gate = self.gate()
        for prompt in prompts:
            self.server.scenarios[prompt] = [Reply(gate=gate)]
        outcome = []

        def execute():
            try:
                self.cursor.execute("SELECT /*+ OPT_PARAM('rowsets_max_rows', 128) */ "
                                    "id, AI_COMPLETE('contract_model', prompt) FROM inputs ORDER BY id")
                outcome.append(self.cursor.fetchall())
            except Exception as error:
                outcome.append(error)

        worker = threading.Thread(target=execute, daemon=True)
        worker.start()
        try:
            with self.server.condition:
                self.assertTrue(self.server.condition.wait_for(
                    lambda: len(self.server.audit) == len(prompts), timeout=3),
                    f"only {len(self.server.audit)} of {len(prompts)} requests reached the server before a response")
                self.assertTrue(all(record["response"] is None for record in self.server.audit))
        finally:
            gate.set()
            worker.join(5)
        self.assertFalse(worker.is_alive())
        self.assertEqual(outcome, [tuple((index, answer(prompt)) for index, prompt in enumerate(prompts))])
        self.assertEqual(self.server.counts, Counter(prompts))
        self.assertEqual(self.server.peak, len(prompts))

    def test_submission_crosses_preparation_boundary(self):
        prompts = [f"window-{index}" for index in range(64)]
        self.load(prompts)
        gate = self.gate()
        self.server.scenarios[prompts[0]] = [Reply(gate=gate)]
        outcome = []

        def execute():
            try:
                outcome.append(self.query())
            except Exception as error:
                outcome.append(error)

        worker = threading.Thread(target=execute, daemon=True)
        worker.start()
        try:
            with self.server.condition:
                progressed = self.server.condition.wait_for(
                    lambda: len(self.server.audit) >= 33, timeout=3)
        finally:
            gate.set()
            worker.join(5)
        self.assertFalse(worker.is_alive(), "query did not finish after releasing the response")
        self.assertTrue(progressed, "an unfinished request blocked submission beyond the first 32 rows")
        self.assertEqual(outcome, [tuple((index, answer(prompt)) for index, prompt in enumerate(prompts))])
        self.assertEqual(self.server.counts, Counter(prompts))
        self.assertLessEqual(self.server.peak, len(prompts))

    def test_large_batch_preserves_all_rows(self):
        prompts = [f"large-{index}" for index in range(2048)]
        self.load(prompts)
        self.cursor.execute("SET ob_query_timeout = 60000000")
        self.cursor.execute("SELECT /*+ OPT_PARAM('rowsets_max_rows', 2048) */ "
                            "id, AI_COMPLETE('contract_model', prompt) FROM inputs ORDER BY id")
        self.assertEqual(self.cursor.fetchall(), tuple((index, answer(prompt)) for index, prompt in enumerate(prompts)))
        self.assertEqual(self.server.counts, Counter(prompts))
        self.assertLessEqual(self.server.peak, len(prompts))

    def test_duplicate_inputs_are_not_deduplicated(self):
        prompts = ["duplicate"] * 17
        self.load(prompts)
        self.assertEqual(self.query(), tuple((index, answer(prompt)) for index, prompt in enumerate(prompts)))
        self.assertEqual(self.server.counts, Counter(prompts))

    def test_unicode_nul_and_quotes_round_trip(self):
        prompts = ["quote\" backslash\\ newline\n", "nul\x00tail", "\u6d4b\u8bd5\U0001f680"]
        self.load(prompts)
        expected = tuple((index, answer(prompt)) for index, prompt in enumerate(prompts))
        self.assertEqual(self.query(), expected)
        self.assertEqual(self.server.counts, Counter(prompts))

    def test_request_preserves_model_prompt_and_options(self):
        prompts = ["first input", "second\ninput"]
        self.load(prompts)
        options = {"temperature": 0, "max_tokens": 32, "stop": ["STOP"]}
        result = self.query("AI_COMPLETE('contract_model', prompt, "
                            "'{\"temperature\":0,\"max_tokens\":32,\"stop\":[\"STOP\"]}')")
        self.assertEqual(result, tuple((index, answer(prompt)) for index, prompt in enumerate(prompts)))
        self.assertEqual(len(self.server.requests), len(prompts))
        for body in self.server.requests:
            self.assertEqual(body["model"], "mock")
            self.assertEqual(body["messages"], [{"role": "user", "content": body["messages"][0]["content"]}])
            self.assertEqual({key: body[key] for key in options}, options)
        self.assertEqual(Counter(body["messages"][0]["content"] for body in self.server.requests), Counter(prompts))
        self.assertTrue(all(entry["headers"]["authorization"] == "Bearer contract-test-only"
                            for entry in self.server.audit))

    def test_constrained_output_rejects_schema_violation(self):
        prompt = "schema-invalid"
        self.load([prompt])
        schema = {"type": "object", "properties": {"label": {"type": "string", "enum": ["ENGINE", "BODY"]}},
                  "required": ["label"], "additionalProperties": False}
        options = {"response_format": {"type": "json_schema", "json_schema": {
            "name": "classification", "strict": True, "schema": schema}}}
        response = {"choices": [{"finish_reason": "stop", "message": {"content": '{"label":"engine"}'}}]}
        self.server.scenarios[prompt] = [Reply(body=json.dumps(response).encode())]
        sql_error(self, 4070, lambda: self.cursor.execute(
            "SELECT AI_COMPLETE('contract_model', prompt, %s) FROM inputs", (json.dumps(options),)))
        self.assertEqual(self.server.counts, Counter({prompt: 1}))
        self.assertEqual(self.server.requests[0]["response_format"], options["response_format"])

    def test_constrained_output_preserves_batch_and_scalar_results(self):
        prompts = [f"structured-{index}" for index in range(3)]
        self.load(prompts)
        schema = {"type": "object", "properties": {
            "ID": {"type": "integer"}, "Label": {"type": "string", "enum": ["ENGINE", "BODY"]},
            "Flags": {"type": "array", "items": {"type": "boolean"}, "minItems": 2, "maxItems": 2}},
            "required": ["ID", "Label", "Flags"], "additionalProperties": False}
        texts = [f' {{"ID": {index}, "Label": "ENGINE", "Flags": [true, false]}}\n'
                 for index in range(len(prompts))]
        for prompt, text in zip(prompts, texts):
            self.server.scenarios[prompt] = [Reply(body=completion(text, finish_reason="stop"))]
        self.server.scenarios[prompts[0]][0].peers = len(prompts)
        expected = tuple(enumerate(texts))
        self.assertEqual(self.query_constrained(schema, temperature=0), expected)
        self.server.scenarios[prompts[0]][0].peers = 0
        self.assertEqual(self.query_constrained(schema, model="model", temperature=0), expected)
        self.assertEqual(self.server.counts, Counter({prompt: 2 for prompt in prompts}))
        for body in self.server.requests:
            self.assertEqual(body["response_format"], constrained_options(schema)["response_format"])
            self.assertEqual(body["temperature"], 0)
            self.assertEqual(body["model"], "mock")
            self.assertEqual(len(body["messages"]), 1)
            self.assertIn(body["messages"][0]["content"], prompts)

    def test_constrained_output_handles_dynamic_config(self):
        self.load(["integer-output", "boolean-output"])
        configs = [constrained_options({"type": "integer"}), constrained_options({"type": "boolean"})]
        self.server.scenarios["integer-output"] = [Reply(body=completion("7", finish_reason="stop"))]
        self.server.scenarios["boolean-output"] = [Reply(body=completion("true", finish_reason="stop"))]
        self.cursor.execute("SELECT id, AI_COMPLETE('contract_model', prompt, "
                            "CASE WHEN id = 0 THEN %s ELSE %s END) FROM inputs ORDER BY id",
                            tuple(json.dumps(config) for config in configs))
        self.assertEqual(self.cursor.fetchall(), ((0, "7"), (1, "true")))
        self.assertEqual([body["response_format"] for body in self.server.requests],
                         [config["response_format"] for config in configs])

    def test_constrained_output_supports_scalar_and_array_types(self):
        cases = [({"type": "boolean"}, "true"), ({"type": "null"}, "null"),
                 ({"type": "integer", "minimum": 0, "maximum": 10}, "7"),
                 ({"type": "number", "minimum": 0, "maximum": 2}, "1.25"),
                 ({"type": "string", "enum": ["ENGINE", "BODY"]}, '"ENGINE"'),
                 ({"type": "string", "minLength": 1, "maxLength": 2}, '"\u6d4b\u8bd5"'),
                 ({"type": "string", "minLength": 1, "maxLength": 1}, '"\U0001f642"'),
                 ({"type": "string", "minLength": 2, "maxLength": 2}, '"e\u0301"'),
                 ({"type": "string", "minLength": 0, "maxLength": 0}, '""'),
                 ({"type": "string", "minLength": 3, "maxLength": 3}, '"a\\u0000b"'),
                 ({"type": "array", "items": {"type": "integer"}, "minItems": 3,
                   "maxItems": 3, "uniqueItems": True}, "[1,2,3]")]
        for index, (schema, text) in enumerate(cases):
            with self.subTest(schema=schema):
                self.cursor.execute("TRUNCATE TABLE inputs")
                prompt = f"typed-{index}"
                self.load([prompt])
                self.server.scenarios[prompt] = [Reply(body=completion(text, finish_reason="stop"))]
                self.assertEqual(self.query_constrained(schema), ((0, text),))
                self.assertEqual(self.server.counts[prompt], 1)

    def test_constrained_output_checks_nested_and_length_constraints(self):
        object_schema = {"type": "object", "properties": {"Value": {"type": "integer"}},
                         "required": ["Value"], "additionalProperties": False}
        array_schema = {"type": "array", "items": {"type": "integer"},
                        "minItems": 2, "maxItems": 2, "uniqueItems": True}
        cases = [(object_schema, '{}'), (object_schema, '{"Value":"1"}'),
                 (object_schema, '{"Value":1,"extra":0}'), (array_schema, '[1]'),
                 (array_schema, '[1,2,3]'), (array_schema, '[1,"2"]'), (array_schema, '[1,1]'),
                 ({"type": "integer"}, "true"), ({"type": "integer"}, "1.5"),
                 ({"type": "number", "minimum": 1, "maximum": 2}, "3"),
                 ({"type": "string", "minLength": 2}, '"a"'),
                 ({"type": "string", "minLength": 3}, '"\u6d4b\u8bd5"'),
                 ({"type": "string", "minLength": 2}, '"\U0001f642"'),
                 ({"type": "string", "maxLength": 0}, '"a"'),
                 ({"type": "string", "maxLength": 2}, '"abc"')]
        for index, (schema, text) in enumerate(cases):
            with self.subTest(schema=schema, text=text):
                self.cursor.execute("TRUNCATE TABLE inputs")
                prompt = f"constraint-{index}"
                self.load([prompt])
                self.server.scenarios[prompt] = [Reply(body=completion(text, finish_reason="stop"))]
                sql_error(self, 4070, lambda: self.query_constrained(schema))
                self.assertEqual(self.server.counts[prompt], 1)

    def test_constrained_schema_unicode_lengths_match_json_schema_valid(self):
        for value, length in (("abc", 3), ("\u6d4b\u8bd5", 2), ("\U0001f642", 1),
                              ("e\u0301", 2), ("", 0), ("a\x00b", 3)):
            schemas = [({"maxLength": length}, 1), ({"minLength": length + 1}, 0),
                       ({"type": "string", "minLength": length, "maxLength": length}, 1),
                       ({"type": "string", "minLength": length + 1}, 0)]
            for schema, expected in schemas:
                with self.subTest(value=value, schema=schema):
                    self.cursor.execute("SELECT JSON_SCHEMA_VALID(%s, %s)",
                                        (json.dumps(schema), json.dumps(value)))
                    self.assertEqual(self.cursor.fetchone(), (expected,))
        self.assertFalse(self.server.audit)

    def test_constrained_invalid_schema_is_rejected_before_http(self):
        self.load(["invalid-schema"])
        schemas = [None, False, [], {}, {"type": "invalid"}, {"type": ["string", "null"]},
                   {"type": "string", "enum": []}, {"type": "string", "enum": "ENGINE"},
                   {"type": "string", "minLength": "4"}, {"type": "string", "pattern": "^a$"},
                   {"type": "string", "const": "ENGINE"}, {"type": "object", "$ref": "#/missing"},
                   {"type": "object", "required": [42]}, {"type": "object", "properties": []},
                   {"type": "object", "properties": {"nested": {"type": "string", "const": "X"}}},
                   {"type": "object", "properties": {"nested": {}}},
                   {"type": "array", "items": True}, {"type": "array", "minItems": -1}]
        for schema in schemas:
            with self.subTest(schema=schema):
                sql_error(self, 1210, lambda: self.query_constrained(schema))
                self.assertFalse(self.server.audit)

    def test_constrained_invalid_config_is_rejected_before_http(self):
        self.load(["invalid-config"])
        config = constrained_options({"type": "boolean"})
        definition = config["response_format"]["json_schema"]
        configs = [{"response_format": []}, {"response_format": {}},
                   {"response_format": {"type": "json_schema"}}]
        for change in ({"strict": False}, {"strict": "true"}, {"name": ""}, {"name": 5}):
            configs.append({"response_format": {"type": "json_schema", "json_schema": definition | change}})
        configs.append({"response_format": {"type": "json_schema", "json_schema": {
            "name": "missing_strict", "schema": {"type": "boolean"}}}})
        for change in ({"stream": True}, {"stream": "false"}, {"n": 2}, {"n": 0},
                       {"structured_outputs": {"json": {"type": "boolean"}}}, {"guided_grammar": "root ::= 'true'"}):
            configs.append(config | change)
        for invalid in configs:
            with self.subTest(config=invalid):
                sql_error(self, 1210, lambda: self.cursor.execute(
                    "SELECT AI_COMPLETE('contract_model', prompt, %s) FROM inputs", (json.dumps(invalid),)))
                self.assertFalse(self.server.audit)

    def test_constrained_output_rejects_incomplete_or_refused_responses(self):
        replies = [completion('"ENGINE"', finish_reason=reason)
                   for reason in (None, "length", "content_filter", "tool_calls", "", 1)]
        replies += [completion('"ENGINE"', finish_reason="stop", refusal=refusal)
                    for refusal in ("refused", "", False)]
        replies += [completion(content, finish_reason="stop") for content in (None, {}, ["ENGINE"])]
        for index, body in enumerate(replies):
            with self.subTest(index=index):
                self.cursor.execute("TRUNCATE TABLE inputs")
                prompt = f"incomplete-{index}"
                self.load([prompt])
                self.server.scenarios[prompt] = [Reply(body=body)]
                sql_error(self, 4070, lambda: self.query_constrained({"type": "string", "enum": ["ENGINE"]}))
                self.assertEqual(self.server.counts[prompt], 1)

    def test_constrained_output_requires_strict_json(self):
        cases = [({"type": "boolean"}, "TRUE"), ({"type": "boolean"}, "False"),
                 ({"type": "number"}, "NaN"), ({"type": "number"}, "Infinity"),
                 ({"type": "object"}, '{"value":1,}'), ({"type": "object"}, '{} trailing'),
                 ({"type": "object"}, '```json\n{}\n```')]
        for index, (schema, text) in enumerate(cases):
            with self.subTest(text=text):
                self.cursor.execute("TRUNCATE TABLE inputs")
                prompt = f"json-syntax-{index}"
                self.load([prompt])
                self.server.scenarios[prompt] = [Reply(body=completion(text, finish_reason="stop"))]
                sql_error(self, (4070, 3140, 5447), lambda: self.query_constrained(schema))
                self.assertEqual(self.server.counts[prompt], 1)

    def test_constrained_output_respects_case_skips(self):
        self.load([None, "selected", ""])
        self.server.scenarios["selected"] = [Reply(body=completion("true", finish_reason="stop"))]
        config = json.dumps(constrained_options({"type": "boolean"}))
        self.cursor.execute("SELECT id, CASE WHEN id = 1 THEN AI_COMPLETE('contract_model', prompt, %s) "
                            "ELSE 'skip' END FROM inputs ORDER BY id", (config,))
        self.assertEqual(self.cursor.fetchall(), ((0, "skip"), (1, "true"), (2, "skip")))
        self.assertEqual(self.server.counts, Counter({"selected": 1}))

    def test_constrained_endpoint_rejection_does_not_downgrade(self):
        self.load(["unsupported-endpoint"])
        self.server.scenarios["unsupported-endpoint"] = [Reply(400, b'{"error":"unsupported response_format"}')]
        schema = {"type": "boolean"}
        sql_error(self, 4216, lambda: self.query_constrained(schema))
        self.assertEqual(self.server.counts, Counter({"unsupported-endpoint": 1}))
        self.assertEqual(self.server.requests[0]["response_format"], constrained_options(schema)["response_format"])

    def test_constrained_native_provider_is_rejected_before_http(self):
        self.load(["native-provider"])
        self.cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL(%s, %s)",
                            ("native_schema_model", json.dumps({"type": "completion", "model_name": "mock"})))
        endpoint = {"ai_model_name": "native_schema_model", "url": f"http://127.0.0.1:{self.server.server_port}/",
                    "access_key": "contract-test-only", "provider": "aliyun-dashscope", "request_model_name": "mock"}
        self.cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT(%s, %s)",
                            ("native_schema_endpoint", json.dumps(endpoint)))
        sql_error(self, 1235, lambda: self.query_constrained({"type": "boolean"}, model="'native_schema_model'"))
        self.assertFalse(self.server.audit)

    def test_case_skips_invalid_unselected_inputs(self):
        self.load([None, "", "valid", None])
        self.assertEqual(self.query("CASE WHEN id = 2 THEN AI_COMPLETE('contract_model', prompt) ELSE 'skip' END"),
                         ((0, "skip"), (1, "skip"), (2, answer("valid")), (3, "skip")))
        self.assertEqual(self.server.counts, Counter({"valid": 1}))

    def test_limit_zero_sends_nothing(self):
        self.load(["not-needed"] * 24)
        self.assertEqual(self.query(suffix="LIMIT 0"), ())
        self.assertFalse(self.server.audit)

    def test_limit_does_not_expand_logical_inputs(self):
        self.load([f"limited-{index}" for index in range(24)])
        expected = tuple((index, answer(f"limited-{index}")) for index in range(3))
        self.assertEqual(self.query(suffix="ORDER BY id LIMIT 3"), expected)
        self.assertEqual(self.server.counts, Counter(f"limited-{index}" for index in range(3)))

    def test_permanent_http_errors_have_transport_error_code(self):
        for status in (400, 401, 403, 404, 422, 501):
            with self.subTest(status=status):
                prompt = f"http-{status}"
                self.cursor.execute("TRUNCATE TABLE inputs")
                self.load([prompt])
                self.server.scenarios[prompt] = [Reply(status, b"not json")]
                sql_error(self, 4216, self.query)
                self.assertEqual(self.server.counts[prompt], 1)

    def test_invalid_arguments_have_argument_error_code(self):
        for prompt in (None, ""):
            with self.subTest(prompt=prompt):
                self.cursor.execute("TRUNCATE TABLE inputs")
                self.load([prompt])
                sql_error(self, 1210, self.query)
                self.assertFalse(self.server.audit)

    def test_non_object_response_has_json_error_code(self):
        self.load(["array"])
        self.server.scenarios["array"] = [Reply(body=b"[]")]
        sql_error(self, 3140, self.query)
        self.assertEqual(self.server.counts["array"], 1)

    def test_malformed_response_has_json_syntax_error_code(self):
        self.load(["broken-json"])
        self.server.scenarios["broken-json"] = [Reply(body=b'{"broken')]
        sql_error(self, (3140, 5447), self.query)
        self.assertEqual(self.server.counts["broken-json"], 1)

    def test_single_provider_error_has_data_error_code(self):
        self.load(["provider-error"])
        self.server.scenarios["provider-error"] = [Reply(body=b'{"unexpected":true}')]
        sql_error(self, 4070, self.query)
        self.assertEqual(self.server.counts["provider-error"], 1)

    def test_accepted_post_with_lost_response_is_not_retried(self):
        for field_name in ("drop", "truncate"):
            with self.subTest(fault=field_name):
                self.cursor.execute("TRUNCATE TABLE inputs")
                self.load([field_name])
                self.server.scenarios[field_name] = [Reply(**{field_name: True})]
                sql_error(self, 4216, self.query)
                self.assertEqual(self.server.counts[field_name], 1)

    def test_retry_after_seconds_sets_a_real_lower_bound(self):
        self.load(["limited", "healthy"])
        self.server.scenarios["limited"] = [Reply(429, b"not json", {"rEtRy-AfTeR": "4"}), Reply()]
        self.assertEqual(self.query(), ((0, answer("limited")), (1, answer("healthy"))))
        attempts = [entry for entry in self.server.audit if entry["prompt"] == "limited"]
        self.assertEqual(len(attempts), 2)
        self.assertGreaterEqual(attempts[1]["received"] - attempts[0]["response"], 3.95)
        self.assertEqual(self.server.counts["healthy"], 1)

    def test_retry_after_http_date_sets_a_real_lower_bound(self):
        date = formatdate(time.time() + 5, usegmt=True)
        self.load(["dated"])
        self.server.scenarios["dated"] = [Reply(503, b"{}", {"Retry-After": date}), Reply()]
        self.assertEqual(self.query(), ((0, answer("dated")),))
        self.assertEqual(len(self.server.audit), 2)
        self.assertGreaterEqual(self.server.audit[1]["wall_received"], parsedate_to_datetime(date).timestamp() - 0.05)

    def test_retry_exhaustion_is_finite(self):
        self.load(["always-429"])
        self.server.scenarios["always-429"] = [Reply(429, b"{}")]
        sql_error(self, 4216, self.query)
        self.assertEqual(self.server.counts["always-429"], 4)

    def test_transient_http_statuses_retry_only_failed_rows(self):
        for status in (500, 502, 503, 504):
            with self.subTest(status=status):
                self.cursor.execute("TRUNCATE TABLE inputs")
                prompt, healthy = f"retry-{status}", f"healthy-{status}"
                self.load([prompt, healthy])
                self.server.scenarios[prompt] = [Reply(status, b"not json"), Reply()]
                self.assertEqual(self.query(), ((0, answer(prompt)), (1, answer(healthy))))
                self.assertEqual(self.server.counts[prompt], 2)
                self.assertEqual(self.server.counts[healthy], 1)

    def test_retry_after_cannot_extend_query_deadline(self):
        self.load(["long-retry"])
        self.server.scenarios["long-retry"] = [Reply(429, b"{}", {"Retry-After": "60"})]
        self.cursor.execute("SET ob_query_timeout = 700000")
        start = time.monotonic()
        sql_error(self, 4012, self.query)
        self.assertGreaterEqual(time.monotonic() - start, 0.5)
        self.assertLess(time.monotonic() - start, 2)
        self.assertEqual(self.server.counts["long-retry"], 1)

    def test_cancel_during_retry_backoff_and_recover(self):
        self.load(["cancel-backoff"])
        self.server.scenarios["cancel-backoff"] = [Reply(429, b"{}", {"Retry-After": "60"})]
        self.cursor.execute("SET ob_query_timeout = 3000000")
        outcome = []

        def execute():
            try:
                self.query()
            except Exception as error:
                outcome.append(error)

        worker = threading.Thread(target=execute, daemon=True)
        worker.start()
        self.addCleanup(worker.join, 5)
        with self.server.condition:
            self.assertTrue(self.server.condition.wait_for(
                lambda: bool(self.server.audit) and self.server.audit[0]["response"] is not None, timeout=2))
        start = time.monotonic()
        with pymysql.connect(unix_socket=self.sql_socket, user="root", autocommit=True) as control:
            with control.cursor() as cursor:
                cursor.execute(f"KILL QUERY {self.connection.thread_id()}")
        worker.join(1.5)
        self.assertFalse(worker.is_alive(), "cancellation waited for the retry delay or query timeout")
        self.assertLess(time.monotonic() - start, 1.5)
        self.assertEqual(len(outcome), 1)
        self.assertIsInstance(outcome[0], pymysql.MySQLError)
        self.assertEqual(outcome[0].args[0], 1317, outcome[0].args)
        self.assertEqual(self.server.counts["cancel-backoff"], 1)
        self.cursor.execute("SET ob_query_timeout = 20000000")
        self.cursor.execute("TRUNCATE TABLE inputs")
        self.load(["after-cancel"])
        self.assertEqual(self.query(), ((0, answer("after-cancel")),))

    def test_repeated_failures_release_model_sockets_and_recover(self):
        credentials = self.connection._sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED,
                                                       struct.calcsize("3i"))
        process_id, _, _ = struct.unpack("3i", credentials)
        self.assertGreater(process_id, 0)
        peak_descriptors = 0
        with pymysql.connect(unix_socket=self.sql_socket, user="root", autocommit=True) as control:
            with control.cursor() as cancel_cursor:
                for failure, code in (("cancel", 1317), ("provider", 4070), ("schema", 4070)):
                    expression = "AI_COMPLETE('contract_model', prompt)"
                    if failure == "schema":
                        config = self.connection.escape(json.dumps(constrained_options({"type": "boolean"})))
                        expression = f"AI_COMPLETE('contract_model', prompt, {config})"
                    for iteration in range(12):
                        with self.subTest(failure=failure, iteration=iteration):
                            self.cursor.execute("TRUNCATE TABLE inputs")
                            prompts = [f"resource-{failure}-{iteration}-{index}" for index in range(8)]
                            self.load(prompts)
                            peers = self.gate()
                            failed = self.gate()
                            for prompt in prompts:
                                self.server.scenarios[prompt] = [Reply(gate=peers)]
                            if failure != "cancel":
                                body = (completion('"not-a-boolean"', finish_reason="stop")
                                        if failure == "schema" else b'{"unexpected":true}')
                                self.server.scenarios[prompts[0]] = [Reply(body=body, gate=failed)]
                            outcome = []

                            def execute():
                                try:
                                    outcome.append(self.query(expression))
                                except Exception as error:
                                    outcome.append(error)

                            worker = threading.Thread(target=execute, daemon=True)
                            worker.start()
                            try:
                                with self.server.condition:
                                    self.assertTrue(self.server.condition.wait_for(
                                        lambda: all(self.server.counts[prompt] == 1 for prompt in prompts),
                                        timeout=3), "not all held requests reached the model service")
                                descriptors = model_socket_fds(process_id, self.server.server_port)
                                self.assertTrue(descriptors, "socket oracle did not see the active requests")
                                peak_descriptors = max(peak_descriptors, len(descriptors))
                                if failure == "cancel":
                                    cancel_cursor.execute(f"KILL QUERY {self.connection.thread_id()}")
                                else:
                                    failed.set()
                                worker.join(3)
                                self.assertFalse(worker.is_alive(), "failed query still holds its peers")
                                self.assertEqual(len(outcome), 1)
                                self.assertIsInstance(outcome[0], pymysql.MySQLError)
                                self.assertEqual(outcome[0].args[0], code, outcome[0].args)
                                self.assertFalse(model_socket_fds(process_id, self.server.server_port),
                                                 "query returned but model socket descriptors remain open")
                            finally:
                                failed.set()
                                peers.set()
                                worker.join(5)
                            with self.server.condition:
                                self.assertTrue(self.server.condition.wait_for(lambda: self.server.active == 0,
                                                                               timeout=3))
                            self.assertEqual([self.server.counts[prompt] for prompt in prompts], [1] * 8)
                            for prompt in prompts:
                                self.server.scenarios[prompt] = [
                                    Reply(body=completion("true", finish_reason="stop"))
                                    if failure == "schema" else Reply()]
                            self.assertEqual(self.query(expression), tuple(
                                (index, "true" if failure == "schema" else answer(prompt))
                                for index, prompt in enumerate(prompts)))
                            self.assertFalse(model_socket_fds(process_id, self.server.server_port),
                                             "successful recovery retained model sockets")
                            self.assertEqual([self.server.counts[prompt] for prompt in prompts], [2] * 8)
        print(f"RESOURCE SQL: 36 failed batches and 36 recoveries; peak model socket FDs={peak_descriptors}; "
              "zero after every query", flush=True)

    def test_response_limit_uses_valid_json_at_boundary(self):
        for size in (RESPONSE_LIMIT - 1, RESPONSE_LIMIT, RESPONSE_LIMIT + 1):
            with self.subTest(size=size):
                self.cursor.execute("TRUNCATE TABLE inputs")
                prompt = f"size-{size}"
                self.load([prompt])
                raw = sized_completion(size)
                self.server.scenarios[prompt] = [Reply(body=raw)]
                if size > RESPONSE_LIMIT:
                    sql_error(self, 4019, self.query)
                else:
                    expected = json.loads(raw)["choices"][0]["message"]["content"]
                    self.assertEqual(self.query(), ((0, expected),))
                self.assertEqual(self.server.counts[prompt], 1)

    def test_batch_response_budget_is_not_just_per_response(self):
        prompts = [f"budget-{index}" for index in range(9)]
        self.load(prompts)
        raw = sized_completion(RESPONSE_LIMIT)
        for prompt in prompts:
            self.server.scenarios[prompt] = [Reply(body=raw)]
        sql_error(self, 4019, self.query)
        self.assertLessEqual(self.server.peak, len(prompts))
        self.assertTrue(all(count == 1 for count in self.server.counts.values()))

    def test_serialized_request_limit_counts_escaping(self):
        self.load(["\x01" * (1024 * 1024)])
        sql_error(self, 4019, self.query)
        self.assertFalse(self.server.audit)

    def fail_fast(self, reply, code, expression="AI_COMPLETE('contract_model', prompt)"):
        prompts = ["fatal"] + [f"held-{index}" for index in range(71)]
        self.load(prompts)
        gate = self.gate()
        for prompt in prompts[1:]:
            self.server.scenarios[prompt] = [Reply(gate=gate)]
        reply.peers = 2
        self.server.scenarios["fatal"] = [reply]
        self.cursor.execute("SET ob_query_timeout = 2000000")
        start = time.monotonic()
        with self.assertRaises(pymysql.MySQLError) as caught:
            self.query(expression)
        elapsed = time.monotonic() - start
        evidence = f"elapsed={elapsed:.3f}s submitted={len(self.server.audit)} error={caught.exception.args}"
        self.assertEqual(caught.exception.args[0], code, evidence)
        self.assertLess(elapsed, 1.5, evidence)
        self.assertLessEqual(len(self.server.audit), len(prompts))
        self.assertTrue(all(count == 1 for count in self.server.counts.values()))
        self.assertEqual(self.server.counts["fatal"], 1)

    def test_http_error_cancels_active_peers(self):
        self.fail_fast(Reply(400, b"{}"), 4216)

    def test_provider_error_cancels_active_peers(self):
        self.fail_fast(Reply(body=b'{"unexpected":true}'), 4070)

    def test_constrained_output_error_cancels_active_peers(self):
        config = self.connection.escape(json.dumps(constrained_options({"type": "boolean"})))
        self.fail_fast(Reply(body=completion('"not-a-boolean"', finish_reason="stop")), 4070,
                       f"AI_COMPLETE('contract_model', prompt, {config})")

    def test_json_error_cancels_active_peers(self):
        self.fail_fast(Reply(body=b"[]"), 3140)

    def test_embedding_native_whole_batch_preserves_order_and_duplicates(self):
        prompts = [f"native-{index}" for index in range(72)] + ["native-5", "\u4e2d\u6587\n\"\\\x00"]
        expected = self.load_embeddings(prompts)
        self.assertEqual(self.query_embeddings(), expected)
        self.assertEqual(len(self.server.requests), 1, "one SQL batch must produce one native HTTP request")
        self.assertEqual(self.server.requests[0], {"model": "mock-embed", "input": prompts, "dimensions": 3})
        self.assertEqual(self.server.counts, Counter({tuple(prompts): 1}))

    def test_embedding_multiple_sql_batches_preserve_all_rows_and_tail(self):
        prompts = [f"batch-tail-{index}" for index in range(270)]
        expected = self.load_embeddings(prompts)
        self.assertEqual(self.query_embeddings(), expected)
        sizes = [len(body["input"]) for body in self.server.requests]
        self.assertGreater(len(sizes), 1)
        self.assertLess(len(sizes), len(prompts))
        self.assertTrue(all(0 < size <= 128 for size in sizes), sizes)
        self.assertTrue(any(size < 128 for size in sizes), sizes)
        self.assertEqual(Counter(text for body in self.server.requests for text in body["input"]), Counter(prompts))

    def test_embedding_pipeline_submits_next_batch_before_first_response(self):
        prompts = [f"pipeline-{index}" for index in range(270)]
        expected = self.load_embeddings(prompts)
        gate = self.gate()
        self.server.default_reply = Reply(gate=gate)
        outcome = []

        def execute():
            try:
                outcome.append(self.query_embeddings())
            except Exception as error:
                outcome.append(error)

        worker = threading.Thread(target=execute, daemon=True)
        worker.start()
        try:
            with self.server.condition:
                self.assertTrue(self.server.condition.wait_for(lambda: len(self.server.audit) >= 2, timeout=3),
                                f"only {len(self.server.audit)} SQL batches submitted before the first response")
                self.assertEqual(len(self.server.audit), 2, "the pipeline must bound the number of pending batches")
                self.assertTrue(all(record["response"] is None for record in self.server.audit))
        finally:
            gate.set()
            worker.join(5)
        self.assertFalse(worker.is_alive(), "pipeline did not drain its final batch")
        self.assertEqual(outcome, [expected])
        self.assertTrue(all(0 < len(body["input"]) <= 128 for body in self.server.requests))
        self.assertGreater(len(self.server.requests), 2)
        self.assertEqual(Counter(text for body in self.server.requests for text in body["input"]), Counter(prompts))

    def start_embedding_query(self, operation=None):
        outcome = []

        def execute():
            try:
                outcome.append((operation or self.query_embeddings)())
            except Exception as error:
                outcome.append(error)

        worker = threading.Thread(target=execute, daemon=True)
        worker.start()
        return worker, outcome

    def test_embedding_pipeline_refills_after_first_batch_finishes(self):
        prompts = [f"refill-{index}" for index in range(400)]
        expected = self.load_embeddings(prompts)
        first_gate, other_gate = self.gate(), self.gate()
        self.server.default_reply = Reply(gate=other_gate)
        self.server.scenarios[prompts[0]] = [Reply(gate=first_gate)]
        worker, outcome = self.start_embedding_query()
        try:
            with self.server.condition:
                self.assertTrue(self.server.condition.wait_for(lambda: len(self.server.audit) == 2, timeout=3))
            first_gate.set()
            with self.server.condition:
                self.assertTrue(self.server.condition.wait_for(lambda: len(self.server.audit) >= 3, timeout=3),
                                "a completed batch must free a slot without waiting for its unfinished peer")
                self.assertEqual(len(self.server.audit), 3)
                self.assertEqual(sum(record["response"] is None for record in self.server.audit), 2)
        finally:
            first_gate.set()
            other_gate.set()
            worker.join(5)
        self.assertFalse(worker.is_alive())
        self.assertEqual(outcome, [expected])
        self.assertEqual(Counter(text for body in self.server.requests for text in body["input"]), Counter(prompts))

    def test_embedding_pipeline_later_error_cancels_held_first_batch(self):
        prompts = [f"later-error-{index}" for index in range(270)]
        expected = self.load_embeddings(prompts)
        process_id = struct.unpack("3i", self.connection._sock.getsockopt(
            socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i")))[0]
        for reply, code in ((Reply(body=b'{"unexpected":true}'), 4070), (Reply(400, b"{}"), 4216)):
            with self.subTest(code=code):
                gate = self.gate()
                before = len(self.server.audit)
                self.server.scenarios[prompts[0]] = [Reply(gate=gate)]
                reply.peers = before + 2
                self.server.default_reply = reply
                self.cursor.execute("SET ob_query_timeout = 3000000")
                try:
                    start = time.monotonic()
                    sql_error(self, code, self.query_embeddings)
                    self.assertLess(time.monotonic() - start, 2.5)
                    self.assertEqual(len(self.server.audit), before + 2)
                    self.assertTrue(any(record["response"] is None for record in self.server.audit[before:]))
                    self.assertFalse(model_socket_fds(process_id, self.server.server_port))
                finally:
                    gate.set()
                self.server.scenarios.clear()
                self.server.default_reply = Reply()
                self.cursor.execute("SET ob_query_timeout = 20000000")
                self.assertEqual(self.query_embeddings(), expected)

    def test_embedding_pipeline_cancel_releases_both_requests_and_recovers(self):
        prompts = [f"cancel-pipeline-{index}" for index in range(270)]
        expected = self.load_embeddings(prompts)
        gate = self.gate()
        self.server.default_reply = Reply(gate=gate)
        worker, outcome = self.start_embedding_query()
        try:
            with self.server.condition:
                self.assertTrue(self.server.condition.wait_for(lambda: len(self.server.audit) == 2, timeout=3))
            process_id = struct.unpack("3i", self.connection._sock.getsockopt(
                socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i")))[0]
            self.assertEqual(len(model_socket_fds(process_id, self.server.server_port)), 2)
            with pymysql.connect(unix_socket=self.sql_socket, user="root", autocommit=True) as control:
                with control.cursor() as cursor:
                    cursor.execute(f"KILL QUERY {self.connection.thread_id()}")
            worker.join(3)
            self.assertFalse(worker.is_alive())
            self.assertEqual(len(outcome), 1)
            self.assertIsInstance(outcome[0], pymysql.MySQLError)
            self.assertEqual(outcome[0].args[0], 1317)
            self.assertEqual(len(self.server.audit), 2)
            self.assertFalse(model_socket_fds(process_id, self.server.server_port))
        finally:
            gate.set()
            worker.join(5)
        self.server.default_reply = Reply()
        self.assertEqual(self.query_embeddings(), expected)

    def test_embedding_pipeline_deadline_releases_both_requests_and_recovers(self):
        prompts = [f"deadline-pipeline-{index}" for index in range(270)]
        expected = self.load_embeddings(prompts)
        gate = self.gate()
        self.server.default_reply = Reply(gate=gate)
        self.cursor.execute("SET ob_query_timeout = 500000")
        try:
            sql_error(self, 4012, self.query_embeddings)
            self.assertEqual(len(self.server.audit), 2)
            process_id = struct.unpack("3i", self.connection._sock.getsockopt(
                socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i")))[0]
            self.assertFalse(model_socket_fds(process_id, self.server.server_port))
        finally:
            gate.set()
        self.server.default_reply = Reply()
        self.cursor.execute("SET ob_query_timeout = 20000000")
        self.assertEqual(self.query_embeddings(), expected)

    def test_embedding_pipeline_out_of_order_preserves_other_columns(self):
        prompts = [f"owned-{index}:" + "\u4e2d\n\x00" * (2048 + index % 13) for index in range(400)]
        prompts[10] = prompts[8]
        self.load_embeddings(prompts)
        self.cursor.execute("UPDATE inputs SET model = CONCAT('row-model-', id)")
        gate = self.gate()
        self.server.scenarios[prompts[-1]] = [Reply(gate=gate)]

        def query():
            self.cursor.execute("SELECT /*+ OPT_PARAM('rowsets_max_rows', 128) */ "
                                "id, prompt, model, AI_EMBED('contract_embed', prompt, 3) FROM inputs ORDER BY id DESC")
            return tuple((row, text, model, json.loads(vector))
                         for row, text, model, vector in self.cursor.fetchall())

        worker, outcome = self.start_embedding_query(query)
        try:
            with self.server.condition:
                self.assertTrue(self.server.condition.wait_for(
                    lambda: len(self.server.audit) >= 2 and bool(self.server.finished), timeout=3))
                self.assertEqual(len(self.server.audit), 2, "ready results behind the first batch must remain bounded")
                self.assertTrue(any(record["response"] is None for record in self.server.audit))
        finally:
            gate.set()
            worker.join(5)
        self.assertFalse(worker.is_alive())
        expected = tuple((index, prompts[index], f"row-model-{index}", self.server.embedding_vectors[prompts[index]])
                         for index in reversed(range(len(prompts))))
        self.assertEqual(outcome, [expected])
        self.assertEqual(Counter(text for body in self.server.requests for text in body["input"]), Counter(prompts))

    def test_embedding_pipeline_plan_selection_and_fallback(self):
        base = "SELECT id, AI_EMBED('contract_embed', prompt, 3) FROM inputs"
        cases = [(base + " ORDER BY id", True), (base + " WHERE id > 0 ORDER BY id DESC", True),
                 (base + " ORDER BY id LIMIT 3", False), (base + " ORDER BY 2", False),
                 ("SELECT id, CASE WHEN id > 0 THEN AI_EMBED('contract_embed', prompt, 3) END FROM inputs", False),
                 ("SELECT id, AI_EMBED(model, prompt, 3) FROM inputs", False),
                 ("SELECT id, AI_EMBED('contract_embed', prompt, id + 1) FROM inputs", False),
                 ("SELECT AI_EMBED('contract_embed', 'constant', 3)", False)]
        for query, expected in cases:
            with self.subTest(query=query):
                self.cursor.execute("EXPLAIN " + query)
                plan = "\n".join(str(row[0]) for row in self.cursor.fetchall())
                self.assertEqual("AI FUNCTION PIPELINE" in plan, expected, plan)
        self.assertFalse(self.server.requests)

    def test_embedding_pipeline_scalar_engine_fallback(self):
        prompts = ["scalar-pipeline-left", "scalar-pipeline-middle", "scalar-pipeline-right"]
        expected = self.load_embeddings(prompts)
        for batch_size in (0, 1):
            with self.subTest(batch_size=batch_size):
                query = (f"SELECT /*+ OPT_PARAM('rowsets_max_rows', {batch_size}) */ "
                         "id, AI_EMBED('contract_embed', prompt, 3) FROM inputs ORDER BY id")
                self.cursor.execute("EXPLAIN " + query)
                self.assertIn("AI FUNCTION PIPELINE", "\n".join(str(row[0]) for row in self.cursor.fetchall()))
                before = len(self.server.requests)
                self.cursor.execute(query)
                self.assertEqual(tuple((row, json.loads(value)) for row, value in self.cursor.fetchall()), expected)
                self.assertEqual([body["input"] for body in self.server.requests[before:]], [[text] for text in prompts])

    def test_embedding_pipeline_filtered_rows_and_limit_do_not_add_calls(self):
        prompts = [f"filter-pipeline-{index}" for index in range(400)]
        expected = self.load_embeddings(prompts)
        self.cursor.execute("SELECT /*+ OPT_PARAM('rowsets_max_rows', 128) */ "
                            "id, AI_EMBED('contract_embed', prompt, 3) FROM inputs WHERE id % 3 = 0 ORDER BY id")
        self.assertEqual(tuple((row, json.loads(value)) for row, value in self.cursor.fetchall()), expected[::3])
        self.assertEqual(Counter(text for body in self.server.requests for text in body["input"]), Counter(prompts[::3]))
        before = len(self.server.requests)
        self.cursor.execute("SELECT id, AI_EMBED('contract_embed', prompt, 3) FROM inputs ORDER BY id LIMIT 3")
        self.assertEqual(tuple((row, json.loads(value)) for row, value in self.cursor.fetchall()), expected[:3])
        self.assertEqual([text for body in self.server.requests[before:] for text in body["input"]], prompts[:3])

    def test_embedding_native_provider_formats_and_dimensions(self):
        prompts = ["provider-left", "provider-right"]
        expected = self.load_embeddings(prompts)
        for provider in ("openai", "aliyun-openai", "hunyuan-openai", "siliconflow", "aliyun-dashscope"):
            with self.subTest(provider=provider):
                self.cursor.execute("CALL DBMS_AI_SERVICE.ALTER_AI_MODEL_ENDPOINT(%s, %s)",
                                    ("contract_embed_endpoint", json.dumps({"provider": provider})))
                self.server.embedding_format = "dashscope" if provider == "aliyun-dashscope" else "openai"
                before = len(self.server.requests)
                self.assertEqual(self.query_embeddings(), expected)
                self.assertEqual(len(self.server.requests), before + 1)
                expected_body = ({"model": "mock-embed", "input": {"texts": prompts}, "parameters": {"dimension": 3}}
                                 if provider == "aliyun-dashscope" else
                                 {"model": "mock-embed", "input": prompts, "dimensions": 3})
                self.assertEqual(self.server.requests[-1], expected_body)
                self.assertEqual(self.server.audit[-1]["headers"]["content-type"], "application/json")

    def test_embedding_native_optional_and_dynamic_dimensions(self):
        prompts = ["dimension-left", "dimension-right", "dimension-last"]
        expected = self.load_embeddings(prompts)
        self.assertEqual(self.query_embeddings("AI_EMBED('contract_embed', prompt)"), expected)
        self.assertEqual(self.server.requests, [{"model": "mock-embed", "input": prompts}])
        self.server.embedding_vectors = {text: [index + 0.5] * (index + 2)
                                         for index, text in enumerate(prompts)}
        expected = tuple((index, self.server.embedding_vectors[text]) for index, text in enumerate(prompts))
        self.assertEqual(self.query_embeddings("AI_EMBED('contract_embed', prompt, id + 2)"), expected)
        self.assertEqual(len(self.server.requests), 4)
        self.assertEqual(self.server.requests[1:], [
            {"model": "mock-embed", "input": [text], "dimensions": index + 2}
            for index, text in enumerate(prompts)])

    def test_embedding_native_case_skips_and_empty_input(self):
        self.load(["selected-left", None, "selected-right"])
        self.server.embedding_vectors = {"selected-left": [0.5, -1, 2], "selected-right": [0.25, 3, 4]}
        results = self.query("CASE WHEN id = 1 THEN 'skipped' ELSE AI_EMBED('contract_embed', prompt, 3) END")
        self.assertEqual(tuple((row, value if row == 1 else json.loads(value)) for row, value in results),
                         ((0, [0.5, -1, 2]), (1, "skipped"), (2, [0.25, 3, 4])))
        self.assertEqual(len(self.server.requests), 1)
        self.assertEqual(self.server.requests[0]["input"], ["selected-left", "selected-right"])
        self.assertEqual(self.query("CASE WHEN id >= 0 THEN 'skipped' ELSE AI_EMBED('contract_embed', prompt) END"),
                         ((0, "skipped"), (1, "skipped"), (2, "skipped")))
        self.cursor.execute("TRUNCATE TABLE inputs")
        self.assertEqual(self.query_embeddings(), ())
        self.assertEqual(len(self.server.requests), 1)

    def test_embedding_native_invalid_input_rejected_before_http(self):
        for invalid in (None, ""):
            with self.subTest(input=invalid):
                self.cursor.execute("TRUNCATE TABLE inputs")
                self.load(["valid-before-error", invalid])
                sql_error(self, 1210, self.query_embeddings)
                self.assertFalse(self.server.requests)
        self.cursor.execute("TRUNCATE TABLE inputs")
        self.load(["valid-dimension-input"])
        for dimension in ("0", "-1", "CAST(NULL AS SIGNED)"):
            with self.subTest(dimension=dimension):
                sql_error(self, 1210, lambda: self.query_embeddings(f"AI_EMBED('contract_embed', prompt, {dimension})"))
                self.assertFalse(self.server.requests)

    def test_embedding_native_invalid_results_rejected_and_recover(self):
        prompts = ["invalid-left", "invalid-middle", "invalid-right"]
        expected = self.load_embeddings(prompts)
        entries = [{"index": index, "embedding": self.server.embedding_vectors[text]}
                   for index, text in enumerate(prompts)]
        bad_responses = [("missing data", {}), ("null data", {"data": None}),
                         ("object data", {"data": {}}), ("empty data", {"data": []}),
                         ("missing result", {"data": entries[:2]}),
                         ("extra result", {"data": entries + [{"index": 3, "embedding": [1, 2, 3]}]}),
                         ("null item", {"data": [None] + entries[1:]}),
                         ("missing index", {"data": [{"embedding": [1, 2, 3]}] + entries[1:]})]
        for index in (-1, 3, 1, False, 0.5, "0", None, 2**63):
            bad_responses.append((f"invalid index {index!r}",
                                  {"data": [{"index": index, "embedding": [1, 2, 3]}] + entries[1:]}))
        for vector in (None, "vector", {}, [], [True, 0, 1], [None, 0, 1], ["0", 0, 1], [[1], 0, 1]):
            bad_responses.append((f"invalid vector {vector!r}",
                                  {"data": [{"index": 0, "embedding": vector}] + entries[1:]}))
        for name, response in bad_responses:
            with self.subTest(response=name):
                before = len(self.server.requests)
                self.server.scenarios[tuple(prompts)] = [Reply(body=json.dumps(response).encode())]
                sql_error(self, 4070, self.query_embeddings)
                self.assertEqual(len(self.server.requests), before + 1, "invalid provider data must not be retried")
                self.server.scenarios.clear()
                self.assertEqual(self.query_embeddings(), expected)

    def test_embedding_native_dimension_mismatch_and_nonfinite_values(self):
        prompts = ["dimension-mismatch", "dimension-healthy"]
        expected = self.load_embeddings(prompts)
        self.server.scenarios[tuple(prompts)] = [Reply(body=json.dumps({"data": [
            {"index": 0, "embedding": [1, 2]}, {"index": 1, "embedding": [1, 2, 3]}]}).encode())]
        sql_error(self, 1210, self.query_embeddings)
        sql_error(self, 4070, lambda: self.query_embeddings("AI_EMBED('contract_embed', prompt)"))
        for value in ("NaN", "Infinity", "-Infinity"):
            with self.subTest(value=value):
                raw = ('{"data":[{"index":0,"embedding":[' + value + ',0,1]},'
                       '{"index":1,"embedding":[1,2,3]}]}').encode()
                self.server.scenarios[tuple(prompts)] = [Reply(body=raw)]
                sql_error(self, (4070, 3140, 5447), self.query_embeddings)
        self.server.scenarios.clear()
        self.assertEqual(self.query_embeddings(), expected)

    def test_embedding_native_dashscope_malformed_responses(self):
        prompts = ["shape-left", "shape-right"]
        expected = self.load_embeddings(prompts)
        self.cursor.execute("CALL DBMS_AI_SERVICE.ALTER_AI_MODEL_ENDPOINT(%s, %s)",
                            ("contract_embed_endpoint", '{"provider":"aliyun-dashscope"}'))
        self.server.embedding_format = "dashscope"
        invalid_responses = [{}, {"output": False}, {"output": {"embeddings": {}}},
                             {"output": {"embeddings": [None]}},
                             {"output": {"embeddings": [{"embedding": [1, 2, 3]}]}},
                             {"output": {"embeddings": [{"text_index": 0, "embedding": [1, 2, 3]},
                                                         {"text_index": 0, "embedding": [4, 5, 6]}]}}]
        for response in invalid_responses:
            with self.subTest(response=response):
                before = len(self.server.requests)
                self.server.scenarios[tuple(prompts)] = [Reply(body=json.dumps(response).encode())]
                sql_error(self, 4070, self.query_embeddings)
                self.assertEqual(len(self.server.requests), before + 1)
        self.server.scenarios.clear()
        self.assertEqual(self.query_embeddings(), expected)

    def test_embedding_unsupported_provider_is_rejected_before_http(self):
        expected = self.load_embeddings(["supported-left", "supported-right"])
        sql_error(self, 11116, lambda: self.cursor.execute(
            "CALL DBMS_AI_SERVICE.ALTER_AI_MODEL_ENDPOINT(%s, %s)",
            ("contract_embed_endpoint", '{"provider":"ollama"}')))
        self.assertFalse(self.server.requests)
        self.assertEqual(self.query_embeddings(), expected)

    def test_embedding_native_retry_preserves_whole_request(self):
        prompts = ["retry-batch-left", "retry-batch-right"]
        expected = self.load_embeddings(prompts)
        self.server.scenarios[tuple(prompts)] = [Reply(502, b"{}"), Reply()]
        self.assertEqual(self.query_embeddings(), expected)
        self.assertEqual(self.server.counts, Counter({tuple(prompts): 2}))
        self.assertEqual(self.server.requests, [{"model": "mock-embed", "input": prompts, "dimensions": 3}] * 2)

    def test_embedding_native_dropped_response_is_not_retried(self):
        prompts = ["drop-batch-left", "drop-batch-right"]
        self.load_embeddings(prompts)
        self.server.scenarios[tuple(prompts)] = [Reply(drop=True)]
        sql_error(self, 4216, self.query_embeddings)
        self.assertEqual(self.server.counts, Counter({tuple(prompts): 1}))

    def test_embedding_split_uses_encoded_bytes_and_retries_only_failed_request(self):
        prompts = [f"split-{index}:" + "\\" * (900 * 1024) for index in range(3)]
        self.assertLess(sum(len(text) for text in prompts), REQUEST_LIMIT)
        expected = self.load_embeddings(prompts)
        self.server.scenarios[tuple(prompts[:2])] = [Reply(502, b"{}"), Reply()]
        self.assertEqual(self.query_embeddings(), expected)
        self.assertEqual(self.server.counts, Counter({tuple(prompts[:2]): 2, prompts[2]: 1}))
        self.assertEqual(sorted(len(body["input"]) for body in self.server.requests), [1, 2, 2])
        self.assertTrue(all(record["request_bytes"] <= REQUEST_LIMIT for record in self.server.audit))
        self.assertEqual(Counter(text for body in self.server.requests for text in body["input"]),
                         Counter({prompts[0]: 2, prompts[1]: 2, prompts[2]: 1}))

    def test_embedding_split_count_error_cancels_held_peer(self):
        prompts = [f"fail-fast-{index}:" + "\\" * (900 * 1024) for index in range(3)]
        self.load_embeddings(prompts)
        gate = self.gate()
        self.server.scenarios[prompts[2]] = [Reply(gate=gate)]
        self.server.scenarios[tuple(prompts[:2])] = [Reply(peers=2, body=json.dumps({
            "data": [{"index": 0, "embedding": [1, 2, 3]}]}).encode())]
        self.cursor.execute("SET ob_query_timeout = 3000000")
        start = time.monotonic()
        sql_error(self, 4070, self.query_embeddings)
        self.assertLess(time.monotonic() - start, 2.5)
        self.assertEqual(len(self.server.requests), 2)
        self.assertTrue(any(record["prompt"] == prompts[2] and record["response"] is None
                            for record in self.server.audit))

    def test_embedding_oversized_single_input_rejected_before_http(self):
        for text in ("x" * (REQUEST_LIMIT + 1), "\\" * (REQUEST_LIMIT // 2 + 1)):
            with self.subTest(raw_bytes=len(text)):
                self.cursor.execute("TRUNCATE TABLE inputs")
                self.load(["valid-before-oversize", text])
                sql_error(self, 4019, self.query_embeddings)
                self.assertFalse(self.server.requests)

    def test_embedding_native_response_limit_and_recovery(self):
        prompts = ["response-limit-left", "response-limit-right"]
        expected = self.load_embeddings(prompts)
        raw = b'{"data":[],"padding":"' + b"x" * RESPONSE_LIMIT + b'"}'
        self.server.scenarios[tuple(prompts)] = [Reply(body=raw)]
        sql_error(self, 4019, self.query_embeddings)
        self.assertEqual(self.server.counts, Counter({tuple(prompts): 1}))
        self.server.scenarios.clear()
        self.assertEqual(self.query_embeddings(), expected)

    def test_embedding_native_deadline_and_recovery(self):
        prompts = ["deadline-left", "deadline-right"]
        expected = self.load_embeddings(prompts)
        gate = self.gate()
        self.server.scenarios[tuple(prompts)] = [Reply(gate=gate)]
        self.cursor.execute("SET ob_query_timeout = 500000")
        sql_error(self, 4012, self.query_embeddings)
        self.assertEqual(self.server.counts, Counter({tuple(prompts): 1}))
        gate.set()
        self.cursor.execute("SET ob_query_timeout = 20000000")
        self.server.scenarios.clear()
        self.assertEqual(self.query_embeddings(), expected)

    def test_embedding_native_cancel_releases_socket_and_recovers(self):
        prompts = ["cancel-native-left", "cancel-native-right"]
        expected = self.load_embeddings(prompts)
        gate = self.gate()
        self.server.scenarios[tuple(prompts)] = [Reply(gate=gate)]
        outcome = []

        def execute():
            try:
                outcome.append(self.query_embeddings())
            except Exception as error:
                outcome.append(error)

        worker = threading.Thread(target=execute, daemon=True)
        worker.start()
        try:
            with self.server.condition:
                self.assertTrue(self.server.condition.wait_for(lambda: len(self.server.audit) == 1, timeout=3))
            process_id = struct.unpack("3i", self.connection._sock.getsockopt(
                socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i")))[0]
            self.assertTrue(model_socket_fds(process_id, self.server.server_port))
            with pymysql.connect(unix_socket=self.sql_socket, user="root", autocommit=True) as control:
                with control.cursor() as cursor:
                    cursor.execute(f"KILL QUERY {self.connection.thread_id()}")
            worker.join(3)
            self.assertFalse(worker.is_alive())
            self.assertEqual(len(outcome), 1)
            self.assertIsInstance(outcome[0], pymysql.MySQLError)
            self.assertEqual(outcome[0].args[0], 1317)
            self.assertFalse(model_socket_fds(process_id, self.server.server_port))
        finally:
            gate.set()
            worker.join(5)
        self.assertEqual(self.server.counts, Counter({tuple(prompts): 1}))
        self.server.scenarios.clear()
        self.assertEqual(self.query_embeddings(), expected)

    def test_embedding_scalar_fallback_preserves_vectors_and_retries(self):
        prompts = ["embedding-retry", "embedding-healthy"]
        self.load(prompts)
        self.cursor.execute("UPDATE inputs SET model = 'contract_embed'")
        vector = [0.125, -0.25, 0.5]
        raw = json.dumps({"data": [{"index": 0, "embedding": vector}]}).encode()
        self.server.scenarios[prompts[0]] = [Reply(502, b"{}"), Reply(body=raw)]
        self.server.scenarios[prompts[1]] = [Reply(body=raw)]
        results = self.query("AI_EMBED(model, CAST(prompt AS CHAR), 3)")
        self.assertEqual(tuple((row, json.loads(value)) for row, value in results), ((0, vector), (1, vector)))
        self.assertEqual(self.server.counts, Counter({prompts[0]: 2, prompts[1]: 1}))
        self.assertTrue(all(body["model"] == "mock-embed" and body["dimensions"] == 3
                            for body in self.server.requests))

    def test_endpoint_credentials_refresh_between_executions(self):
        self.load(["credential-check"])
        query = "AI_COMPLETE('contract_model', prompt)"
        self.assertEqual(self.query(query), ((0, answer("credential-check")),))
        self.assertEqual(self.server.audit[-1]["headers"]["authorization"], "Bearer contract-test-only")
        try:
            self.cursor.execute("CALL DBMS_AI_SERVICE.ALTER_AI_MODEL_ENDPOINT(%s, %s)",
                                ("contract_endpoint", '{"access_key":"rotated-test-only"}'))
            self.assertEqual(self.query(query), ((0, answer("credential-check")),))
            self.assertEqual(self.server.audit[-1]["headers"]["authorization"], "Bearer rotated-test-only")
            self.assertEqual(self.server.counts["credential-check"], 2)
        finally:
            self.cursor.execute("CALL DBMS_AI_SERVICE.ALTER_AI_MODEL_ENDPOINT(%s, %s)",
                                ("contract_endpoint", '{"access_key":"contract-test-only"}'))


def run_contracts(connection, server, sql_socket):
    RuntimeContracts.connection = connection
    RuntimeContracts.server = server
    RuntimeContracts.sql_socket = sql_socket
    result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(RuntimeContracts))
    if not result.wasSuccessful():
        raise SystemExit(1)


if __name__ == "__main__":
    if "--self-test" in sys.argv:
        unittest.main(argv=[sys.argv[0]], defaultTest="HarnessTests", verbosity=2)
    else:
        runtime.main(run_contracts, sql_timeout=75)