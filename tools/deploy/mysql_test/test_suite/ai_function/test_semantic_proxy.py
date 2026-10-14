"""Five focused, loopback-only semantic embedding-proxy SQL smoke cases.

Use --binary /path/to/new/seekdb or --socket /path/to/private/sql.sock.
Repeat --case to select methods; --list-cases starts no services. The binary
must contain the new proxy implementation; this module never builds it.

Fixed four-dimensional vectors exercise reject-only cosine routing at 0.5,
reordered OpenAI response indexes, query-local caching, FILTER-before-MAP,
per-call FILTER configurations and explicit failure recovery.
Provider input/attempt counts, not timing or model effectiveness,
are the oracles. No real models, FILE inputs, JOIN/AGG execution or PX run.
Socket mode drops only uniquely named owned SQL objects and closes its own
connection; binary mode reuses the existing private Unix-socket launcher.
"""

import argparse
from collections import Counter
from contextlib import ExitStack
import json
import os
from pathlib import Path
import re
import unittest
import uuid

import test_semantic_runtime as semantic
from test_file_runtime import PrivateDatabase
from test_runtime_contracts import Reply, completion, sql_error
from test_semantic_cache import SocketDatabase, build_suite, case_names, selected_cases
from test_semantic_lift import provider_snapshot, render_explain


BATCH_ROWS = 4
INSTRUCTION = "Proxy smoke instruction: "
RETRIEVAL = "Proxy smoke retrieval text"
REQUEST_MODELS = ("mock-semantic-proxy-completion", "mock-semantic-proxy-embedding")
VECTORS = {
    RETRIEVAL: (1.0, 0.0, 0.0, 0.0),
    "far-0": (0.0, 1.0, 0.0, 0.0),
    "near-0": (1.0, 0.0, 0.0, 0.0),
    "main-no": (1.0, 0.0, 0.0, 0.0),
    "edge": (1.0, 1.0, 1.0, 1.0),
    "far-1": (-1.0, 0.0, 0.0, 0.0),
    "near-1": (2.0, 0.0, 0.0, 0.0),
    "far-2": (1.0, 2.0, 0.0, 0.0),
    "near-2": (1.0, 0.0, 1.0, 0.0),
}
COMPLETION_VALUES = {text: text != "main-no" for text in VECTORS if text != RETRIEVAL}


def configure_fixtures(fixtures):
    main, embedding = fixtures.mocks
    embedding.embedding_vectors = {text: list(vector) for text, vector in VECTORS.items()}
    main.default_reply = Reply(status=400, body=b'{"error":"unconfigured proxy smoke input"}')
    main.scenarios = {
        INSTRUCTION + text: [
            Reply(body=completion(json.dumps({"value": value}), finish_reason="stop"))
        ]
        for text, value in COMPLETION_VALUES.items()
    }
    return main, embedding


class SemanticProxySmoke(unittest.TestCase):
    database = None

    def setUp(self):
        self.cursor = self.database.connection.cursor()
        self.addCleanup(self.cursor.close)
        self.fixtures = semantic.MockFixtures()
        self.addCleanup(self.fixtures.close)
        self.main, self.embedding = configure_fixtures(self.fixtures)
        token = uuid.uuid4().hex[:12]
        self.schema = "semantic_proxy_" + token
        self.models = tuple(self.schema + "_" + kind for kind in ("completion", "embedding"))
        self.created_models = []
        self.created_endpoints = []
        self.schema_created = False
        self.addCleanup(self.drop_owned_sql_objects)
        self.cursor.execute(f"CREATE DATABASE {self.schema}")
        self.schema_created = True
        self.cursor.execute(f"USE {self.schema}")
        self.cursor.execute("SET ob_query_timeout=30000000, ai_pipeline_slots=2")
        self.cursor.execute("CREATE TABLE proxy_inputs (id INT PRIMARY KEY, prompt LONGTEXT NOT NULL)")
        for model, kind, wire_model, server, path in zip(
                self.models, ("completion", "dense_embedding"), REQUEST_MODELS,
                self.fixtures.mocks, ("chat/completions", "embeddings")):
            self.cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL(%s,%s)", (
                model, json.dumps({"type": kind, "model_name": wire_model}),
            ))
            self.created_models.append(model)
            endpoint = model + "_endpoint"
            self.cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT(%s,%s)", (
                endpoint, json.dumps({
                    "ai_model_name": model, "provider": "openai",
                    "request_model_name": wire_model, "access_key": "local-proxy-smoke-only",
                    "url": f"http://127.0.0.1:{server.server_port}/v1/{path}",
                }),
            ))
            self.created_endpoints.append(endpoint)

    def drop_owned_sql_objects(self):
        with ExitStack() as cleanup:
            if self.schema_created:
                cleanup.callback(self.cursor.execute, f"DROP DATABASE {self.schema}")
            for model in self.created_models:
                cleanup.callback(self.cursor.execute, "CALL DBMS_AI_SERVICE.DROP_AI_MODEL(%s)",
                                 (model,))
            for endpoint in self.created_endpoints:
                cleanup.callback(self.cursor.execute,
                                 "CALL DBMS_AI_SERVICE.DROP_AI_MODEL_ENDPOINT(%s)", (endpoint,))

    def execute(self, statement):
        self.cursor.execute(statement)
        return self.cursor.fetchall()

    def load(self, prompts):
        self.cursor.executemany("INSERT INTO proxy_inputs VALUES (%s,%s)", list(enumerate(prompts)))

    def filter_call(self, *, instruction=INSTRUCTION, document="prompt", options=None):
        quote = self.database.connection.escape
        config = {"temperature": 0} if options is None else options
        return (f"AI_FILTER({quote(self.models[0])}, AI_INPUT({quote(instruction)}, {document}), "
                f"{quote(json.dumps(config))})")

    def statement(self, *, fields="id, prompt", where=None, hints=None):
        options = {
            "enable_semantic_cache": "true",
            "enable_semantic_proxy": "true",
            "semantic_proxy_model": self.models[1],
            "semantic_proxy_query": RETRIEVAL,
            "semantic_proxy_threshold": "0.5",
        }
        options.update(hints or {})
        quote = self.database.connection.escape
        parameters = " ".join(
            f"OPT_PARAM({quote(name)}, {quote(value)})"
            for name, value in options.items() if value is not None)
        predicate = self.filter_call() if where is None else where
        return (f"SELECT /*+ PARALLEL(1) OPT_PARAM('rowsets_max_rows', {BATCH_ROWS}) "
                f"{parameters} */ {fields} FROM proxy_inputs WHERE {predicate} ORDER BY id")

    def work(self, offsets=(0, 0), *, map_instruction=None):
        main_requests, embedding_requests = (
            provider_snapshot(server).requests[offset:]
            for server, offset in zip(self.fixtures.mocks, offsets))
        for body in main_requests:
            self.assertEqual(body["model"], REQUEST_MODELS[0])
            self.assertEqual(body.get("temperature"), 0)
            self.assertEqual([message["role"] for message in body["messages"]], ["user"])
            self.assertNotIn("semantic_proxy", body, "engine-only options reached the main model")
            prompt = body["messages"][0]["content"]
            if map_instruction is None or not prompt.startswith(map_instruction):
                semantic.assert_filter_request(self, body)
        embedding_inputs = []
        for body in embedding_requests:
            self.assertEqual(body["model"], REQUEST_MODELS[1])
            self.assertIsInstance(body["input"], list)
            self.assertTrue(body["input"], "embedding requests must not contain empty arrays")
            self.assertTrue(all(isinstance(text, str) for text in body["input"]))
            embedding_inputs.extend(body["input"])
        return (Counter(embedding_inputs),
                Counter(body["messages"][0]["content"] for body in main_requests),
                embedding_requests)

    def test_proxy_routing_cache_and_off(self):
        prompts = ["far-0", "near-0", "main-no", "edge", "far-1", "near-1",
                   "far-2", "near-2", "near-0", "far-0", "edge"]
        self.load(prompts)
        statement = self.statement(fields="prompt")
        plan = render_explain(self.execute("EXPLAIN " + statement))
        self.assertRegex(plan, r"(?i)\bfunction_cache\s*=")
        self.assertRegex(plan, r"(?i)\bembedding_proxy\s*=\s*REJECT_ONLY\b")
        self.assertRegex(plan, r"(?i)\bapproximate\s*=\s*true\b")
        threshold = re.search(r"(?i)\bthreshold\s*=\s*([-+0-9.eE]+)", plan)
        self.assertIsNotNone(threshold, plan)
        self.assertEqual(float(threshold.group(1)), 0.5)
        self.assertEqual(self.work()[:2], (Counter(), Counter()))
        verified = {"near-0", "main-no", "edge", "near-1", "near-2"}
        expected = tuple((text,) for text in prompts if text in verified and COMPLETION_VALUES[text])
        self.assertEqual(self.execute(statement), expected, "proxy changed output bag or order")
        embeddings, completions, requests = self.work()
        self.assertEqual(embeddings, Counter([RETRIEVAL, *dict.fromkeys(prompts)]))
        self.assertEqual(completions, Counter(INSTRUCTION + text for text in verified))
        document_batches = [body for body in requests
                            if any(text != RETRIEVAL for text in body["input"])]
        self.assertGreaterEqual(len(document_batches), 2, "fixture must span native embedding batches")
        self.assertTrue(any(len(body["input"]) > 1 for body in document_batches),
                        "fixture must exercise reordered response indexes")
        before = (len(self.main.requests), len(self.embedding.requests))
        off = self.statement(fields="prompt", hints={
            "enable_semantic_proxy": "false", "semantic_proxy_model": None,
            "semantic_proxy_query": None, "semantic_proxy_threshold": None,
        })
        off_plan = render_explain(self.execute("EXPLAIN " + off))
        self.assertRegex(off_plan, r"(?i)\bfunction_cache\s*=")
        self.assertNotRegex(off_plan, r"(?i)\bembedding_proxy\s*=\s*REJECT_ONLY\b")
        self.assertEqual(self.work(before)[:2], (Counter(), Counter()))
        expected = tuple((text,) for text in prompts if COMPLETION_VALUES[text])
        self.assertEqual(self.execute(off), expected, "cache-only changed output bag or order")
        self.assertEqual(self.work(before)[:2], (
            Counter(), Counter(INSTRUCTION + text for text in dict.fromkeys(prompts)),
        ))

    def test_proxy_filter_then_normal_map(self):
        prompts = ["far-0", "near-0", "main-no", "near-0", "edge", "far-1"]
        self.load(prompts)
        map_instruction = "Proxy smoke map: "
        survivors = {"near-0", "edge"}
        self.main.scenarios.update({
            map_instruction + text: [Reply(body=completion("mapped:" + text, finish_reason="stop"))]
            for text in survivors
        })
        quote = self.database.connection.escape
        fields = (f"id,prompt,AI_MAP({quote(self.models[0])}, "
                  f"AI_INPUT({quote(map_instruction)},prompt),'{{\"temperature\":0}}')")
        predicate = self.filter_call(options={
            "temperature": 0, "semantic_proxy": {"query": RETRIEVAL},
        })
        statement = self.statement(fields=fields, where=predicate, hints={
            "semantic_proxy_query": "unused global retrieval override",
        })
        plan = render_explain(self.execute("EXPLAIN " + statement))
        self.assertEqual(len(re.findall(r"\bembedding_proxy=REJECT_ONLY\b", plan)), 1, plan)
        expected = tuple((index, text, "mapped:" + text)
                         for index, text in enumerate(prompts) if text in survivors)
        self.assertEqual(self.execute(statement), expected)
        embeddings, completions, _requests = self.work(map_instruction=map_instruction)
        self.assertEqual(embeddings, Counter([RETRIEVAL, *dict.fromkeys(prompts)]))
        self.assertEqual(completions, Counter([
            INSTRUCTION + "near-0", INSTRUCTION + "main-no", INSTRUCTION + "edge",
            map_instruction + "near-0", map_instruction + "edge",
        ]), "MAP must run only for unique inputs retained by FILTER and the main model")

    def test_multiple_filter_proxy_options(self):
        secondary_instruction = "Secondary proxy smoke instruction: "
        secondary_query = "Secondary proxy smoke retrieval text"
        self.embedding.embedding_vectors.update({
            secondary_query: [0.0, 1.0, 0.0, 0.0],
            "second-good": [0.0, 1.0, 0.0, 0.0],
            "second-far": [1.0, 0.0, 0.0, 0.0],
            "second-edge": [1.0, 1.0, 0.0, 0.0],
        })
        self.main.scenarios.update({
            secondary_instruction + text: [
                Reply(body=completion(json.dumps({"value": True}), finish_reason="stop"))
            ]
            for text in ("second-good", "second-far", "second-edge")
        })
        self.cursor.execute("ALTER TABLE proxy_inputs ADD COLUMN second_prompt LONGTEXT")
        rows = [
            (0, "near-0", "second-good"), (1, "far-0", "second-good"),
            (2, "near-0", "second-far"), (3, "near-0", "second-good"),
            (4, "near-0", "second-edge"),
        ]
        self.cursor.executemany("INSERT INTO proxy_inputs VALUES (%s,%s,%s)", rows)
        first = self.filter_call(options={
            "temperature": 0,
            "semantic_proxy": {"model": self.models[1], "query": RETRIEVAL},
        })
        second = self.filter_call(
            instruction=secondary_instruction, document="second_prompt", options={
                "temperature": 0,
                "semantic_proxy": {
                    "model": self.models[1], "query": secondary_query, "threshold": 0.75,
                },
            })
        statement = self.statement(fields="id,prompt,second_prompt",
                                   where=first + " AND " + second, hints={
                                       "semantic_proxy_model": None, "semantic_proxy_query": None,
                                   })
        plan = render_explain(self.execute("EXPLAIN " + statement))
        self.assertRegex(plan, r"\bproxy_calls=2\b")
        self.assertRegex(plan, r"\bthresholds=\[(?:0\.5,0\.75|0\.75,0\.5)\]")
        self.assertEqual(self.execute(statement), (rows[0], rows[3]), "independent proxy routes changed the bag")
        self.assertEqual(self.work()[:2], (
            Counter([RETRIEVAL, secondary_query, "near-0", "far-0",
                     "second-good", "second-far", "second-edge"]),
            Counter([INSTRUCTION + "near-0", secondary_instruction + "second-good"]),
        ))
        before = (len(self.main.requests), len(self.embedding.requests))
        off = self.statement(fields="id,prompt,second_prompt", where=first + " AND " + second, hints={
            "enable_semantic_proxy": "false", "semantic_proxy_model": None,
            "semantic_proxy_query": None, "semantic_proxy_threshold": None,
        })
        self.assertEqual(self.execute(off), tuple(rows), "disabled per-call proxies changed exact execution")
        self.assertEqual(self.work(before)[:2], (
            Counter(), Counter([INSTRUCTION + "near-0", INSTRUCTION + "far-0",
                                *(secondary_instruction + text for text in
                                  ("second-good", "second-far", "second-edge"))]),
        ))

    def test_invalid_parameters_and_unsupported_shapes(self):
        self.load(["near-0"])
        invalid = (
            {"enable_semantic_cache": None},
            {"enable_semantic_cache": "false"},
            {"semantic_proxy_model": None},
            {"semantic_proxy_query": None},
            {"semantic_proxy_threshold": None},
            {"semantic_proxy_threshold": "NaN"},
            {"semantic_proxy_model": self.models[0]},
        )
        for hints in invalid:
            with self.subTest(hints=hints):
                sql_error(self, 1210, lambda: self.execute(self.statement(hints=hints)))
                self.assertEqual(self.work()[:2], (Counter(), Counter()))
        for override in (None, [], {"query": ""}, {"model": 7},
                         {"threshold": "0.5"}, {"threshold": 1.01}, {"typo": 0.5}):
            with self.subTest(proxy_options=override):
                predicate = self.filter_call(options={
                    "temperature": 0, "semantic_proxy": override,
                })
                sql_error(self, 1210, lambda: self.execute(self.statement(where=predicate)))
                self.assertEqual(self.work()[:2], (Counter(), Counter()))
        predicate = self.filter_call()
        unsupported = (
            ("negated", "id, prompt", "NOT (" + predicate + ")"),
            ("or", "id, prompt", "(" + predicate + ") OR id=0"),
            ("case", "id, prompt", "CASE WHEN id=0 THEN " + predicate + " ELSE FALSE END"),
            ("projected_shared", "id, " + predicate + " AS kept", predicate),
            ("map_only", f"id, AI_MAP('{self.models[0]}', prompt)", "id>=0"),
            ("plain_prompt", "id, prompt", f"AI_FILTER('{self.models[0]}',prompt)"),
        )
        for label, fields, where in unsupported:
            with self.subTest(shape=label):
                sql_error(self, 1235, lambda: self.execute(self.statement(fields=fields, where=where)))
                self.assertEqual(self.work()[:2], (Counter(), Counter()))

    def test_bad_vector_fails_and_fresh_query_recovers(self):
        self.load(["near-0"])
        self.embedding.embedding_vectors[RETRIEVAL] = []
        statement = self.statement()
        error = sql_error(self, 4070, lambda: self.execute(statement))
        self.assertGreaterEqual(len(error.args), 2)
        self.assertTrue(error.args[1], "invalid embedding must have an explicit diagnostic")
        embeddings, completions, _requests = self.work()
        self.assertEqual(embeddings[RETRIEVAL], 1)
        self.assertEqual(completions, Counter(), "bad embeddings must not fall back to completion")
        before = (len(self.main.requests), len(self.embedding.requests))
        self.embedding.embedding_vectors[RETRIEVAL] = list(VECTORS[RETRIEVAL])
        self.assertEqual(self.execute(statement), ((0, "near-0"),))
        self.assertEqual(self.work(before)[:2], (
            Counter([RETRIEVAL, "near-0"]), Counter([INSTRUCTION + "near-0"]),
        ))


def main(arguments=None):
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--binary", type=Path, help="New proxy-enabled seekdb binary; never builds")
    source.add_argument("--socket", type=Path, help="Existing private Unix socket; never stops its server")
    parser.add_argument("--case", action="append", default=[], choices=case_names(SemanticProxySmoke))
    parser.add_argument("--list-cases", action="store_true")
    options = parser.parse_args(arguments)
    selected = selected_cases(SemanticProxySmoke, options.case)
    if options.list_cases:
        print("\n".join(selected))
        return 0
    if options.binary is None and options.socket is None:
        parser.error("--binary or --socket is required unless --list-cases is selected")
    if options.binary is not None:
        binary = options.binary.resolve()
        if not binary.is_file() or not os.access(binary, os.X_OK):
            parser.error(f"--binary is not an executable file: {binary}")
        database = PrivateDatabase(binary)
    else:
        sql_socket = options.socket.resolve()
        if not sql_socket.is_socket():
            parser.error(f"--socket is not a Unix socket: {sql_socket}")
        database = SocketDatabase(sql_socket)
    try:
        database.start()
        SemanticProxySmoke.database = database
        result = unittest.TextTestRunner(verbosity=2, failfast=True).run(
            build_suite(SemanticProxySmoke, selected))
        return 0 if result.wasSuccessful() else 1
    finally:
        try:
            database.close()
        finally:
            SemanticProxySmoke.database = None


if __name__ == "__main__":
    raise SystemExit(main())
