"""Three loopback-only, all-SQL-candidate AI_JOIN proxy smoke cases.

Use --binary /path/to/new/seekdb or --socket /path/to/private/sql.sock.
Repeat --case to select methods; --list-cases starts no services. The binary
must contain JOIN proxy support; this module never builds or benchmarks it.

Fixed vectors and reversed OpenAI result indexes test reject-only cosine,
query-local side/pair coalescing, original prompt formats and duplicate bags.
No retrieval query, index, Top-K, FILE, PX or real/cloud model calls are used.
Owned SQL objects, private deployment and socket cleanup reuse the FILTER
proxy smoke helpers. Import/fixture sanity is not SQL integration evidence.
"""

import argparse
from collections import Counter
import json
import os
from pathlib import Path
import re
import unittest

import test_file_runtime as files
import test_semantic_cache as cache
import test_semantic_join as joins
import test_semantic_lift as lift
import test_semantic_proxy as proxy
import test_semantic_runtime as semantic
from test_runtime_contracts import Reply, completion, sql_error


BATCH_ROWS = proxy.BATCH_ROWS
RULE = "Respect the ordered left and right entries."
OPTIONS_SQL = """'{"temperature":0}'"""
VECTORS = {
    "oak": (1.0, 0.0, 0.0, 0.0),
    "pine": (2.0, 0.0, 0.0, 0.0),
    "edge": (1.0, 1.0, 1.0, 1.0),
    "far": (1.0, 2.0, 0.0, 0.0),
    "main-no": (1.0, 0.0, 0.0, 0.0),
    "abc": (1.0, 0.0, 0.0, 0.0),
    "cab": (1.0, 0.0, 0.0, 0.0),
    "bca": (2.0, 0.0, 0.0, 0.0),
}


def pair_key(left, right):
    return joins.pair_key(joins.pair_payload(RULE, left, right))


def ordered_prompt(left, right):
    return "".join(block["text"] for block in
                   files.join_content(RULE, "".join(left), "".join(right)))


def request_identity(prompt):
    if prompt.startswith("AI_JOIN rule:\n"):
        return prompt
    return joins.pair_key(joins.parse_join_prompt(prompt))


class JoinProxyScenarios(joins.JoinScenarios):
    def get(self, prompt, default=None):
        if prompt.startswith("AI_JOIN rule:\n"):
            return dict.get(self, prompt, default)
        return super().get(prompt, default)


def main_value(left, right):
    return right != "main-no" and (left, right) != ("pine", "oak")


class SemanticJoinProxySmoke(unittest.TestCase):
    database = None
    execute = proxy.SemanticProxySmoke.execute
    drop_owned_sql_objects = proxy.SemanticProxySmoke.drop_owned_sql_objects
    control_connection = cache.SemanticCacheContracts.control_connection
    kill_query = cache.SemanticCacheContracts.kill_query

    def setUp(self):
        proxy.SemanticProxySmoke.setUp(self)
        self.addCleanup(self.finish_pending)
        self.main.scenarios = JoinProxyScenarios()
        self.main.default_reply = Reply(400, joins.UNEXPECTED_PAIR)
        self.embedding.embedding_vectors = {text: list(vector) for text, vector in VECTORS.items()}
        self.cursor.execute("CREATE TABLE proxy_right ("
                            "id INT PRIMARY KEY, join_id INT NOT NULL, prompt LONGTEXT NOT NULL)")

    def finish_pending(self):
        self.fixtures.release()
        for pending in self.fixtures.pending:
            pending.thread.join(5)
        if any(pending.thread.is_alive() for pending in self.fixtures.pending):
            self.kill_query()
            for pending in self.fixtures.pending:
                pending.wait()

    def load(self, left, right):
        self.cursor.executemany("INSERT INTO proxy_inputs VALUES (%s,%s)", list(enumerate(left)))
        self.cursor.executemany("INSERT INTO proxy_right VALUES (%s,%s,%s)", [
            (left_id * len(right) + index, left_id, text)
            for left_id in range(len(left)) for index, text in enumerate(right)
        ])
        return [(left_text, right_text) for left_text in left for right_text in right]

    def reply(self, identity, value):
        self.assertIs(type(value), bool)
        self.main.scenarios[identity] = [
            Reply(body=completion(json.dumps({"value": value}), finish_reason="stop"))
        ]

    def call(self, *, left="l.prompt", right="r.prompt"):
        return joins.join_call(model=f"'{self.models[0]}'", rule=f"'{RULE}'",
                               left=left, right=right, options=OPTIONS_SQL)

    def statement(self, *, call=None, fields="l.prompt, r.prompt", where=None, hints=None):
        options = {
            "enable_semantic_cache": "true", "enable_semantic_proxy": "true",
            "semantic_proxy_model": self.models[1], "semantic_proxy_threshold": "0.5",
        }
        options.update(hints or {})
        quote = self.database.connection.escape
        parameters = " ".join(
            f"OPT_PARAM({quote(name)}, {quote(value)})"
            for name, value in options.items() if value is not None)
        predicate = self.call() if call is None else call
        on = "l.id=r.join_id" + (" AND (" + predicate + ")" if predicate else "")
        outer = "" if where is None else " WHERE " + where
        return (f"SELECT /*+ PARALLEL(1) LEADING(l r) USE_HASH(r) "
                f"OPT_PARAM('rowsets_max_rows', {BATCH_ROWS}) {parameters} */ {fields} "
                f"FROM proxy_inputs l INNER JOIN proxy_right r ON {on}{outer} ORDER BY l.id,r.id")

    def offsets(self):
        return tuple(len(lift.provider_snapshot(server).requests) for server in self.fixtures.mocks)

    def work(self, offsets=(0, 0)):
        main_requests, embedding_requests = (
            lift.provider_snapshot(server).requests[offset:]
            for server, offset in zip(self.fixtures.mocks, offsets))
        for body in main_requests:
            self.assertEqual(body["model"], proxy.REQUEST_MODELS[0])
            self.assertEqual(body.get("temperature"), 0)
            self.assertEqual([message["role"] for message in body["messages"]], ["user"])
            semantic.assert_filter_request(self, body)
            self.assertEqual(body["response_format"], joins.JOIN_FORMAT)
        inputs = []
        for body in embedding_requests:
            self.assertEqual(body["model"], proxy.REQUEST_MODELS[1])
            self.assertIsInstance(body["input"], list)
            self.assertTrue(body["input"])
            self.assertTrue(all(isinstance(text, str) for text in body["input"]))
            inputs.extend(body["input"])
        return (Counter(inputs),
                Counter(request_identity(body["messages"][0]["content"]) for body in main_requests),
                embedding_requests)

    def explain(self, statement, *, enabled):
        before = self.offsets()
        plan = lift.ExplainPlan(lift.render_explain(self.execute("EXPLAIN " + statement)))
        stage = cache.assert_candidate_boundary(self, plan)
        self.assertEqual(plan.one("HASH JOIN").parent_id, stage.operator_id, plan.text)
        self.assertEqual([node.operator for node in plan.nodes.values()
                          if node.operator.startswith("SEMANTIC ")], ["SEMANTIC JOIN"], plan.text)
        self.assertRegex(stage.annotation, r"(?i)\bfunction_cache\s*=")
        if enabled:
            self.assertRegex(stage.annotation, r"(?i)\bembedding_proxy\s*=\s*REJECT_ONLY\b")
            self.assertRegex(stage.annotation, r"(?i)\bproxy_input\s*=\s*LEFT_RIGHT_TEXT\b")
            self.assertRegex(stage.annotation, r"(?i)\bapproximate\s*=\s*true\b")
            threshold = re.search(r"(?i)\bthreshold\s*=\s*([-+0-9.eE]+)", stage.annotation)
            self.assertIsNotNone(threshold, plan.text)
            self.assertEqual(float(threshold.group(1)), 0.5)
        else:
            self.assertNotRegex(plan.text, r"(?i)\bembedding_proxy\s*=\s*REJECT_ONLY\b")
        self.assertEqual(self.work(before)[:2], (Counter(), Counter()))

    def test_routing_duplicate_bag_side_reuse_and_off(self):
        candidates = self.load(["oak", "oak", "pine"], ["oak", "pine", "edge", "far", "main-no"])
        distinct = set(candidates)
        for left, right in distinct:
            self.reply(pair_key(left, right), main_value(left, right))
        statement = self.statement()
        self.explain(statement, enabled=True)
        gate = self.fixtures.gate()
        self.embedding.default_reply = Reply(gate=gate)
        pending = self.fixtures.start(lambda: self.execute(statement))
        semantic.wait_evidence(
            self, self.embedding, lambda: bool(self.embedding.requests), pending,
            message="held side-vector request did not reach the embedding fixture")
        semantic.assert_held(self, self.embedding, [gate])
        with self.embedding.condition:
            held = Counter(text for body in self.embedding.requests for text in body["input"])
        self.assertTrue(held)
        self.assertTrue(all(count == 1 for count in held.values()),
                        "pending side-vector inputs must coalesce")
        with self.main.condition:
            self.assertFalse(self.main.requests, "completion ran while all side vectors were held")
        self.assertFalse(pending.done.is_set())
        gate.set()
        expected = tuple(pair for pair in candidates if pair[1] != "far" and main_value(*pair))
        self.assertEqual(pending.result(timeout=10), expected, "proxy changed duplicate bag or order")
        embeddings, completions, requests = self.work()
        self.assertEqual(embeddings, Counter(["oak", "pine", "edge", "far", "main-no"]))
        self.assertEqual(completions, Counter(pair_key(*pair) for pair in distinct if pair[1] != "far"))
        self.assertTrue(any(len(body["input"]) > 1 for body in requests),
                        "fixture must exercise reordered embedding response indexes")
        self.assertGreater(len(candidates), 2 * BATCH_ROWS)
        before = self.offsets()
        off = self.statement(hints={
            "enable_semantic_proxy": "false", "semantic_proxy_model": None,
            "semantic_proxy_threshold": None,
        })
        self.explain(off, enabled=False)
        expected = tuple(pair for pair in candidates if main_value(*pair))
        self.assertEqual(self.execute(off), expected, "cache-only changed duplicate bag or order")
        self.assertEqual(self.work(before)[:2], (
            Counter(), Counter(pair_key(*pair) for pair in distinct),
        ))

    def test_ordered_tuple_identity_and_validation(self):
        self.load(["c"], ["bc"])
        ordered = (
            ("AI_INPUT('ab', l.prompt)", "AI_INPUT('a', r.prompt)", ["ab", "c"], ["a", "bc"], True),
            ("AI_INPUT(l.prompt, 'ab')", "AI_INPUT(r.prompt, 'a')", ["c", "ab"], ["bc", "a"], False),
        )
        for left_sql, right_sql, left, right, value in ordered:
            with self.subTest(left=left, right=right):
                identity = ordered_prompt(left, right)
                self.reply(identity, value)
                before = self.offsets()
                statement = self.statement(call=self.call(left=left_sql, right=right_sql))
                self.assertEqual(self.execute(statement), (("c", "bc"),) if value else ())
                self.assertEqual(self.work(before)[:2], (
                    Counter(list(dict.fromkeys(["".join(left), "".join(right)]))), Counter([identity]),
                ))
        invalid = (
            {"enable_semantic_cache": "false"}, {"semantic_proxy_model": None},
            {"semantic_proxy_threshold": None}, {"semantic_proxy_threshold": 0.5},
            {"semantic_proxy_threshold": "1.01"}, {"semantic_proxy_threshold": "-1.01"},
            {"semantic_proxy_query": ""}, {"semantic_proxy_query": "unused retrieval text"},
        )
        before = self.offsets()
        for hints in invalid:
            with self.subTest(hints=hints):
                sql_error(self, 1210, lambda: self.execute(self.statement(hints=hints)))
                self.assertEqual(self.work(before)[:2], (Counter(), Counter()))
        call = self.call()
        unsupported = (
            ("negated", "NOT (" + call + ")", "l.id,r.id", None),
            ("or", "(" + call + ") OR l.id=0", "l.id,r.id", None),
            ("case", "CASE WHEN l.id=0 THEN " + call + " ELSE FALSE END", "l.id,r.id", None),
            ("projected_shared", call, "l.id,r.id," + call + " AS kept", None),
            ("outer_predicate", "", "l.id,r.id", call),
        )
        for label, predicate, fields, where in unsupported:
            with self.subTest(shape=label):
                sql_error(self, 1235, lambda: self.execute(
                    self.statement(call=predicate, fields=fields, where=where)))
                self.assertEqual(self.work(before)[:2], (Counter(), Counter()))

    def test_embedding_failure_and_fresh_query_recovery(self):
        self.load(["oak"], ["pine"])
        identity = pair_key("oak", "pine")
        self.reply(identity, True)
        self.embedding.default_reply = Reply(400, b'{"error":"permanent JOIN embedding fixture error"}')
        statement = self.statement()
        error = sql_error(self, 4216, lambda: self.execute(statement))
        self.assertGreaterEqual(len(error.args), 2)
        self.assertTrue(error.args[1], "embedding failure must have an explicit diagnostic")
        embeddings, completions, _requests = self.work()
        self.assertTrue(embeddings)
        self.assertTrue(embeddings <= Counter(["oak", "pine"]), "failed side vectors were retried")
        self.assertEqual(completions, Counter(), "embedding failure must not fall back to completion")
        before = self.offsets()
        self.embedding.default_reply = Reply()
        self.assertEqual(self.execute(statement), (("oak", "pine"),))
        self.assertEqual(self.work(before)[:2], (Counter(["oak", "pine"]), Counter([identity])))


def main(arguments=None):
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--binary", type=Path, help="New JOIN-proxy-enabled binary; never builds")
    source.add_argument("--socket", type=Path, help="Existing private Unix socket; never stops its server")
    parser.add_argument("--case", action="append", default=[], choices=proxy.case_names(SemanticJoinProxySmoke))
    parser.add_argument("--list-cases", action="store_true")
    options = parser.parse_args(arguments)
    selected = proxy.selected_cases(SemanticJoinProxySmoke, options.case)
    if options.list_cases:
        print("\n".join(selected))
        return 0
    if options.binary is None and options.socket is None:
        parser.error("--binary or --socket is required unless --list-cases is selected")
    if options.binary is not None:
        binary = options.binary.resolve()
        if not binary.is_file() or not os.access(binary, os.X_OK):
            parser.error(f"--binary is not an executable file: {binary}")
        database = proxy.PrivateDatabase(binary)
    else:
        sql_socket = options.socket.resolve()
        if not sql_socket.is_socket():
            parser.error(f"--socket is not a Unix socket: {sql_socket}")
        database = proxy.SocketDatabase(sql_socket)
    try:
        database.start()
        SemanticJoinProxySmoke.database = database
        result = unittest.TextTestRunner(verbosity=2, failfast=False).run(
            proxy.build_suite(SemanticJoinProxySmoke, selected))
        return 0 if result.wasSuccessful() else 1
    finally:
        try:
            database.close()
        finally:
            SemanticJoinProxySmoke.database = None


if __name__ == "__main__":
    raise SystemExit(main())
