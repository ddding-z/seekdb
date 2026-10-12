"""Private SQL/mock contracts for the native, bounded asynchronous AI_AGG operator."""

from collections import Counter
from decimal import Decimal
import json
import os
import sys
import unittest

import test_semantic_runtime as semantic
from test_runtime_contracts import Reply, completion, sql_error


DEFAULT_OPTIONS = {
    "context_window": 4096,
    "tokenizer": "byte_bpe",
    "template_tokens": 256,
    "max_output_tokens": 256,
    "max_partial_bytes": 1024,
    "provider_options": {"temperature": 0},
}


def decode_request(prompt):
    request = json.loads(prompt)
    if set(request) != {"operation", "phase", "instruction", "inputs", "source_count"}:
        raise AssertionError(f"unexpected AI_AGG prompt fields: {request!r}")
    if request["operation"] != "ai_agg" or request["phase"] not in {
            "direct", "extract", "combine", "finalize"}:
        raise AssertionError(f"unexpected AI_AGG phase: {request!r}")
    if not isinstance(request["instruction"], str) or not request["instruction"]:
        raise AssertionError("AI_AGG instruction must be nonempty")
    if not isinstance(request["inputs"], list) or not request["inputs"] or not all(
            isinstance(text, str) for text in request["inputs"]):
        raise AssertionError("AI_AGG inputs must be a nonempty list of strings")
    if type(request["source_count"]) is not int or request["source_count"] < 1:
        raise AssertionError("AI_AGG source_count must be positive SQL metadata")
    return request


def aggregate_answer(request):
    if request["phase"] in {"direct", "extract"}:
        items = [text.split("|", 1)[0] for text in request["inputs"]]
    else:
        notes = [json.loads(text) for text in request["inputs"]]
        if sum(note["count"] for note in notes) != request["source_count"]:
            raise AssertionError("AI_AGG intermediate notes lost source coverage")
        items = [item for note in notes for item in note["items"]]
    return json.dumps({"items": items, "count": request["source_count"]},
                      ensure_ascii=False, separators=(",", ":"))


class AggregateScenarios(dict):
    def __init__(self):
        super().__init__()
        self.respond = lambda request: Reply(
            body=completion(aggregate_answer(request), finish_reason="stop"))

    def get(self, prompt, default=None):
        if prompt in self:
            return super().get(prompt, default)
        if isinstance(prompt, str) and prompt.startswith('{"operation":"ai_agg",'):
            return [self.respond(decode_request(prompt))]
        return default


class AggregateHarnessTests(unittest.TestCase):
    def test_mock_distinguishes_pointwise_and_collective_requests(self):
        scenarios = AggregateScenarios()
        default = [Reply(status=400)]
        self.assertIs(scenarios.get("ordinary row", default), default)
        request = {"operation": "ai_agg", "phase": "direct", "instruction": "collect",
                   "inputs": ["duplicate", "duplicate", ""], "source_count": 3}
        reply = scenarios.get(json.dumps(request, separators=(",", ":")))[0]
        body = json.loads(reply.body)
        self.assertEqual(body["choices"][0]["finish_reason"], "stop")
        self.assertEqual(json.loads(body["choices"][0]["message"]["content"]),
                         {"items": ["duplicate", "duplicate", ""], "count": 3})

    def test_hierarchical_mock_preserves_order_duplicates_and_counts(self):
        notes = ['{"items":["b","a"],"count":2}', '{"items":["a"],"count":1}']
        request = {"operation": "ai_agg", "phase": "combine", "instruction": "collect",
                   "inputs": notes, "source_count": 3}
        answer = aggregate_answer(decode_request(json.dumps(request)))
        self.assertEqual(json.loads(answer), {"items": ["b", "a", "a"], "count": 3})
        request["source_count"] = 4
        with self.assertRaises(AssertionError):
            aggregate_answer(request)

    def test_prompt_oracle_rejects_wrong_shapes(self):
        valid = {"operation": "ai_agg", "phase": "direct", "instruction": "collect",
                 "inputs": ["row"], "source_count": 1}
        for changes in ({"source_count": True}, {"source_count": 0}, {"inputs": []},
                        {"inputs": [1]}, {"phase": "map"}, {"instruction": ""}):
            with self.subTest(changes=changes), self.assertRaises(AssertionError):
                decode_request(json.dumps({**valid, **changes}))
        with self.assertRaises(AssertionError):
            decode_request(json.dumps({**valid, "unexpected": 1}))


class SemanticAggregateContracts(semantic.SemanticContracts):
    def setUp(self):
        super().setUp()
        self.cursor.execute("SET ob_log_level='WDIAG'")
        for mock in self.fixtures.mocks:
            mock.scenarios = AggregateScenarios()

    def aggregate(self, input_expr="prompt", *, model="semantic_a", instruction="collect",
                  options=None):
        options = DEFAULT_OPTIONS if options is None else options
        return (f"AI_AGG({self.connection.escape(model)}, {input_expr}, "
                f"{self.connection.escape(instruction)}, "
                f"{self.connection.escape(json.dumps(options, separators=(',', ':')))})")

    def requests(self, mock=None):
        mocks = [mock] if mock is not None else self.fixtures.mocks
        return [decode_request(body["messages"][-1]["content"])
                for current in mocks for body in current.requests
                if body["messages"][-1]["content"].startswith('{"operation":"ai_agg",')]

    def result(self, rows, count=None):
        return aggregate_answer({"phase": "direct", "inputs": rows,
                                 "source_count": len(rows) if count is None else count})

    def assert_request_budgets(self, options):
        for mock in self.fixtures.mocks:
            for body in mock.requests:
                if body["messages"][-1]["content"].startswith('{"operation":"ai_agg",'):
                    text_bytes = sum(len(message["content"].encode("utf-8"))
                                     for message in body["messages"])
                    self.assertLessEqual(
                        text_bytes + options["template_tokens"] + options["max_output_tokens"],
                        options["context_window"])
                    self.assertEqual(body["messages"][0]["role"], "system")
                    self.assertIn("For extract", body["messages"][0]["content"])
                    self.assertEqual(body["max_tokens"], options["max_output_tokens"])
                    self.assertNotIn("context_window", body)
                    self.assertNotIn("tokenizer", body)

    def test_small_scalar_single_request_and_ordinary_aggregates(self):
        prompts = ["alpha", None, "alpha", "", "beta"]
        self.load(prompts)
        fields = self.aggregate() + ", COUNT(*), COUNT(prompt), SUM(id), AVG(id), MIN(id), MAX(id)"
        self.assert_plan_labels(self.statement(fields, ""), ("SEMANTIC AGG",))
        self.assertEqual(self.select(fields, ""), (
            (self.result(["alpha", "alpha", "", "beta"]), 5, 4, Decimal(10),
             Decimal(2), 0, 4),))
        self.assertEqual(len(self.requests()), 1)
        self.assertEqual(self.requests()[0]["phase"], "direct")
        self.assertEqual(self.requests()[0]["inputs"], ["alpha", "alpha", "", "beta"])
        self.assert_request_budgets(DEFAULT_OPTIONS)

    def test_grouped_null_keys_and_mixed_aggregates(self):
        prompts = ["a", "b", None, "c", "d", None]
        flags = [None, 0, 0, None, 1, 1]
        self.load(prompts, flags=flags)
        fields = "guard, " + self.aggregate() + ", COUNT(*), SUM(id)"
        self.assertEqual(self.select(fields, "GROUP BY guard ORDER BY guard"), (
            (None, self.result(["a", "c"]), 2, Decimal(3)),
            (0, self.result(["b"]), 2, Decimal(3)),
            (1, self.result(["d"]), 2, Decimal(9)),))
        self.assertEqual(len(self.requests()), 3)

    def test_grouping_uses_sql_collation_not_byte_equality(self):
        self.load(["first", "second", "third", "null-key"],
                  models=["A", "a", "B", None])
        fields = "model COLLATE utf8mb4_general_ci AS g, " + self.aggregate() + ", COUNT(*)"
        self.assertEqual(self.select(fields, "GROUP BY g ORDER BY g"), (
            (None, self.result(["null-key"]), 1),
            ("A", self.result(["first", "second"]), 2),
            ("B", self.result(["third"]), 1),))
        self.assertEqual(len(self.requests()), 3)

    def test_lob_grouping_keys_compare_contents_not_reference_ids(self):
        key = "K" * 12000
        self.load([key, key, "other", None])
        fields = "prompt, " + self.aggregate("CAST(id AS CHAR)") + ", COUNT(*), SUM(id)"
        self.assertEqual(self.select(fields, "GROUP BY prompt ORDER BY prompt"), (
            (None, self.result(["3"]), 1, Decimal(3)),
            (key, self.result(["0", "1"]), 2, Decimal(1)),
            ("other", self.result(["2"]), 1, Decimal(2)),))
        self.assertEqual(len(self.requests()), 3)

    def test_empty_scalar_and_all_null_groups_make_no_requests(self):
        self.load([])
        self.assertEqual(self.select(self.aggregate() + ", COUNT(*), SUM(id)", ""),
                         ((None, 0, None),))
        self.assertEqual(self.select("guard, " + self.aggregate(), "GROUP BY guard"), ())
        self.load([None, None])
        self.assertEqual(self.select("guard, " + self.aggregate() + ", COUNT(*)",
                                     "GROUP BY guard ORDER BY guard"),
                         ((0, None, 1), (1, None, 1)))
        self.assertFalse(self.requests())

    def test_empty_model_text_is_a_nonnull_result_and_empty_input_is_a_row(self):
        self.load(["", None, "duplicate", "duplicate"])
        self.mock_a.scenarios.respond = lambda request: Reply(
            body=completion("", finish_reason="stop"))
        inner = self.statement(self.aggregate() + " AS summary", "")
        self.cursor.execute(
            "SELECT summary, summary IS NULL, CHAR_LENGTH(summary) FROM (" + inner + ") d")
        self.assertEqual(self.cursor.fetchall(), (("", 0, 0),))
        self.assertEqual(len(self.requests()), 1)
        self.assertEqual(self.requests()[0]["inputs"], ["", "duplicate", "duplicate"])
        self.assertEqual(self.requests()[0]["source_count"], 3)

    def test_aggregate_result_reuse_does_not_replay_requests(self):
        self.load(["x", "x", "y"])
        inner = self.statement(self.aggregate() + " AS summary", "")
        self.cursor.execute("SELECT summary, summary, CHAR_LENGTH(summary) FROM (" + inner + ") d")
        expected = self.result(["x", "x", "y"])
        self.assertEqual(self.cursor.fetchall(), ((expected, expected, len(expected)),))
        self.assertEqual(len(self.requests()), 1)

    def test_large_plain_text_result_retains_longtext_type_and_lob_ownership(self):
        self.load(["row"])
        result = 'answer:"' + "x" * 18000 + '"\n'
        self.mock_a.scenarios.respond = lambda request: Reply(
            body=completion(result, finish_reason="stop"))
        options = {**DEFAULT_OPTIONS, "context_window": 65536,
                   "max_output_tokens": 32768, "max_partial_bytes": 32768}
        inner = self.statement(self.aggregate(options=options) + " AS summary", "")
        self.cursor.execute(
            "SELECT summary, summary, CHAR_LENGTH(summary) FROM (" + inner + ") d")
        self.assertEqual(self.cursor.fetchall(), ((result, result, len(result)),))
        self.assertEqual(self.cursor.description[0][1], semantic.FIELD_TYPE.BLOB)
        self.assertEqual(len(self.requests()), 1)

    def test_null_groups_do_not_block_later_ready_group(self):
        self.load([None] * 64 + ["last"], flags=list(range(65)))
        self.assertEqual(
            self.select("guard, " + self.aggregate() + ", COUNT(*)",
                        "GROUP BY guard ORDER BY guard"),
            tuple((index, None if index < 64 else self.result(["last"]), 1)
                  for index in range(65)))
        self.assertEqual(len(self.requests()), 1)
        self.assertEqual(self.requests()[0]["inputs"], ["last"])

    def test_multiple_aggregates_share_query_limits(self):
        self.load(["one", "two", "three"])
        fields = self.aggregate(instruction="first task") + ", " + self.aggregate(
            model="semantic_b", instruction="second task") + ", COUNT(*)"
        expected = self.result(["one", "two", "three"])
        self.assertEqual(self.select(fields, ""), ((expected, expected, 3),))
        self.assertEqual(len(self.requests(self.mock_a)), 1)
        self.assertEqual(len(self.requests(self.mock_b)), 1)

    def test_filter_map_inputs_and_post_aggregate_map(self):
        prompts = [f"input-{index}" for index in range(9)]
        self.load(prompts)
        accepted = []
        for index, prompt in enumerate(prompts):
            self.filter_reply(self.mock_a, prompt, index % 2 == 0)
            self.mock_b.scenarios[prompt] = [Reply(body=completion(f"tag-{index}"))]
            if index % 2 == 0:
                accepted.append(f"tag-{index}")
        aggregate = self.aggregate("AI_MAP('semantic_b', prompt)")
        expected = self.result(accepted)
        post = "post:" + expected
        self.mock_b.scenarios[post] = [Reply(body=completion("post-aggregate-ready"))]
        inner = self.statement(aggregate + " AS summary, COUNT(*) AS n",
                               "WHERE AI_FILTER('semantic_a', prompt)")
        self.cursor.execute(
            "SELECT summary, n, AI_MAP('semantic_b', CONCAT('post:', summary)) FROM (" + inner + ") d")
        self.assertEqual(self.cursor.fetchall(), ((expected, len(accepted), "post-aggregate-ready"),))
        self.assertEqual(len(self.requests(self.mock_a)), 1)
        self.assertEqual(self.requests(self.mock_a)[0]["inputs"], accepted)
        self.assert_counts(self.mock_b, [prompt for index, prompt in enumerate(prompts)
                                        if index % 2 == 0] + [post])

    def test_having_consumes_semantic_aggregate_result(self):
        self.load(["left", "right"])
        expected = self.result(["left", "right"])
        self.filter_reply(self.mock_b, expected, True)
        self.assertEqual(self.select(self.aggregate() + " AS summary, COUNT(*)",
                                     "HAVING AI_FILTER('semantic_b', summary)"), ((expected, 2),))
        self.assertEqual(len(self.requests(self.mock_a)), 1)
        self.assert_counts(self.mock_b, [expected])

    def test_hierarchy_covers_all_rows_with_multiple_combine_levels(self):
        prompts = [f"r{index:02d}|" + "x" * 420 for index in range(32)]
        options = {**DEFAULT_OPTIONS, "context_window": 1536, "template_tokens": 128,
                   "max_output_tokens": 128, "max_partial_bytes": 512}
        self.load(prompts)
        self.assertEqual(self.select(self.aggregate(options=options) + ", COUNT(*), SUM(id)", ""),
                         ((self.result(prompts), 32, Decimal(496)),))
        requests = self.requests()
        leaves = [request for request in requests if request["phase"] == "extract"]
        leaves.sort(key=lambda request: prompts.index(request["inputs"][0]))
        self.assertGreaterEqual(len(leaves), 8)
        self.assertEqual(sum(request["source_count"] for request in leaves), 32)
        self.assertEqual([text for request in leaves for text in request["inputs"]], prompts)
        self.assertGreaterEqual(max(request["source_count"] for request in requests
                                    if request["phase"] == "combine"), 16)
        self.assertEqual([request["source_count"] for request in requests
                          if request["phase"] == "finalize"], [32])
        self.assert_request_budgets(options)

    def test_multiple_hierarchical_aggregates_retain_null_and_ordinary_row_counts(self):
        prompts = [None if index % 5 == 0 else f"r{index:02d}|" + "x" * 420
                   for index in range(32)]
        options = {**DEFAULT_OPTIONS, "context_window": 1536, "template_tokens": 128,
                   "max_output_tokens": 128, "max_partial_bytes": 512}
        self.load(prompts)
        fields = (self.aggregate(options=options, instruction="first task") + ", " +
                  self.aggregate(options=options, model="semantic_b", instruction="second task") +
                  ", COUNT(*), COUNT(prompt), SUM(id)")
        rows = [prompt for prompt in prompts if prompt is not None]
        expected = self.result(rows)
        self.assertEqual(self.select(fields, ""), ((expected, expected, 32, len(rows), Decimal(496)),))
        for mock in self.fixtures.mocks:
            requests = self.requests(mock)
            self.assertEqual(sum(request["source_count"] for request in requests
                                 if request["phase"] == "extract"), len(rows))
            self.assertEqual([request["source_count"] for request in requests
                              if request["phase"] == "finalize"], [len(rows)])
        self.assert_request_budgets(options)

    def test_groups_submit_concurrently_before_any_reply(self):
        prompts = [f"g{index}" for index in range(4)]
        self.load(prompts)
        gate = self.fixtures.gate()
        self.mock_a.scenarios.respond = lambda request: Reply(
            body=completion(aggregate_answer(request), finish_reason="stop"), gate=gate)
        pending = self.fixtures.start(lambda: self.select(
            "guard, " + self.aggregate(), "GROUP BY guard ORDER BY guard"))
        semantic.wait_evidence(
            self, self.mock_a, lambda: len(self.requests()) == 2, pending,
            message="AI_AGG groups were not concurrently submitted")
        semantic.assert_held(self, self.mock_a, [gate])
        self.assertGreaterEqual(self.mock_a.peak, 2)
        gate.set()
        self.assertEqual(pending.result(), ((0, self.result(["g0", "g2"])),
                                           (1, self.result(["g1", "g3"]))))

    def test_ready_combine_does_not_wait_for_a_whole_layer(self):
        prompts = [f"r{index:02d}|" + "x" * 420 for index in range(16)]
        options = {**DEFAULT_OPTIONS, "context_window": 1536, "template_tokens": 128,
                   "max_output_tokens": 128, "max_partial_bytes": 512}
        self.load(prompts)
        gate = self.fixtures.gate()

        def reply(request):
            held = request["phase"] == "extract" and any(
                text.startswith("r02|") for text in request["inputs"])
            return Reply(body=completion(aggregate_answer(request), finish_reason="stop"),
                         gate=gate if held else None)

        self.mock_a.scenarios.respond = reply
        pending = self.fixtures.start(lambda: self.select(self.aggregate(options=options), ""))
        semantic.wait_evidence(
            self, self.mock_a, lambda: any(
                request["phase"] == "extract" and any(
                    text.startswith("r02|") for text in request["inputs"])
                for request in self.requests()) and any(
                    request["phase"] == "combine" and request["source_count"] >= 4
                    for request in self.requests()), pending,
            message="a ready subtree could not combine while another leaf remained gated")
        self.assertFalse(pending.done.is_set())
        self.assertFalse(gate.is_set())
        gate.set()
        self.assertEqual(pending.result(), ((self.result(prompts),),))

    def test_reverse_completion_preserves_the_same_reduction_graph(self):
        prompts = [f"r{index:02d}|" + "x" * 420 for index in range(9)]
        options = {**DEFAULT_OPTIONS, "context_window": 1536, "template_tokens": 128,
                   "max_output_tokens": 128, "max_partial_bytes": 512}
        self.load(prompts)
        fields = self.aggregate(options=options)
        self.assertEqual(self.select(fields, ""), ((self.result(prompts),),))
        graph = Counter(json.dumps(request, sort_keys=True) for request in self.requests())
        self.mock_a.reset()
        first, second = self.fixtures.gate(), self.fixtures.gate()

        def reply(request):
            prefix = request["inputs"][0].split("|", 1)[0] if request["phase"] == "extract" else ""
            return Reply(body=completion(aggregate_answer(request), finish_reason="stop"),
                         gate=first if prefix == "r00" else second if prefix == "r01" else None)

        self.mock_a.scenarios.respond = reply
        pending = self.fixtures.start(lambda: self.select(fields, ""))
        semantic.wait_evidence(self, self.mock_a, lambda: len(self.requests()) == 2, pending,
                               message="two deterministic leaves were not submitted")
        second.set()
        semantic.wait_evidence(self, self.mock_a, lambda: len(self.requests()) >= 3, pending,
                               message="later leaves could not use a released request slot")
        first.set()
        self.assertEqual(pending.result(), ((self.result(prompts),),))
        self.assertEqual(Counter(json.dumps(request, sort_keys=True) for request in self.requests()),
                         graph)

    def test_utf8_and_json_escaping_are_in_the_context_bound(self):
        prompts = [f"row{index}|" + ("\u6f22\n\\\"" * 35) for index in range(6)]
        options = {**DEFAULT_OPTIONS, "context_window": 2048, "template_tokens": 128,
                   "max_output_tokens": 128, "max_partial_bytes": 512}
        self.load(prompts)
        self.assertEqual(self.select(self.aggregate(options=options), ""),
                         ((self.result(prompts),),))
        self.assertGreater(len(self.requests()), 1)
        self.assert_request_budgets(options)

    def test_invalid_arguments_and_unsupported_shapes_make_no_requests(self):
        self.load(["row"])
        options = self.connection.escape(json.dumps(DEFAULT_OPTIONS))
        unsupported = [
            f"SELECT AI_AGG(NULL, prompt, 'task', {options}) FROM semantic_inputs",
            f"SELECT AI_AGG('', prompt, 'task', {options}) FROM semantic_inputs",
            f"SELECT AI_AGG('semantic_a', prompt, NULL, {options}) FROM semantic_inputs",
            f"SELECT AI_AGG('semantic_a', prompt, '', {options}) FROM semantic_inputs",
            f"SELECT AI_AGG(model, prompt, 'task', {options}) FROM semantic_inputs",
            f"SELECT AI_AGG('semantic_a', prompt, model, {options}) FROM semantic_inputs",
            f"SELECT AI_AGG('semantic_a', prompt, 'task', opts) FROM semantic_inputs",
            f"SELECT AI_AGG('semantic_a', SUM(id), 'task', {options}) FROM semantic_inputs",
            f"SELECT AI_AGG('semantic_a', id, 'task', {options}) FROM semantic_inputs",
            f"SELECT AI_AGG('semantic_a', JSON_OBJECT('id', id), 'task', {options}) FROM semantic_inputs",
            f"SELECT AI_AGG('semantic_a', ROW(id, prompt), 'task', {options}) FROM semantic_inputs",
            f"SELECT AI_AGG('semantic_a', TO_FILE('/not-opened/aiagg.png'), 'task', {options}) FROM semantic_inputs",
            f"SELECT AI_AGG('semantic_a', prompt, 'task', {options}) FROM semantic_inputs GROUP BY guard WITH ROLLUP",
            f"SELECT AI_AGG('semantic_a', prompt, 'task', {options}) OVER () FROM semantic_inputs",
            f"SELECT AI_AGG(DISTINCT 'semantic_a', prompt, 'task', {options}) FROM semantic_inputs",
            "SELECT AI_AGG('semantic_a', prompt) FROM semantic_inputs",
        ]
        for statement in unsupported:
            with self.subTest(statement=statement), self.assertRaises(semantic.pymysql.MySQLError):
                self.cursor.execute(statement)
            self.assertFalse(self.requests())

    def test_options_require_explicit_context_and_disallow_overrides(self):
        self.load(["row"])
        invalid = [{}, {**DEFAULT_OPTIONS, "context_window": 0},
                   {**DEFAULT_OPTIONS, "context_window": 1.5},
                   {**DEFAULT_OPTIONS, "tokenizer": "guess-four-characters"},
                   {**DEFAULT_OPTIONS, "max_calls": 0},
                   {**DEFAULT_OPTIONS, "max_levels": 0},
                   {**DEFAULT_OPTIONS, "max_partial_bytes": 0},
                   {**DEFAULT_OPTIONS, "template_tokens": 4096},
                   {**DEFAULT_OPTIONS, "ignored_typo": 1}]
        invalid.extend({**DEFAULT_OPTIONS, "provider_options": {name: value}}
                       for name, value in (("model", "wrong"), ("messages", []),
                                           ("max_tokens", 1), ("stream", True),
                                           ("response_format", {}), ("n", 2), ("stop", "x")))
        for options in invalid:
            with self.subTest(options=options):
                sql_error(self, 1210, lambda: self.select(self.aggregate(options=options), ""))
            self.assertFalse(self.requests())
        sql_error(self, 1210, lambda: self.select("AI_AGG('semantic_a', prompt, 'task')", ""))
        self.assertFalse(self.requests())

    def test_oversized_row_and_partial_are_errors_not_truncation(self):
        self.load(["x" * 5000])
        sql_error(self, 4019, lambda: self.select(self.aggregate(), ""))
        self.assertFalse(self.requests())
        self.load(["row"])
        options = {**DEFAULT_OPTIONS, "max_partial_bytes": 128}
        self.mock_a.scenarios.respond = lambda request: Reply(
            body=completion("x" * 129, finish_reason="stop"))
        sql_error(self, 4019, lambda: self.select(self.aggregate(options=options), ""))
        self.assertEqual(len(self.requests()), 1)
        self.assert_no_model_sockets()

    def test_calls_and_levels_are_bounded_without_partial_success(self):
        prompts = [f"r{index:02d}|" + "x" * 420 for index in range(16)]
        self.load(prompts)
        options = {**DEFAULT_OPTIONS, "context_window": 1536, "template_tokens": 128,
                   "max_output_tokens": 128, "max_partial_bytes": 512}
        for bounds in ({"max_calls": 3}, {"max_levels": 1}):
            with self.subTest(bounds=bounds):
                self.mock_a.reset()
                sql_error(self, 4019, lambda: self.select(
                    self.aggregate(options={**options, **bounds}), ""))
                self.assertFalse(any(request["phase"] == "finalize" for request in self.requests()))
                self.assert_no_model_sockets()

    def test_provider_envelope_is_complete_text_and_not_retried(self):
        self.load(["row"])
        choice = {"finish_reason": "stop", "message": {"content": "answer"}}
        invalid = [
            {"choices": []}, {"choices": [choice, choice]},
            {"choices": [{"message": {"content": "answer"}}]},
            {"choices": [{**choice, "finish_reason": "length"}]},
            {"choices": [{**choice, "finish_reason": "tool_calls"}]},
            {"choices": [{**choice, "message": {"content": {"value": "answer"}}}]},
            {"choices": [{**choice, "message": {"content": "answer", "refusal": "refused"}}]},
        ]
        for payload in invalid:
            with self.subTest(payload=payload):
                before = len(self.requests())
                self.mock_a.scenarios.respond = lambda request: Reply(
                    body=json.dumps(payload).encode("utf-8"))
                sql_error(self, 4070, lambda: self.select(self.aggregate(), ""))
                self.assertEqual(len(self.requests()), before + 1)
                self.assert_no_model_sockets()

    def test_query_budget_exhaustion_is_not_a_deadline(self):
        self.load(["row"])
        self.cursor.execute("SET ai_pipeline_memory_limit=1048576, ob_query_timeout=3000000")
        options = {**DEFAULT_OPTIONS, "context_window": 4 * 1024 * 1024,
                   "max_partial_bytes": 2 * 1024 * 1024}
        self.load(["x" * 300000])
        sql_error(self, 4019, lambda: self.select(self.aggregate(options=options), ""))
        self.assert_no_model_sockets()
        self.cursor.execute("SET ai_pipeline_memory_limit=DEFAULT, ob_query_timeout=20000000")
        self.load(["recovered"])
        self.assertEqual(self.select(self.aggregate(), ""), ((self.result(["recovered"]),),))

    def test_retry_then_success_preserves_one_collective_task(self):
        self.load(["retry"])
        request = {"operation": "ai_agg", "phase": "direct", "instruction": "collect",
                   "inputs": ["retry"], "source_count": 1}
        self.mock_a.scenarios[json.dumps(request, separators=(",", ":"))] = [
            Reply(status=503, headers={"Retry-After": "0"}),
            Reply(body=completion(aggregate_answer(request), finish_reason="stop")),
        ]
        self.assertEqual(self.select(self.aggregate(), ""), ((self.result(["retry"]),),))
        self.assertEqual(len(self.requests()), 2)
        self.assertEqual(self.requests()[0], self.requests()[1])
        self.assert_no_model_sockets()

    def exercise_abort(self, *, cancel):
        self.load(["abandoned"])
        gate = self.fixtures.gate()
        self.mock_a.scenarios.respond = lambda request: Reply(
            body=completion(aggregate_answer(request), finish_reason="stop"), gate=gate)
        if not cancel:
            self.cursor.execute("SET ob_query_timeout=800000")
        pending = self.fixtures.start(lambda: self.select(self.aggregate(), ""))
        semantic.wait_evidence(self, self.mock_a, lambda: len(self.requests()) == 1, pending,
                               message="abort probe never submitted its collective request")
        if cancel:
            self.kill_query()
        pending.expect_sql_error(self, 1317 if cancel else 4012, timeout=5)
        self.assert_no_model_sockets()
        self.cursor.execute("SET ob_query_timeout=20000000")
        self.load(["recovered"])
        self.mock_a.scenarios.respond = lambda request: Reply(
            body=completion(aggregate_answer(request), finish_reason="stop"))
        self.assertEqual(self.select(self.aggregate(), ""), ((self.result(["recovered"]),),))
        self.assertFalse(gate.is_set())
        gate.set()

    def test_cancellation_releases_resources_before_old_reply(self):
        self.exercise_abort(cancel=True)

    def test_deadline_releases_resources_before_old_reply(self):
        self.exercise_abort(cancel=False)


def main(arguments=None):
    arguments = list(sys.argv[1:] if arguments is None else arguments)
    if "--case" not in arguments and "--self-test" not in arguments:
        for name in sorted(SemanticAggregateContracts.__dict__):
            if name.startswith("test_"):
                arguments.extend(["--case", name])
    semantic.SemanticContracts = SemanticAggregateContracts
    semantic.SemanticHarnessTests = AggregateHarnessTests
    options = semantic.parse_options(arguments)
    if options.list_cases:
        for name in options.case or semantic.case_names(
                AggregateHarnessTests if options.self_test else SemanticAggregateContracts):
            print(name)
    elif options.self_test:
        semantic.run_suite(AggregateHarnessTests, options.case)
    else:
        binary = options.binary.resolve()
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise SystemExit(f"--binary is not an executable file: {binary}")
        previous = sys.argv
        try:
            sys.argv = [previous[0], "--binary", str(binary)]
            semantic.runtime.main(
                lambda *args: semantic.run_contracts(*args, options.case),
                sql_timeout=75, description=__doc__, normal_shutdown=True)
        finally:
            sys.argv = previous


if __name__ == "__main__":
    main()
