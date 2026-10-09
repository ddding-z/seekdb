"""Independent first-version INNER AI_JOIN SQL contracts.

--self-test runs only loopback fixtures and assertion negative controls.
--binary /path/to/seekdb uses the existing private unix-socket launcher.
Repeat --case test_name to select contracts; --list-cases never starts SQL.
There is deliberately no option to connect to an existing database.

Expected candidates are the complete Cartesian product after ordinary SQL
predicates, with one completion per demanded candidate/call. Later AND calls
are demanded only for pairs accepted by earlier calls. Duplicate texts are not
duplicate rows. Prompt matching parses rule/left/right rather than assuming
serialization whitespace; their declared roles and key order are part of the
contract. Held response gates, not elapsed-time comparisons, provide
asynchronous overlap and cleanup evidence. This suite makes no native join
PX-worker-release claim.
"""

import argparse
from collections import Counter
from contextlib import redirect_stderr
from dataclasses import dataclass
import io
import json
import os
from pathlib import Path
import re
import reprlib
import socket
import struct
import sys
import unittest

import pymysql

import test_runtime as runtime
import test_semantic_runtime as semantic
from test_runtime_contracts import Reply, completion, sql_error


BATCH_ROWS = semantic.BATCH_ROWS
ROW_COUNT = 2 * BATCH_ROWS + 3
RULE = "Compare the left entry to the right entry."
RULE_SQL = "'" + RULE + "'"
JOIN_SCHEMA = {
    "type": "object",
    "properties": {"value": {"type": "boolean"}},
    "required": ["value"],
    "additionalProperties": False,
}
JOIN_FORMAT = {
    "type": "json_schema",
    "json_schema": {"name": "ai_join", "strict": True, "schema": JOIN_SCHEMA},
}
UNEXPECTED_PAIR = b'{"error":"unexpected AI_JOIN candidate"}'


def unique_object(pairs):
    result = {}
    for name, value in pairs:
        if name in result:
            raise ValueError(f"duplicate JSON key: {name}")
        result[name] = value
    return result


def reject_json_constant(value):
    raise ValueError(f"non-JSON constant: {value}")


def parse_join_prompt(prompt):
    if not isinstance(prompt, str):
        raise AssertionError("AI_JOIN prompt must be a SQL string")
    try:
        payload = json.loads(prompt, object_pairs_hook=unique_object,
                             parse_constant=reject_json_constant)
    except ValueError as error:
        raise AssertionError(f"AI_JOIN prompt is not strict JSON: {error}") from error
    if not isinstance(payload, dict) or list(payload) != ["rule", "left", "right"]:
        raise AssertionError("AI_JOIN prompt keys must be exactly rule, left, right in that order")
    if not isinstance(payload["rule"], str):
        raise AssertionError("AI_JOIN rule must remain a string")
    return payload


def pair_payload(rule, left, right):
    return {"rule": rule, "left": left, "right": right}


def pair_key(payload):
    return json.dumps([payload["rule"], payload["left"], payload["right"]],
                      ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def prompt_text(payload, *, indent=None):
    return json.dumps(payload, ensure_ascii=False, indent=indent, allow_nan=False)


class JoinScenarios(dict):
    """Adapt the existing exact-string mock lookup without changing its handler."""

    def get(self, prompt, default=None):
        return super().get(pair_key(parse_join_prompt(prompt)), default)


def request_counts(mock):
    with mock.condition:
        counts = dict(mock.counts)
        request_count = len(mock.requests)
    normalized = Counter()
    for prompt, count in counts.items():
        normalized[pair_key(parse_join_prompt(prompt))] += count
    if sum(normalized.values()) != request_count:
        raise AssertionError("mock request bodies and prompt counts disagree")
    return normalized


def assert_join_counts(test, mock, expected):
    actual = request_counts(mock)
    wanted = Counter(pair_key(payload) for payload in expected)
    test.assertTrue(actual == wanted,
                    f"wrong AI_JOIN pair multiplicity: actual={reprlib.repr(dict(actual))}, "
                    f"expected={reprlib.repr(dict(wanted))}")


def assert_join_request(test, body, caller_format=None):
    test.assertIn("messages", body)
    test.assertTrue(body["messages"], "AI_JOIN request has no user prompt")
    test.assertEqual(body["messages"][-1]["role"], "user")
    payload = parse_join_prompt(body["messages"][-1]["content"])
    semantic.assert_filter_request(test, body, caller_format)
    test.assertEqual(body["response_format"]["json_schema"]["schema"], JOIN_SCHEMA)
    if caller_format is None:
        test.assertEqual(body["response_format"]["json_schema"]["name"], "ai_join")
    return payload


def normalized_join_records(records):
    return [{**record, "prompt": pair_key(parse_join_prompt(record["prompt"]))}
            for record in records]


def assert_join_dependencies(test, inner_records, outer_records, dependencies):
    semantic.assert_dependencies(test, inner_records, normalized_join_records(outer_records), dependencies)


def assert_join_chain_dependencies(test, first_records, second_records, dependencies):
    assert_join_dependencies(test, normalized_join_records(first_records), second_records, dependencies)


def join_call(*, model="'semantic_a'", rule=RULE_SQL, left="l.txt", right="r.txt", options=None):
    arguments = [model, rule, left, right]
    if options is not None:
        arguments.append(options)
    return "AI_JOIN(" + ", ".join(arguments) + ")"


def assert_join_plan_annotation(test, plan, *, reversed_inputs=False):
    normalized = plan.upper().replace("`", "")
    test.assertRegex(normalized, r"\bCANDIDATE_POLICY\s*=\s*ALL_SQL_CANDIDATES\b")
    columns = {
        "left": r"(?:[A-Z_][A-Z0-9_]*\.)?(?:L|SEMANTIC_JOIN_LEFT)\.TXT",
        "right": r"(?:[A-Z_][A-Z0-9_]*\.)?(?:R|SEMANTIC_JOIN_RIGHT)\.TXT",
    }
    def argument(value, cast_type):
        return (r"(?:" + value + r"|CAST\s*\(\s*" + value +
                r"(?:\s*,\s*|\s+AS\s+)" + cast_type + r"(?:\(\d+\))?\s*\))")

    left, right = (("right", "left") if reversed_inputs else ("left", "right"))
    predicate = (r"\bPAIR_PREDICATES\s*\(\s*\[\s*AI_JOIN\s*\(\s*"
                 + argument(r"'SEMANTIC_A'", "VARCHAR") + r"\s*,\s*"
                 + argument("'" + re.escape(RULE.upper()) + "'", "VARCHAR") + r"\s*,\s*"
                 + argument(columns[left], r"(?:LONGTEXT|VARCHAR)") + r"\s*,\s*"
                 + argument(columns[right], r"(?:LONGTEXT|VARCHAR)") + r"\s*\)\s*\]\s*\)")
    test.assertRegex(normalized, predicate,
                     "SEMANTIC JOIN annotation lost the original model/rule/left/right binding")


def assert_unmaterialized_join_rescan(test, plan):
    ancestors = []
    semantic_joins = 0
    for line in plan.upper().splitlines():
        match = re.match(r"^\s*\|?\s*\d+\s*\|([^|]+)\|", line)
        if match is None:
            continue
        cell = match.group(1)
        operation = re.match(r"([^A-Z]*)([A-Z].*?)\s*$", cell)
        if operation is None:
            continue
        depth = len(operation.group(1))
        operator = operation.group(2)
        while ancestors and ancestors[-1][0] >= depth:
            ancestors.pop()
        if operator == "SEMANTIC JOIN":
            semantic_joins += 1
            outer_joins = [index for index, (_depth, name) in enumerate(ancestors)
                           if name.startswith("NESTED-LOOP JOIN")]
            test.assertTrue(outer_joins, "SEMANTIC JOIN is not below an outer rescan NLJ")
            path = [name for _depth, name in ancestors[outer_joins[-1] + 1:]]
            test.assertFalse(any("MATERIAL" in name for name in path),
                             f"cached SEMANTIC JOIN between the outer NLJ and inner subplan: {path}")
        ancestors.append((depth, operator))
    test.assertEqual(semantic_joins, 1, "expected one SEMANTIC JOIN in the EXPLAIN operator tree")


@dataclass(frozen=True)
class JoinInput:
    id: int
    text: str | None
    score: int = 0
    bucket: int = 0
    rule: str | None = RULE
    model: str = "semantic_a"
    options: str = '{"temperature":0.25}'


class JoinHarnessTests(unittest.TestCase):
    def setUp(self):
        self.fixtures = semantic.MockFixtures()
        self.addCleanup(self.fixtures.close)
        self.first, self.second = self.fixtures.mocks
        for mock in self.fixtures.mocks:
            mock.scenarios = JoinScenarios()
            mock.default_reply = Reply(400, UNEXPECTED_PAIR)

    def fixture_reply(self, mock, payload, value, *, gate=None):
        raw = completion(json.dumps({"value": value}), finish_reason="stop")
        mock.scenarios[pair_key(payload)] = [Reply(body=raw, gate=gate)]
        return raw

    def test_prompt_matching_ignores_whitespace_not_argument_roles(self):
        payload = pair_payload('rule:"\\\n\0\u4e2d\u6587', "left entry", {"id": 9, "text": "right"})
        raw = self.fixture_reply(self.first, payload, False)
        for prompt in (prompt_text(payload), prompt_text(payload, indent=2),
                       "\n" + prompt_text(payload, indent="\t") + " \n"):
            self.assertEqual(semantic.post_completion(self.first, prompt), (200, raw))
        assert_join_counts(self, self.first, [payload] * 3)
        reversed_payload = pair_payload(payload["rule"], payload["right"], payload["left"])
        self.assertEqual(semantic.post_completion(self.first, prompt_text(reversed_payload)),
                         (400, UNEXPECTED_PAIR))
        assert_join_counts(self, self.first, [payload] * 3 + [reversed_payload])

    def test_prompt_oracle_rejects_wrong_key_order_shape_and_non_json(self):
        invalid = [
            '{"left":"a","rule":"r","right":"b"}',
            '{"rule":"r","right":"b","left":"a"}',
            '{"rule":"r","left":"a"}',
            '{"rule":"r","left":"a","right":"b","extra":0}',
            '{"rule":null,"left":"a","right":"b"}',
            '{"rule":"r","rule":"r","left":"a","right":"b"}',
            '{"rule":"r","left":{"id":1,"id":2},"right":"b"}',
            '{"rule":"r","left":NaN,"right":"b"}',
            '[{"rule":"r","left":"a","right":"b"}]',
            '```json\n{"rule":"r","left":"a","right":"b"}\n```',
            '{"rule":', "", None,
        ]
        for prompt in invalid:
            with self.subTest(prompt=prompt), self.assertRaises(AssertionError):
                parse_join_prompt(prompt)

    def test_native_json_values_are_not_equivalent_to_json_looking_strings(self):
        native = pair_payload("native", {"text": "a"}, [False, None, 3])
        strings = pair_payload("native", '{"text":"a"}', "[false,null,3]")
        empty = pair_payload("", "", "")
        self.assertNotEqual(pair_key(native), pair_key(strings))
        first = self.fixture_reply(self.first, native, True)
        second = self.fixture_reply(self.first, strings, False)
        third = self.fixture_reply(self.first, empty, True)
        self.assertEqual(semantic.post_completion(self.first, prompt_text(native)), (200, first))
        self.assertEqual(semantic.post_completion(self.first, prompt_text(strings)), (200, second))
        self.assertEqual(semantic.post_completion(self.first, prompt_text(empty)), (200, third))
        assert_join_counts(self, self.first, [native, strings, empty])

    def test_schema_oracle_locks_default_name_strict_boolean_and_caller_format(self):
        payload = pair_payload(RULE, "left", "right")
        body = {"messages": [{"role": "user", "content": prompt_text(payload)}],
                "response_format": JOIN_FORMAT}
        self.assertEqual(assert_join_request(self, body), payload)
        custom = {**JOIN_FORMAT, "json_schema": {
            **JOIN_FORMAT["json_schema"], "name": "caller_join_schema"}}
        assert_join_request(self, {**body, "response_format": custom}, custom)
        native = JOIN_FORMAT["json_schema"]
        invalid = [
            {"name": "ai_filter"},
            {"name": ""},
            {"strict": False},
            {"strict": 1},
            {"schema": {**JOIN_SCHEMA, "additionalProperties": 0}},
            {"schema": {**JOIN_SCHEMA, "required": []}},
            {"schema": {**JOIN_SCHEMA, "properties": {"value": {"type": "string"}}}},
        ]
        for changes in invalid:
            with self.subTest(changes=changes), self.assertRaises(AssertionError):
                assert_join_request(self, {**body, "response_format": {
                    **JOIN_FORMAT, "json_schema": {**native, **changes}}})
        with self.assertRaises(AssertionError):
            assert_join_request(self, {**body, "Schema": None})
        with self.assertRaises(AssertionError):
            assert_join_request(self, {**body, "response_format": custom})
        with self.assertRaises(AssertionError):
            assert_join_request(self, body, custom)

    def test_explain_annotation_oracle_requires_all_candidates_and_original_arguments(self):
        prefix = "SEMANTIC JOIN candidate_policy=ALL_SQL_CANDIDATES, "
        annotation = prefix + "pair_predicates([" + join_call() + "])"
        assert_join_plan_annotation(self, annotation)
        qualified = join_call(left="semantic_join_left.txt", right="semantic_join_right.txt")
        assert_join_plan_annotation(self, prefix + "pair_predicates([" + qualified + "])")
        reversed_call = join_call(left="r.txt", right="l.txt")
        reversed_annotation = prefix + "pair_predicates([" + reversed_call + "])"
        assert_join_plan_annotation(self, reversed_annotation, reversed_inputs=True)
        cast_call = join_call(
            model="CAST('semantic_a', VARCHAR(1048576))",
            rule="CAST(" + RULE_SQL + ",\n VARCHAR(1048576))",
            left="CAST(l.txt, LONGTEXT(536870911))",
            right="CAST(r.txt, LONGTEXT(536870911))")
        assert_join_plan_annotation(self, prefix + "pair_predicates([" + cast_call + "])")
        reversed_cast = cast_call.replace("CAST(l.txt,", "CAST(swap.txt,").replace(
            "CAST(r.txt,", "CAST(l.txt,").replace("CAST(swap.txt,", "CAST(r.txt,")
        assert_join_plan_annotation(self, prefix + "pair_predicates([" + reversed_cast + "])",
                                    reversed_inputs=True)
        invalid = [
            annotation.replace("ALL_SQL_CANDIDATES", "ANN_PRUNED"),
            prefix + "pair_predicates(nil)",
            prefix + "other([" + join_call() + "])",
            prefix + "pair_predicates([" + join_call(model="'semantic_b'") + "])",
            prefix + "pair_predicates([" + join_call(rule="'different rule'") + "])",
            prefix + "pair_predicates([" + join_call(right="l.txt") + "])",
            reversed_annotation,
            prefix + "pair_predicates([" + reversed_cast + "])",
            prefix + "pair_predicates([" + cast_call.replace("'semantic_a'", "'semantic_b'") + "])",
        ]
        for plan in invalid:
            with self.subTest(plan=plan), self.assertRaises(AssertionError):
                assert_join_plan_annotation(self, plan)
        with self.assertRaises(AssertionError):
            assert_join_plan_annotation(self, annotation, reversed_inputs=True)

    def test_rescan_plan_oracle_allows_candidate_material_not_cached_semantic_results(self):
        tree = [
            (0, "SORT", ""),
            (1, "NESTED-LOOP JOIN", ""),
            (2, "TABLE FULL SCAN", "D"),
            (2, "SUBPLAN SCAN", "J"),
            (3, "SEMANTIC JOIN", ""),
            (4, "NESTED-LOOP JOIN", ""),
            (5, "TABLE FULL SCAN", "R"),
            (5, "MATERIAL", ""),
            (6, "TABLE FULL SCAN", "L"),
        ]

        def plan(rows, prefix="", unicode_tree=False):
            lines = []
            for index, (depth, operator, name) in enumerate(rows):
                indentation = " " * depth
                if unicode_tree and depth:
                    indentation = " " * (2 * depth - 2) + "\u2514\u2500"
                lines.append(f"{prefix}{index}|{indentation}{operator}|{name}|1|1|")
            return "\n".join(lines)

        assert_unmaterialized_join_rescan(self, plan(tree))
        assert_unmaterialized_join_rescan(self, plan(tree, "|"))
        assert_unmaterialized_join_rescan(self, plan(tree, "|", unicode_tree=True))
        driver_material = tree[:2] + [(2, "MATERIAL", ""), (3, "TABLE FULL SCAN", "D")] + tree[3:]
        assert_unmaterialized_join_rescan(self, plan(driver_material))
        root_material = [(0, "MATERIAL", "")] + [
            (depth + 1, operator, name) for depth, operator, name in tree]
        assert_unmaterialized_join_rescan(self, plan(root_material))
        cached_subplan = [*tree]
        cached_subplan[3] = (2, "MATERIAL", "J")
        cached_semantic = tree[:4] + [(3, "MATERIAL", "")] + [
            (depth + 1, operator, name) for depth, operator, name in tree[4:]]
        for rows in (cached_subplan, cached_semantic):
            for unicode_tree in (False, True):
                with self.subTest(rows=rows, unicode_tree=unicode_tree), self.assertRaisesRegex(
                        AssertionError, "cached SEMANTIC JOIN"):
                    assert_unmaterialized_join_rescan(self, plan(rows, "|", unicode_tree))
        no_outer_nlj = [*tree]
        no_outer_nlj[1] = (1, "HASH JOIN", "")
        with self.assertRaisesRegex(AssertionError, "outer rescan NLJ"):
            assert_unmaterialized_join_rescan(self, plan(no_outer_nlj))
        with self.assertRaisesRegex(AssertionError, "expected one SEMANTIC JOIN"):
            assert_unmaterialized_join_rescan(self, "SEMANTIC JOIN\nMATERIAL")

    def test_multiplicity_oracle_rejects_missing_duplicate_and_unexpected_pairs(self):
        first = pair_payload("counts", "same-left", "same-right")
        second = pair_payload("counts", "different-left", "same-right")
        for payload in (first, second):
            self.fixture_reply(self.first, payload, True)
        for payload in (first, first, second):
            semantic.post_completion(self.first, prompt_text(payload))
        assert_join_counts(self, self.first, [first, first, second])
        for expected in ([first, second], [first] * 3 + [second],
                         [first, first, pair_payload("counts", "same-right", "different-left")]):
            with self.subTest(expected=expected), self.assertRaises(AssertionError):
                assert_join_counts(self, self.first, expected)

    def test_unconfigured_pair_fails_explicitly_without_a_boolean_default(self):
        payload = pair_payload("not-configured", "left", "right")
        self.assertEqual(semantic.post_completion(self.first, prompt_text(payload)),
                         (400, UNEXPECTED_PAIR))
        assert_join_counts(self, self.first, [payload])

    def test_two_loopback_join_services_hold_before_either_response(self):
        payloads = [pair_payload("service-a", "left", "right"),
                    pair_payload("service-b", "right", "left")]
        gates = [self.fixtures.gate(), self.fixtures.gate()]
        bodies = [self.fixture_reply(mock, payload, value, gate=gate)
                  for mock, payload, value, gate in
                  zip(self.fixtures.mocks, payloads, (True, False), gates)]
        pending = [self.fixtures.start(
            lambda mock=mock, payload=payload: semantic.post_completion(mock, prompt_text(payload)))
            for mock, payload in zip(self.fixtures.mocks, payloads)]
        for mock, operation, gate in zip(self.fixtures.mocks, pending, gates):
            self.assertEqual(mock.server_address[0], "127.0.0.1")
            semantic.wait_evidence(self, mock, lambda mock=mock: len(mock.requests) == 1,
                                   operation, message="gated join fixture was not reached")
            semantic.assert_held(self, mock, [gate])
        gates[1].set()
        self.assertEqual(pending[1].result(), (200, bodies[1]))
        self.assertFalse(pending[0].done.is_set())
        semantic.assert_held(self, self.first, [gates[0]])
        gates[0].set()
        self.assertEqual(pending[0].result(), (200, bodies[0]))
        for mock, payload in zip(self.fixtures.mocks, payloads):
            assert_join_counts(self, mock, [payload])

    def test_dependency_oracle_requires_the_matching_map_response(self):
        payload = pair_payload("depends", "left", "mapped-right")
        outer = [{"prompt": prompt_text(payload, indent=2), "received": 5, "response": None}]
        dependencies = {pair_key(payload): "raw-right"}
        valid = [{"prompt": "raw-right", "received": 1, "response": 4}]
        assert_join_dependencies(self, valid, outer, dependencies)
        invalid = [[], [{"prompt": "raw-right", "received": 1, "response": None}],
                   [{"prompt": "raw-right", "received": 1, "response": 6}],
                   [{"prompt": "different-row", "received": 1, "response": 4}]]
        for inner in invalid:
            with self.subTest(inner=inner), self.assertRaises(AssertionError):
                assert_join_dependencies(self, inner, outer, dependencies)
        reversed_outer = [{**outer[0], "prompt": prompt_text(
            pair_payload("depends", "mapped-right", "left"))}]
        with self.assertRaises(AssertionError):
            assert_join_dependencies(self, valid, reversed_outer, dependencies)
        first = pair_payload("first AND rule", "left", "right")
        second = pair_payload("second AND rule", "left", "right")
        chain = {pair_key(second): pair_key(first)}
        first_record = {"prompt": prompt_text(first, indent=2), "received": 1, "response": 4}
        second_records = [{"prompt": prompt_text(second), "received": 5, "response": None}]
        assert_join_chain_dependencies(self, [first_record], second_records, chain)
        for records in ([], [{**first_record, "response": None}], [{**first_record, "response": 6}]):
            with self.subTest(chain=records), self.assertRaises(AssertionError):
                assert_join_chain_dependencies(self, records, second_records, chain)

    def test_pending_sql_failure_is_not_success_or_overlap_evidence(self):
        def fail():
            raise pymysql.OperationalError(1210, "fixture failure")

        pending = self.fixtures.start(fail)
        pending.expect_sql_error(self, 1210)
        with self.assertRaises(pymysql.OperationalError):
            pending.result()
        with self.assertRaises(AssertionError):
            pending.expect_sql_error(self, 4012)
        with self.assertRaises(AssertionError):
            semantic.wait_evidence(self, self.first, lambda: bool(self.first.requests), pending,
                                   message="failure is not join overlap")
        success = self.fixtures.start(lambda: ())
        with self.assertRaises(AssertionError):
            success.expect_sql_error(self, 1210)

    def test_new_pair_finishes_while_an_old_fixture_response_stays_held(self):
        old = pair_payload("old", "left", "right")
        new = pair_payload("new", "recovered-left", "recovered-right")
        gate = self.fixtures.gate()
        old_body = self.fixture_reply(self.first, old, True, gate=gate)
        new_body = self.fixture_reply(self.first, new, False)
        pending = self.fixtures.start(lambda: semantic.post_completion(self.first, prompt_text(old)))
        semantic.wait_evidence(self, self.first, lambda: len(self.first.requests) == 1, pending,
                               message="old response was not held")
        self.assertEqual(semantic.post_completion(self.first, prompt_text(new)), (200, new_body))
        semantic.assert_held(self, self.first, [gate], {prompt_text(old)})
        self.assertFalse(pending.done.is_set())
        assert_join_counts(self, self.first, [old, new])
        gate.set()
        self.assertEqual(pending.result(), (200, old_body))

    def test_duplicate_prompts_keep_independent_fixture_request_ordinals(self):
        payload = pair_payload("duplicate", "same", "same")
        raw = prompt_text(payload)
        bodies = [completion(json.dumps({"value": value}), finish_reason="stop")
                  for value in (True, False, True)]
        self.first.scenarios[pair_key(payload)] = [Reply(body=body) for body in bodies]
        for body in bodies:
            self.assertEqual(semantic.post_completion(self.first, raw), (200, body))
        assert_join_counts(self, self.first, [payload] * 3)

    def test_cli_selectors_are_independent_and_cannot_accept_an_external_socket(self):
        options = parse_options(["--binary", "/not-executed/seekdb",
                                 "--case", "test_cartesian_candidates_exact_pairs_and_plan",
                                 "--case", "test_empty_sides_do_not_issue_http"])
        self.assertEqual(len(options.case), 2)
        options = parse_options(["--self-test", "--case",
                                 "test_native_json_values_are_not_equivalent_to_json_looking_strings"])
        self.assertTrue(options.self_test)
        self.assertNotIn("test_map_longtext_and_filter_sql_boolean",
                         case_names(SemanticJoinContracts))
        parse_options(["--list-cases"])
        options = parse_options(["--list-cases", "--case",
                                 "test_bad_arity_is_1582_inside_inner_join_on_before_http"])
        self.assertEqual(options.case, ["test_bad_arity_is_1582_inside_inner_join_on_before_http"])
        for arguments in (["--self-test", "--binary", "/not-executed/seekdb"],
                          ["--self-test", "--socket", "/existing/sql.sock"],
                          ["--self-test", "--case", "test_cartesian_candidates_exact_pairs_and_plan"],
                          ["--case", "test_empty_sides_do_not_issue_http"],
                          ["--list-cases", "--case", "test_does_not_exist"]):
            with self.subTest(arguments=arguments), redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as caught:
                    parse_options(arguments)
                self.assertEqual(caught.exception.code, 2)


class SemanticJoinContracts(unittest.TestCase):
    assert_plan_labels = semantic.SemanticContracts.assert_plan_labels
    assert_no_model_sockets = semantic.SemanticContracts.assert_no_model_sockets
    kill_query = semantic.SemanticContracts.kill_query

    @classmethod
    def setUpClass(cls):
        with cls.connection.cursor() as cursor:
            cursor.execute("SET ob_query_timeout = 30000000")
            cursor.execute("CREATE DATABASE ai_semantic_join_contract_test")
            cursor.execute("USE ai_semantic_join_contract_test")
            for suffix in ("a", "b"):
                model = f"semantic_{suffix}"
                cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL(%s, %s)",
                               (model, json.dumps({"type": "completion", "model_name": f"mock-{suffix}"})))
                endpoint = {
                    "ai_model_name": model,
                    "url": f"http://127.0.0.1:{cls.server.server_port}/v1/chat/completions",
                    "access_key": "semantic-join-fixture-only",
                    "provider": "openai",
                    "request_model_name": f"mock-{suffix}",
                }
                cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT(%s, %s)",
                               (f"{model}_endpoint", json.dumps(endpoint)))
            for side in ("left", "right"):
                cursor.execute(f"CREATE TABLE semantic_join_{side} ("
                               "id INT PRIMARY KEY, txt LONGTEXT, score INT, bucket INT, "
                               "rule_text LONGTEXT, model VARCHAR(64), opts LONGTEXT)")
            cursor.execute("CREATE TABLE semantic_join_drivers (id INT PRIMARY KEY)")

    def setUp(self):
        self.fixtures = semantic.MockFixtures()
        self.mock_a, self.mock_b = self.fixtures.mocks
        self.join_mocks = set(self.fixtures.mocks)
        self.join_formats = {mock: {} for mock in self.fixtures.mocks}
        self.cursor = self.connection.cursor()
        self.addCleanup(self.close_case)
        self.connection_id = self.connection.thread_id()
        self.cursor.execute("SET ob_query_timeout = 20000000")
        self.cursor.execute("SET ai_pipeline_slots = 2, ai_pipeline_memory_limit = DEFAULT")
        for suffix, mock in zip(("a", "b"), self.fixtures.mocks):
            mock.scenarios = JoinScenarios()
            mock.default_reply = Reply(400, UNEXPECTED_PAIR)
            self.cursor.execute("CALL DBMS_AI_SERVICE.ALTER_AI_MODEL_ENDPOINT(%s, %s)",
                                (f"semantic_{suffix}_endpoint", json.dumps({
                                    "url": f"http://127.0.0.1:{mock.server_port}/v1/chat/completions",
                                    "provider": "openai",
                                })))
        self.load_rows([], [])
        self.cursor.execute("TRUNCATE TABLE semantic_join_drivers")
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
        for mock in self.join_mocks:
            for body in mock.requests:
                payload = parse_join_prompt(body["messages"][-1]["content"])
                assert_join_request(self, body, self.join_formats[mock].get(pair_key(payload)))

    def load_rows(self, left, right):
        self.left_rows, self.right_rows = tuple(left), tuple(right)
        for side, rows in (("left", self.left_rows), ("right", self.right_rows)):
            self.cursor.execute(f"TRUNCATE TABLE semantic_join_{side}")
            if rows:
                self.cursor.executemany(f"INSERT INTO semantic_join_{side} VALUES "
                                        "(%s, %s, %s, %s, %s, %s, %s)",
                                        [(row.id, row.text, row.score, row.bucket, row.rule,
                                          row.model, row.options) for row in rows])

    def statement(self, *, fields="l.id, r.id", on=None, suffix="ORDER BY l.id, r.id",
                  from_sql="semantic_join_left l INNER JOIN semantic_join_right r", hints=""):
        on = join_call() if on is None else on
        return (f"SELECT /*+ OPT_PARAM('rowsets_max_rows', {BATCH_ROWS}) {hints} */ "
                f"{fields} FROM {from_sql} ON {on} {suffix}")

    def execute(self, statement, parameters=None):
        self.cursor.execute(statement, parameters)
        return self.cursor.fetchall()

    def pair_reply(self, payload, value, *, mock=None, gate=None, caller_format=None):
        self.assertIs(type(value), bool)
        mock = self.mock_a if mock is None else mock
        key = pair_key(payload)
        mock.scenarios[key] = [Reply(body=completion(
            json.dumps({"value": value}), finish_reason="stop"), gate=gate)]
        self.join_formats[mock][key] = caller_format

    def seed_pairs(self, *, accepts, cpu=None, rule=RULE, left_value=None, right_value=None,
                   mock=None, gate=None, caller_format=None):
        expected_rows, payloads = [], []
        for left in self.left_rows:
            for right in self.right_rows:
                if cpu is not None and not cpu(left, right):
                    continue
                value = accepts(left, right) if callable(accepts) else (left.id, right.id) in accepts
                payload = pair_payload(
                    rule(left, right) if callable(rule) else rule,
                    left.text if left_value is None else left_value(left, right),
                    right.text if right_value is None else right_value(left, right))
                self.pair_reply(payload, value, mock=mock, gate=gate, caller_format=caller_format)
                payloads.append(payload)
                if value:
                    expected_rows.append((left.id, right.id))
        return tuple(sorted(expected_rows)), payloads

    def assert_no_http(self):
        for mock in self.fixtures.mocks:
            with mock.condition:
                self.assertFalse(mock.requests)
                self.assertFalse(mock.counts)

    def wait_two_held_batches(self, mock, pending, gates):
        semantic.wait_evidence(self, mock, lambda: len(mock.requests) >= 2 * BATCH_ROWS,
                               pending, message="slots=2 must admit candidate batch 2 before any reply")
        semantic.assert_held(self, mock, gates)
        self.assertEqual(len(semantic.audit(mock)), 2 * BATCH_ROWS,
                         "held requests exceeded the two candidate-batch admission limit")
        self.assertTrue(all(count == 1 for count in request_counts(mock).values()))
        self.assertFalse(pending.done.is_set())

    def start_held_join(self, prefix):
        self.load_rows([JoinInput(7, prefix + "-left")],
                       [JoinInput(index, f"{prefix}-right-{index}") for index in range(ROW_COUNT)])
        gate = self.fixtures.gate()
        rows, payloads = self.seed_pairs(accepts=lambda _left, _right: True, gate=gate)
        statement = self.statement()
        self.assert_plan_labels(statement, ("SEMANTIC JOIN",))
        pending = self.fixtures.start(lambda: self.execute(statement))
        return pending, gate, rows, payloads

    def recover_while_old_responses_are_held(self, gates, old_payloads):
        self.assertTrue(gates)
        self.assertTrue(all(not gate.is_set() for gate in gates))
        self.assert_no_model_sockets()
        self.cursor.execute("SET ob_query_timeout = 20000000")
        self.load_rows([JoinInput(901, "recovered-left")], [JoinInput(902, "recovered-right")])
        rows, payloads = self.seed_pairs(accepts=lambda _left, _right: True)
        recovery = self.fixtures.start(lambda: self.execute(self.statement()))
        self.assertEqual(recovery.result(timeout=3), rows)
        self.assertEqual(self.connection.thread_id(), self.connection_id)
        self.assertTrue(all(not gate.is_set() for gate in gates),
                        "same-connection recovery required releasing abandoned responses")
        assert_join_counts(self, self.mock_a, old_payloads + payloads)
        assert_join_counts(self, self.mock_b, [])

    def test_cartesian_candidates_exact_pairs_and_plan(self):
        self.load_rows([JoinInput(2, "left-two"), JoinInput(5, "left-five"), JoinInput(9, "left-nine")],
                       [JoinInput(3, "right-three"), JoinInput(8, "right-eight")])
        accepted = {(2, 3), (2, 8), (9, 8)}
        rows, payloads = self.seed_pairs(accepts=accepted)
        self.assertEqual(len(payloads), 6)
        plan = self.assert_plan_labels(self.statement(), ("SEMANTIC JOIN",))
        assert_join_plan_annotation(self, plan)
        self.assertNotIn("SEMANTIC FILTER", plan)
        self.assert_no_http()
        self.assertEqual(self.execute(self.statement()), rows)
        self.assertEqual(rows, ((2, 3), (2, 8), (9, 8)))
        assert_join_counts(self, self.mock_a, payloads)
        assert_join_counts(self, self.mock_b, [])

    def test_primary_key_self_join_preserves_binary_candidate_bindings(self):
        inputs = [JoinInput(2, "self-two"), JoinInput(5, "self-five"), JoinInput(8, "self-eight")]
        self.load_rows(inputs, [])
        payloads = []
        for row in inputs:
            payload = pair_payload(RULE, row.text, row.text)
            self.pair_reply(payload, row.id % 2 == 0)
            payloads.append(payload)
        statement = self.statement(
            from_sql="semantic_join_left l INNER JOIN semantic_join_left r",
            on="l.id = r.id AND " + join_call())
        self.assert_plan_labels(statement, ("SEMANTIC JOIN",))
        self.assert_no_http()
        self.assertEqual(self.execute(statement), ((2, 2), (8, 8)))
        assert_join_counts(self, self.mock_a, payloads)
        assert_join_counts(self, self.mock_b, [])

    def test_all_false_drains_every_candidate_without_losing_the_tail(self):
        self.load_rows([JoinInput(index, f"false-left-{index}") for index in range(3)],
                       [JoinInput(index, f"false-right-{index}") for index in range(13)])
        rows, payloads = self.seed_pairs(accepts=set())
        self.assertEqual(len(payloads), 39)
        self.assertEqual(self.execute(self.statement()), rows)
        self.assertEqual(rows, ())
        assert_join_counts(self, self.mock_a, payloads)

    def test_asymmetric_matching_preserves_argument_roles(self):
        self.load_rows([JoinInput(1, "author:alice"), JoinInput(2, "author:bob")],
                       [JoinInput(10, "book:bob"), JoinInput(11, "book:alice"),
                        JoinInput(12, "book:unrelated")])
        rows, payloads = self.seed_pairs(accepts={(1, 11), (2, 10)})
        self.assertEqual(self.execute(self.statement()), ((1, 11), (2, 10)))
        self.assertEqual(rows, ((1, 11), (2, 10)))
        assert_join_counts(self, self.mock_a, payloads)

    def test_reversed_operands_preserve_declared_not_physical_roles(self):
        self.load_rows([JoinInput(4, "author:alice"), JoinInput(6, "author:bob")],
                       [JoinInput(10, "book:bob"), JoinInput(12, "book:alice")])
        rows, payloads = self.seed_pairs(
            accepts={(4, 12)}, left_value=lambda _left, right: right.text,
            right_value=lambda left, _right: left.text)
        statement = self.statement(on=join_call(left="r.txt", right="l.txt"))
        plan = self.assert_plan_labels(statement, ("SEMANTIC JOIN",))
        assert_join_plan_annotation(self, plan, reversed_inputs=True)
        self.assertEqual(self.execute(statement), rows)
        self.assertEqual(rows, ((4, 12),))
        assert_join_counts(self, self.mock_a, payloads)

    def test_reversed_syntactic_join_sides_preserve_declared_roles(self):
        self.load_rows([JoinInput(3, "logical-left")],
                       [JoinInput(10, "logical-right-a"), JoinInput(11, "logical-right-b")])
        rows, payloads = self.seed_pairs(accepts={(3, 11)})
        statement = self.statement(from_sql="semantic_join_right r INNER JOIN semantic_join_left l")
        plan = self.assert_plan_labels(statement, ("SEMANTIC JOIN",))
        assert_join_plan_annotation(self, plan)
        self.assertEqual(self.execute(statement), rows)
        assert_join_counts(self, self.mock_a, payloads)

    def test_duplicate_texts_preserve_independent_ids_and_multiple_matches(self):
        self.load_rows([JoinInput(index, "duplicate-left") for index in (2, 7, 20)],
                       [JoinInput(index, text) for index, text in
                        ((3, "duplicate-right"), (11, "duplicate-right"), (18, "no-match"))])
        accepted = {(left, right) for left in (2, 7, 20) for right in (3, 11)}
        rows, payloads = self.seed_pairs(accepts=accepted)
        self.assertEqual(self.execute(self.statement()), tuple(sorted(accepted)))
        self.assertEqual(rows, tuple(sorted(accepted)))
        assert_join_counts(self, self.mock_a, payloads)
        self.assertEqual(request_counts(self.mock_a),
                         Counter({pair_key(pair_payload(RULE, "duplicate-left", "duplicate-right")): 6,
                                  pair_key(pair_payload(RULE, "duplicate-left", "no-match")): 3}))

    def test_empty_sides_do_not_issue_http(self):
        for left, right in (([], [JoinInput(1, "right")]),
                            ([JoinInput(1, "left")], []), ([], [])):
            with self.subTest(left=left, right=right):
                self.load_rows(left, right)
                self.assertEqual(self.execute(self.statement()), ())
                self.assert_no_http()

    def test_native_json_objects_arrays_scalars_and_null_are_not_double_encoded(self):
        left_values = [{"id": 2, "text": 'left:"\\\n\0\u4e2d\u6587', "flag": False},
                       [1, None, {"nested": "left"}], 3, True, None, "json scalar"]
        right_values = [{"right": [False, 5]}, ["right", None]]
        self.load_rows([JoinInput(index, json.dumps(value, ensure_ascii=False))
                        for index, value in enumerate(left_values)],
                       [JoinInput(index + 20, json.dumps(value))
                        for index, value in enumerate(right_values)])
        rows, payloads = self.seed_pairs(
            accepts=lambda left, right: (left.id + right.id) % 2 == 0,
            left_value=lambda left, _right: json.loads(left.text),
            right_value=lambda _left, right: json.loads(right.text))
        statement = self.statement(on=join_call(left="CAST(l.txt AS JSON)", right="CAST(r.txt AS JSON)"))
        self.assertEqual(self.execute(statement), rows)
        assert_join_counts(self, self.mock_a, payloads)

    def test_mixed_string_json_and_cpu_rule_preserve_values(self):
        self.load_rows([JoinInput(1, '{"looks":"like JSON"}', rule='r:"\\\n\0\u4e2d\u6587'),
                        JoinInput(4, "", rule="empty left"),
                        JoinInput(8, 'string:"\\\n\0\u4e2d\u6587', rule="native string")],
                       [JoinInput(10, '{"id":10,"values":[false,null]}', rule="right rule")])
        rows, payloads = self.seed_pairs(
            accepts=lambda _left, _right: True,
            rule=lambda left, right: left.rule + "/" + right.rule,
            right_value=lambda _left, right: json.loads(right.text))
        statement = self.statement(on=join_call(
            rule="CONCAT(l.rule_text, '/', r.rule_text)", right="CAST(r.txt AS JSON)"))
        self.assertEqual(self.execute(statement), rows)
        assert_join_counts(self, self.mock_a, payloads)
        for body in self.mock_a.requests:
            payload = parse_join_prompt(body["messages"][-1]["content"])
            self.assertIsInstance(payload["left"], str)
            self.assertIsInstance(payload["right"], dict)

    def test_empty_string_payloads_are_valid_pair_values(self):
        self.load_rows([JoinInput(1, ""), JoinInput(2, "left")],
                       [JoinInput(3, ""), JoinInput(4, "right")])
        all_payloads = []
        for rule in (RULE, ""):
            with self.subTest(rule=rule):
                rows, payloads = self.seed_pairs(accepts={(1, 3), (2, 3)}, rule=rule)
                statement = self.statement(on=join_call(rule="%s"))
                self.assertEqual(self.execute(statement, (rule,)), rows)
                all_payloads.extend(payloads)
        assert_join_counts(self, self.mock_a, all_payloads)

    def test_lob_payloads_and_sql_string_recipes_survive_candidate_batches(self):
        left = [JoinInput(index, f"lob-left-{index}:" + "x" * 131073) for index in range(2)]
        right = [JoinInput(index, f"right:{index}") for index in range(19)]
        self.load_rows(left, right)
        rows, payloads = self.seed_pairs(
            accepts=lambda left, right: (left.id + right.id) % 3 == 0,
            left_value=lambda left, _right: f"{left.id}|" + left.text,
            right_value=lambda _left, right: right.text.upper())
        statement = self.statement(on=join_call(
            left="CONCAT(CAST(l.id AS CHAR), '|', l.txt)", right="UPPER(r.txt)"))
        self.assertEqual(len(payloads), 38)
        self.assertEqual(self.execute(statement), rows)
        assert_join_counts(self, self.mock_a, payloads)

    def test_static_options_and_matching_native_formats_are_preserved(self):
        formats = [None, JOIN_FORMAT, {**JOIN_FORMAT, "json_schema": {
            "name": "caller_join_schema", "strict": True,
            "schema": {"additionalProperties": False, "required": ["value"],
                       "properties": {"value": {"type": "boolean"}}, "type": "object"}}}]
        all_payloads = []
        for index, response_format in enumerate(formats):
            with self.subTest(response_format=response_format):
                self.load_rows([JoinInput(1, "option-left")],
                               [JoinInput(2, "option-true"), JoinInput(3, "option-false")])
                rule = f"options-{index}"
                options = {"temperature": 0.25, "max_tokens": 32, "schema": {"type": "string"}}
                if response_format is not None:
                    options["response_format"] = response_format
                rows, payloads = self.seed_pairs(
                    accepts={(1, 2)}, rule=rule, caller_format=response_format)
                all_payloads.extend(payloads)
                statement = self.statement(on=join_call(rule="%s", options="%s"))
                self.assertEqual(self.execute(statement, (rule, json.dumps(options))), rows)
                keys = {pair_key(payload) for payload in payloads}
                for body in self.mock_a.requests:
                    if pair_key(parse_join_prompt(body["messages"][-1]["content"])) in keys:
                        self.assertEqual(body["model"], "mock-a")
                        self.assertEqual({name: body[name] for name in options}, options)
                        assert_join_request(self, body, response_format)
        assert_join_counts(self, self.mock_a, all_payloads)
        assert_join_counts(self, self.mock_b, [])

    def test_boolean_whitespace_and_absent_or_null_refusal_are_accepted(self):
        self.load_rows([JoinInput(1, "left")],
                       [JoinInput(2, "true"), JoinInput(3, "false")])
        rows, payloads = self.seed_pairs(accepts={(1, 2)})
        self.mock_a.scenarios[pair_key(payloads[0])] = [
            Reply(body=completion(' \n {"value": true}\t', finish_reason="stop"))]
        self.mock_a.scenarios[pair_key(payloads[1])] = [Reply(body=json.dumps({
            "choices": [{"finish_reason": "stop", "message": {
                "content": '{"value":false}', "refusal": None}}]}).encode("utf-8"))]
        self.assertEqual(self.execute(self.statement()), rows)
        assert_join_counts(self, self.mock_a, payloads)

    def test_not_consumes_a_boolean_join_result_not_a_truthy_string(self):
        self.load_rows([JoinInput(1, "left")],
                       [JoinInput(2, "true"), JoinInput(3, "false")])
        _rows, payloads = self.seed_pairs(accepts={(1, 2)})
        self.assertEqual(self.execute(self.statement(on="NOT " + join_call())), ((1, 3),))
        assert_join_counts(self, self.mock_a, payloads)

    def test_equi_predicate_reduces_candidates_before_http(self):
        self.load_rows([JoinInput(index, f"equi-left-{index}", bucket=index % 2) for index in range(5)],
                       [JoinInput(index + 10, f"equi-right-{index}", bucket=index % 3)
                        for index in range(6)])
        rows, payloads = self.seed_pairs(
            cpu=lambda left, right: left.bucket == right.bucket,
            accepts=lambda left, right: (left.id + right.id) % 3 != 0)
        statement = self.statement(on="l.bucket = r.bucket AND " + join_call())
        self.assert_plan_labels(statement, ("SEMANTIC JOIN",))
        self.assertEqual(self.execute(statement), rows)
        self.assertEqual(len(payloads), 10)
        assert_join_counts(self, self.mock_a, payloads)

    def test_inequality_cpu_conditions_are_applied_regardless_of_textual_order(self):
        all_payloads = []
        for index, ai_first in enumerate((False, True)):
            self.load_rows([JoinInput(value, f"ineq-left-{value}", score=value) for value in range(4)],
                           [JoinInput(value, f"ineq-right-{value}", score=value) for value in range(5)])
            rule = f"inequality-{index}"
            rows, payloads = self.seed_pairs(
                cpu=lambda left, right: left.score < right.score and left.id + right.id <= 5,
                accepts=lambda left, right: (left.id + right.id) % 2 == 0, rule=rule)
            all_payloads.extend(payloads)
            ordinary = "l.score < r.score AND l.id + r.id <= 5"
            semantic_call = join_call(rule="%s")
            on = (semantic_call + " AND " + ordinary if ai_first else
                  ordinary + " AND " + semantic_call)
            self.assertEqual(self.execute(self.statement(on=on), (rule,)), rows)
        assert_join_counts(self, self.mock_a, all_payloads)

    def test_where_side_predicates_compose_before_semantic_candidates(self):
        self.load_rows([JoinInput(index, f"where-left-{index}", score=index) for index in range(5)],
                       [JoinInput(index + 10, f"where-right-{index}", score=index)
                        for index in range(4)])
        rows, payloads = self.seed_pairs(
            cpu=lambda left, right: left.score >= 2 and right.score != 1,
            accepts=lambda left, right: (left.id + right.id) % 2 == 0)
        statement = self.statement(suffix="WHERE l.score >= 2 AND r.score <> 1 ORDER BY l.id, r.id")
        self.assertEqual(self.execute(statement), rows)
        self.assertEqual(len(payloads), 9)
        assert_join_counts(self, self.mock_a, payloads)

    def test_order_limit_offset_consume_matches_not_unfiltered_pairs(self):
        self.load_rows([JoinInput(index, f"ordered-left-{index}", score=index * 7)
                        for index in range(5)],
                       [JoinInput(index + 20, f"ordered-right-{index}", score=index * 3)
                        for index in range(14)])
        rows, payloads = self.seed_pairs(
            accepts=lambda left, right: (left.id + right.id) % 4 != 1)
        scores = {(left.id, right.id): left.score + right.score
                  for left in self.left_rows for right in self.right_rows}
        ordered = sorted(rows, key=lambda pair: (-scores[pair], -pair[0], pair[1]))
        statement = self.statement(
            fields="l.id, r.id, l.score + r.score AS total",
            suffix="ORDER BY total DESC, l.id DESC, r.id LIMIT 7 OFFSET 3")
        self.assert_plan_labels(statement, ("SEMANTIC JOIN", "SORT"))
        self.assertEqual(self.execute(statement),
                         tuple((*pair, scores[pair]) for pair in ordered[3:10]))
        self.assertEqual(len(payloads), 70)
        assert_join_counts(self, self.mock_a, payloads)

    def test_group_having_order_limit_and_offset_compose_after_join(self):
        self.load_rows([JoinInput(index, f"group-left-{index}", bucket=index % 4)
                        for index in range(8)],
                       [JoinInput(index + 20, f"group-right-{index}") for index in range(5)])
        rows, payloads = self.seed_pairs(
            accepts=lambda left, right: (left.id + right.id) % 3 != 0)
        buckets = {left.id: left.bucket for left in self.left_rows}
        counts = Counter(buckets[left_id] for left_id, _right_id in rows)
        expected = sorted(((bucket, count) for bucket, count in counts.items() if count >= 3),
                          key=lambda item: (-item[1], item[0]))[1:3]
        statement = self.statement(fields="l.bucket, COUNT(*) AS n",
                                   suffix="GROUP BY l.bucket HAVING n >= 3 "
                                          "ORDER BY n DESC, l.bucket LIMIT 2 OFFSET 1")
        self.assert_plan_labels(statement, ("SEMANTIC JOIN", "GROUP"))
        self.assertEqual(self.execute(statement), tuple(expected))
        assert_join_counts(self, self.mock_a, payloads)

    def test_limit_zero_never_issues_http(self):
        self.load_rows([JoinInput(1, "left")], [JoinInput(2, "right")])
        self.assertEqual(self.execute(self.statement(suffix="LIMIT 0")), ())
        self.assert_no_http()

    def test_reexecution_uses_fresh_pair_state_on_the_same_connection(self):
        self.load_rows([JoinInput(1, "repeat-left"), JoinInput(2, "repeat-left")],
                       [JoinInput(10, "repeat-right"), JoinInput(11, "other-right")])
        first_rows, payloads = self.seed_pairs(accepts=lambda _left, right: right.id == 10)
        statement = self.statement()
        self.assertEqual(self.execute(statement), first_rows)
        second_rows, second_payloads = self.seed_pairs(accepts=lambda _left, right: right.id == 11)
        self.assertEqual(self.execute(statement), second_rows)
        self.assertEqual(self.connection.thread_id(), self.connection_id)
        assert_join_counts(self, self.mock_a, payloads + second_payloads)

    def test_sql_prepared_reexecution_resets_candidate_and_completion_state(self):
        self.load_rows([JoinInput(1, "prepared-left")],
                       [JoinInput(2, "prepared-a"), JoinInput(3, "prepared-b")])
        first_rows, payloads = self.seed_pairs(accepts={(1, 2)})
        self.cursor.execute("PREPARE semantic_join_stmt FROM %s", (self.statement(),))
        try:
            self.assertEqual(self.execute("EXECUTE semantic_join_stmt"), first_rows)
            second_rows, second_payloads = self.seed_pairs(accepts={(1, 3)})
            self.assertEqual(self.execute("EXECUTE semantic_join_stmt"), second_rows)
            assert_join_counts(self, self.mock_a, payloads + second_payloads)
        finally:
            self.cursor.execute("DEALLOCATE PREPARE semantic_join_stmt")

    def test_nested_loop_rescans_an_unmaterialized_semantic_join(self):
        self.load_rows([JoinInput(1, "rescan-left-a"), JoinInput(2, "rescan-left-b")],
                       [JoinInput(10, "rescan-right-a"), JoinInput(11, "rescan-right-b")])
        rows, payloads = self.seed_pairs(accepts={(1, 10), (2, 11)})
        self.assertEqual(len(payloads), 4)
        self.cursor.executemany("INSERT INTO semantic_join_drivers VALUES (%s)", [(3,), (7,), (9,)])
        inner = self.statement(fields="l.id AS left_id, r.id AS right_id", suffix="")
        statement = (f"SELECT /*+ OPT_PARAM('rowsets_max_rows', {BATCH_ROWS}) "
                     "NO_MERGE(j) LEADING(d j) USE_NL(j) NO_USE_NL_MATERIALIZATION(j) */ "
                     "d.id, j.left_id, j.right_id FROM semantic_join_drivers d "
                     f"INNER JOIN ({inner}) j ON 1 = 1 ORDER BY d.id, j.left_id, j.right_id")
        plan = self.assert_plan_labels(statement, ("NESTED-LOOP JOIN", "SEMANTIC JOIN"))
        assert_unmaterialized_join_rescan(self, plan)
        self.assertEqual(self.execute(statement),
                         tuple((driver, *pair) for driver in (3, 7, 9) for pair in rows))
        assert_join_counts(self, self.mock_a, payloads * 3)
        self.assertEqual(sum(request_counts(self.mock_a).values()), 12)

    def test_second_candidate_batch_is_submitted_before_any_reply_with_slots_two(self):
        pending, gate, rows, payloads = self.start_held_join("cross-batch")
        self.wait_two_held_batches(self.mock_a, pending, [gate])
        admitted = request_counts(self.mock_a)
        self.assertTrue(set(admitted) <= {pair_key(payload) for payload in payloads})
        gate.set()
        self.assertEqual(pending.result(), rows)
        assert_join_counts(self, self.mock_a, payloads)
        assert_join_counts(self, self.mock_b, [])

    def test_later_candidate_responses_can_finish_before_held_earlier_pairs(self):
        self.load_rows([JoinInput(7, "reorder-left")],
                       [JoinInput(index, f"reorder-right-{index}") for index in range(ROW_COUNT)])
        first_gate, later_gate = self.fixtures.gate(), self.fixtures.gate()
        rows, payloads = self.seed_pairs(accepts=lambda _left, right: right.id % 3 != 1)
        first_prompts = set()
        for payload, right in zip(payloads, self.right_rows):
            gate = first_gate if right.id < BATCH_ROWS else later_gate
            self.mock_a.scenarios[pair_key(payload)][0].gate = gate
            if right.id < BATCH_ROWS:
                first_prompts.add(pair_key(payload))
        pending = self.fixtures.start(lambda: self.execute(self.statement()))
        self.wait_two_held_batches(self.mock_a, pending, [first_gate, later_gate])
        later_gate.set()
        semantic.wait_evidence(
            self, self.mock_a,
            lambda: any(record["response"] is not None and
                        pair_key(parse_join_prompt(record["prompt"])) not in first_prompts
                        for record in self.mock_a.audit),
            pending, message="later candidate responses did not complete before earlier held pairs")
        raw_first = {record["prompt"] for record in semantic.audit(self.mock_a)
                     if pair_key(parse_join_prompt(record["prompt"])) in first_prompts}
        semantic.assert_held(self, self.mock_a, [first_gate], raw_first)
        self.assertFalse(pending.done.is_set())
        first_gate.set()
        self.assertEqual(pending.result(), rows)
        assert_join_counts(self, self.mock_a, payloads)

    def test_nested_ai_map_is_ready_per_pair_and_overlaps_join_with_later_map_batch(self):
        self.load_rows([JoinInput(7, "nested-left")],
                       [JoinInput(index, f"nested-right-{index}") for index in range(ROW_COUNT)])
        self.join_mocks.remove(self.mock_b)
        self.mock_b.scenarios = {}
        first_gate, later_gate, join_gate = (self.fixtures.gate() for _ in range(3))
        for right in self.right_rows:
            self.mock_b.scenarios[right.text] = [Reply(
                body=completion(f"mapped-right-{right.id}"),
                gate=first_gate if right.id < BATCH_ROWS else later_gate)]
        rows, payloads = self.seed_pairs(
            accepts=lambda _left, right: right.id % 2 == 0,
            right_value=lambda _left, right: f"mapped-right-{right.id}", gate=join_gate)
        dependencies = {pair_key(payload): right.text
                        for payload, right in zip(payloads, self.right_rows)}
        statement = self.statement(on=join_call(right="AI_MAP('semantic_b', r.txt)"))
        self.assert_plan_labels(statement, ("SEMANTIC JOIN",))
        pending = self.fixtures.start(lambda: self.execute(statement))
        semantic.wait_evidence(
            self, self.mock_b, lambda: self.mock_b.counts[self.right_rows[BATCH_ROWS].text] == 1,
            pending, message="nested map batch 2 must arrive while batch 1 remains held")
        semantic.assert_held(self, self.mock_b, [first_gate, later_gate])
        self.assertFalse(self.mock_a.requests, "join ran before its demanded map results")
        first_gate.set()
        first_keys = {pair_key(payload) for payload in payloads[:BATCH_ROWS]}
        semantic.wait_evidence(
            self, self.mock_a, lambda: first_keys <= set(request_counts(self.mock_a)), pending,
            message="join batch 1 must overlap held map batch 2")
        semantic.assert_held(self, self.mock_a, [join_gate])
        semantic.assert_held(self, self.mock_b, [later_gate],
                             {right.text for right in self.right_rows[BATCH_ROWS:]})
        assert_join_counts(self, self.mock_a, payloads[:BATCH_ROWS])
        assert_join_dependencies(self, semantic.audit(self.mock_b), semantic.audit(self.mock_a), dependencies)
        later_gate.set()
        join_gate.set()
        self.assertEqual(pending.result(), rows)
        assert_join_counts(self, self.mock_a, payloads)
        semantic.SemanticContracts.assert_counts(self, self.mock_b,
                                                [right.text for right in self.right_rows])
        assert_join_dependencies(self, semantic.audit(self.mock_b), semantic.audit(self.mock_a), dependencies)

    def test_and_join_services_prune_later_calls_and_overlap_batches(self):
        self.load_rows([JoinInput(7, "and-left")],
                       [JoinInput(index, f"and-right-{index}") for index in range(ROW_COUNT)])
        first_gate, later_gate, gate_b = (self.fixtures.gate() for _ in range(3))
        _rows_a, payloads_a = self.seed_pairs(
            accepts=lambda _left, right: right.id % 3 != 1, rule="first AND rule")
        for payload, right in zip(payloads_a, self.right_rows):
            self.mock_a.scenarios[pair_key(payload)][0].gate = (
                first_gate if right.id < BATCH_ROWS else later_gate)
        rows_b, payloads_b = self.seed_pairs(
            cpu=lambda _left, right: right.id % 3 != 1,
            accepts=lambda _left, right: right.id % 2 == 0,
            rule="second AND rule", mock=self.mock_b, gate=gate_b)
        first_inputs = {right.text for right in self.right_rows[:BATCH_ROWS]}
        first_b = [payload for payload in payloads_b if payload["right"] in first_inputs]
        dependencies = {
            pair_key(payload): pair_key(pair_payload(
                "first AND rule", payload["left"], payload["right"]))
            for payload in payloads_b}
        self.assertEqual(len(payloads_a), 35)
        self.assertEqual(len(payloads_b), 23)
        self.assertEqual(len(first_b), 11)
        self.assertEqual(len(rows_b), 12)
        on = (join_call(rule="'first AND rule'") + " AND " +
              join_call(model="'semantic_b'", rule="'second AND rule'"))
        statement = self.statement(on=on)
        self.assert_plan_labels(statement, ("SEMANTIC JOIN",))
        pending = self.fixtures.start(lambda: self.execute(statement))
        self.wait_two_held_batches(self.mock_a, pending, [first_gate, later_gate])
        assert_join_counts(self, self.mock_b, [])
        first_gate.set()
        first_b_keys = {pair_key(payload) for payload in first_b}
        semantic.wait_evidence(
            self, self.mock_b, lambda: first_b_keys <= set(request_counts(self.mock_b)),
            pending, message="B(batch 1) must overlap A(batch 2) without evaluating rejected pairs")
        later_a_keys = {pair_key(payload) for payload in payloads_a[BATCH_ROWS:]}
        raw_later_a = {record["prompt"] for record in semantic.audit(self.mock_a)
                       if pair_key(parse_join_prompt(record["prompt"])) in later_a_keys}
        semantic.assert_held(self, self.mock_a, [later_gate], raw_later_a)
        semantic.assert_held(self, self.mock_b, [gate_b])
        assert_join_counts(self, self.mock_b, first_b)
        assert_join_chain_dependencies(
            self, semantic.audit(self.mock_a), semantic.audit(self.mock_b), dependencies)
        self.assertFalse(pending.done.is_set())
        later_gate.set()
        gate_b.set()
        self.assertEqual(pending.result(), rows_b)
        assert_join_counts(self, self.mock_a, payloads_a)
        assert_join_counts(self, self.mock_b, payloads_b)
        assert_join_chain_dependencies(
            self, semantic.audit(self.mock_a), semantic.audit(self.mock_b), dependencies)

    def test_cancel_held_join_closes_sockets_and_recovers_on_the_same_connection(self):
        pending, gate, _rows, payloads = self.start_held_join("cancel")
        self.wait_two_held_batches(self.mock_a, pending, [gate])
        old_payloads = [parse_join_prompt(record["prompt"]) for record in semantic.audit(self.mock_a)]
        self.assertTrue({pair_key(payload) for payload in old_payloads} <=
                        {pair_key(payload) for payload in payloads})
        self.kill_query()
        pending.expect_sql_error(self, 1317, timeout=3)
        semantic.assert_held(self, self.mock_a, [gate])
        assert_join_counts(self, self.mock_a, old_payloads)
        self.recover_while_old_responses_are_held([gate], old_payloads)

    def test_deadline_held_join_closes_sockets_and_recovers_on_the_same_connection(self):
        self.cursor.execute("SET ob_query_timeout = 3000000")
        pending, gate, _rows, payloads = self.start_held_join("deadline")
        self.wait_two_held_batches(self.mock_a, pending, [gate])
        old_payloads = [parse_join_prompt(record["prompt"]) for record in semantic.audit(self.mock_a)]
        self.assertTrue({pair_key(payload) for payload in old_payloads} <=
                        {pair_key(payload) for payload in payloads})
        pending.expect_sql_error(self, 4012, timeout=5)
        semantic.assert_held(self, self.mock_a, [gate])
        assert_join_counts(self, self.mock_a, old_payloads)
        self.recover_while_old_responses_are_held([gate], old_payloads)

    def exercise_fail_fast(self, *, status, body, code):
        self.load_rows([JoinInput(7, "fail-fast-left")],
                       [JoinInput(index, f"fail-fast-right-{index}") for index in range(ROW_COUNT)])
        _rows, payloads = self.seed_pairs(accepts=lambda _left, _right: True)
        gates = {pair_key(payload): self.fixtures.gate() for payload in payloads}
        for key, gate in gates.items():
            self.mock_a.scenarios[key][0].gate = gate
        pending = self.fixtures.start(lambda: self.execute(self.statement()))
        self.wait_two_held_batches(self.mock_a, pending, list(gates.values()))
        records = semantic.audit(self.mock_a)
        old_payloads = [parse_join_prompt(record["prompt"]) for record in records]
        bad_prompt = records[BATCH_ROWS]["prompt"]
        bad_key = pair_key(parse_join_prompt(bad_prompt))
        # Select an admitted request, so the test does not assume candidate scan order.
        bad_reply = self.mock_a.scenarios[bad_key][0]
        bad_reply.status, bad_reply.body = status, body
        gates[bad_key].set()
        pending.expect_sql_error(self, code, timeout=3)
        held_gates = [gate for key, gate in gates.items() if key != bad_key]
        semantic.assert_held(self, self.mock_a, held_gates,
                             {record["prompt"] for record in records if record["prompt"] != bad_prompt})
        assert_join_counts(self, self.mock_a, old_payloads)
        self.recover_while_old_responses_are_held(held_gates, old_payloads)

    def test_provider_error_fails_fast_without_releasing_peers_and_recovers(self):
        self.exercise_fail_fast(status=400, body=b'{"error":"permanent provider failure"}', code=4216)

    def test_invalid_boolean_fails_fast_without_releasing_peers_and_recovers(self):
        self.exercise_fail_fast(
            status=200, body=completion('{"value":"true"}', finish_reason="stop"), code=4070)

    def test_transient_retry_does_not_resend_successful_pairs_or_change_request(self):
        self.load_rows([JoinInput(1, "retry-left")],
                       [JoinInput(2, "retry-right"), JoinInput(3, "successful-right")])
        rows, payloads = self.seed_pairs(accepts={(1, 2)})
        key = pair_key(payloads[0])
        self.mock_a.scenarios[key].insert(
            0, Reply(429, b'{"error":"retry pair"}', {"Retry-After": "0"}))
        self.assertEqual(self.execute(self.statement()), rows)
        assert_join_counts(self, self.mock_a, payloads + [payloads[0]])
        retries = [body for body in self.mock_a.requests
                   if pair_key(parse_join_prompt(body["messages"][-1]["content"])) == key]
        self.assertEqual(len(retries), 2)
        self.assertEqual(retries[0], retries[1], "retry changed pair roles, options or native schema")

    def test_bad_arity_is_1582_inside_inner_join_on_before_http(self):
        self.load_rows([JoinInput(1, "left")], [JoinInput(2, "right")])
        arguments = ["'semantic_a'", RULE_SQL, "l.txt", "r.txt", "'{}'",
                     "'extra-six'", "'extra-seven'", "'extra-eight'"]
        for arity in (0, 1, 2, 3, 6, 7, 8):
            call = "AI_JOIN(" + ", ".join(arguments[:arity]) + ")"
            statement = self.statement(on=call)
            for prefix in ("EXPLAIN ", ""):
                with self.subTest(arity=arity, explain=bool(prefix)):
                    sql_error(self, 1582, lambda: self.execute(prefix + statement))
                    self.assert_no_http()
        self.assertEqual(self.execute("SELECT 42"), ((42,),))

    def test_demanded_sql_null_rule_or_payload_is_1210_before_http(self):
        cases = [
            (JoinInput(1, None), JoinInput(2, "right"), RULE_SQL, "l.txt", "r.txt"),
            (JoinInput(1, "left"), JoinInput(2, None), RULE_SQL, "l.txt", "r.txt"),
            (JoinInput(1, "left", rule=None), JoinInput(2, "right"), "l.rule_text", "l.txt", "r.txt"),
            (JoinInput(1, "left"), JoinInput(2, "right"), "NULL", "l.txt", "r.txt"),
            (JoinInput(1, "left"), JoinInput(2, "right"), RULE_SQL,
             "CASE WHEN l.id = 1 THEN NULL ELSE l.txt END", "r.txt"),
        ]
        for left, right, rule, left_expr, right_expr in cases:
            with self.subTest(rule=rule, left=left_expr, right=right_expr):
                self.load_rows([left], [right])
                statement = self.statement(on=join_call(rule=rule, left=left_expr, right=right_expr))
                sql_error(self, 1210, lambda: self.execute(statement))
                self.assert_no_http()
        with self.subTest(model="NULL"):
            sql_error(self, 1210, lambda: self.execute(self.statement(on=join_call(model="NULL"))))
            self.assert_no_http()
        self.assertEqual(self.execute("SELECT 42"), ((42,),))

    def test_ordinary_conditions_do_not_demand_pruned_null_payloads_or_rules(self):
        self.load_rows([JoinInput(1, None, rule=None), JoinInput(2, "valid-left", rule="valid rule")],
                       [JoinInput(3, None), JoinInput(4, "valid-right")])
        rows, payloads = self.seed_pairs(
            cpu=lambda left, right: left.id == 2 and right.id == 4,
            accepts=lambda _left, _right: True, rule=lambda left, _right: left.rule)
        statement = self.statement(on="l.id = 2 AND r.id = 4 AND " + join_call(rule="l.rule_text"))
        self.assertEqual(self.execute(statement), rows)
        assert_join_counts(self, self.mock_a, payloads)

    def test_raw_numeric_date_and_nonstring_rule_types_are_5083(self):
        self.load_rows([JoinInput(1, "2026-01-01")], [JoinInput(2, "2026-01-02")])
        calls = [
            join_call(left="l.id"), join_call(right="r.score"),
            join_call(left="CAST(l.txt AS DATE)"), join_call(right="CAST(r.txt AS DATE)"),
            join_call(rule="l.score"), join_call(rule="CAST(l.txt AS DATE)"),
            join_call(left="l.score > 0"),
        ]
        for call in calls:
            with self.subTest(call=call):
                sql_error(self, 5083, lambda: self.execute("EXPLAIN " + self.statement(on=call)))
                self.assert_no_http()
        self.assertEqual(self.execute("SELECT 42"), ((42,),))

    def test_options_must_be_objects_and_schema_is_reserved_before_http(self):
        self.load_rows([JoinInput(1, "left")], [JoinInput(2, "right")])
        configurations = [([], 1210), (None, 1210), (True, 1210), (1, 1210), ("scalar", 1210),
                          ({"Schema": None}, 1210), ({"Schema": JOIN_SCHEMA}, 1210),
                          ({"Schema": {}}, 1210), ({"Schema": "reserved"}, 1210)]
        for configuration, code in configurations:
            for options in ("%s", "CAST(%s AS JSON)"):
                with self.subTest(configuration=configuration, options=options):
                    statement = self.statement(on=join_call(options=options))
                    sql_error(self, code, lambda: self.execute(statement, (json.dumps(configuration),)))
                    self.assert_no_http()
        sql_error(self, 1210, lambda: self.execute(self.statement(on=join_call(options="NULL"))))
        sql_error(self, (3140, 3141, 5447), lambda: self.execute(
            self.statement(on=join_call(options="%s")), ("not-json",)))
        self.assert_no_http()

    def test_incompatible_native_response_formats_are_1210_before_http(self):
        self.load_rows([JoinInput(1, "left")], [JoinInput(2, "right")])
        native = JOIN_FORMAT["json_schema"]
        invalid_native = [
            {"strict": True, "schema": JOIN_SCHEMA},
            {"name": "conflict", "schema": JOIN_SCHEMA},
            {**native, "name": ""}, {**native, "name": None}, {**native, "name": 7},
            {**native, "strict": False}, {**native, "strict": 1},
            {**native, "schema": {"type": "boolean"}},
            {**native, "schema": {**JOIN_SCHEMA, "additionalProperties": True}},
            {**native, "schema": {**JOIN_SCHEMA, "additionalProperties": 0}},
            {**native, "schema": {**JOIN_SCHEMA, "required": []}},
            {**native, "schema": {**JOIN_SCHEMA, "properties": {"value": {"type": "string"}}}},
        ]
        formats = [None, [], {"type": "text"}, {"type": "json_object"}, {"type": "json_schema"}]
        formats.extend({"type": "json_schema", "json_schema": value} for value in invalid_native)
        for response_format in formats:
            with self.subTest(response_format=response_format):
                options = json.dumps({"response_format": response_format})
                sql_error(self, 1210, lambda: self.execute(
                    self.statement(on=join_call(options="%s")), (options,)))
                self.assert_no_http()

    def test_dynamic_model_or_options_never_use_a_synchronous_fallback(self):
        self.load_rows([JoinInput(1, "left")], [JoinInput(2, "right")])
        for call in (join_call(model="l.model"), join_call(options="l.opts"),
                     join_call(model="r.model"), join_call(options="r.opts")):
            for prefix in ("EXPLAIN ", ""):
                with self.subTest(call=call, explain=bool(prefix)):
                    sql_error(self, 1235, lambda: self.execute(prefix + self.statement(on=call)))
                    self.assert_no_http()

    def test_non_join_clause_placements_are_explicit_1235_before_http(self):
        self.load_rows([JoinInput(1, "left")], [JoinInput(2, "right")])
        call = join_call()
        queries = [
            self.statement(fields=call, on="1 = 1"),
            self.statement(on="1 = 1", suffix="WHERE " + call),
            self.statement(on="1 = 1", suffix="ORDER BY " + call),
            self.statement(fields="COUNT(*)", on="1 = 1", suffix="GROUP BY " + call),
            self.statement(fields="l.id, l.txt, r.id, r.txt", on="1 = 1",
                           suffix="GROUP BY l.id, l.txt, r.id, r.txt HAVING " + call),
        ]
        for query in queries:
            for prefix in ("EXPLAIN ", ""):
                with self.subTest(query=prefix + query):
                    sql_error(self, 1235, lambda: self.execute(prefix + query))
                    self.assert_no_http()

    def test_outer_and_exists_semi_contexts_are_explicit_1235_before_http(self):
        self.load_rows([JoinInput(1, "left")], [JoinInput(2, "right")])
        call = join_call()
        queries = [
            self.statement(from_sql="semantic_join_left l LEFT OUTER JOIN semantic_join_right r"),
            self.statement(from_sql="semantic_join_left l RIGHT OUTER JOIN semantic_join_right r"),
            "SELECT l.id FROM semantic_join_left l WHERE EXISTS "
            f"(SELECT 1 FROM semantic_join_right r WHERE {call})",
            "SELECT l.id FROM semantic_join_left l WHERE NOT EXISTS "
            f"(SELECT 1 FROM semantic_join_right r WHERE {call})",
            "SELECT l.id FROM semantic_join_left l WHERE l.id IN "
            f"(SELECT r.id FROM semantic_join_right r WHERE {call})",
        ]
        for query in queries:
            with self.subTest(query=query):
                sql_error(self, 1235, lambda: self.execute(query))
                self.assert_no_http()

    def test_constant_same_side_and_cross_side_operands_are_explicit_1235(self):
        self.load_rows([JoinInput(1, "left")], [JoinInput(2, "right")])
        operands = [
            ("'constant'", "r.txt"), ("l.txt", "'constant'"), ("'left'", "'right'"),
            ("NULL", "r.txt"), ("l.txt", "NULL"),
            ("CAST('null' AS JSON)", "r.txt"), ("l.txt", "CAST('null' AS JSON)"),
            ("''", "r.txt"), ("l.txt", "''"),
            ("l.txt", "l.rule_text"), ("r.txt", "r.rule_text"),
            ("CONCAT(l.txt, r.txt)", "r.txt"), ("l.txt", "CONCAT(l.txt, r.txt)"),
            ("JSON_OBJECT('left', l.txt, 'right', r.txt)", "r.txt"),
            ("(SELECT l.txt)", "r.txt"), ("l.txt", "(SELECT r.txt)"),
        ]
        for left, right in operands:
            with self.subTest(left=left, right=right):
                statement = self.statement(on=join_call(left=left, right=right))
                sql_error(self, 1235, lambda: self.execute(statement))
                self.assert_no_http()

    def test_provider_envelope_is_strict_and_invalid_envelopes_are_not_retried(self):
        content = '{"value":true}'
        choice = {"finish_reason": "stop", "message": {"content": content}}
        envelopes = [
            {"choices": []}, {"choices": [choice, choice]},
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
        all_payloads = []
        for index, envelope in enumerate(envelopes):
            with self.subTest(envelope=envelope):
                self.load_rows([JoinInput(1, "left")], [JoinInput(2, "right")])
                rule = f"invalid-envelope-{index}"
                _rows, payloads = self.seed_pairs(accepts={(1, 2)}, rule=rule)
                all_payloads.extend(payloads)
                self.mock_a.scenarios[pair_key(payloads[0])] = [
                    Reply(body=json.dumps(envelope).encode("utf-8"))]
                sql_error(self, 4070, lambda: self.execute(
                    self.statement(on=join_call(rule="%s")), (rule,)))
                assert_join_counts(self, self.mock_a, all_payloads)
                assert_join_counts(self, self.mock_b, [])

    def test_nonboolean_missing_extra_duplicate_and_malformed_content_is_rejected(self):
        invalid_data = [
            "true", "false", '"true"', '"false"', "0", "1", "null", "[]",
            '[{"value":true}]', "{}", '{"value":1}', '{"value":0}',
            '{"value":"true"}', '{"value":"false"}', '{"value":null}',
            '{"Value":true}', '{"value":true,"extra":0}',
            '{"value":true,"value":false}', '{"value":true,"value":true}',
        ]
        invalid_syntax = [
            '{"value":', '{"value":true,}', '{"value":true} trailing',
            '```json\n{"value":true}\n```', "",
        ]
        cases = [(text, 4070) for text in invalid_data]
        cases.extend((text, (4070, 3140, 3141, 5447)) for text in invalid_syntax)
        all_payloads = []
        for index, (text, codes) in enumerate(cases):
            with self.subTest(content=text):
                self.load_rows([JoinInput(1, "left")], [JoinInput(2, "right")])
                rule = f"invalid-boolean-{index}"
                _rows, payloads = self.seed_pairs(accepts={(1, 2)}, rule=rule)
                all_payloads.extend(payloads)
                self.mock_a.scenarios[pair_key(payloads[0])] = [
                    Reply(body=completion(text, finish_reason="stop"))]
                sql_error(self, codes, lambda: self.execute(
                    self.statement(on=join_call(rule="%s")), (rule,)))
                assert_join_counts(self, self.mock_a, all_payloads)
        self.load_rows([JoinInput(1, "valid-left")], [JoinInput(2, "valid-right")])
        rows, payloads = self.seed_pairs(accepts=set())
        self.assertEqual(self.execute(self.statement()), rows)
        assert_join_counts(self, self.mock_a, all_payloads + payloads)
        assert_join_counts(self, self.mock_b, [])


def case_names(test_class):
    return unittest.defaultTestLoader.getTestCaseNames(test_class)


def parse_options(arguments=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, help="Built seekdb binary for a new private database")
    parser.add_argument("--case", action="append", default=[], help="Test method name; repeat to select")
    parser.add_argument("--self-test", action="store_true", help="Fixtures only; never start SQL")
    parser.add_argument("--list-cases", action="store_true", help="List selected-suite cases without running")
    options = parser.parse_args(arguments)
    test_class = JoinHarnessTests if options.self_test else SemanticJoinContracts
    unknown = [name for name in options.case if name not in case_names(test_class)]
    if unknown:
        parser.error(f"unknown {test_class.__name__} case(s): {', '.join(unknown)}")
    if options.self_test and options.binary is not None:
        parser.error("--self-test cannot be combined with --binary")
    if not options.self_test and not options.list_cases and options.binary is None:
        parser.error("--binary is required unless --self-test or --list-cases is selected")
    return options


def run_contracts(connection, server, sql_socket, selected):
    SemanticJoinContracts.connection = connection
    SemanticJoinContracts.server = server
    SemanticJoinContracts.sql_socket = sql_socket
    semantic.run_suite(SemanticJoinContracts, selected)


def main(arguments=None):
    options = parse_options(arguments)
    test_class = JoinHarnessTests if options.self_test else SemanticJoinContracts
    if options.list_cases:
        for name in options.case or case_names(test_class):
            print(name)
    elif options.self_test:
        semantic.run_suite(JoinHarnessTests, options.case)
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
