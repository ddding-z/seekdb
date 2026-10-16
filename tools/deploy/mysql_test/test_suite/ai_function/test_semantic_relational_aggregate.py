"""Bounded AUTO contracts for ordinary hash aggregation and hash DISTINCT.

Every successful query checks SQL values/multiplicities and exact loopback
model demand, with query-local caching both enabled and disabled. Held replies
exercise parent progress, collection cancellation and recovery; final aggregate
rows are not streaming results. Retained derived inputs have no inner ORDER BY
or LIMIT. Spill, bypass, PX and other aggregate algorithms are not success
claims; native capability tests must cover their runtime guards.

--list-cases starts no services. --binary owns and cleans a private deployment;
--socket closes only the suite's connection and uniquely named SQL fixtures.
No build or real provider is used.
"""

import argparse
from collections import Counter
from decimal import Decimal
import os
from pathlib import Path
import unittest

from pymysql.constants import FIELD_TYPE

import test_semantic_cache as cache
import test_semantic_rank as rank
import test_semantic_runtime as semantic
from test_file_runtime import PrivateDatabase
from test_runtime_contracts import Reply, completion, sql_error


HASH_HINTS = ("PARALLEL(1) USE_HASH_AGGREGATION USE_HASH_DISTINCT "
              "OPT_PARAM('enable_topn_runtime_filter','false')")


class SemanticRelationalAggregateContracts(rank.SemanticRankContracts):
    def statement(self, fields, *, ordering="auto", enabled="true", caching="false",
                  suffix="", hints=HASH_HINTS, **kwargs):
        if enabled is not None:
            hints += f" OPT_PARAM('enable_semantic_row_async','{enabled}')"
        if ordering is not None:
            hints += f" OPT_PARAM('semantic_output_order','{ordering}')"
        return cache.sql_statement(fields, hints=hints, cache=caching, suffix=suffix, **kwargs)

    def assert_auto(self, node, mode="UNORDERED_READY", reason=None):
        annotation = node.annotation.upper()
        self.assertIn("ROW_ASYNC=" + mode, annotation)
        self.assertIn("OUTPUT_POLICY=AUTO", annotation)
        if reason is not None:
            self.assertIn("ORDER_REASON=" + reason, annotation)

    def demanded(self, prompts, caching):
        return prompts if caching == "false" else dict.fromkeys(prompts)

    def test_sembench_q10_avg_rank_direct_and_retained_derived_preserve_duplicate_bags(self):
        prompts = ["review-1", "review-5", "review-3", "review-2", "review-4", "review-6"] * 6
        flags = [0, 0, 0, 1, 1, 1] * 6
        self.load_inputs(prompts, flags=flags)
        keys = {prompt: self.rank_reply(prompt, int(prompt.rsplit("-", 1)[1]))
                for prompt in dict.fromkeys(prompts)}
        for caching in ("false", "true"):
            direct = self.statement(
                "a.guard_flag AS movieId,AVG(" + self.rank() + ") AS movieScore,COUNT(*)",
                caching=caching, suffix="GROUP BY a.guard_flag ORDER BY movieId")
            source = ("(SELECT guard_flag AS movieId," + self.rank("prompt") +
                      " AS reviewScore FROM cache_inputs) a")
            derived = self.statement(
                "a.movieId,AVG(a.reviewScore) AS movieScore,COUNT(*)", source=source,
                caching=caching, hints=HASH_HINTS + " NO_QUERY_TRANSFORMATION NO_MERGE(a)",
                suffix="GROUP BY a.movieId ORDER BY a.movieId")
            for statement, retained in ((direct, False), (derived, True)):
                with self.subTest(caching=caching, retained=retained):
                    plan = self.explain(statement)
                    aggregate = plan.one("HASH GROUP BY")
                    scorer = plan.one("SEMANTIC RANK")
                    self.assert_auto(scorer, reason="HASH_AGGREGATE_INPUT")
                    self.assertTrue(plan.is_above(aggregate, scorer), plan.text)
                    if retained:
                        scan = plan.one("SUBPLAN SCAN")
                        self.assertTrue(plan.is_above(aggregate, scan), plan.text)
                        self.assertTrue(plan.is_above(scan, scorer), plan.text)
                    self.check_query(
                        statement, ((0, 3.0, 18), (1, 4.0, 18)),
                        first=[keys[prompt] for prompt in self.demanded(prompts, caching)])
                    self.assertEqual(self.cursor.description[1][1], FIELD_TYPE.DOUBLE)

    def test_guarded_numeric_count_sum_avg_min_max_share_only_demanded_scores(self):
        prompts = [None, "", "one", "five", "one", "five"] * 6
        flags = [0, 0, 1, 1, 1, 1] * 6
        self.load_inputs(prompts, flags=flags)
        keys = {prompt: self.rank_reply(prompt, score) for prompt, score in (("one", 1), ("five", 5))}
        score = "CASE WHEN a.guard_flag=1 THEN " + self.rank() + " ELSE NULL END"
        fields = ("a.guard_flag,COUNT(*),COUNT(" + score + "),SUM(" + score + "),AVG(" +
                  score + "),MIN(" + score + "),MAX(" + score + ")")
        shared_fields = ("a.guard_flag,COUNT(*),COUNT(a.score),SUM(a.score),AVG(a.score),"
                         "MIN(a.score),MAX(a.score)")
        shared_source = "(SELECT a.guard_flag," + score + " AS score FROM cache_inputs a) a"
        demanded = [prompt for prompt, flag in zip(prompts, flags) if flag]
        for caching in ("false", "true"):
            variants = (
                (fields, "cache_inputs a", HASH_HINTS, 5, False),
                (shared_fields, shared_source, HASH_HINTS + " NO_QUERY_TRANSFORMATION NO_MERGE(a)",
                 1, True),
            )
            for selected_fields, source, hints, occurrences, retained in variants:
                with self.subTest(caching=caching, retained=retained):
                    statement = self.statement(
                        selected_fields, source=source, hints=hints, caching=caching,
                        suffix="GROUP BY a.guard_flag ORDER BY a.guard_flag")
                    plan = self.explain(statement)
                    plan.one("HASH GROUP BY")
                    self.assert_auto(plan.one("SEMANTIC RANK"), reason="HASH_AGGREGATE_INPUT")
                    if retained:
                        plan.one("SUBPLAN SCAN")
                    repetitions = occurrences if caching == "false" else 1
                    self.check_query(
                        statement, ((0, 12, 0, None, None, None, None),
                                    (1, 24, 24, 72.0, 3.0, 1.0, 5.0)),
                        first=[keys[prompt] for prompt in self.demanded(demanded, caching)] * repetitions)
                    for index in (3, 4, 5, 6):
                        self.assertEqual(self.cursor.description[index][1], FIELD_TYPE.DOUBLE)

    def test_guarded_semantic_group_keys_preserve_null_groups_and_input_multiplicity(self):
        prompts = [None, "", "key-a", "key-b", "key-a", "key-c"] * 6
        flags = [0, 0, 1, 1, 1, 1] * 6
        values = {"key-a": "A", "key-b": "a", "key-c": "B"}
        self.load_inputs(prompts, flags=flags)
        keys = {prompt: self.fixtures.reply(0, prompt, value) for prompt, value in values.items()}
        category = ("LOWER(CASE WHEN a.guard_flag=1 THEN " + self.call("map") +
                    " ELSE NULL END)")
        groups = {}
        for index, (prompt, flag) in enumerate(zip(prompts, flags)):
            value = values[prompt].lower() if flag else None
            groups.setdefault(value, []).append(index)
        expected = [(value, len(indices), Decimal(sum(indices)))
                    for value, indices in groups.items()]
        expected.sort(key=lambda row: (row[0] is not None, row[0] or ""))
        demanded = [prompt for prompt, flag in zip(prompts, flags) if flag]
        for caching in ("false", "true"):
            with self.subTest(caching=caching):
                statement = self.statement(
                    category + " AS category,COUNT(*),SUM(a.id)", caching=caching,
                    suffix="GROUP BY category ORDER BY category")
                plan = self.explain(statement)
                self.assert_auto(plan.one("SEMANTIC MAP"), reason="HASH_AGGREGATE_INPUT")
                plan.one("HASH GROUP BY")
                self.check_query(statement, expected,
                                 first=[keys[prompt] for prompt in self.demanded(demanded, caching)])

    def test_exact_numeric_having_and_topn_score_every_aggregate_input(self):
        prompts = ["two", "two", "five", "one", "negative", "four", "one", "one"] * 5
        flags = [0, 0, 1, 1, 2, 2, 3, 3] * 5
        scores = {"two": 2, "five": 5, "one": 1, "negative": -2, "four": 4}
        self.load_inputs(prompts, flags=flags)
        keys = {prompt: self.rank_reply(prompt, score) for prompt, score in scores.items()}
        for caching in ("false", "true"):
            with self.subTest(caching=caching):
                statement = self.statement(
                    "a.guard_flag,SUM(" + self.rank() + ") AS total,COUNT(*)", caching=caching,
                    suffix="GROUP BY a.guard_flag HAVING total>=10 "
                           "ORDER BY total DESC,a.guard_flag LIMIT 2 OFFSET 1")
                plan = self.explain(statement)
                self.assert_auto(plan.one("SEMANTIC RANK"), reason="HASH_AGGREGATE_INPUT")
                self.assertTrue(plan.is_above(plan.one("TOP-N SORT"), plan.one("HASH GROUP BY")),
                                plan.text)
                self.check_query(
                    statement, ((0, 20.0, 10), (2, 10.0, 10)),
                    first=[keys[prompt] for prompt in self.demanded(prompts, caching)])

    def test_groupby_then_guarded_semantic_having_sort_limit_and_late_projection(self):
        flags = [group for group in range(6) for _ in range(group + 1)]
        self.load_inputs(["not-a-model-input"] * len(flags), flags=flags)
        having = self.call("filter", "CONCAT('having:',a.guard_flag,':',COUNT(*))", provider=1)
        mapped = self.call("map", "CONCAT('project:',a.guard_flag)")
        filter_keys = [
            self.fixtures.reply(1, f"having:{group}:{group + 1}", group != 4, kind="filter")
            for group in range(2, 6)
        ]
        map_keys = [self.fixtures.reply(0, f"project:{group}", f"value:{group}") for group in (3, 2)]
        for caching in ("false", "true"):
            with self.subTest(caching=caching):
                statement = self.statement(
                    "a.guard_flag,COUNT(*) AS n," + mapped, caching=caching,
                    suffix="GROUP BY a.guard_flag HAVING COUNT(*)>=3 AND " + having +
                           " ORDER BY a.guard_flag DESC LIMIT 2 OFFSET 1")
                plan = self.explain(statement)
                aggregate = plan.one("HASH GROUP BY")
                predicate = plan.one("SEMANTIC FILTER")
                sorter = plan.one("TOP-N SORT")
                projection = plan.one("SEMANTIC MAP")
                self.assertTrue(plan.is_above(predicate, aggregate), plan.text)
                self.assertTrue(plan.is_above(sorter, predicate), plan.text)
                self.assertTrue(plan.is_above(projection, sorter), plan.text)
                self.assert_auto(projection, "ORDERED_PREFIX", "FINAL_SQL_ORDER")
                self.check_query(statement, ((3, 4, "value:3"), (2, 3, "value:2")),
                                 first=map_keys, second=filter_keys)

    def test_fast_filtered_score_reaches_hash_groupby_input_but_not_final_output(self):
        prompts = ["slow-head", "ready-peer"]
        self.load_inputs(prompts, flags=[0, 0])
        for caching in ("false", "true"):
            with self.subTest(caching=caching):
                gate = self.fixtures.gate()
                filter_keys = [
                    self.fixtures.reply(0, prompt, True, kind="filter",
                                        gate=gate if index == 0 else None)
                    for index, prompt in enumerate(prompts)
                ]
                rank_keys = [self.rank_reply(prompt, score, provider=1)
                             for prompt, score in zip(prompts, (1, 5))]
                statement = self.statement(
                    "a.guard_flag,AVG(" + self.rank(provider=1) + "),COUNT(*)", caching=caching,
                    where=self.call("filter"), suffix="GROUP BY a.guard_flag ORDER BY a.guard_flag")
                plan = self.explain(statement)
                predicate = plan.one("SEMANTIC FILTER")
                scorer = plan.one("SEMANTIC RANK")
                self.assert_auto(predicate, reason="HASH_AGGREGATE_INPUT")
                self.assert_auto(scorer, reason="HASH_AGGREGATE_INPUT")
                self.assertTrue(plan.is_above(plan.one("HASH GROUP BY"), scorer), plan.text)
                self.assertTrue(plan.is_above(scorer, predicate), plan.text)
                before = self.snapshots()
                offset = len(self.second.requests)
                pending, rows, first = self.stream(statement)
                semantic.wait_evidence(
                    self, self.second,
                    lambda: any(record["prompt"] == "ready-peer" and record["response"] is not None
                                for record in semantic.audit(self.second)[offset:]),
                    pending, message="a ready filtered row never reached the scorer below aggregation")
                semantic.wait_evidence(
                    self, self.first,
                    lambda: any(record["prompt"] == "slow-head" and record["response"] is None
                                for record in semantic.audit(self.first)[len(before[0].requests):]),
                    pending, message="the upstream filter head was not held during downstream progress")
                self.assert_windows(before, first=filter_keys, second=[rank_keys[1]], settled=False)
                self.assertFalse(first.wait(0.15), "a partial group was exposed as a final aggregate")
                self.assertFalse(pending.done.is_set(), repr(pending.error))
                self.assertEqual(rows, [])
                self.assertFalse(gate.is_set())
                gate.set()
                self.assertEqual(pending.result(), ((0, 3.0, 2),))
                self.assert_windows(before, first=filter_keys, second=rank_keys)

    def test_hash_distinct_numeric_values_duplicates_nulls_and_final_order(self):
        prompts = ["two", "ten", "two", None, "negative", "ten", ""] * 5
        flags = [1, 1, 1, 0, 1, 1, 0] * 5
        self.load_inputs(prompts, flags=flags)
        keys = {prompt: self.rank_reply(prompt, value)
                for prompt, value in (("two", 2), ("ten", 10), ("negative", -1.5))}
        score = "CASE WHEN a.guard_flag=1 THEN " + self.rank() + " ELSE NULL END"
        demanded = [prompt for prompt, flag in zip(prompts, flags) if flag]
        for caching in ("false", "true"):
            with self.subTest(caching=caching):
                statement = self.statement("DISTINCT " + score + " AS score", caching=caching,
                                           suffix="ORDER BY score DESC")
                plan = self.explain(statement)
                self.assertTrue(plan.is_above(plan.one("HASH DISTINCT"), plan.one("SEMANTIC RANK")),
                                plan.text)
                self.assert_auto(plan.one("SEMANTIC RANK"), reason="HASH_DISTINCT_INPUT")
                self.check_query(statement, ((10.0,), (2.0,), (-1.5,), (None,)),
                                 first=[keys[prompt] for prompt in self.demanded(demanded, caching)])
                self.assertEqual(self.cursor.description[0][1], FIELD_TYPE.DOUBLE)

    def test_retained_hash_distinct_uses_sql_collation_not_response_byte_equality(self):
        prompts = ["mixed", "upper", "lower", "other", None, ""] * 6
        flags = [1, 1, 1, 1, 0, 0] * 6
        values = {"mixed": "Alpha", "upper": "ALPHA", "lower": "alpha", "other": "Beta"}
        self.load_inputs(prompts, flags=flags)
        keys = {prompt: self.fixtures.reply(0, prompt, value) for prompt, value in values.items()}
        mapped = "CASE WHEN guard_flag=1 THEN " + self.call("map", "prompt") + " ELSE NULL END"
        demanded = [prompt for prompt, flag in zip(prompts, flags) if flag]
        for caching in ("false", "true"):
            for collation, count in (("utf8mb4_general_ci", 1), ("utf8mb4_bin", 3)):
                with self.subTest(caching=caching, collation=collation):
                    source = ("(SELECT /*+ USE_HASH_DISTINCT NO_QUERY_TRANSFORMATION */ DISTINCT "
                              "CAST(" + mapped + " AS CHAR CHARACTER SET utf8mb4) COLLATE " +
                              collation + " AS category FROM cache_inputs) a")
                    statement = self.statement(
                        "LOWER(a.category) AS category", source=source, caching=caching,
                        hints=HASH_HINTS + " NO_QUERY_TRANSFORMATION NO_MERGE(a)",
                        suffix="ORDER BY category")
                    plan = self.explain(statement)
                    distinct = plan.one("HASH DISTINCT")
                    self.assertTrue(plan.is_above(plan.one("SUBPLAN SCAN"), distinct), plan.text)
                    self.assertTrue(plan.is_above(distinct, plan.one("SEMANTIC MAP")), plan.text)
                    self.assert_auto(plan.one("SEMANTIC MAP"), reason="HASH_DISTINCT_INPUT")
                    self.check_query(
                        statement, [(None,)] + [("alpha",)] * count + [("beta",)],
                        first=[keys[prompt] for prompt in self.demanded(demanded, caching)])

    def test_unblocked_hash_distinct_exposes_a_ready_value_before_the_slow_head(self):
        self.load_inputs(["distinct-head", "distinct-peer", "distinct-duplicate"])
        for caching in ("false", "true"):
            with self.subTest(caching=caching):
                gate, duplicate_gate = self.fixtures.gate(), self.fixtures.gate()
                keys = [self.rank_reply("distinct-head", 7, gate=gate),
                        self.rank_reply("distinct-peer", 2),
                        self.rank_reply("distinct-duplicate", 2, gate=duplicate_gate)]
                statement = self.statement("DISTINCT " + self.rank() + " AS score", caching=caching)
                plan = self.explain(statement)
                distinct = plan.one("HASH DISTINCT")
                self.assertNotIn(", BLOCK", distinct.annotation.upper())
                self.assert_auto(plan.one("SEMANTIC RANK"), reason="HASH_DISTINCT_INPUT")
                before = self.snapshots()
                offset = len(self.first.requests)
                pending, rows, first = self.stream(statement)
                self.assertTrue(first.wait(3), f"hash DISTINCT held a ready value: {pending.error!r}")
                self.assertEqual(rows, [(2.0,)])
                self.assertFalse(gate.is_set())
                self.assertFalse(duplicate_gate.is_set())
                duplicate_gate.set()
                semantic.wait_evidence(
                    self, self.first,
                    lambda: any(record["prompt"] == "distinct-duplicate"
                                and record["response"] is not None
                                for record in semantic.audit(self.first)[offset:]),
                    pending, message="the gated duplicate never resumed DISTINCT collection")
                self.assertFalse(pending.done.wait(0.15), repr(pending.error))
                self.assertEqual(rows, [(2.0,)], "hash initialization was reset across a child wait")
                self.assertFalse(gate.is_set())
                gate.set()
                self.assertEqual(Counter(pending.result()), Counter(((7.0,), (2.0,))))
                self.assert_windows(before, first=keys)

    def collection_statement(self, distinct, *, caching="true", extra_hints=""):
        if distinct:
            return self.statement("DISTINCT " + self.rank() + " AS score", caching=caching,
                                  hints=HASH_HINTS + extra_hints, suffix="ORDER BY score DESC")
        return self.statement(
            "a.guard_flag,SUM(" + self.rank() + "),COUNT(*)", caching=caching,
            hints=HASH_HINTS + extra_hints, suffix="GROUP BY a.guard_flag ORDER BY a.guard_flag")

    def exercise_collection_cancel(self, kind, distinct):
        prompts = ["cancel-head", "cancel-peer", "cancel-tail"]
        self.load_inputs(prompts, flags=[0, 0, 1])
        gate = self.fixtures.gate()
        keys = [self.rank_reply(prompt, index + 1, gate=gate if index == 0 else None)
                for index, prompt in enumerate(prompts)]
        hints = " QUERY_TIMEOUT(2500000)" if kind == "deadline" else ""
        statement = self.collection_statement(distinct, extra_hints=hints)
        plan = self.explain(statement)
        plan.one("HASH DISTINCT" if distinct else "HASH GROUP BY")
        self.assert_auto(plan.one("SEMANTIC RANK"),
                         reason="HASH_DISTINCT_INPUT" if distinct else "HASH_AGGREGATE_INPUT")
        before = self.snapshots()
        offset = len(self.first.requests)
        pending, rows, first = self.stream(statement)
        semantic.wait_evidence(
            self, self.first,
            lambda: all(any(record["prompt"] == prompt and record["response"] is not None
                            for record in semantic.audit(self.first)[offset:])
                        for prompt in prompts[1:]),
            pending, message="the suspended collection's peer scores did not finish")
        self.assertFalse(first.is_set())
        self.assertEqual(rows, [])
        if kind == "kill":
            self.kill_query()
        pending.expect_sql_error(self, 1317 if kind == "kill" else (4012, 3024))
        self.assertFalse(gate.is_set(), "collection cancellation required releasing the provider")
        self.assertEqual(rows, [])
        self.assert_no_model_sockets()
        self.assert_windows(before, first=keys, settled=False)
        gate.set()
        expected = ((3.0,), (2.0,), (1.0,)) if distinct else ((0, 3.0, 2), (1, 3.0, 1))
        self.check_query(self.collection_statement(distinct), expected, first=keys)

    def test_kill_suspended_hash_groupby_releases_collection_and_recovers(self):
        self.exercise_collection_cancel("kill", False)

    def test_deadline_suspended_hash_groupby_releases_collection_and_recovers(self):
        self.exercise_collection_cancel("deadline", False)

    def test_kill_suspended_hash_distinct_releases_collection_and_recovers(self):
        self.exercise_collection_cancel("kill", True)

    def test_deadline_suspended_hash_distinct_releases_collection_and_recovers(self):
        self.exercise_collection_cancel("deadline", True)

    def test_invalid_provider_score_fails_collection_and_query_local_cache_recovers(self):
        self.load_inputs(["invalid-score", "valid-score"], flags=[0, 0])
        for distinct in (False, True):
            for caching in ("false", "true"):
                with self.subTest(distinct=distinct, caching=caching):
                    gate = self.fixtures.gate()
                    bad_key = self.rank_replies("invalid-score", [
                        Reply(body=completion('{"value":"not-numeric"}', finish_reason="stop"),
                              gate=gate)])
                    good_key = self.rank_reply("valid-score", 2)
                    statement = self.collection_statement(distinct, caching=caching)
                    before = self.snapshots()
                    offset = len(self.first.requests)
                    pending, rows, first = self.stream(statement)
                    semantic.wait_evidence(
                        self, self.first,
                        lambda: any(record["prompt"] == "valid-score" and record["response"] is not None
                                    for record in semantic.audit(self.first)[offset:]),
                        pending, message="the valid peer did not finish before the invalid held score")
                    self.assertFalse(first.wait(0.15))
                    gate.set()
                    pending.expect_sql_error(self, 4070)
                    self.assertEqual(rows, [])
                    self.assert_no_model_sockets()
                    self.assert_windows(before, first=[bad_key, good_key])
                    self.rank_reply("invalid-score", 3)
                    expected = ((3.0,), (2.0,)) if distinct else ((0, 5.0, 2),)
                    self.check_query(statement, expected, first=[bad_key, good_key])

    def test_empty_input_and_limit_zero_do_not_demand_invalid_semantic_values(self):
        self.load_inputs([None, ""], flags=[1, 1])
        before = self.snapshots()
        for distinct in (False, True):
            fields = "DISTINCT " + self.rank() if distinct else "a.guard_flag,AVG(" + self.rank() + ")"
            grouping = "" if distinct else "GROUP BY a.guard_flag "
            for where, suffix in ((None, grouping + "LIMIT 0"),
                                  ("FALSE", grouping),
                                  ("a.id<0", grouping)):
                with self.subTest(distinct=distinct, where=where, suffix=suffix):
                    self.assertEqual(self.execute(self.statement(fields, where=where, suffix=suffix)), ())
        self.assert_windows(before)

    def assert_rejected_without_work(self, statements):
        before = self.snapshots()
        for statement in statements:
            for prefix in ("EXPLAIN ", ""):
                with self.subTest(sql=statement, explain=bool(prefix)):
                    error = sql_error(self, 1235, lambda: self.execute(prefix + statement))
                    self.assertTrue(error.args[1], "unsupported shape lacked an explicit SQL diagnostic")
        self.assert_windows(before)

    def test_unsafe_aggregate_shapes_fail_before_model_calls(self):
        self.load_inputs(["must-not-be-called"], flags=[1])
        score = self.rank()
        statements = [
            self.statement("a.guard_flag," + aggregate,
                           suffix="GROUP BY a.guard_flag")
            for aggregate in ("COUNT(DISTINCT " + score + ")", "SUM(DISTINCT " + score + ")",
                              "AVG(DISTINCT " + score + ")", "STDDEV_POP(" + score + ")",
                              "GROUP_CONCAT(" + score + " ORDER BY a.id)",
                              f"AI_AGG('{self.models[0]}',a.prompt,'Summarize.')")
        ]
        statements.extend((
            self.statement("a.guard_flag,SUM(" + score + ")",
                           suffix="GROUP BY a.guard_flag WITH ROLLUP"),
            self.statement("AVG(" + score + ")"),
            self.statement("a.id," + score + ",ROW_NUMBER() OVER (ORDER BY a.id)"),
        ))
        self.assert_rejected_without_work(statements)

    def test_nonhash_algorithms_and_strict_unordered_are_not_auto_fallbacks(self):
        self.load_inputs(["must-not-be-called"], flags=[1])
        statements = (
            self.statement("a.guard_flag,AVG(" + self.rank() + ")",
                           hints="PARALLEL(1) NO_USE_HASH_AGGREGATION",
                           suffix="GROUP BY a.guard_flag"),
            self.statement("DISTINCT " + self.rank(), hints="PARALLEL(1) NO_USE_HASH_DISTINCT"),
            self.statement("a.guard_flag,AVG(" + self.rank() + ")", ordering="unordered",
                           suffix="GROUP BY a.guard_flag"),
            self.statement("DISTINCT " + self.rank(), ordering="unordered"),
        )
        self.assert_rejected_without_work(statements)

    def test_derived_order_limit_and_px_collection_fail_before_model_calls(self):
        self.load_inputs(["must-not-be-called"], flags=[1])
        statements = [
            self.statement(
                "a.guard_flag,AVG(" + self.rank() + ")", suffix="GROUP BY a.guard_flag",
                hints=HASH_HINTS + " NO_QUERY_TRANSFORMATION NO_MERGE(a)",
                source="(SELECT id,guard_flag,prompt FROM cache_inputs " + inner + ") a")
            for inner in ("ORDER BY id", "LIMIT 1", "ORDER BY id LIMIT 1")
        ]
        self.cursor.executemany("INSERT INTO cache_px_inputs VALUES (%s,%s)",
                                [(index, "must-not-be-called") for index in range(64)])
        statements.append(self.statement(
            "FLOOR(a.id/4),AVG(" + self.rank() + ")", source="cache_px_inputs a",
            hints="PARALLEL(4) USE_HASH_AGGREGATION", suffix="GROUP BY FLOOR(a.id/4)"))
        self.assert_rejected_without_work(statements)


def main(arguments=None):
    inventory = {name for name in SemanticRelationalAggregateContracts.__dict__ if name.startswith("test_")}
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--binary", type=Path)
    source.add_argument("--socket", type=Path)
    parser.add_argument("--case", action="append", default=[], choices=sorted(inventory))
    parser.add_argument("--list-cases", action="store_true")
    parser.add_argument("--log-dir", type=Path)
    options = parser.parse_args(arguments)
    selected = list(dict.fromkeys(options.case)) or sorted(inventory)
    if options.list_cases:
        print("\n".join(selected))
        return 0
    if options.binary is None and options.socket is None:
        parser.error("--binary or --socket is required")
    if options.log_dir is not None and (options.binary is None or options.log_dir.exists()):
        parser.error("--log-dir needs --binary and a directory that does not exist yet")
    if options.binary is not None:
        binary = options.binary.resolve()
        if not binary.is_file() or not os.access(binary, os.X_OK):
            parser.error("--binary must be executable")
        database = PrivateDatabase(binary)
        database.server_log = database.base / "log/seekdb.log"
    else:
        sql_socket = options.socket.resolve()
        if not sql_socket.is_socket():
            parser.error("--socket must be a Unix socket")
        database = cache.SocketDatabase(sql_socket)
    try:
        database.start()
        SemanticRelationalAggregateContracts.database = database
        suite = unittest.TestSuite(SemanticRelationalAggregateContracts(name) for name in selected)
        result = unittest.TextTestRunner(verbosity=2, failfast=True, resultclass=cache.SQLDiagnosticResult).run(suite)
        return 0 if result.wasSuccessful() else 1
    finally:
        try:
            if options.log_dir is not None:
                cache.retain_deployment_logs(database, options.log_dir)
        finally:
            database.close()
            SemanticRelationalAggregateContracts.database = None


if __name__ == "__main__":
    raise SystemExit(main())
