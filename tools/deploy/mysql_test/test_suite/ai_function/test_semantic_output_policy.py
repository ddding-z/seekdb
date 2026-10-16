"""Stage-level automatic semantic output policy SQL contracts.

All model work uses held loopback fixtures. Auto may reorder a region before
a full sort, but must preserve final SQL order and projection demand after
LIMIT. Strict unordered and default ordered behavior keep their contracts.
Ordinary hash aggregation/DISTINCT and restricted INNER HASH JOIN are tested
separately. CTE/materialization, derived ORDER BY/LIMIT, ordered aggregates,
unsupported join/aggregate algorithms and windows remain excluded. Local TEXT
PX without SQL ordering is covered separately by test_semantic_px_row_async.py.
"""

import argparse
import os
from pathlib import Path
import unittest

from pymysql.constants import FIELD_TYPE

import test_semantic_cache as cache
import test_semantic_rank as rank
import test_semantic_runtime as semantic
import test_semantic_unordered as unordered
from test_file_runtime import PrivateDatabase
from test_runtime_contracts import Reply, completion, sql_error


class SemanticOutputPolicyContracts(rank.SemanticRankContracts):
    def statement(self, fields, *, ordering="auto", enabled="true", caching="false",
                  suffix="", hints="PARALLEL(1)", **kwargs):
        if enabled is not None:
            hints += f" OPT_PARAM('enable_semantic_row_async','{enabled}')"
        if ordering is not None:
            hints += f" OPT_PARAM('semantic_output_order','{ordering}')"
        return cache.sql_statement(fields, hints=hints, cache=caching, suffix=suffix, **kwargs)

    def assert_policy(self, node, mode, reason):
        annotation = node.annotation.upper()
        self.assertIn("ROW_ASYNC=" + mode, annotation)
        self.assertIn("OUTPUT_POLICY=AUTO", annotation)
        self.assertIn("ORDER_REASON=" + reason, annotation)

    def test_order_free_auto_region_exposes_a_ready_later_row(self):
        self.load_inputs(["slow", "fast"])
        for caching in ("false", "true"):
            with self.subTest(caching=caching):
                gate = self.fixtures.gate()
                self.fixtures.reply(0, "slow", "held", gate=gate)
                self.fixtures.reply(0, "fast", "ready")
                statement = self.statement("a.id," + self.call("map"), caching=caching)
                self.assert_policy(self.explain(statement).one("SEMANTIC MAP"),
                                   "UNORDERED_READY", "NO_ORDER_REQUIREMENT")
                pending, rows, first = self.stream(statement)
                self.assertTrue(first.wait(3), f"auto waited for the slow head: {pending.error!r}")
                self.assertEqual(rows, [(1, "ready")])
                self.assertFalse(gate.is_set())
                gate.set()
                self.assertCountEqual(pending.result(), ((0, "held"), (1, "ready")))

    def exercise_ordered_projection(self, suffix, selected):
        prompts = [f"ordered-{index}" for index in range(8)]
        self.load_inputs(prompts)
        for caching in ("false", "true"):
            with self.subTest(caching=caching, suffix=suffix):
                gate = self.fixtures.gate()
                keys = [self.fixtures.reply(0, prompt, "value:" + prompt,
                                            gate=gate if index == selected[0] else None)
                        for index, prompt in enumerate(prompts)]
                statement = self.statement("a.id," + self.call("map"),
                                           suffix=suffix, caching=caching)
                self.assert_policy(self.explain(statement).one("SEMANTIC MAP"),
                                   "ORDERED_PREFIX", "FINAL_SQL_ORDER")
                before = self.snapshots()
                offset = len(self.first.requests)
                pending, rows, first = self.stream(statement)
                semantic.wait_evidence(
                    self, self.first,
                    lambda: any(record["prompt"] == prompts[selected[1]] and record["response"] is not None
                                for record in semantic.audit(self.first)[offset:]),
                    pending, message="the later selected row did not finish")
                self.assertFalse(first.wait(0.15), "auto broke the established final SQL order")
                self.assertEqual(rows, [])
                self.assertFalse(gate.is_set())
                gate.set()
                self.assertEqual(pending.result(),
                                 tuple((index, "value:" + prompts[index]) for index in selected))
                self.assert_windows(before, first=[keys[index] for index in selected])

    def test_sorted_limit_projection_preserves_order_and_only_demands_selected_rows(self):
        self.exercise_ordered_projection("ORDER BY a.id DESC LIMIT 3 OFFSET 2", [5, 4, 3])

    def test_index_order_without_a_sort_still_requires_ordered_projection(self):
        self.exercise_ordered_projection("ORDER BY a.id LIMIT 3", [0, 1, 2])

    def test_unordered_rank_before_numeric_topn_scores_all_candidates(self):
        prompts = ["two", "ten", "two", "negative", "four", "ten"]
        scores = {"two": 2.0, "ten": 10.0, "negative": -1.5, "four": 4.0}
        self.load_inputs(prompts)
        for caching in ("false", "true"):
            with self.subTest(caching=caching):
                gate = self.fixtures.gate()
                keys = {prompt: self.rank_reply(prompt, value, gate=gate if prompt == "two" else None)
                        for prompt, value in scores.items()}
                statement = self.statement("a.id," + self.rank() + " AS score", caching=caching,
                                           suffix="ORDER BY score DESC,a.id LIMIT 3 OFFSET 1")
                plan = self.explain(statement)
                self.assert_policy(plan.one("SEMANTIC RANK"), "UNORDERED_READY", "SORT_RESTORES_ORDER")
                self.assertTrue(plan.is_above(plan.one("TOP-N SORT"), plan.one("SEMANTIC RANK")), plan.text)
                before = self.snapshots()
                offset = len(self.first.requests)
                pending, rows, first = self.stream(statement)
                semantic.wait_evidence(
                    self, self.first,
                    lambda: any(record["prompt"] == "ten" and record["response"] is not None
                                for record in semantic.audit(self.first)[offset:]),
                    pending, message="a fast score did not finish before the held candidate")
                self.assertFalse(first.wait(0.15), "exact Top-N skipped an unseen candidate")
                self.assertEqual(rows, [])
                gate.set()
                self.assertEqual(pending.result(), ((5, 10.0), (4, 4.0), (0, 2.0)))
                demanded = prompts if caching == "false" else dict.fromkeys(prompts)
                self.assert_windows(before, first=[keys[prompt] for prompt in demanded])

    def test_filter_rank_sort_limit_map_selects_policy_per_stage(self):
        prompts = [f"hybrid-{index}" for index in range(10)]
        self.load_inputs(prompts)
        for caching in ("false", "true"):
            with self.subTest(caching=caching):
                filter_keys = [self.fixtures.reply(0, prompt, index % 2 == 0, kind="filter")
                               for index, prompt in enumerate(prompts)]
                rank_keys = [self.rank_reply(prompts[index], index + 0.25, provider=1)
                             for index in range(0, len(prompts), 2)]
                map_keys = [self.fixtures.reply(0, "map:" + prompts[index], "value:" + prompts[index])
                            for index in (8, 6, 4)]
                mapped = self.call("map", "CONCAT('map:',a.prompt)")
                statement = self.statement(
                    "a.id," + mapped, caching=caching, where=self.call("filter"),
                    suffix="ORDER BY " + self.rank(provider=1) + " DESC,a.id LIMIT 3")
                plan = self.explain(statement)
                self.assert_policy(plan.one("SEMANTIC FILTER"), "UNORDERED_READY", "SORT_RESTORES_ORDER")
                self.assert_policy(plan.one("SEMANTIC RANK"), "UNORDERED_READY", "SORT_RESTORES_ORDER")
                self.assert_policy(plan.one("SEMANTIC MAP"), "ORDERED_PREFIX", "FINAL_SQL_ORDER")
                self.check_query(statement, [(index, "value:" + prompts[index]) for index in (8, 6, 4)],
                                 first=filter_keys + map_keys, second=rank_keys)

    def test_full_sort_restores_order_after_unordered_projection(self):
        prompts = [f"sort-{index}" for index in range(35)]
        flags = [index % 3 for index in range(len(prompts))]
        self.load_inputs(prompts, flags=flags)
        keys = [self.fixtures.reply(0, prompt, "value:" + prompt) for prompt in prompts]
        statement = self.statement("a.id," + self.call("map"),
                                   suffix="ORDER BY a.guard_flag DESC,a.id")
        plan = self.explain(statement)
        self.assert_policy(plan.one("SEMANTIC MAP"), "UNORDERED_READY", "SORT_RESTORES_ORDER")
        self.check_query(statement,
                         [(index, "value:" + prompts[index])
                          for index in sorted(range(len(prompts)), key=lambda index: (-flags[index], index))],
                         first=keys)

    def test_computed_sort_error_precedes_projection_model_work(self):
        self.load_inputs(["not-json", "also-not-json"])
        before = self.snapshots()
        statement = self.statement(self.call("map"), suffix="ORDER BY JSON_EXTRACT(a.prompt,'$.sort')")
        self.assert_policy(self.explain(statement).one("SEMANTIC MAP"),
                           "ORDERED_PREFIX", "FINAL_SQL_ORDER")
        sql_error(self, (3140, 3141), lambda: self.execute(statement))
        self.assert_windows(before)
        self.assert_no_model_sockets()

    def test_limit_without_order_does_not_move_projection_before_row_selection(self):
        prompts = [f"limit-{index}" for index in range(35)]
        self.load_inputs(prompts)
        keys = [self.rank_reply(prompt, index + 0.5) for index, prompt in enumerate(prompts)]
        statement = self.statement("a.id," + self.rank(), suffix="LIMIT 3 OFFSET 2")
        self.assert_policy(self.explain(statement).one("SEMANTIC RANK"),
                           "UNORDERED_READY", "NO_ORDER_REQUIREMENT")
        before = self.snapshots()
        rows = self.execute(statement)
        self.assertEqual(len(rows), 3)
        self.assertTrue(all(score == index + 0.5 for index, score in rows), rows)
        self.assertEqual(self.cursor.description[1][1], FIELD_TYPE.DOUBLE)
        self.assert_windows(before, first=[keys[index] for index, _score in rows])

    def test_simple_derived_input_supports_merged_and_retained_query_blocks(self):
        prompts = [f"derived-{index}" for index in range(12)]
        self.load_inputs(prompts, flags=[index % 2 for index in range(len(prompts))])
        keys = [self.fixtures.reply(0, prompt, "value:" + prompt) for prompt in prompts]
        source = "(SELECT id,prompt FROM cache_inputs WHERE guard_flag=1) a"
        for hint in ("", "NO_QUERY_TRANSFORMATION NO_MERGE(a)"):
            with self.subTest(hint=hint):
                statement = self.statement("a.id," + self.call("map"), source=source,
                                           hints="PARALLEL(1) " + hint,
                                           suffix="ORDER BY a.id DESC LIMIT 2")
                plan = self.explain(statement)
                self.assert_policy(plan.one("SEMANTIC MAP"), "ORDERED_PREFIX", "FINAL_SQL_ORDER")
                if hint:
                    plan.one("SUBPLAN SCAN")
                self.check_query(statement, ((11, "value:derived-11"), (9, "value:derived-9")),
                                 first=[keys[11], keys[9]])

    def test_retained_derived_semantic_output_feeds_a_ready_outer_rank(self):
        self.load_inputs(["inner-slow", "inner-fast"])
        gate = self.fixtures.gate()
        self.fixtures.reply(0, "inner-slow", "slow-value", gate=gate)
        self.fixtures.reply(0, "inner-fast", "fast-value")
        self.rank_reply("slow-value", 7.25, provider=1)
        self.rank_reply("fast-value", 2.5, provider=1)
        inner_map = self.call("map", prompt="prompt")
        source = f"(SELECT id,{inner_map} AS mapped FROM cache_inputs) a"
        statement = self.statement("a.id," + self.rank("a.mapped", provider=1), source=source,
                                   hints="PARALLEL(1) NO_QUERY_TRANSFORMATION NO_MERGE(a)")
        plan = self.explain(statement)
        plan.one("SUBPLAN SCAN")
        self.assert_policy(plan.one("SEMANTIC MAP"), "UNORDERED_READY", "NO_ORDER_REQUIREMENT")
        self.assert_policy(plan.one("SEMANTIC RANK"), "UNORDERED_READY", "NO_ORDER_REQUIREMENT")
        pending, rows, first = self.stream(statement)
        self.assertTrue(first.wait(3), f"derived boundary blocked a ready row: {pending.error!r}")
        self.assertEqual(rows, [(1, 2.5)])
        self.assertFalse(gate.is_set())
        gate.set()
        self.assertCountEqual(pending.result(), ((0, 7.25), (1, 2.5)))

    def test_outer_cpu_projection_and_sort_propagate_policy_into_retained_derived_input(self):
        prompts = ["inner-duplicate", "inner-other", "inner-duplicate"] * 12
        self.load_inputs(prompts)
        keys = {prompt: self.fixtures.reply(0, prompt, "value:" + prompt) for prompt in set(prompts)}
        source = f"(SELECT id,{self.call('map', 'prompt')} AS mapped FROM cache_inputs) a"
        for suffix, reason in (("", "NO_ORDER_REQUIREMENT"),
                               ("ORDER BY a.mapped DESC,a.id LIMIT 5", "SORT_RESTORES_ORDER")):
            with self.subTest(suffix=suffix):
                statement = self.statement("a.id,a.mapped", source=source, suffix=suffix, caching="true",
                                           hints="PARALLEL(1) NO_QUERY_TRANSFORMATION NO_MERGE(a)")
                plan = self.explain(statement)
                plan.one("SUBPLAN SCAN")
                self.assert_policy(plan.one("SEMANTIC MAP"), "UNORDERED_READY", reason)
                before = self.snapshots()
                result = self.execute(statement)
                expected = [(index, "value:" + prompt) for index, prompt in enumerate(prompts)]
                if suffix:
                    expected.sort(key=lambda row: row[0])
                    expected.sort(key=lambda row: row[1], reverse=True)
                    expected = expected[:5]
                    self.assertEqual(result, tuple(expected))
                else:
                    self.assertCountEqual(result, expected)
                self.assert_windows(before, first=list(keys.values()))

    def exercise_retained_sort_cancel(self, kind):
        self.load_inputs(["cancel-head", "cancel-peer"])
        gate = self.fixtures.gate()
        self.rank_reply("cancel-head", 3, gate=gate)
        self.rank_reply("cancel-peer", 2)
        source = f"(SELECT id,{self.rank('prompt')} AS score FROM cache_inputs) a"
        hints = "PARALLEL(1) NO_QUERY_TRANSFORMATION NO_MERGE(a)"
        if kind == "deadline":
            hints += " QUERY_TIMEOUT(1200000)"
        statement = self.statement("a.id,a.score", source=source, hints=hints,
                                   suffix="ORDER BY a.score DESC,a.id LIMIT 1")
        plan = self.explain(statement)
        plan.one("SUBPLAN SCAN")
        self.assert_policy(plan.one("SEMANTIC RANK"), "UNORDERED_READY", "SORT_RESTORES_ORDER")
        query_id = self.execute("SELECT CONNECTION_ID()")[0][0]
        offset = len(self.first.requests)
        pending, rows, first = self.stream(statement)
        semantic.wait_evidence(
            self, self.first,
            lambda: any(record["prompt"] == "cancel-peer" and record["response"] is not None
                        for record in semantic.audit(self.first)[offset:]),
            pending, message="the sort's peer score did not finish")
        self.assertFalse(first.is_set())
        if kind == "kill":
            killer = cache.pymysql.connect(unix_socket=self.database.sql_socket, user="root", autocommit=True)
            try:
                with killer.cursor() as cursor:
                    cursor.execute(f"KILL QUERY {query_id}")
            finally:
                killer.close()
        pending.expect_sql_error(self, 1317 if kind == "kill" else (4012, 3024))
        self.assertFalse(gate.is_set(), "query cancellation must not require provider release")
        self.assertEqual(rows, [])
        self.assert_no_model_sockets()
        gate.set()
        self.assertEqual(self.execute(statement), ((0, 3.0),))

    def test_kill_while_sort_and_derived_scan_are_suspended_cancels_and_recovers(self):
        self.exercise_retained_sort_cancel("kill")

    def test_deadline_while_sort_and_derived_scan_are_suspended_cancels_and_recovers(self):
        self.exercise_retained_sort_cancel("deadline")

    def test_guarded_invalid_inputs_and_shared_results_remain_demand_driven(self):
        prompts = [None, "", "safe", "safe"] * 9
        flags = [0, 0, 1, 1] * 9
        self.load_inputs(prompts, flags=flags)
        mapped = "CASE WHEN a.guard_flag=1 THEN " + self.call("map", provider=1) + " ELSE 'skip' END"
        for caching in ("false", "true"):
            with self.subTest(caching=caching):
                filter_key = self.fixtures.reply(0, "safe", True, kind="filter")
                map_key = self.fixtures.reply(1, "safe", "mapped")
                repetitions = flags.count(1) if caching == "false" else 1
                self.check_query(
                    self.statement("a.id," + mapped, caching=caching,
                                   where="a.guard_flag=0 OR " + self.call("filter")),
                    [(index, "mapped" if flag else "skip") for index, flag in enumerate(flags)],
                    first=[filter_key] * repetitions, second=[map_key] * repetitions, ordered=False)

    def test_empty_queries_and_limit_zero_issue_no_model_work(self):
        self.load_inputs(["unused"])
        before = self.snapshots()
        for kwargs in ({"suffix": "LIMIT 0"}, {"suffix": "ORDER BY a.id LIMIT 0"},
                       {"where": "FALSE"}, {"where": "a.id<0"}):
            with self.subTest(kwargs=kwargs):
                self.assertEqual(self.execute(self.statement("a.id," + self.call("map"), **kwargs)), ())
        self.assert_windows(before)

    def test_invalid_score_before_sort_finishes_is_an_error_and_connection_recovers(self):
        self.load_inputs(["bad", "good"])
        gate = self.fixtures.gate()
        self.rank_replies("bad", [Reply(body=completion('{"value":"not-a-number"}', finish_reason="stop"),
                                        gate=gate)])
        self.rank_reply("good", 2)
        statement = self.statement("a.id," + self.rank() + " AS score",
                                   suffix="ORDER BY score DESC LIMIT 1")
        offset = len(self.first.requests)
        pending, rows, first = self.stream(statement)
        semantic.wait_evidence(
            self, self.first,
            lambda: any(record["prompt"] == "good" and record["response"] is not None
                        for record in semantic.audit(self.first)[offset:]),
            pending, message="the peer score did not finish")
        self.assertFalse(first.wait(0.15))
        gate.set()
        pending.expect_sql_error(self, 4070)
        self.assertEqual(rows, [])
        self.assert_no_model_sockets()
        self.rank_reply("bad", 3)
        self.assertEqual(self.execute(statement), ((0, 3.0),))

    def test_auto_and_unordered_require_row_async_and_strict_unordered_stays_strict(self):
        self.load_inputs(["policy"])
        before = self.snapshots()
        for enabled in (None, "false"):
            for ordering in ("auto", "AuTo", "unordered"):
                statement = self.statement(self.call("map"), enabled=enabled, ordering=ordering)
                for prefix in ("EXPLAIN ", ""):
                    with self.subTest(enabled=enabled, ordering=ordering, prefix=prefix):
                        sql_error(self, 1210, lambda: self.execute(prefix + statement))
        for kwargs in ({"suffix": "ORDER BY a.id"},
                       {"source": "(SELECT id,prompt FROM cache_inputs) a"}):
            statement = self.statement(self.call("map"), ordering="unordered", **kwargs)
            for prefix in ("EXPLAIN ", ""):
                sql_error(self, 1235, lambda: self.execute(prefix + statement))
        self.assert_windows(before)

    def test_auto_rejects_unsafe_query_boundaries_before_rewriting_or_model_work(self):
        self.load_inputs(["scope"])
        fields = "a.id," + self.call("map")
        sources = (
            "(SELECT id,prompt FROM cache_inputs LIMIT 1) a",
            "(SELECT id,prompt FROM cache_inputs ORDER BY id) a",
            "(SELECT id,GROUP_CONCAT(prompt ORDER BY id) AS prompt FROM cache_inputs GROUP BY id) a",
            "(SELECT DISTINCT id,prompt FROM cache_inputs LIMIT 1) a",
            "cache_inputs a LEFT JOIN cache_right b ON a.id=b.join_id",
            "cache_inputs a,cache_right b",
            "cache_inputs a JOIN cache_right b ON 1=1",
        )
        statements = [self.statement(fields, source=source) for source in sources]
        statements.extend([
            self.statement("a.id"),
            self.statement("COUNT(DISTINCT " + self.rank() + ")"),
            self.statement("AVG(" + self.rank() + ")"),
            self.statement("a.guard_flag," + self.call("map"),
                           suffix="GROUP BY a.guard_flag,a.prompt WITH ROLLUP"),
            self.statement(fields + ",ROW_NUMBER() OVER (ORDER BY a.id)"),
            self.statement(fields + ",(SELECT MAX(row_id) FROM cache_right)"),
            self.statement(fields, suffix="FOR UPDATE"),
            self.statement(fields) + " UNION ALL SELECT id,prompt FROM cache_inputs",
            "WITH source_rows AS (SELECT id,prompt FROM cache_inputs) " +
            self.statement(fields, source="source_rows a"),
            self.statement(f"AI_AGG('{self.models[0]}',a.prompt,'Summarize.')"),
            self.statement(self.call("map", options="a.opts")),
        ])
        before = self.snapshots()
        for statement in statements:
            with self.subTest(sql=statement):
                for prefix in ("EXPLAIN ", ""):
                    sql_error(self, 1235, lambda: self.execute(prefix + statement))
        self.cursor.executemany("INSERT INTO cache_px_inputs VALUES (%s,%s)",
                                [(index, "scope") for index in range(256)])
        parallel = self.statement(fields, source="cache_px_inputs a", hints="PARALLEL(4)",
                                  suffix="ORDER BY a.id")
        for prefix in ("EXPLAIN ", ""):
            sql_error(self, 1235, lambda: self.execute(prefix + parallel))
        self.assert_windows(before)

    def test_late_topn_runtime_filter_is_not_an_unchecked_auto_sort_boundary(self):
        prompts = [f"topn-{index}" for index in range(512)]
        self.load_inputs(prompts, flags=[index % 7 for index in range(len(prompts))])
        self.execute("CALL DBMS_STATS.GATHER_TABLE_STATS(%s,%s)", (self.schema, "cache_inputs"))
        hints = "PARALLEL(1) OPT_PARAM('enable_topn_runtime_filter','true')"
        suffix = "ORDER BY a.guard_flag DESC,a.id LIMIT 2"
        control = self.statement("a.id," + self.call("map"), ordering="ordered", hints=hints, suffix=suffix)
        self.assertIn("topn_filter", self.explain(control).text.lower())
        keys = [self.fixtures.reply(0, prompts[index], "value:" + prompts[index]) for index in (6, 13)]
        expected = [(index, "value:" + prompts[index]) for index in (6, 13)]
        self.check_query(control, expected, first=keys)
        automatic = self.statement("a.id," + self.call("map"), hints=hints, suffix=suffix)
        before = self.snapshots()
        for prefix in ("EXPLAIN ", ""):
            sql_error(self, 1235, lambda: self.execute(prefix + automatic))
        self.assert_windows(before)
        no_filter = self.statement("a.id," + self.call("map"), suffix=suffix,
                                   hints="PARALLEL(1) OPT_PARAM('enable_topn_runtime_filter','false')")
        self.assert_policy(self.explain(no_filter).one("SEMANTIC MAP"),
                           "ORDERED_PREFIX", "FINAL_SQL_ORDER")
        self.check_query(no_filter, expected, first=keys)

    def test_invalid_output_policies_are_not_automatic_fallbacks(self):
        self.load_inputs(["invalid"])
        before = self.snapshots()
        for value in ("automatic", "auto ", "ready", "", "1"):
            statement = self.statement(self.call("map"), ordering=value)
            for prefix in ("EXPLAIN ", ""):
                sql_error(self, 1210, lambda: self.execute(prefix + statement))
        numeric = self.statement(self.call("map")).replace("'semantic_output_order','auto'",
                                                           "'semantic_output_order',1")
        for prefix in ("EXPLAIN ", ""):
            sql_error(self, 1210, lambda: self.execute(prefix + numeric))
        self.assert_windows(before)

    def test_auto_is_case_insensitive_and_default_policy_is_unchanged(self):
        self.load_inputs(["case"])
        key = self.fixtures.reply(0, "case", "value")
        for ordering, enabled, annotation in (
                ("AuTo", "true", "UNORDERED_READY"),
                (None, "true", "ORDERED_PREFIX"),
                ("ordered", "true", "ORDERED_PREFIX"),
                (None, None, None)):
            statement = self.statement(self.call("map"), ordering=ordering, enabled=enabled)
            plan = self.explain(statement).text.upper()
            if annotation:
                self.assertIn("ROW_ASYNC=" + annotation, plan)
            else:
                self.assertNotIn("ROW_ASYNC=", plan)
            if ordering != "AuTo":
                self.assertNotIn("OUTPUT_POLICY=AUTO", plan)
            self.check_query(statement, (("value",),), first=[key])


class SemanticAutoProxyContracts(unordered.SemanticUnorderedProxyContracts):
    def statement(self, *, hints=None, **kwargs):
        options = {"semantic_output_order": "auto"}
        options.update(hints or {})
        return super().statement(hints=options, **kwargs)

    def test_auto_proxy_filter_preserves_ready_row_visibility_and_cache_demand(self):
        super().test_proxy_filter_fast_later_row_reaches_sql_output()
        plan = "\n".join(row[0] for row in self.execute("EXPLAIN " + self.statement())).upper()
        self.assertIn("OUTPUT_POLICY=AUTO", plan)
        self.assertIn("ORDER_REASON=NO_ORDER_REQUIREMENT", plan)


def main(arguments=None):
    classes = (SemanticOutputPolicyContracts, SemanticAutoProxyContracts)
    inventory = {name: test_class for test_class in classes
                 for name in test_class.__dict__ if name.startswith("test_")}
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
        for test_class in classes:
            test_class.database = database
        suite = unittest.TestSuite()
        for test_class in classes:
            suite.addTests(test_class(name) for name in selected if inventory[name] is test_class)
        result = unittest.TextTestRunner(verbosity=2, failfast=True, resultclass=cache.SQLDiagnosticResult).run(suite)
        return 0 if result.wasSuccessful() else 1
    finally:
        try:
            if options.log_dir is not None:
                cache.retain_deployment_logs(database, options.log_dir)
        finally:
            database.close()
            for test_class in classes:
                test_class.database = None


if __name__ == "__main__":
    raise SystemExit(main())
