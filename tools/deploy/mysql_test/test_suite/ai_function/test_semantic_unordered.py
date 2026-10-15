"""Opt-in unordered, row-asynchronous TEXT MAP/FILTER/RANK SQL contracts.

Held first rows and first slots must not block visible SQL output or reuse of
other slots. Tests assert exact result bags, not a total HTTP-completion order.
The default and explicit ordered policies retain their existing barriers.
All providers are loopback fixtures; --binary owns a private database.
"""

import argparse
from collections import Counter
import os
from pathlib import Path
import unittest
from unittest import mock

from pymysql.constants import FIELD_TYPE

import test_semantic_cache as cache
import test_semantic_proxy as proxy
import test_semantic_rank as rank
import test_semantic_row_async as row_async
import test_semantic_runtime as semantic
from test_file_runtime import PrivateDatabase
from test_runtime_contracts import Reply, completion, sql_error


BATCH_ROWS = semantic.BATCH_ROWS
UNORDERED_HINT = ("OPT_PARAM('enable_semantic_row_async','true') "
                  "OPT_PARAM('semantic_output_order','unordered')")


class SemanticUnorderedContracts(rank.SemanticRankContracts):
    def statement(self, fields, *, enabled="true", ordering="unordered", caching="false",
                  suffix="", hints="PARALLEL(1)", **kwargs):
        if enabled is not None:
            hints += f" OPT_PARAM('enable_semantic_row_async','{enabled}')"
        if ordering is not None:
            hints += f" OPT_PARAM('semantic_output_order','{ordering}')"
        return cache.sql_statement(fields, hints=hints, suffix=suffix, cache=caching, **kwargs)

    def wait_requests(self, server, predicate, pending, message):
        semantic.wait_evidence(self, server, predicate, pending, message=message)

    def test_fast_later_row_is_visible_while_first_row_is_held(self):
        self.load_inputs(["held-first", "ready-later"])
        for caching in ("false", "true"):
            with self.subTest(caching=caching):
                head = self.fixtures.gate()
                offset = len(self.first.requests)
                self.fixtures.reply(0, "held-first", "slow", gate=head)
                self.fixtures.reply(0, "ready-later", "fast")
                statement = self.statement("a.id," + self.call("map"), caching=caching)
                self.assertIn("ROW_ASYNC=UNORDERED_READY", self.explain(statement).text.upper())
                pending, rows, first = self.stream(statement)
                self.wait_requests(self.first, lambda: len(self.first.requests) >= offset + 2,
                                   pending, "both rows must enter the unchanged request group")
                self.assertTrue(first.wait(3), f"ready row remained buffered: {pending.error!r}")
                self.assertEqual(rows, [(1, "fast")])
                self.assertFalse(head.is_set())
                self.assertFalse(pending.done.is_set(), repr(pending.error))
                head.set()
                self.assertCountEqual(pending.result(), ((0, "slow"), (1, "fast")))
                self.assert_no_model_sockets()

    def test_ordered_and_default_policies_keep_their_head_barriers(self):
        self.load_inputs(["control-head", "control-peer"])
        for enabled, ordering, explain in (
                ("true", None, "ROW_ASYNC=ORDERED_PREFIX"),
                ("true", "ordered", "ROW_ASYNC=ORDERED_PREFIX"),
                (None, None, None), ("false", "ordered", None)):
            with self.subTest(enabled=enabled, ordering=ordering):
                head = self.fixtures.gate()
                offset = len(self.first.requests)
                self.fixtures.reply(0, "control-head", True, kind="filter", gate=head)
                self.fixtures.reply(0, "control-peer", True, kind="filter")
                statement = self.statement("a.id", where=self.call("filter"), enabled=enabled,
                                           ordering=ordering, suffix="LIMIT 1")
                plan = self.explain(statement).text.upper()
                if explain:
                    self.assertIn(explain, plan)
                else:
                    self.assertNotIn("ROW_ASYNC=", plan)
                pending = self.fixtures.start(lambda: self.execute(statement))
                self.wait_requests(
                    self.first, lambda: any(record["prompt"] == "control-peer" and
                                            record["response"] is not None
                                            for record in semantic.audit(self.first)[offset:]),
                    pending, "the fast control response was not sent")
                self.assertFalse(pending.done.wait(0.15), "ordered/default bypassed the held head")
                head.set()
                self.assertEqual(pending.result(), ((0,),))

    def test_filter_limit_one_returns_a_fast_later_match_and_cancels_head(self):
        prompts = [f"limit-{index}" for index in range(3 * BATCH_ROWS + 5)]
        self.load_inputs(prompts)
        for caching in ("false", "true"):
            with self.subTest(caching=caching):
                head, fast = self.fixtures.gate(), self.fixtures.gate()
                offset = len(self.first.requests)
                for index, prompt in enumerate(prompts):
                    self.fixtures.reply(0, prompt, True, kind="filter",
                                        gate=fast if index == 3 else head)
                pending = self.fixtures.start(lambda: self.execute(self.statement(
                    "a.id", where=self.call("filter"), caching=caching, suffix="LIMIT 1")))
                self.wait_requests(self.first, lambda: len(self.first.requests) >= offset + BATCH_ROWS,
                                   pending, "LIMIT must not shrink the fixed HTTP request group")
                fast.set()
                self.assertEqual(pending.result(timeout=3), ((3,),))
                self.assertFalse(head.is_set())
                self.assertLessEqual(len(self.first.requests) - offset, 2 * BATCH_ROWS)
                self.assert_no_model_sockets()
                head.set()

    def test_later_slot_overtakes_a_fully_held_first_slot(self):
        prompts = [f"slot-{index}" for index in range(2 * BATCH_ROWS)]
        self.load_inputs(prompts)
        for caching in ("false", "true"):
            with self.subTest(caching=caching):
                head = self.fixtures.gate()
                offset = len(self.first.requests)
                for index, prompt in enumerate(prompts):
                    self.fixtures.reply(0, prompt, "value:" + prompt,
                                        gate=head if index < BATCH_ROWS else None)
                pending, rows, first = self.stream(self.statement(
                    "a.id," + self.call("map"), caching=caching))
                self.wait_requests(self.first, lambda: len(self.first.requests) >= offset + 2 * BATCH_ROWS,
                                   pending, "two slots did not reach the provider")
                self.assertTrue(first.wait(3), f"later slot was blocked: {pending.error!r}")
                self.assertTrue(all(index >= BATCH_ROWS for index, _value in rows), rows)
                self.assertFalse(head.is_set())
                self.assertFalse(pending.done.is_set(), repr(pending.error))
                head.set()
                self.assertCountEqual(pending.result(),
                                      ((index, "value:" + prompt)
                                       for index, prompt in enumerate(prompts)))

    def test_non_head_slot_is_recycled_while_first_slot_remains_held(self):
        prompts = [f"reuse-{index}" for index in range(6 * BATCH_ROWS + 3)]
        self.load_inputs(prompts)
        for caching in ("false", "true"):
            with self.subTest(caching=caching):
                head = self.fixtures.gate()
                offset = len(self.first.requests)
                for index, prompt in enumerate(prompts):
                    self.fixtures.reply(0, prompt, "value:" + prompt,
                                        gate=head if index < BATCH_ROWS else None)
                pending, rows, first = self.stream(self.statement(
                    "a.id," + self.call("map"), caching=caching))
                self.wait_requests(self.first, lambda: len(self.first.requests) >= offset + len(prompts),
                                   pending, "a consumed later slot was not recycled past the held head")
                self.assertTrue(first.wait(3), f"no output while slots were recycled: {pending.error!r}")
                self.assertFalse(head.is_set())
                self.assertFalse(pending.done.is_set(), repr(pending.error))
                self.assertTrue(all(index >= BATCH_ROWS for index, _value in rows), rows)
                with self.first.condition:
                    self.assertLessEqual(self.first.active, 2 * BATCH_ROWS,
                                         "live requests exceeded the two-slot window")
                head.set()
                result = pending.result()
                self.assertCountEqual(result, ((index, "value:" + prompt)
                                              for index, prompt in enumerate(prompts)))
                self.assertEqual(len(result), len(prompts))
                self.assertEqual(Counter(record["prompt"] for record in
                                         semantic.audit(self.first)[offset:]), Counter(prompts))

    def test_slow_filter_head_does_not_block_map_and_numeric_rank(self):
        self.load_inputs(["slow-filter", "fast-filter"])
        for kind in ("map", "rank"):
            for caching in ("false", "true"):
                with self.subTest(kind=kind, caching=caching):
                    head = self.fixtures.gate()
                    self.fixtures.reply(0, "slow-filter", True, kind="filter", gate=head)
                    self.fixtures.reply(0, "fast-filter", True, kind="filter")
                    if kind == "map":
                        self.fixtures.reply(1, "slow-filter", "slow-mapped")
                        self.fixtures.reply(1, "fast-filter", "fast-mapped")
                        expected = ((0, "slow-mapped"), (1, "fast-mapped"))
                    else:
                        self.rank_reply("slow-filter", 7.25, provider=1)
                        self.rank_reply("fast-filter", 2.5, provider=1)
                        expected = ((0, 7.25), (1, 2.5))
                    statement = self.statement("a.id," + self.call(kind, provider=1),
                                               where=self.call("filter"), caching=caching)
                    plan = self.explain(statement)
                    plan.one("SEMANTIC FILTER")
                    plan.one("SEMANTIC " + kind.upper())
                    self.assertEqual(plan.text.upper().count("ROW_ASYNC=UNORDERED_READY"), 2)
                    pending, rows, first = self.stream(statement)
                    self.assertTrue(first.wait(3), f"filter head blocked {kind}: {pending.error!r}")
                    self.assertEqual(rows, [expected[1]])
                    self.assertFalse(head.is_set())
                    head.set()
                    self.assertCountEqual(pending.result(), expected)

    def test_rank_projection_has_native_double_values_and_preserves_duplicate_rows(self):
        prompts = ["score-two", "score-ten", "score-two", "score-negative"] * 10
        values = {"score-two": 2.0, "score-ten": 10.0, "score-negative": -1.5}
        self.load_inputs(prompts)
        for caching in ("false", "true"):
            with self.subTest(caching=caching):
                keys = {prompt: self.rank_reply(prompt, value) for prompt, value in values.items()}
                demanded = prompts if caching == "false" else dict.fromkeys(prompts)
                self.check_query(
                    self.statement("a.id," + self.rank(), caching=caching),
                    ((index, values[prompt]) for index, prompt in enumerate(prompts)),
                    first=[keys[prompt] for prompt in demanded], ordered=False)
                self.assertEqual(self.cursor.description[1][1], FIELD_TYPE.DOUBLE)

    def test_projection_after_relational_limit_only_scores_selected_rows(self):
        prompts = [f"project-limit-{index}" for index in range(35)]
        self.load_inputs(prompts)
        keys = [self.rank_reply(prompt, index + 0.5) for index, prompt in enumerate(prompts)]
        statement = self.statement("a.id," + self.rank(), suffix="LIMIT 3 OFFSET 2")
        self.explain(statement).one("SEMANTIC RANK")
        before = self.snapshots()
        rows = self.execute(statement)
        self.assertEqual(len(rows), 3)
        self.assertTrue(all(score == index + 0.5 for index, score in rows), rows)
        self.assert_windows(before, first=[keys[index] for index, _score in rows])

    def test_rejected_slots_do_not_end_the_stream_or_starve_a_later_match(self):
        prompts = [f"rejected-{index}" for index in range(4 * BATCH_ROWS + 2)]
        self.load_inputs(prompts)
        for index, prompt in enumerate(prompts):
            self.fixtures.reply(0, prompt, index == 3 * BATCH_ROWS + 1, kind="filter")
        self.assertEqual(self.execute(self.statement("a.id", where=self.call("filter"))),
                         ((3 * BATCH_ROWS + 1,),))
        self.assertEqual(self.first.counts, Counter(prompts))
        for prompt in prompts:
            self.fixtures.reply(0, prompt, False, kind="filter")
        self.assertEqual(self.execute(self.statement("a.id", where=self.call("filter"))), ())
        self.assert_no_model_sockets()

    def test_empty_input_and_limit_zero_do_not_issue_model_requests(self):
        self.load_inputs(["never-request"])
        before = self.snapshots()
        for kwargs in ({"suffix": "LIMIT 0"}, {"where": "a.id<0"}, {"where": "FALSE"}):
            with self.subTest(kwargs=kwargs):
                self.assertEqual(self.execute(self.statement(self.call("map"), **kwargs)), ())
        self.assert_windows(before)

    def test_nested_retry_does_not_block_a_ready_peer_or_duplicate_sql_rows(self):
        self.load_inputs(["retry", "retry", "fast"])
        for caching in ("false", "true"):
            with self.subTest(caching=caching):
                retry = self.fixtures.gate()
                self.fixtures.replies(0, "retry", [
                    Reply(status=429, body=b'{"error":"retry"}', headers={"Retry-After": "0"}),
                    Reply(body=completion("retried", finish_reason="stop"), gate=retry),
                ])
                self.fixtures.reply(0, "fast", "fast-value")
                for text in ("retried", "fast-value"):
                    self.fixtures.reply(1, "outer:" + text, text + "-mapped")
                nested = self.call("map", "CONCAT('outer:'," + self.call("map") + ")", provider=1)
                pending, rows, first = self.stream(self.statement("a.id," + nested, caching=caching))
                self.assertTrue(first.wait(4), f"retry blocked a ready peer: {pending.error!r}")
                self.assertEqual(rows, [(2, "fast-value-mapped")])
                self.assertFalse(retry.is_set())
                retry.set()
                self.assertCountEqual(pending.result(),
                                      ((0, "retried-mapped"), (1, "retried-mapped"),
                                       (2, "fast-value-mapped")))

    def test_case_and_or_skip_invalid_inputs_and_preserve_carried_values(self):
        prompts = [None, "", "safe", "safe", None, ""] * 8
        flags = [0, 0, 1, 1, 0, 0] * 8
        self.load_inputs(prompts, flags=flags)
        predicate = "a.guard_flag=0 OR " + self.call("filter")
        mapped = "CASE WHEN a.guard_flag=1 THEN " + self.call("map", provider=1) + " ELSE 'skip' END"
        score = "CASE WHEN a.guard_flag=1 THEN " + self.rank(provider=1) + " ELSE -1.0 END"
        for caching in ("false", "true"):
            with self.subTest(caching=caching):
                filter_key = self.fixtures.reply(0, "safe", True, kind="filter")
                map_key = self.fixtures.reply(1, "safe", "mapped")
                rank_key = self.rank_reply("safe", 4.25, provider=1)
                repetitions = flags.count(1) if caching == "false" else 1
                fields = f"a.id,a.prompt,{mapped},{score}"
                expected = ((index, prompt, "mapped" if flags[index] else "skip",
                             4.25 if flags[index] else -1.0)
                            for index, prompt in enumerate(prompts))
                self.check_query(self.statement(fields, where=predicate, caching=caching),
                                 expected, first=[filter_key] * repetitions,
                                 second=[map_key, rank_key] * repetitions, ordered=False)
                and_predicate = "a.guard_flag=1 AND " + self.call("filter")
                self.check_query(self.statement("a.id", where=and_predicate, caching=caching),
                                 ((index,) for index, flag in enumerate(flags) if flag),
                                 first=[filter_key] * repetitions, ordered=False)
        for index in (0, 1):
            before = self.snapshots()
            sql_error(self, 1210, lambda: self.execute(
                self.statement(self.call("map"), where=f"a.id={index}")))
            self.assert_windows(before)

    def test_output_datums_survive_recycled_slots_and_large_carried_payloads(self):
        prompts = [f"payload-{index}" for index in range(5 * BATCH_ROWS + 3)]
        self.load_inputs(prompts)
        payloads = {prompt: prompt + ":" + ("x" * (19000 + index))
                    for index, prompt in enumerate(prompts)}
        head = self.fixtures.gate()
        for index, prompt in enumerate(prompts):
            self.fixtures.reply(0, prompt, payloads[prompt],
                                gate=head if index < BATCH_ROWS else None)
        mapped = self.call("map")
        carried = "CONCAT('carried:',a.prompt,REPEAT('c',10001))"
        statement = self.statement(f"a.id,{carried},{mapped},CONCAT('wrapped:',{mapped}),LENGTH({mapped})",
                                   caching="true")
        pending = self.fixtures.start(lambda: self.execute(statement))
        self.wait_requests(self.first, lambda: len(self.first.requests) == len(prompts),
                           pending, "large output did not survive repeated non-head slot reuse")
        self.assertFalse(head.is_set())
        head.set()
        self.assertCountEqual(pending.result(),
                              ((index, "carried:" + prompt + "c" * 10001, payloads[prompt],
                                "wrapped:" + payloads[prompt], len(payloads[prompt]))
                               for index, prompt in enumerate(prompts)))
        self.assert_no_model_sockets()

    def exercise_error_after_ready_row(self, kind):
        self.load_inputs(["bad-head", "valid-peer"])
        for caching in ("false", "true"):
            with self.subTest(kind=kind, caching=caching):
                head = self.fixtures.gate()
                invalid = [Reply(body=completion('{"value":"invalid"}', finish_reason="stop"), gate=head)]
                if kind == "rank":
                    self.rank_replies("bad-head", invalid)
                    self.rank_reply("valid-peer", 3.5)
                    expected = (1, 3.5)
                else:
                    self.fixtures.replies(0, "bad-head", invalid, kind="filter")
                    self.fixtures.reply(0, "valid-peer", True, kind="filter")
                    expected = (1, 1)
                pending, rows, first = self.stream(self.statement(
                    "a.id," + self.call(kind), caching=caching))
                self.assertTrue(first.wait(3), f"no ready peer before error: {pending.error!r}")
                self.assertEqual(rows, [expected])
                self.assertFalse(head.is_set())
                head.set()
                pending.expect_sql_error(self, 4070)
                self.assert_no_model_sockets()
                self.assertEqual(self.execute(self.statement(
                    "a.id," + self.call(kind), where="a.id=1", caching=caching)), (expected,))

    def test_invalid_rank_after_visible_ready_row_is_a_final_sql_error(self):
        self.exercise_error_after_ready_row("rank")

    def test_invalid_filter_after_visible_ready_row_is_a_final_sql_error(self):
        self.exercise_error_after_ready_row("filter")

    def exercise_abort(self, cancel):
        self.load_inputs(["abort-head", "abort-peer"])
        for caching in ("false", "true"):
            with self.subTest(cancel=cancel, caching=caching):
                head = self.fixtures.gate()
                self.cursor.execute("SET ob_query_timeout=%s", (20000000 if cancel else 3000000,))
                self.fixtures.reply(0, "abort-head", "held", gate=head)
                self.fixtures.reply(0, "abort-peer", "visible")
                pending, rows, first = self.stream(self.statement(
                    "a.id," + self.call("map"), caching=caching))
                self.assertTrue(first.wait(2), f"no ready row before abort: {pending.error!r}")
                self.assertEqual(rows, [(1, "visible")])
                if cancel:
                    self.kill_query()
                pending.expect_sql_error(self, 1317 if cancel else 4012)
                self.assertFalse(head.is_set())
                self.assert_no_model_sockets()
                self.cursor.execute("SET ob_query_timeout=20000000")
                self.fixtures.reply(0, "abort-peer", "recovered")
                self.assertEqual(self.execute(self.statement(
                    "a.id," + self.call("map"), where="a.id=1", caching=caching)),
                    ((1, "recovered"),))
                head.set()

    def test_kill_held_first_row_cancels_and_recovers(self):
        self.exercise_abort(True)

    def test_timeout_held_first_row_cancels_and_recovers(self):
        self.exercise_abort(False)

    def test_cache_memory_quota_remains_explicit_and_releases_resources(self):
        statement = cache.sql_statement

        def unordered_statement(fields, **kwargs):
            kwargs["hints"] = kwargs.get("hints", "") + " " + UNORDERED_HINT
            kwargs.setdefault("suffix", "")
            return statement(fields, **kwargs)

        with mock.patch.object(cache, "sql_statement", side_effect=unordered_statement):
            super().test_bounded_non_evicting_cache_reports_quota_failure_and_releases_memory()

    def test_invalid_policy_and_disabled_row_async_fail_before_model_work(self):
        self.load_inputs(["invalid-policy"])
        before = self.snapshots()
        for ordering in ("", "true", "false", "ready", "unordered,ordered"):
            statement = self.statement(self.call("map"), ordering=ordering)
            for prefix in ("EXPLAIN ", ""):
                with self.subTest(ordering=ordering, prefix=prefix):
                    sql_error(self, 1210, lambda: self.execute(prefix + statement))
        for enabled in (None, "false"):
            statement = self.statement(self.call("map"), enabled=enabled)
            for prefix in ("EXPLAIN ", ""):
                with self.subTest(enabled=enabled, prefix=prefix):
                    sql_error(self, 1210, lambda: self.execute(prefix + statement))
        numeric = self.statement(self.call("map")).replace(
            "'semantic_output_order','unordered'", "'semantic_output_order',1")
        for prefix in ("EXPLAIN ", ""):
            sql_error(self, 1210, lambda: self.execute(prefix + numeric))
        self.assert_windows(before)

    def test_policy_is_case_insensitive(self):
        self.load_inputs(["case-policy"])
        key = self.fixtures.reply(0, "case-policy", "value")
        for ordering in ("UnOrDeReD", "ORDERED"):
            with self.subTest(ordering=ordering):
                statement = self.statement(self.call("map"), ordering=ordering)
                self.assertIn("ROW_ASYNC=" + ("UNORDERED_READY" if ordering == "UnOrDeReD"
                                             else "ORDERED_PREFIX"), self.explain(statement).text.upper())
                self.check_query(statement, (("value",),), first=[key])

    def test_unsupported_relational_shapes_fail_explain_and_select_without_model_work(self):
        self.load_inputs(["scope"])
        fields = "a.id," + self.call("map")
        statements = [
            self.statement(fields, suffix="ORDER BY a.id"),
            self.statement(fields, suffix="ORDER BY a.id LIMIT 1"),
            self.statement(fields, suffix="ORDER BY 1 DESC LIMIT 1 OFFSET 1"),
            self.statement(self.rank() + " AS score", suffix="ORDER BY score DESC LIMIT 1"),
            self.statement("a.guard_flag," + self.rank(), suffix="GROUP BY a.guard_flag,a.prompt"),
            self.statement("AVG(" + self.rank() + ")"),
            self.statement("DISTINCT " + self.call("map")),
            self.statement(fields, source="cache_inputs a JOIN cache_right b ON a.id=b.join_id"),
            self.statement("a.id,b.row_id", source="cache_inputs a JOIN cache_right b ON " +
                           self.join_call()),
            self.statement("a.id," + self.call("map"), source="(SELECT * FROM cache_inputs LIMIT 1) a"),
            "WITH source_rows AS (SELECT * FROM cache_inputs) " +
            self.statement(fields, source="source_rows a"),
            self.statement(f"AI_AGG('{self.models[0]}',a.prompt,'Summarize.')"),
            self.statement(self.call("map", "a.prompt", options="a.opts")),
            self.statement("AI_MAP(a.model,a.prompt)"),
            self.statement(fields + ",ROW_NUMBER() OVER (ORDER BY a.id)"),
            self.statement(fields + ",(SELECT MAX(b.row_id) FROM cache_right b)"),
            self.statement(fields, suffix="FOR UPDATE"),
            self.statement(fields) + " UNION ALL SELECT a.id,a.prompt FROM cache_inputs a",
        ]
        before = self.snapshots()
        for statement in statements:
            with self.subTest(sql=statement):
                for prefix in ("EXPLAIN ", ""):
                    sql_error(self, 1235, lambda: self.execute(prefix + statement))
        self.cursor.executemany("INSERT INTO cache_px_inputs VALUES (%s,%s)",
                                [(index, "scope") for index in range(256)])
        px = self.statement(fields, source="cache_px_inputs a", hints="PARALLEL(4)")
        for prefix in ("EXPLAIN ", ""):
            sql_error(self, 1235, lambda: self.execute(prefix + px))
        self.assert_windows(before)


class SemanticUnorderedProxyContracts(row_async.SemanticRowAsyncProxyContracts):
    def statement(self, *, hints=None, suffix="", **kwargs):
        options = {"semantic_output_order": "unordered"}
        options.update(hints or {})
        return super().statement(hints=options, suffix=suffix, **kwargs)

    def test_proxy_filter_fast_later_row_reaches_sql_output(self):
        self.load(["near-0", "near-1", "far-0", "near-1"])
        head = self.fixtures.gate()
        self.main.scenarios[proxy.INSTRUCTION + "near-0"] = [
            Reply(body=completion('{"value":true}', finish_reason="stop"), gate=head)]
        self.main.scenarios[proxy.INSTRUCTION + "near-1"] = [
            Reply(body=completion('{"value":true}', finish_reason="stop"))]
        for text in ("near-0", "near-1"):
            self.main.scenarios["mapped:" + text] = [Reply(body=completion("value:" + text))]
        quote = self.database.connection.escape
        fields = f"id,prompt,AI_MAP({quote(self.models[0])},CONCAT('mapped:',prompt),'{{\"temperature\":0}}')"
        statement = self.statement(fields=fields)
        plan = proxy.render_explain(self.execute("EXPLAIN " + statement))
        self.assertEqual(plan.upper().count("ROW_ASYNC=UNORDERED_READY"), 2)
        pending, rows, first = rank.SemanticRankContracts.stream(self, statement)
        self.assertTrue(first.wait(3), f"proxy head blocked ready SQL rows: {pending.error!r}")
        self.assertTrue(all(row[1] == "near-1" for row in rows), rows)
        self.assertFalse(head.is_set())
        head.set()
        self.assertCountEqual(pending.result(), ((0, "near-0", "value:near-0"),
                                                (1, "near-1", "value:near-1"),
                                                (3, "near-1", "value:near-1")))
        embeddings, completions, _requests = self.work(map_instruction="mapped:")
        self.assertEqual(embeddings, Counter([proxy.RETRIEVAL, "near-0", "near-1", "far-0"]))
        self.assertEqual(completions, Counter([proxy.INSTRUCTION + "near-0",
                                              proxy.INSTRUCTION + "near-1",
                                              "mapped:near-0", "mapped:near-1"]))


def cases():
    return {name: test_class
            for test_class in (SemanticUnorderedContracts, SemanticUnorderedProxyContracts)
            for name in test_class.__dict__ if name.startswith("test_")}


def main(arguments=None):
    inventory = cases()
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--binary", type=Path)
    source.add_argument("--socket", type=Path)
    parser.add_argument("--log-dir", type=Path)
    parser.add_argument("--case", action="append", default=[], choices=sorted(inventory))
    parser.add_argument("--list-cases", action="store_true")
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
        for test_class in set(inventory.values()):
            test_class.database = database
        suite = unittest.TestSuite()
        for test_class in (SemanticUnorderedContracts, SemanticUnorderedProxyContracts):
            suite.addTests(test_class(name) for name in selected if inventory[name] is test_class)
        result = unittest.TextTestRunner(verbosity=2, failfast=True).run(suite)
        return 0 if result.wasSuccessful() else 1
    finally:
        try:
            if options.log_dir is not None:
                cache.retain_deployment_logs(database, options.log_dir)
        finally:
            database.close()
            for test_class in set(inventory.values()):
                test_class.database = None


if __name__ == "__main__":
    raise SystemExit(main())
