"""Strict numeric AI_RANK SQL contracts for SemBench-style per-row scoring.

Scores are finite SQL DOUBLE values, not pairwise ranks or strings cast by SQL.
Ordinary SQL implements AVG, GROUP BY and ORDER BY/LIMIT. All model work uses
loopback fixtures; held responses prove async progress and cancellation.
--binary owns a private database; --socket never stops the supplied server.
"""

import argparse
from collections import Counter
import copy
import os
from pathlib import Path
import threading
import unittest

import pymysql
from pymysql.constants import FIELD_TYPE

import test_semantic_cache as cache
import test_file_runtime as files
import test_semantic_proxy as proxy
import test_semantic_runtime as semantic
from test_file_runtime import PrivateDatabase
from test_runtime_contracts import Reply, completion, sql_error


RANK_FORMAT = {
    "type": "json_schema",
    "json_schema": {
        "name": "ai_rank", "strict": True,
        "schema": {
            "type": "object", "properties": {"value": {"type": "number"}},
            "required": ["value"], "additionalProperties": False,
        },
    },
}
ROW_HINT = "OPT_PARAM('enable_semantic_row_async','true')"


class SemanticRankContracts(cache.SemanticCacheContracts):
    def statement(self, fields, *, hints="PARALLEL(1)", **kwargs):
        return cache.sql_statement(fields, hints=hints, **kwargs)

    def rank(self, prompt="a.prompt", **kwargs):
        return self.call("rank", prompt, **kwargs)

    def rank_replies(self, prompt, replies, *, provider=0, options=None):
        config = dict(options or {})
        config.setdefault("response_format", RANK_FORMAT)
        return self.fixtures.replies(provider, prompt, replies, options=config)

    def rank_reply(self, prompt, value, *, provider=0, options=None, gate=None):
        return self.rank_replies(prompt, [
            Reply(body=completion(cache.json_text({"value": value}), finish_reason="stop"), gate=gate)
        ], provider=provider, options=options)

    def stream(self, statement):
        rows = []
        first = threading.Event()

        def consume():
            with self.database.connection.cursor(pymysql.cursors.SSCursor) as cursor:
                cursor.execute(statement)
                for row in cursor:
                    rows.append(row)
                    first.set()
            return tuple(rows)

        return self.fixtures.start(consume), rows, first

    def test_sembench_q9_scores_are_double_not_text_or_fixed_range(self):
        scores = [1, 2, 3, 4, 5, 4.5, -2.25, 10, 1e100]
        prompts = [f"review-{index}" for index in range(len(scores))]
        self.load_inputs(prompts)
        keys = [self.rank_reply(prompt, value) for prompt, value in zip(prompts, scores)]
        statement = self.statement("a.id," + self.rank() + " AS reviewScore")
        self.explain(statement).one("SEMANTIC RANK")
        self.check_query(statement, list(enumerate(map(float, scores))), first=keys)
        self.assertEqual(self.cursor.description[1][1], FIELD_TYPE.DOUBLE)
        for body in self.first.requests:
            self.assertEqual(body["response_format"], RANK_FORMAT)

    def test_sembench_q10_grouped_average_direct_and_derived(self):
        scores = [1, 5, 3, 2, 4, 6]
        prompts = [f"group-review-{index}" for index in range(len(scores))]
        self.load_inputs(prompts)
        keys = [self.rank_reply(prompt, value) for prompt, value in zip(prompts, scores)]
        direct = self.statement(
            "FLOOR(a.id/3) AS movieId, AVG(" + self.rank() + ") AS movieScore",
            suffix="GROUP BY FLOOR(a.id/3) ORDER BY movieId")
        derived = (
            "SELECT movieId, AVG(reviewScore) FROM (" +
            self.statement("FLOOR(a.id/3) AS movieId," + self.rank() + " AS reviewScore",
                           suffix="") +
            ") scored GROUP BY movieId ORDER BY movieId")
        for statement in (direct, derived):
            with self.subTest(sql=statement):
                self.explain(statement).one("SEMANTIC RANK")
                self.check_query(statement, ((0, 3.0), (1, 4.0)), first=keys)

    def test_order_by_numeric_score_topk_offset_preserves_ties_and_duplicates(self):
        prompts = ["two", "ten", "two", "negative", "ten", "four"]
        scores = {"two": 2.0, "ten": 10.0, "negative": -1.5, "four": 4.0}
        self.load_inputs(prompts)
        keys = {prompt: self.rank_reply(prompt, score) for prompt, score in scores.items()}
        statement = self.statement("a.id," + self.rank() + " AS score",
                                   suffix="ORDER BY score DESC,a.id LIMIT 3 OFFSET 1")
        self.check_query(statement, ((4, 10.0), (5, 4.0), (0, 2.0)),
                         first=[keys[prompt] for prompt in prompts])
        self.assertEqual(len(self.first.requests), len(prompts),
                         "score-dependent LIMIT cannot skip unseen candidates")

    def test_alias_order_and_projection_materialize_scores_once(self):
        self.load_inputs(["alias-a", "alias-b", "alias-c"])
        keys = [self.rank_reply(prompt, score) for prompt, score in
                (("alias-a", 2), ("alias-b", 10), ("alias-c", 4))]
        statement = self.statement("a.id," + self.rank() + " AS score",
                                   suffix="ORDER BY score,a.id")
        self.check_query(statement, ((0, 2.0), (2, 4.0), (1, 10.0)), first=keys)
        self.assertEqual(len(self.first.requests), 3)

    def test_projection_after_relational_limit_only_scores_survivors(self):
        prompts = [f"limit-{index}" for index in range(35)]
        self.load_inputs(prompts)
        keys = [self.rank_reply(prompts[index], index + 0.5) for index in range(2, 5)]
        statement = self.statement("a.id," + self.rank(), suffix="ORDER BY a.id LIMIT 3 OFFSET 2")
        self.check_query(statement, ((2, 2.5), (3, 3.5), (4, 4.5)), first=keys)

    def test_relational_and_semantic_filter_prune_rank_inputs(self):
        prompts = [f"filtered-{index}" for index in range(8)]
        self.load_inputs(prompts)
        filter_keys = [
            self.fixtures.reply(1, prompts[index], index % 2 == 0, kind="filter")
            for index in range(2, 8)
        ]
        rank_keys = [self.rank_reply(prompts[index], index) for index in (2, 4, 6)]
        statement = self.statement("a.id," + self.rank(),
                                   where="a.id>=2 AND " + self.call("filter", provider=1))
        self.check_query(statement, ((2, 2.0), (4, 4.0), (6, 6.0)),
                         first=rank_keys, second=filter_keys)

    def test_semantic_join_filter_then_rank_grouped_average(self):
        self.load_inputs(["style-0", "style-1"], flags=[0, 0])
        images = ["image-0", "image-1", "not-socks"]
        self.cursor.executemany(
            "INSERT INTO cache_right VALUES (%s,%s,%s,%s,%s)",
            [(index, 0, 1, image, image) for index, image in enumerate(images)])
        filter_keys = [self.fixtures.reply(1, image, image != "not-socks", kind="filter")
                       for image in images]
        pair_keys = [
            self.fixtures.reply(1, cache.joins.pair_payload(
                cache.JOIN_RULE, style, image),
                (style, image) != ("style-1", "image-1"), kind="join")
            for style in ("style-0", "style-1") for image in images[:2]
        ]
        rank_keys = [self.rank_reply(prompt, score) for prompt, score in
                     (("style-0/image-0", 2), ("style-0/image-1", 4),
                      ("style-1/image-0", 5))]
        for caching in ("false", "true"):
            with self.subTest(cache=caching):
                statement = self.statement(
                    "a.id,AVG(" + self.rank("CONCAT(a.prompt,'/',b.prompt)") + ")",
                    source="cache_inputs a JOIN cache_right b ON "
                           "a.guard_flag=b.join_id AND " + self.join_call(),
                    where="a.id<2 AND " + self.call("filter", "b.prompt", provider=1),
                    suffix="GROUP BY a.id ORDER BY a.id", cache=caching,
                    hints="LEADING(a b) USE_HASH(b) PARALLEL(1)")
                plan = self.explain(statement)
                for label in ("SEMANTIC JOIN", "SEMANTIC FILTER", "SEMANTIC RANK"):
                    plan.one(label)
                self.check_query(statement, ((0, 3.0), (1, 5.0)),
                                 first=rank_keys, second=filter_keys + pair_keys)

    def test_cached_rank_lifting_reduces_calls_and_keeps_join_bag(self):
        prompts, right = self.load_multiplying()
        keys = {prompt: self.rank_reply(prompt, 1 if prompt == "shared-2" else 4)
                for prompt in dict.fromkeys(prompts)}
        expected = [(index, category) for _row, index, keep, _prompt, category in right
                    if keep and prompts[index] != "shared-2"]
        for caching, enabled_lift in (("false", "true"), ("true", "false"), ("true", "true")):
            with self.subTest(cache=caching, lift=enabled_lift):
                statement = self.filter_join(cache=caching, enabled_lift=enabled_lift,
                                             predicate=self.rank() + ">=3")
                plan = self.explain(statement)
                if caching == enabled_lift == "true":
                    cache.lift.assert_lifted(self, plan)
                    demanded = list(dict.fromkeys(prompts[::4]))
                else:
                    if enabled_lift == "true":
                        cache.assert_kept(self, plan)
                    else:
                        cache.lift.assert_below_join(self, plan)
                    demanded = list(dict.fromkeys(prompts)) if caching == "true" else prompts
                self.check_query(statement, expected, first=[keys[prompt] for prompt in demanded])
        rounded = self.filter_join(cache="true", enabled_lift="true",
                                   predicate=self.rank() + ">=9007199254740993")
        cache.lift.assert_lifted(self, self.explain(rounded))
        self.check_query(rounded, (), first=[keys[prompt] for prompt in
                                           dict.fromkeys(prompts[::4])])
        unsafe_cast = self.filter_join(cache="true", enabled_lift="true",
                                       predicate="CAST(" + self.rank() + " AS SIGNED)>=3")
        cache.assert_kept(self, self.explain(unsafe_cast))
        self.check_query(unsafe_cast, expected, first=list(keys.values()))

    def test_rank_predicate_uses_numeric_comparison(self):
        self.load_inputs(["low", "middle", "high"])
        keys = [self.rank_reply(prompt, score) for prompt, score in
                (("low", 2), ("middle", 4), ("high", 10))]
        statement = self.statement("a.id", where=self.rank() + ">=4")
        self.explain(statement).one("SEMANTIC FILTER")
        self.check_query(statement, ((1,), (2,)), first=keys)

    def test_case_guard_skips_invalid_inputs_and_constant_calls_stay_row_local(self):
        self.load_inputs(["valid", None, "valid"], flags=[1, 0, 1])
        key = self.rank_reply("valid", 4.25)
        statement = self.statement(
            "a.id,CASE WHEN a.guard_flag=1 THEN " + self.rank() + " ELSE -1 END")
        self.check_query(statement, ((0, 4.25), (1, -1.0), (2, 4.25)), first=[key, key])
        key = self.rank_reply("constant", 3)
        self.check_query(self.statement("a.id," + self.rank("'constant'")),
                         ((0, 3.0), (1, 3.0), (2, 3.0)), first=[key] * 3)

    def test_map_to_rank_and_rank_to_cpu_to_map_dependencies(self):
        self.load_inputs(["source"])
        mapped = self.fixtures.reply(0, "source", "prepared")
        ranked = self.rank_reply("prepared", 4.5, provider=1)
        expression = self.rank(self.call("map"), provider=1)
        self.check_query(self.statement("a.id," + expression), ((0, 4.5),),
                         first=[mapped], second=[ranked])
        ranked = self.rank_reply("source", 2.75)
        mapped = self.fixtures.reply(1, "bucket:2", "second-stage")
        expression = self.call(
            "map", "CONCAT('bucket:',CAST(FLOOR(" + self.rank() + ") AS CHAR))", provider=1)
        self.check_query(self.statement("a.id," + expression), ((0, "second-stage"),),
                         first=[ranked], second=[mapped])

    def test_cache_coalesces_duplicate_scores_without_removing_sql_rows(self):
        prompts = ["shared-a" if index % 2 else "shared-b" for index in range(35)]
        self.load_inputs(prompts)
        keys = {prompt: self.rank_reply(prompt, value)
                for prompt, value in (("shared-a", 1.25), ("shared-b", 4.5))}
        expected = [(index, 1.25 if prompt == "shared-a" else 4.5)
                    for index, prompt in enumerate(prompts)]
        for caching in ("false", "true"):
            with self.subTest(caching=caching):
                calls = list(keys) if caching == "true" else prompts
                self.check_query(self.statement("a.id," + self.rank(), cache=caching),
                                 expected, first=[keys[prompt] for prompt in calls])

    def test_cache_separates_rank_from_map_even_for_identical_wire_requests(self):
        self.load_inputs(["equal-wire"] * 3)
        rank_key = self.rank_reply("equal-wire", 3.5)
        options = self.cursor.mogrify("%s", (cache.json_text({"response_format": RANK_FORMAT}),))
        mapped = self.call("map", options=options)
        self.check_query(
            self.statement("a.id," + self.rank() + "," + mapped, cache="true"),
            [(index, 3.5, '{"value":3.5}') for index in range(3)],
            first=[rank_key, rank_key])

    def test_matching_schema_and_generation_options_are_preserved(self):
        self.load_inputs(["custom-schema"])
        response_format = copy.deepcopy(RANK_FORMAT)
        response_format["json_schema"]["name"] = "review_score"
        options = {"response_format": response_format, "temperature": 0.25, "max_tokens": 128}
        key = self.rank_reply("custom-schema", 4.125, options=options)
        sql_options = self.cursor.mogrify("%s", (cache.json_text(options),))
        self.check_query(self.statement("a.id," + self.rank(options=sql_options)),
                         ((0, 4.125),), first=[key])
        self.assertEqual(self.first.requests[-1]["response_format"], response_format)

    def test_conflicting_schema_reserved_options_and_proxy_are_errors(self):
        self.load_inputs(["invalid-options"])
        conflicts = [
            {"Schema": {}}, {"response_format": {"type": "json_object"}},
            {"semantic_proxy": {"model": "embedding", "query": "x", "threshold": 0}},
        ]
        for path, value in (("type", "boolean"), ("strict", False), ("name", "")):
            response_format = copy.deepcopy(RANK_FORMAT)
            if path == "type":
                response_format["json_schema"]["schema"]["properties"]["value"]["type"] = value
            else:
                response_format["json_schema"][path] = value
            conflicts.append({"response_format": response_format})
        for config in conflicts:
            with self.subTest(config=config):
                before = self.snapshots()
                options = self.cursor.mogrify("%s", (cache.json_text(config),))
                sql_error(self, 1210, lambda: self.execute(
                    self.statement(self.rank(options=options))))
                self.assert_windows(before)

    def test_static_model_options_and_demanded_null_inputs_fail_explicitly(self):
        self.load_inputs(["valid"])
        for expression in (self.call("rank").replace(f"'{self.models[0]}'", "a.model"),
                           self.rank(options="a.opts")):
            before = self.snapshots()
            for prefix in ("EXPLAIN ", ""):
                sql_error(self, 1235, lambda: self.execute(prefix + self.statement(expression)))
            self.assert_windows(before)
        for expression in (self.rank("NULL"), self.rank(options="NULL"),
                           self.rank(options="'[]'"),
                           f"AI_RANK(NULL,a.prompt)", f"AI_RANK('',a.prompt)"):
            before = self.snapshots()
            sql_error(self, 1210, lambda: self.execute(self.statement(expression)))
            self.assert_windows(before)

    def test_malformed_numeric_results_are_errors_with_and_without_cache(self):
        self.load_inputs(["bad-score"])
        invalid = [
            "4.5", '{"value":"4.5"}', '{"value":true}', '{"value":null}',
            '{"value":4,"extra":0}', '{"value":4,"value":5}',
            '{"value":1e309}', '{"value":1e-400}', '{"value":NaN}', "{",
        ]
        for caching in ("false", "true"):
            for content in invalid:
                with self.subTest(caching=caching, content=content):
                    self.rank_replies("bad-score", [
                        Reply(body=completion(content, finish_reason="stop"))
                    ])
                    expected = 3140 if content in ('{"value":1e309}', '{"value":NaN}', "{") else 4070
                    sql_error(self, expected, lambda: self.execute(
                        self.statement(self.rank(), cache=caching)))
                    self.assert_no_model_sockets()
            key = self.rank_reply("bad-score", 2.5)
            self.check_query(self.statement(self.rank(), cache=caching), ((2.5,),), first=[key])

    def test_refusal_and_truncated_scores_are_not_cached_successes(self):
        self.load_inputs(["incomplete-score"])
        for caching in ("false", "true"):
            for reply in (completion('{"value":4.5}', finish_reason="length"),
                          completion('{"value":4.5}', finish_reason="stop", refusal="refused")):
                with self.subTest(cache=caching, reply=reply):
                    key = self.rank_replies("incomplete-score", [Reply(body=reply)])
                    before = self.snapshots()
                    sql_error(self, 4070, lambda: self.execute(
                        self.statement(self.rank(), cache=caching)))
                    self.assert_windows(before, first=[key])
                    self.assert_no_model_sockets()
            key = self.rank_reply("incomplete-score", 3.75)
            self.check_query(self.statement(self.rank(), cache=caching), ((3.75,),), first=[key])

    def test_empty_input_and_limit_zero_do_no_model_work(self):
        self.load_inputs(["unneeded"])
        for statement in (
                self.statement(self.rank(), where="a.id<0"),
                self.statement(self.rank(), suffix="LIMIT 0"),
                self.statement("AVG(" + self.rank() + ")", where="a.id<0", suffix="")):
            before = self.snapshots()
            expected = ((None,),) if "AVG(" in statement else ()
            self.assertEqual(self.execute(statement), expected)
            self.assert_windows(before)

    def test_batches_overlap_before_any_score_reply(self):
        prompts = [f"overlap-{index}" for index in range(35)]
        self.load_inputs(prompts)
        gate = self.fixtures.gate()
        keys = [self.rank_reply(prompt, index, gate=gate) for index, prompt in enumerate(prompts)]
        before = self.snapshots()
        pending = self.fixtures.start(lambda: self.execute(self.statement("a.id," + self.rank())))
        semantic.wait_evidence(
            self, self.first, lambda: len(self.first.requests) >= 2 * semantic.BATCH_ROWS,
            pending, message="second rank batch did not overlap the held first batch")
        self.assertFalse(gate.is_set())
        self.assertFalse(pending.done.is_set(), repr(pending.error))
        gate.set()
        self.assertEqual(pending.result(), tuple((i, float(i)) for i in range(len(prompts))))
        self.assert_windows(before, first=keys)

    def test_row_async_small_numeric_prefix_is_visible_to_client(self):
        self.load_inputs(["prefix", "tail"])
        for caching in ("false", "true"):
            with self.subTest(caching=caching):
                head, tail = self.fixtures.gate(), self.fixtures.gate()
                offset = len(self.first.requests)
                self.rank_reply("prefix", 4.25, gate=head)
                self.rank_reply("tail", 2.5, gate=tail)
                statement = self.statement("a.id," + self.rank(), suffix="",
                                           hints="PARALLEL(1) " + ROW_HINT, cache=caching)
                plan = self.explain(statement)
                plan.one("SEMANTIC RANK")
                self.assertIn("ROW_ASYNC=ORDERED_PREFIX", plan.text.upper())
                pending, rows, first = self.stream(statement)
                semantic.wait_evidence(
                    self, self.first, lambda: len(self.first.requests) >= offset + 2,
                    pending, message="both score rows did not enter one request group")
                head.set()
                self.assertTrue(first.wait(3), f"score prefix was buffered: {pending.error!r}")
                self.assertEqual(rows, [(0, 4.25)])
                self.assertFalse(tail.is_set())
                self.assertFalse(pending.done.is_set(), repr(pending.error))
                tail.set()
                self.assertEqual(pending.result(), ((0, 4.25), (1, 2.5)))

    def test_retry_preserves_scores_and_does_not_resend_successful_requests(self):
        self.load_inputs(["retry", "peer"])
        retry_key = self.rank_replies("retry", [
            Reply(status=429, body=b'{"error":"retry"}', headers={"Retry-After": "0"}),
            Reply(body=completion('{"value":4.5}', finish_reason="stop")),
        ])
        peer_key = self.rank_reply("peer", 2)
        self.check_query(self.statement("a.id," + self.rank()),
                         ((0, 4.5), (1, 2.0)), first=[retry_key, retry_key, peer_key])

    def test_invalid_score_after_visible_prefix_remains_a_sql_error(self):
        self.load_inputs(["valid-prefix", "bad-tail"])
        for caching in ("false", "true"):
            with self.subTest(cache=caching):
                tail = self.fixtures.gate()
                self.rank_reply("valid-prefix", 3.5)
                self.rank_replies("bad-tail", [
                    Reply(body=completion('{"value":"not-a-score"}', finish_reason="stop"), gate=tail)
                ])
                statement = self.statement("a.id," + self.rank(), suffix="",
                                           hints="PARALLEL(1) " + ROW_HINT, cache=caching)
                pending, rows, first = self.stream(statement)
                self.assertTrue(first.wait(3), f"no numeric prefix: {pending.error!r}")
                self.assertEqual(rows, [(0, 3.5)])
                self.assertFalse(tail.is_set())
                tail.set()
                pending.expect_sql_error(self, 4070)
                self.assert_no_model_sockets()
                self.assertEqual(self.execute(self.statement(
                    self.rank(), where="a.id=0", cache=caching)), ((3.5,),))

    def test_cancel_held_rank_and_recover_without_releasing_provider(self):
        self.load_inputs(["cancel"])
        gate = self.fixtures.gate()
        self.rank_reply("cancel", 1, gate=gate)
        pending = self.fixtures.start(lambda: self.execute(
            self.statement(self.rank(), cache="true")))
        semantic.wait_evidence(self, self.first, lambda: len(self.first.requests) == 1,
                               pending, message="rank request did not reach the provider")
        self.kill_query()
        pending.expect_sql_error(self, 1317)
        self.assertFalse(gate.is_set())
        self.assert_no_model_sockets()
        self.rank_reply("cancel", 5)
        self.assertEqual(self.execute(self.statement(self.rank(), cache="true")), ((5.0,),))

    def test_local_px_rank_shares_cache_and_releases_workers(self):
        if self.database.server_log is None:
            self.skipTest("socket mode needs server logs for actual PX task/thread evidence")
        prompt = "shared-px-score"
        self.cursor.executemany("INSERT INTO cache_px_inputs VALUES (%s,%s)",
                                [(index, prompt) for index in range(256)])
        statement = self.statement(
            "a.id," + self.rank(), source="cache_px_inputs a", suffix="",
            hints="PARALLEL(4)", cache="true")
        plan = self.explain(statement)
        plan.one("PX COORDINATOR")
        plan.one("SEMANTIC RANK")
        self.assertIn("DOP=4", plan.text.upper())
        gate = self.fixtures.gate()
        key = self.rank_reply(prompt, 4.5, gate=gate)
        pending, before = self.start_held_px(statement, key, gate)
        gate.set()
        self.assertEqual(Counter(pending.result(timeout=20)),
                         Counter((index, 4.5) for index in range(256)))
        self.assert_windows(before, first=[key])

    def test_file_input_keeps_media_blocks_and_numeric_output(self):
        if not isinstance(self.database, PrivateDatabase):
            self.skipTest("media policy changes are restricted to the owned private database")
        media = files.MediaFiles()
        self.addCleanup(media.close)
        root = self.execute("SELECT @@GLOBAL.ai_media_root")[0][0]
        self.addCleanup(self.cursor.execute, "SET GLOBAL ai_media_root=%s", (root,))
        self.cursor.execute("SET GLOBAL ai_media_root=%s", (str(media.root),))
        asset = media.asset("png", 54)
        model = self.models[0] + "_media"
        endpoint = model + "_endpoint"
        self.cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL(%s,%s)", (
            model, cache.json_text({
                "type": "completion", "model_name": "mock-rank-media",
                "input_modalities": ["text", "image"],
            }),
        ))
        self.created_models.append(model)
        self.cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT(%s,%s)", (
            endpoint, cache.json_text({
                "ai_model_name": model, "provider": "openai",
                "request_model_name": "mock-rank-media",
                "access_key": "loopback-rank-fixture-only",
                "url": f"http://127.0.0.1:{self.first.server_port}/v1/chat/completions",
            }),
        ))
        self.created_endpoints.append(endpoint)
        content = [files.text_block("Score image relevance: "), asset.block()]
        self.first.RequestHandlerClass = files.FileHandler
        self.first.scenarios = {files.content_key(content): [
            Reply(body=completion('{"value":9.5}', finish_reason="stop"))
        ]}
        descriptor = self.cursor.mogrify("%s", (files.descriptor_text(asset.descriptor()),))
        expression = f"AI_RANK('{model}',AI_INPUT('Score image relevance: ',TO_FILE({descriptor})))"
        statement = "SELECT /*+ PARALLEL(1) OPT_PARAM('rowsets_max_rows',16) */ " + expression
        self.assertEqual(self.execute(statement), ((9.5,),))
        self.assertEqual(self.cursor.description[0][1], FIELD_TYPE.DOUBLE)
        self.assertEqual(len(self.first.requests), 1)
        files.assert_content(self, self.first.requests[0], content, assets=[asset],
                             forbidden_paths=[asset.path])
        self.assertEqual(self.first.requests[0]["response_format"], RANK_FORMAT)
        opted_in = statement.replace("/*+", "/*+ " + ROW_HINT, 1)
        sql_error(self, 1235, lambda: self.execute(opted_in))
        self.assertEqual(len(self.first.requests), 1)


class SemanticRankProxyContracts(proxy.SemanticProxySmoke):
    def test_filter_proxy_then_numeric_rank_only_scores_kept_rows(self):
        self.load(["near-0", "far-0", "main-no", "near-0"])
        for prompt in ("near-0",):
            self.main.scenarios["score:" + prompt] = [
                Reply(body=completion('{"value":4.5}', finish_reason="stop"))
            ]
        expression = f"AI_RANK('{self.models[0]}',AI_INPUT('score:',prompt),'{{\"temperature\":0}}')"
        for row_async in (False, True):
            with self.subTest(row_async=row_async):
                before = (len(self.main.requests), len(self.embedding.requests))
                statement = self.statement(fields="id," + expression)
                if row_async:
                    statement = statement.replace("/*+", "/*+ " + ROW_HINT, 1)
                plan = proxy.render_explain(self.execute("EXPLAIN " + statement)).upper()
                self.assertIn("SEMANTIC RANK", plan)
                self.assertIn("EMBEDDING_PROXY=REJECT_ONLY", plan)
                self.assertEqual(self.execute(statement), ((0, 4.5), (3, 4.5)))
                main = self.main.requests[before[0]:]
                self.assertEqual(Counter(body["messages"][0]["content"] for body in main), Counter([
                    proxy.INSTRUCTION + "near-0", proxy.INSTRUCTION + "main-no", "score:near-0",
                ]))
                ranked = [body for body in main if body["messages"][0]["content"] == "score:near-0"]
                self.assertEqual(ranked[0]["response_format"], RANK_FORMAT)


def cases():
    return {name: test_class for test_class in (SemanticRankContracts, SemanticRankProxyContracts)
            for name in test_class.__dict__ if name.startswith("test_")}


def main(arguments=None):
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--binary", type=Path)
    source.add_argument("--socket", type=Path)
    parser.add_argument("--log-dir", type=Path)
    inventory = cases()
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
        for test_class in (SemanticRankContracts, SemanticRankProxyContracts):
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
