"""Deterministic serial FILE row-asynchronous MAP/FILTER/RANK SQL contracts.

Only --binary starts services, using the existing owned private DB launcher.
--list-cases is offline; repeat --case to select contracts. All media is
synthetic and all HTTP endpoints are loopback storage/provider fixtures.

Held partial downloads and provider replies must remain held while independent
rows advance and, for unordered output, become visible through SSCursor.
Default/ordered output retains its prefix. Assertions compare exact SQL bags,
request identities and query-scoped download counts, not HTTP arrival order.
Auto permits ready FILE scores below full sort/exact Top-N, but preserves final
SQL order and selected-row demand for FILE projection after ORDER BY/LIMIT.
AUTO also composes FILE progress with hash grouping, streaming DISTINCT and a
retained semantic probe of an ordinary INNER HASH JOIN.
FILE is completely downloaded and validated before a provider request; these
contracts do not describe byte/token streaming or FILE model-result caching.
"""

import argparse
from collections import Counter
import os
from pathlib import Path
import unittest

import pymysql
from pymysql.constants import FIELD_TYPE

import test_file_runtime as files
import test_remote_file_runtime as remote
import test_semantic_cache as cache
import test_semantic_output_policy as output_policy
import test_semantic_rank as rank
import test_semantic_runtime as semantic


BATCH_ROWS = semantic.BATCH_ROWS
STATIC_OPTIONS = {"temperature": 0.25}


class SemanticFileRowAsyncContracts(remote.RemoteFileContracts):
    models = (remote.MODEL_A, remote.MODEL_B)
    stream = rank.SemanticRankContracts.stream
    assert_auto_policy = output_policy.SemanticOutputPolicyContracts.assert_policy

    def setUp(self):
        super().setUp()
        self.wire_expectations = {server: set() for server in self.fixtures.mocks}
        self.cursor.execute("SET ai_pipeline_memory_limit=DEFAULT")
        self.configure(local=True)

    def close_case(self):
        super().close_case()
        for server in self.fixtures.mocks:
            for body in server.requests:
                self.assertIn(cache.json_text(body), self.wire_expectations[server],
                              "provider request changed its model, options or native schema")

    def literal(self, value):
        return self.cursor.mogrify("%s", (value,))

    def call(self, kind, expression="a.asset", *, provider=0, options=None):
        options_sql = None if options is None else self.literal(files.descriptor_text(options))
        return cache.SemanticCacheContracts.call(
            self, kind, expression, provider=provider, options=options_sql)

    def statement(self, fields, *, ordering="unordered", caching="false",
                  where=None, suffix="", source="remote_values a", extra_hints="",
                  enabled_lift=None):
        hints = "PARALLEL(1) OPT_PARAM('enable_semantic_row_async','true')"
        if ordering is not None:
            hints += " OPT_PARAM('semantic_output_order'," + self.literal(ordering) + ")"
        if extra_hints:
            hints += " " + extra_hints
        return cache.sql_statement(
            fields, source=source, hints=hints, cache=caching, enabled_lift=enabled_lift,
            where=where, suffix=suffix)

    def reply(self, provider, content, value, *, assets=(), kind="map",
              options=None, gate=None):
        server = self.fixtures.mocks[provider]
        reply = self.seed(server, content, value, assets=assets, boolean=kind != "map")
        reply.gate = gate
        expected_options = dict(options or {})
        if kind == "filter":
            expected_options["response_format"] = semantic.MATCHING_FILTER_FORMAT
        elif kind == "rank":
            expected_options["response_format"] = rank.RANK_FORMAT
        elif kind != "map":
            raise ValueError("FILE fixture kind must be map, filter or rank")
        key = cache.json_text({
            "model": "mock-remote-media",
            "messages": [{"role": "user", "content": content}],
            **expected_options,
        })
        self.wire_expectations[server].add(key)
        return key

    def work(self):
        providers = []
        for server in self.fixtures.mocks:
            with server.condition:
                self.assertFalse(server.fixture_errors, server.fixture_errors)
                providers.append(tuple(cache.json_text(body) for body in server.requests))
        self.assertFalse(self.storage.fixture_errors, self.storage.fixture_errors)
        return tuple(providers), self.storage.counts()

    def assert_work(self, before, *, first=(), second=(), downloads=()):
        providers, storage = self.work()
        for old, new, expected in zip(before[0], providers, (first, second)):
            self.assertEqual(new[:len(old)], old, "provider history changed within a query window")
            self.assertEqual(Counter(new[len(old):]), Counter(expected),
                             "wrong provider request identities or duplicate demand")
        self.assertEqual(storage, before[1] + Counter(("GET", target) for target in downloads),
                         "wrong query-scoped GET count, unexpected HEAD/retry, or skipped FILE I/O")

    def explain(self, statement):
        before = self.work()
        plan = cache.lift.ExplainPlan(cache.lift.render_explain(
            self.execute("EXPLAIN " + statement)))
        self.assert_work(before)
        return plan

    def assert_async_plan(self, statement, *, ordering="unordered", kinds=("MAP",)):
        plan = self.explain(statement).text.upper()
        policy = "UNORDERED_READY" if ordering == "unordered" else "ORDERED_PREFIX"
        self.assertIn("ROW_ASYNC=" + policy, plan)
        for kind in kinds:
            self.assertIn("SEMANTIC " + kind, plan)
        self.assertNotIn("PX COORDINATOR", plan)
        return plan

    def wait_provider(self, provider, key, pending, before, *, responded=False):
        server = self.fixtures.mocks[provider]
        offset = len(before[0][provider])
        semantic.wait_evidence(
            self, server,
            lambda: any(cache.json_text(record["body"]) == key and
                        (not responded or record["response"] is not None)
                        for record in server.audit[offset:]),
            pending, message="the required row-dependent provider work did not advance")

    def wait_download(self, asset, pending, *, before_gate=None):
        self.storage.wait(
            lambda: pending.done.is_set() or any(
                record["target"] == asset.target and
                (before_gate is None or record["before_gate"] == before_gate)
                for record in self.storage.requests),
            "the held FILE download did not start")
        self.assertTrue(any(
            record["target"] == asset.target and
            (before_gate is None or record["before_gate"] == before_gate)
            for record in self.storage.requests),
            f"query finished before its held download: {pending.error!r}")

    def assert_unrequested(self, provider, key, before):
        self.assertNotIn(key, self.work()[0][provider][len(before[0][provider]):],
                         "an incomplete input reached its dependent provider request")

    def assert_visible_peer(self, pending, rows, first, expected, *gates):
        self.assertTrue(all(not gate.is_set() for gate in gates), "a hold gate was released early")
        self.assertTrue(first.wait(3), f"ready SQL row remained buffered: {pending.error!r}")
        self.assertTrue(all(not gate.is_set() for gate in gates), "SQL evidence required gate release")
        self.assertEqual(tuple(rows), (expected,))
        self.assertFalse(pending.done.is_set(), repr(pending.error))

    def test_auto_file_rank_before_full_sort_and_exact_topn(self):
        assets = [self.storage.asset(seed=600 + index) for index in range(5)]
        scores = (2.0, 10.0, -2.5, 10.0, 0.125)
        expected = ((1, 10.0), (3, 10.0), (0, 2.0), (4, 0.125), (2, -2.5))
        self.insert([(index, f"score-{index}:", asset.descriptor(
            include_size=True, version=asset.version())) for index, asset in enumerate(assets)])
        for label, suffix, wanted in (
                ("SORT", "ORDER BY score DESC,a.id", expected),
                ("TOP-N SORT", "ORDER BY score DESC,a.id LIMIT 3 OFFSET 1", expected[1:4])):
            with self.subTest(sort=label):
                head = self.fixtures.gate()
                self.storage.routes[assets[0].target].gate = head
                self.storage.routes[assets[0].target].before_gate = len(assets[0].raw) - 1
                keys = [
                    self.reply(0, [files.text_block(f"score-{index}:"), asset.block()], score,
                               assets=[asset], kind="rank", options=STATIC_OPTIONS)
                    for index, (asset, score) in enumerate(zip(assets, scores))
                ]
                statement = self.statement(
                    "a.id," + self.call("rank", "AI_INPUT(a.text_value,a.asset)",
                                       options=STATIC_OPTIONS) + " AS score",
                    ordering="auto", caching="true", suffix=suffix)
                plan = self.explain(statement)
                stage = plan.one("SEMANTIC RANK")
                self.assert_auto_policy(stage, "UNORDERED_READY", "SORT_RESTORES_ORDER")
                self.assertTrue(plan.is_above(plan.one(label), stage), plan.text)
                self.assertNotIn("PX COORDINATOR", plan.text.upper())
                before = self.work()
                pending, rows, first = self.stream(statement)
                self.wait_download(assets[0], pending, before_gate=len(assets[0].raw) - 1)
                for key in keys[1:]:
                    self.wait_provider(0, key, pending, before, responded=True)
                self.assert_unrequested(0, keys[0], before)
                self.assertFalse(head.is_set())
                self.assertFalse(first.wait(0.15), "sort exposed rows before every candidate was scored")
                self.assertEqual(rows, [])
                self.assertFalse(pending.done.is_set(), repr(pending.error))
                head.set()
                result = pending.result()
                self.assertEqual(result, wanted)
                self.assertTrue(all(type(score) is float for _index, score in result))
                self.assert_work(before, first=keys, downloads=[asset.target for asset in assets])
                self.assert_no_sockets()

    def test_auto_file_map_after_sorted_limit_preserves_selected_order_and_demand(self):
        download_gate, provider_gate = self.fixtures.gate(), self.fixtures.gate()
        selected = (4, 3)
        assets = {
            4: self.storage.asset(seed=610, gate=download_gate),
            3: self.storage.asset("jpeg", 611),
        }
        self.storage.routes[assets[4].target].before_gate = len(assets[4].raw) - 1
        descriptors = {
            index: files.descriptor(self.media.root / f"unselected-{index}-must-not-open.png")
            for index in range(6)
        }
        descriptors.update({index: asset.descriptor(include_size=True, version=asset.version())
                            for index, asset in assets.items()})
        self.insert([(index, f"selected-{index}:", descriptors[index]) for index in range(6)])
        contents = {index: [files.text_block(f"selected-{index}:"), assets[index].block()]
                    for index in selected}
        keys = {
            index: self.reply(0, contents[index], f"mapped-{index}", assets=[assets[index]],
                              options=STATIC_OPTIONS, gate=provider_gate if index == 4 else None)
            for index in selected
        }
        statement = self.statement(
            "a.id," + self.call("map", "AI_INPUT(a.text_value,a.asset)", options=STATIC_OPTIONS),
            ordering="auto", caching="true", suffix="ORDER BY a.id DESC LIMIT 2 OFFSET 1")
        plan = self.explain(statement)
        self.assert_auto_policy(plan.one("SEMANTIC MAP"), "ORDERED_PREFIX", "FINAL_SQL_ORDER")
        self.assertNotIn("PX COORDINATOR", plan.text.upper())
        before = self.work()
        pending, rows, first = self.stream(statement)
        self.wait_download(assets[4], pending, before_gate=len(assets[4].raw) - 1)
        self.wait_provider(0, keys[3], pending, before, responded=True)
        self.assert_unrequested(0, keys[4], before)
        self.assertFalse(download_gate.is_set())
        self.assertFalse(provider_gate.is_set())
        self.assertFalse(first.wait(0.15), "late FILE projection broke final SQL order")
        self.assertEqual(rows, [])
        self.assertFalse(pending.done.is_set(), repr(pending.error))
        download_gate.set()
        self.wait_provider(0, keys[4], pending, before)
        semantic.assert_held(self, self.first, [provider_gate], {files.content_key(contents[4])})
        self.assertFalse(first.wait(0.15), "ordered FILE projection bypassed a held provider")
        self.assertEqual(rows, [])
        self.assertFalse(pending.done.is_set(), repr(pending.error))
        provider_gate.set()
        self.assertEqual(pending.result(), tuple((index, f"mapped-{index}") for index in selected))
        self.assert_work(before, first=keys.values(),
                         downloads=[assets[index].target for index in selected])
        self.assert_no_sockets()

    def test_auto_file_rank_hash_grouping_remains_blocking(self):
        head = self.fixtures.gate()
        assets = [self.storage.asset(seed=620 + index) for index in range(2)]
        self.insert([(index, f"group-{index}:", asset.descriptor())
                     for index, asset in enumerate(assets)])
        keys = [
            self.reply(0, [files.text_block(f"group-{index}:"), asset.block()], score,
                       assets=[asset], kind="rank", options=STATIC_OPTIONS,
                       gate=head if index == 0 else None)
            for index, (asset, score) in enumerate(zip(assets, (2.0, 10.0)))
        ]
        score = self.call("rank", "AI_INPUT(a.text_value,a.asset)", options=STATIC_OPTIONS)
        statement = self.statement(
            "a.id DIV 2 AS bucket,COUNT(*),AVG(" + score + ")", ordering="auto",
            extra_hints="USE_HASH_AGGREGATION", suffix="GROUP BY a.id DIV 2 ORDER BY bucket",
            enabled_lift="true")
        plan = self.explain(statement)
        stage = plan.one("SEMANTIC RANK")
        self.assert_auto_policy(stage, "UNORDERED_READY", "HASH_AGGREGATE_INPUT")
        self.assertTrue(plan.is_above(plan.one("HASH GROUP BY"), stage), plan.text)
        before = self.work()
        pending, rows, first = self.stream(statement)
        self.wait_provider(0, keys[0], pending, before)
        self.wait_provider(0, keys[1], pending, before, responded=True)
        self.assertFalse(head.is_set())
        self.assertFalse(first.wait(0.15), "hash grouping exposed an incomplete group")
        self.assertEqual(rows, [])
        self.assertFalse(pending.done.is_set(), repr(pending.error))
        head.set()
        self.assertEqual(pending.result(), ((0, 2, 6.0),))
        self.assert_work(before, first=keys, downloads=[asset.target for asset in assets])
        self.assert_no_sockets()

    def test_auto_file_hash_distinct_retains_seen_values_across_download_wait(self):
        head = self.fixtures.gate()
        assets = [self.storage.asset(seed=630, gate=head), self.storage.asset("jpeg", 631)]
        self.storage.routes[assets[0].target].before_gate = len(assets[0].raw) - 1
        self.insert([(index, f"distinct-{index}:", asset.descriptor())
                     for index, asset in enumerate(assets)])
        keys = [
            self.reply(0, [files.text_block(f"distinct-{index}:"), asset.block()], "same",
                       assets=[asset])
            for index, asset in enumerate(assets)
        ]
        statement = self.statement(
            "DISTINCT " + self.call("map", "AI_INPUT(a.text_value,a.asset)"),
            ordering="auto", caching="true", extra_hints="USE_HASH_DISTINCT", enabled_lift="true")
        plan = self.explain(statement)
        stage = plan.one("SEMANTIC MAP")
        self.assert_auto_policy(stage, "UNORDERED_READY", "HASH_DISTINCT_INPUT")
        self.assertTrue(plan.is_above(plan.one("HASH DISTINCT"), stage), plan.text)
        before = self.work()
        pending, rows, first = self.stream(statement)
        self.wait_download(assets[0], pending, before_gate=len(assets[0].raw) - 1)
        self.wait_provider(0, keys[1], pending, before, responded=True)
        self.assert_unrequested(0, keys[0], before)
        self.assert_visible_peer(pending, rows, first, ("same",), head)
        head.set()
        self.assertEqual(pending.result(), (("same",),))
        self.assert_work(before, first=keys, downloads=[asset.target for asset in assets])
        self.assert_no_sockets()

    def test_auto_file_probe_hash_join_exposes_ready_text_map(self):
        head = self.fixtures.gate()
        assets = [self.storage.asset(seed=640, gate=head), self.storage.asset("jpeg", 641)]
        self.storage.routes[assets[0].target].before_gate = len(assets[0].raw) - 1
        self.insert([(index, f"probe-{index}:", asset.descriptor())
                     for index, asset in enumerate(assets)])
        vision_keys = [
            self.reply(0, [files.text_block(f"probe-{index}:"), asset.block()], f"seen-{index}",
                       assets=[asset])
            for index, asset in enumerate(assets)
        ]
        text_keys = [
            self.reply(1, f"joined:seen-{index}", f"joined-{index}")
            for index in range(2)
        ]
        vision = self.call("map", "AI_INPUT(a.text_value,a.asset)")
        source = ("remote_values b JOIN (SELECT id," + vision +
                  " AS vision_value FROM remote_values a) p ON b.id=p.id")
        statement = self.statement(
            "p.id,b.text_value," +
            self.call("map", "CONCAT('joined:',p.vision_value)", provider=1),
            ordering="auto", source=source,
            extra_hints="LEADING(b p) USE_HASH(p) NO_SWAP_JOIN_INPUTS(p) NO_MERGE(p)",
            enabled_lift="true")
        plan = self.explain(statement)
        self.assertTrue(plan.is_above(plan.one("HASH JOIN"), plan.one("SUBPLAN SCAN")), plan.text)
        self.assertIn("HASH_JOIN_PROBE", plan.text)
        self.assertNotIn("PX COORDINATOR", plan.text.upper())
        before = self.work()
        pending, rows, first = self.stream(statement)
        self.wait_download(assets[0], pending, before_gate=len(assets[0].raw) - 1)
        self.wait_provider(1, text_keys[1], pending, before, responded=True)
        self.assert_unrequested(0, vision_keys[0], before)
        self.assert_unrequested(1, text_keys[0], before)
        self.assert_visible_peer(pending, rows, first, (1, "probe-1:", "joined-1"), head)
        head.set()
        self.assertCountEqual(pending.result(), tuple(
            (index, f"probe-{index}:", f"joined-{index}") for index in range(2)))
        self.assert_work(before, first=vision_keys, second=text_keys,
                         downloads=[asset.target for asset in assets])
        self.assert_no_sockets()

    def test_held_remote_download_allows_unordered_image_and_sql_output(self):
        head = self.fixtures.gate()
        held = self.storage.asset(seed=100, gate=head)
        fast = self.storage.asset("jpeg", 101)
        self.storage.routes[held.target].before_gate = len(held.raw) - 1
        self.insert([(0, "held:", held.descriptor(include_size=True, version=held.version())),
                     (1, "fast:", fast.descriptor(include_size=True, version=fast.version()))])
        held_key = self.reply(0, [files.text_block("held:"), held.block()], "slow",
                              assets=[held], options=STATIC_OPTIONS)
        fast_key = self.reply(0, [files.text_block("fast:"), fast.block()], "fast",
                              assets=[fast], options=STATIC_OPTIONS)
        statement = self.statement(
            "a.id," + self.call("map", "AI_INPUT(a.text_value,a.asset)", options=STATIC_OPTIONS),
            caching="true")
        self.assert_async_plan(statement)
        before = self.work()
        pending, rows, first = self.stream(statement)
        self.wait_download(held, pending, before_gate=len(held.raw) - 1)
        self.wait_provider(0, fast_key, pending, before, responded=True)
        self.assert_unrequested(0, held_key, before)
        self.assert_visible_peer(pending, rows, first, (1, "fast"), head)
        head.set()
        self.assertCountEqual(pending.result(), ((0, "slow"), (1, "fast")))
        self.assert_work(before, first=[held_key, fast_key], downloads=[held.target, fast.target])
        self.assert_no_sockets()

    def test_default_and_ordered_keep_prefix_while_later_work_starts(self):
        assets = [self.storage.asset(seed=110 + index) for index in range(2)]
        self.insert([(index, f"vision-{index}:", asset.descriptor())
                     for index, asset in enumerate(assets)])
        for ordering in (None, "ordered"):
            with self.subTest(ordering=ordering):
                head = self.fixtures.gate()
                vision_keys = [
                    self.reply(0, [files.text_block(f"vision-{index}:"), asset.block()],
                               f"seen-{index}", assets=[asset], gate=head if index == 0 else None)
                    for index, asset in enumerate(assets)
                ]
                text_keys = [self.reply(1, f"follow:seen-{index}", f"mapped-{index}")
                             for index in range(2)]
                inner = self.call("map", "AI_INPUT(a.text_value,a.asset)")
                outer = self.call("map", "CONCAT('follow:'," + inner + ")", provider=1)
                statement = self.statement("a.id," + outer, ordering=ordering, caching="true")
                self.assert_async_plan(statement, ordering=ordering)
                before = self.work()
                pending, rows, first = self.stream(statement)
                self.wait_provider(0, vision_keys[0], pending, before)
                self.wait_provider(1, text_keys[1], pending, before, responded=True)
                self.assert_unrequested(1, text_keys[0], before)
                self.assertFalse(head.is_set())
                self.assertFalse(first.wait(0.15), "ordered output bypassed its held prefix")
                self.assertEqual(rows, [])
                self.assertFalse(pending.done.is_set(), repr(pending.error))
                head.set()
                self.assertEqual(pending.result(), ((0, "mapped-0"), (1, "mapped-1")))
                self.assert_work(before, first=vision_keys, second=text_keys,
                                 downloads=[asset.target for asset in assets])
                self.assert_no_sockets()

    def test_slow_vision_head_allows_later_text_map_and_rank(self):
        assets = [self.storage.asset(seed=120 + index) for index in range(2)]
        self.insert([(index, f"vision-{index}:", asset.descriptor())
                     for index, asset in enumerate(assets)])
        for kind in ("map", "rank"):
            for caching in ("false", "true"):
                with self.subTest(kind=kind, caching=caching):
                    head = self.fixtures.gate()
                    vision_keys = [
                        self.reply(0, [files.text_block(f"vision-{index}:"), asset.block()],
                                   f"seen-{index}", assets=[asset],
                                   gate=head if index == 0 else None)
                        for index, asset in enumerate(assets)
                    ]
                    results = ("mapped-0", "mapped-1") if kind == "map" else (-2.5, 10.25)
                    text_keys = [
                        self.reply(1, f"follow:seen-{index}", value, kind=kind,
                                   options=STATIC_OPTIONS)
                        for index, value in enumerate(results)
                    ]
                    inner = self.call("map", "AI_INPUT(a.text_value,a.asset)")
                    outer = self.call(kind, "CONCAT('follow:'," + inner + ")", provider=1,
                                      options=STATIC_OPTIONS)
                    statement = self.statement("a.id," + outer, caching=caching)
                    plan = self.assert_async_plan(statement, kinds=(kind.upper(),))
                    self.assertIn("AI_MAP(", plan, "the nested vision recipe disappeared")
                    before = self.work()
                    pending, rows, first = self.stream(statement)
                    self.wait_provider(0, vision_keys[0], pending, before)
                    self.wait_provider(1, text_keys[1], pending, before, responded=True)
                    self.assert_unrequested(1, text_keys[0], before)
                    self.assert_visible_peer(pending, rows, first, (1, results[1]), head)
                    head.set()
                    self.assertCountEqual(pending.result(), tuple(enumerate(results)))
                    self.assert_work(before, first=vision_keys, second=text_keys,
                                     downloads=[asset.target for asset in assets])
                    semantic.assert_dependencies(
                        self, semantic.audit(self.first)[len(before[0][0]):],
                        semantic.audit(self.second)[len(before[0][1]):],
                        {files.content_key(f"follow:seen-{index}"):
                         files.content_key([files.text_block(f"vision-{index}:"), asset.block()])
                         for index, asset in enumerate(assets)})
                    self.assert_no_sockets()

    def test_file_filter_prunes_text_map_and_rank_without_head_blocking(self):
        assets = [self.storage.asset(seed=130 + index) for index in range(3)]
        self.insert([(index, f"kept-{index}", asset.descriptor())
                     for index, asset in enumerate(assets)])
        for kind in ("map", "rank"):
            for ordering in ("ordered", "unordered"):
                with self.subTest(kind=kind, ordering=ordering):
                    head = self.fixtures.gate()
                    filter_keys = [
                        self.reply(0, [files.text_block("keep:"), asset.block()], index != 1,
                                   assets=[asset], kind="filter", options=STATIC_OPTIONS,
                                   gate=head if index == 0 else None)
                        for index, asset in enumerate(assets)
                    ]
                    results = {0: "mapped-0", 2: "mapped-2"} if kind == "map" else {0: -1.5, 2: 2.5}
                    text_keys = {
                        index: self.reply(1, f"kept-{index}", value, kind=kind)
                        for index, value in results.items()
                    }
                    predicate = self.call(
                        "filter", "AI_INPUT('keep:',a.asset)", options=STATIC_OPTIONS)
                    statement = self.statement(
                        "a.id," + self.call(kind, "a.text_value", provider=1),
                        where=predicate, ordering=ordering, caching="true")
                    self.assert_async_plan(
                        statement, ordering=ordering, kinds=("FILTER", kind.upper()))
                    before = self.work()
                    pending, rows, first = self.stream(statement)
                    self.wait_provider(0, filter_keys[0], pending, before)
                    self.wait_provider(0, filter_keys[2], pending, before, responded=True)
                    self.assert_unrequested(1, text_keys[0], before)
                    self.assertFalse(head.is_set())
                    if ordering == "unordered":
                        self.wait_provider(1, text_keys[2], pending, before, responded=True)
                        self.assert_visible_peer(pending, rows, first, (2, results[2]), head)
                    else:
                        self.assertFalse(first.wait(0.15), "ordered FILTER bypassed its held prefix")
                        self.assertEqual(rows, [])
                        self.assertFalse(pending.done.is_set(), repr(pending.error))
                    head.set()
                    result = pending.result()
                    self.assertCountEqual(result, tuple(results.items()))
                    if ordering == "ordered":
                        self.assertEqual(result, tuple(results.items()))
                    self.assert_work(before, first=filter_keys, second=text_keys.values(),
                                     downloads=[asset.target for asset in assets])
                    self.assert_no_sockets()

    def test_non_head_slot_reuses_window_while_first_slot_is_held(self):
        count = 5 * BATCH_ROWS + 3
        head = self.fixtures.gate()
        assets = [self.storage.asset(seed=200 + index) for index in range(count)]
        self.insert([(index, f"slot-{index}:", asset.descriptor())
                     for index, asset in enumerate(assets)])
        keys = [
            self.reply(0, [files.text_block(f"slot-{index}:"), asset.block()], f"value-{index}",
                       assets=[asset], gate=head if index < BATCH_ROWS else None)
            for index, asset in enumerate(assets)
        ]
        statement = self.statement(
            "a.id," + self.call("map", "AI_INPUT(a.text_value,a.asset)"), caching="true")
        self.assert_async_plan(statement)
        before = self.work()
        pending, rows, first = self.stream(statement)
        for key in keys[:BATCH_ROWS]:
            self.wait_provider(0, key, pending, before)
        self.wait_provider(0, keys[4 * BATCH_ROWS], pending, before)
        self.assertFalse(head.is_set())
        self.assertTrue(first.wait(3), f"recycled slots did not yield SQL rows: {pending.error!r}")
        self.assertTrue(all(index >= BATCH_ROWS for index, _value in tuple(rows)), rows)
        self.assertFalse(pending.done.is_set(), repr(pending.error))
        with self.first.condition:
            self.assertLessEqual(
                sum(record["response"] is None for record in self.first.audit),
                2 * BATCH_ROWS, "unanswered provider requests exceeded the two retained slots")
        head.set()
        result = pending.result()
        self.assertEqual(len(result), count)
        self.assertCountEqual(result, ((index, f"value-{index}") for index in range(count)))
        self.assert_work(before, first=keys, downloads=[asset.target for asset in assets])
        self.assert_no_sockets()

    def test_query_cache_bypasses_file_results_but_shares_text_and_media(self):
        asset = self.storage.asset(seed=300)
        count = 2 * BATCH_ROWS + 3
        self.insert([(index, "duplicate", asset.descriptor()) for index in range(count)])
        for kind in ("map", "filter", "rank"):
            for caching in ("false", "true"):
                with self.subTest(kind=kind, caching=caching):
                    value = {"map": "file-value", "filter": True, "rank": 2.5}[kind]
                    file_key = self.reply(
                        0, [files.text_block("file:"), asset.block()], value,
                        assets=[asset], kind=kind, options=STATIC_OPTIONS)
                    text_key = self.reply(1, "text:duplicate", "text-value")
                    fields = ("a.id," +
                              self.call(kind, "AI_INPUT('file:',a.asset)", options=STATIC_OPTIONS) +
                              "," + self.call("map", "CONCAT('text:',a.text_value)", provider=1))
                    statement = self.statement(fields, caching=caching)
                    before = self.work()
                    self.assertCountEqual(
                        self.execute(statement), ((index, value, "text-value") for index in range(count)))
                    self.assert_work(
                        before, first=[file_key] * count,
                        second=[text_key] * (1 if caching == "true" else count),
                        downloads=[asset.target])
                    self.assert_no_sockets()

    def test_case_and_or_skip_invalid_file_without_io(self):
        valid = self.storage.asset(seed=310)
        missing = files.descriptor(self.media.root / "must-not-be-opened.png",
                                   mime_type="image/png")
        unregistered = {
            "format_version": 1,
            "uri": self.storage.root + "must-not-be-fetched?signature=skip-only",
            "mime_type": "image/png",
        }
        self.insert([(0, "skip-local", missing), (1, "skip-remote", unregistered),
                     (2, "safe", valid.descriptor())])
        for mixed in (False, True):
            with self.subTest(mixed=mixed):
                expression = "AI_INPUT('guard:',a.asset)" if mixed else "a.asset"
                content = ([files.text_block("guard:"), valid.block()] if mixed else [valid.block()])
                map_key = self.reply(0, content, "mapped", assets=[valid])
                rank_key = self.reply(1, content, 4.25, assets=[valid], kind="rank")
                fields = (
                    "a.id,CASE WHEN a.id=2 THEN " + self.call("map", expression) +
                    " ELSE 'skip' END,CASE WHEN a.id=2 THEN " +
                    self.call("rank", expression, provider=1) + " ELSE -1.0 END")
                before = self.work()
                self.assertCountEqual(
                    self.execute(self.statement(fields, caching="true")),
                    ((0, "skip", -1.0), (1, "skip", -1.0), (2, "mapped", 4.25)))
                self.assert_work(before, first=[map_key], second=[rank_key], downloads=[valid.target])
                filter_key = self.reply(0, content, True, assets=[valid], kind="filter")
                for predicate, expected in (
                        ("a.id=2 AND " + self.call("filter", expression), ((2,),)),
                        ("a.id<>2 OR " + self.call("filter", expression), ((0,), (1,), (2,)))):
                    before = self.work()
                    self.assertCountEqual(
                        self.execute(self.statement("a.id", where=predicate, caching="true")), expected)
                    self.assert_work(before, first=[filter_key], downloads=[valid.target])
                self.assert_no_sockets()

    def test_limit_zero_and_empty_inputs_do_no_work(self):
        remote_invalid = {
            "format_version": 1,
            "uri": self.storage.root + "empty-input-must-not-fetch?signature=skip-only",
            "mime_type": "image/png",
        }
        local_invalid = files.descriptor(self.media.root / "empty-input-must-not-open.png")
        self.insert([(0, "unused", remote_invalid), (1, "unused", local_invalid)])
        before = self.work()
        for expression in ("a.asset", "AI_INPUT('never:',a.asset)"):
            for kind in ("map", "filter", "rank"):
                fields = "a.id," + self.call(kind, expression, options=STATIC_OPTIONS)
                for kwargs in ({"suffix": "LIMIT 0"}, {"where": "a.id<0"}, {"where": "FALSE"}):
                    with self.subTest(expression=expression, kind=kind, kwargs=kwargs):
                        self.assertEqual(self.execute(self.statement(fields, caching="true", **kwargs)), ())
        self.cursor.execute("TRUNCATE TABLE remote_values")
        for kind in ("map", "filter", "rank"):
            self.assertEqual(self.execute(self.statement(
                self.call(kind, "AI_INPUT('empty:',a.asset)"), caching="true")), ())
        self.assert_work(before)
        self.assert_no_sockets()

    def test_mixed_text_media_parts_preserve_exact_order_and_bytes(self):
        assets = [self.storage.asset("jpeg", 320), self.storage.asset("png", 321)]
        audio = self.media.asset("wav", 322)
        self.insert([(index, f"middle-{index}", asset.descriptor())
                     for index, asset in enumerate(assets)])
        keys = []
        for index, asset in enumerate(assets):
            content = [
                files.text_block("prefix:"), asset.block(), files.text_block(f"middle-{index}"),
                audio.block(), files.text_block(":repeat:"), asset.block(), files.text_block(":tail"),
            ]
            keys.append(self.reply(0, content, f"mixed-{index}", assets=[asset, audio, asset],
                                   options=STATIC_OPTIONS))
        audio_sql = "TO_FILE(" + self.literal(files.descriptor_text(audio.descriptor())) + ")"
        expression = ("AI_INPUT('prefix:',a.asset,a.text_value," + audio_sql +
                      ",':repeat:',a.asset,':tail')")
        statement = self.statement(
            "a.id," + self.call("map", expression, options=STATIC_OPTIONS), caching="true")
        self.assert_async_plan(statement)
        before = self.work()
        self.assertCountEqual(self.execute(statement), ((0, "mixed-0"), (1, "mixed-1")))
        self.assert_work(before, first=keys, downloads=[asset.target for asset in assets])
        self.assert_no_sockets()

    def test_file_rank_returns_native_double_with_small_numeric_scores(self):
        assets = [self.media.asset("png", 330), self.storage.asset("jpeg", 331),
                  self.storage.asset("png", 332)]
        values = (-2.5, 10.0, 0.125)
        self.insert([(index, "rank", asset.descriptor())
                     for index, asset in enumerate(assets)])
        for mixed in (False, True):
            for ordering in (None, "ordered", "unordered"):
                with self.subTest(mixed=mixed, ordering=ordering):
                    keys = [
                        self.reply(0, [files.text_block("score:"), asset.block()] if mixed
                                   else [asset.block()], value, assets=[asset], kind="rank",
                                   options=STATIC_OPTIONS)
                        for asset, value in zip(assets, values)
                    ]
                    expression = "AI_INPUT('score:',a.asset)" if mixed else "a.asset"
                    statement = self.statement(
                        "a.id," + self.call("rank", expression, options=STATIC_OPTIONS),
                        ordering=ordering, caching="true")
                    self.assert_async_plan(statement, ordering=ordering, kinds=("RANK",))
                    before = self.work()
                    rows = self.execute(statement)
                    self.assertEqual(self.cursor.description[1][1], FIELD_TYPE.DOUBLE)
                    self.assertTrue(all(type(value) is float for _index, value in rows))
                    self.assertCountEqual(rows, tuple(enumerate(values)))
                    if ordering != "unordered":
                        self.assertEqual(rows, tuple(enumerate(values)))
                    self.assert_work(before, first=keys, downloads=[asset.target for asset in assets
                                                                  if isinstance(asset, remote.RemoteAsset)])
                    self.assert_no_sockets()

    def exercise_provider_error(self, kind):
        assets = [self.storage.asset(seed=400 + index) for index in range(2)]
        self.insert([(index, "error", asset.descriptor()) for index, asset in enumerate(assets)])
        head = self.fixtures.gate()
        bad_key = self.reply(0, [assets[0].block()], "invalid-native-value", assets=[assets[0]],
                             kind=kind, gate=head)
        value = 3.5 if kind == "rank" else True
        peer_key = self.reply(0, [assets[1].block()], value, assets=[assets[1]], kind=kind)
        statement = (self.statement("a.id," + self.call("rank"), caching="true")
                     if kind == "rank" else
                     self.statement("a.id", where=self.call("filter"), caching="true"))
        expected = (1, 3.5) if kind == "rank" else (1,)
        self.assert_async_plan(statement, kinds=(kind.upper(),))
        before = self.work()
        pending, rows, first = self.stream(statement)
        self.wait_provider(0, bad_key, pending, before)
        self.wait_provider(0, peer_key, pending, before, responded=True)
        self.assert_visible_peer(pending, rows, first, expected, head)
        head.set()
        error = pending.expect_sql_error(self, 4070)
        self.assertTrue(error.args[1], "provider validation error needs a final SQL diagnostic")
        self.assert_work(before, first=[bad_key, peer_key],
                         downloads=[asset.target for asset in assets])
        self.assert_no_sockets()
        recovery = (self.statement("a.id," + self.call("rank"), where="a.id=1", caching="true")
                    if kind == "rank" else
                    self.statement("a.id", where="a.id=1 AND " + self.call("filter"), caching="true"))
        before = self.work()
        self.assertEqual(self.execute(recovery), (expected,))
        self.assert_work(before, first=[peer_key], downloads=[assets[1].target])
        self.assert_no_sockets()

    def test_invalid_filter_after_visible_peer_is_final_sql_error(self):
        self.exercise_provider_error("filter")

    def test_invalid_rank_after_visible_peer_is_final_sql_error(self):
        self.exercise_provider_error("rank")

    def test_download_validation_error_after_visible_peer_never_calls_model(self):
        head = self.fixtures.gate()
        bad = self.storage.asset(seed=410, gate=head)
        fast = self.storage.asset(seed=411)
        self.storage.routes[bad.target].before_gate = len(bad.raw) - 1
        descriptor = bad.descriptor(include_size=True, version="sha256:" + "0" * 64)
        self.insert([(0, "bad-hash", descriptor), (1, "valid", fast.descriptor())])
        bad_key = self.reply(0, [bad.block()], "must-not-be-requested", assets=[bad])
        fast_key = self.reply(0, [fast.block()], "visible", assets=[fast])
        statement = self.statement("a.id," + self.call("map"), caching="true")
        before = self.work()
        pending, rows, first = self.stream(statement)
        self.wait_download(bad, pending, before_gate=len(bad.raw) - 1)
        self.wait_provider(0, fast_key, pending, before, responded=True)
        self.assert_unrequested(0, bad_key, before)
        self.assert_visible_peer(pending, rows, first, (1, "visible"), head)
        head.set()
        pending.wait()
        self.assertIsInstance(pending.error, pymysql.MySQLError)
        self.assertIsInstance(pending.error.args[0], int)
        self.assertNotIn(pending.error.args[0], (1317, 4012),
                         "invalid media failed only because it was cancelled or timed out")
        self.assertTrue(pending.error.args[1], "invalid media needs an explicit SQL diagnostic")
        self.assertNotIn(bad.uri, str(pending.error), "FILE error leaked its remote URL")
        self.assert_work(before, first=[fast_key], downloads=[bad.target, fast.target])
        self.assert_no_sockets()
        before = self.work()
        self.assertEqual(self.execute(self.statement(
            "a.id," + self.call("map"), where="a.id=1", caching="true")), ((1, "visible"),))
        self.assert_work(before, first=[fast_key], downloads=[fast.target])
        self.assert_no_sockets()

    def exercise_abort(self, cancel):
        download_gate, provider_gate = self.fixtures.gate(), self.fixtures.gate()
        held = self.storage.asset(seed=420, gate=download_gate)
        provider = self.storage.asset(seed=421)
        fast = self.storage.asset(seed=422)
        self.storage.routes[held.target].before_gate = len(held.raw) - 1
        assets = (held, provider, fast)
        self.insert([(index, f"abort-{index}", asset.descriptor())
                     for index, asset in enumerate(assets)])
        held_key = self.reply(0, [held.block()], "not-downloaded", assets=[held])
        provider_key = self.reply(0, [provider.block()], "held-provider", assets=[provider],
                                  gate=provider_gate)
        fast_key = self.reply(0, [fast.block()], "visible", assets=[fast])
        self.cursor.execute("SET ob_query_timeout=%s", (20000000 if cancel else 3000000,))
        statement = self.statement("a.id," + self.call("map"), caching="true")
        before = self.work()
        pending, rows, first = self.stream(statement)
        self.wait_download(held, pending, before_gate=len(held.raw) - 1)
        self.wait_provider(0, provider_key, pending, before)
        self.wait_provider(0, fast_key, pending, before, responded=True)
        self.assert_unrequested(0, held_key, before)
        self.assert_visible_peer(pending, rows, first, (2, "visible"), download_gate, provider_gate)
        if cancel:
            self.kill_query()
        pending.expect_sql_error(self, 1317 if cancel else 4012, timeout=8)
        self.storage.wait(
            lambda: self.storage.active == 0 and any(
                record["target"] == held.target and record["disconnected"]
                for record in self.storage.requests),
            "abort did not close the held download before releasing its gate")
        self.assertFalse(download_gate.is_set())
        self.assertFalse(provider_gate.is_set())
        self.assert_work(before, first=[provider_key, fast_key],
                         downloads=[asset.target for asset in assets])
        self.assert_no_sockets()
        self.cursor.execute("SET ob_query_timeout=20000000")
        fast_key = self.reply(0, [fast.block()], "recovered", assets=[fast])
        before = self.work()
        self.assertEqual(self.execute(self.statement(
            "a.id," + self.call("map"), where="a.id=2", caching="true")), ((2, "recovered"),))
        self.assert_work(before, first=[fast_key], downloads=[fast.target])
        self.assert_no_sockets()
        self.assertFalse(download_gate.is_set(), "recovery released the cancelled download")
        self.assertFalse(provider_gate.is_set(), "recovery released the cancelled provider")

    def test_kill_cancels_download_and_provider_without_releasing_gates(self):
        self.exercise_abort(True)

    def test_timeout_cancels_download_and_provider_without_releasing_gates(self):
        self.exercise_abort(False)

    def test_limit_bounds_media_pressure_and_cancels_held_downloads(self):
        ready = self.fixtures.gate()
        gates = [self.fixtures.gate() for _ in range(6 * BATCH_ROWS + 3)]
        assets = [self.storage.asset(seed=500 + index, gate=gate)
                  for index, gate in enumerate(gates)]
        self.insert([(index, "pressure", asset.descriptor()) for index, asset in enumerate(assets)])
        keys = [
            self.reply(0, [asset.block()], True, assets=[asset], kind="filter", gate=ready)
            for index, asset in enumerate(assets)
        ]
        statement = self.statement(
            "a.id", where=self.call("filter"), caching="true", suffix="LIMIT 1")
        self.assert_async_plan(statement, kinds=("FILTER",))
        before = self.work()
        pending = self.fixtures.start(lambda: self.execute(statement))
        self.storage.wait(lambda: self.storage.active >= 2,
                          "media pressure did not admit independent held downloads")
        with self.storage.condition:
            admitted = {record["target"] for record in self.storage.requests}
            self.assertLessEqual(len(admitted), 4, "held downloads exceeded ai_media_parallel")
        selected = min(index for index, asset in enumerate(assets) if asset.target in admitted)
        self.assertTrue(all(not gate.is_set() for gate in gates))
        gates[selected].set()
        self.wait_provider(0, keys[selected], pending, before)
        self.storage.wait(
            lambda: any(record["target"] == assets[selected].target and record["finished"] is not None
                        for record in self.storage.requests) and
            sum(record["target"] != assets[selected].target for record in self.storage.requests) >= 2,
            "media pressure did not start independent held downloads")
        self.assertTrue(all(index == selected or not gate.is_set()
                            for index, gate in enumerate(gates)))
        self.assertFalse(ready.is_set())
        self.assertFalse(pending.done.is_set(), repr(pending.error))
        with self.storage.condition:
            held = [record for record in self.storage.requests
                    if record["target"] != assets[selected].target]
            self.assertLessEqual(len(held), 4, "held downloads exceeded ai_media_parallel")
            self.assertTrue(all(record["finished"] is None and not record["disconnected"]
                                for record in held), "held downloads ended before cancellation")
            self.assertLessEqual(len(self.storage.requests), 2 * BATCH_ROWS,
                                 "held downloads exceeded the retained-row window")
        ready.set()
        self.assertEqual(pending.result(timeout=5), ((selected,),))
        self.storage.wait(
            lambda: self.storage.active == 0 and all(
                record["target"] == assets[selected].target or record["disconnected"]
                for record in self.storage.requests),
            "LIMIT did not cancel held downloads before gate release")
        self.assertTrue(all(index == selected or not gate.is_set()
                            for index, gate in enumerate(gates)))
        self.assert_no_sockets()
        with self.storage.condition:
            requested = [record["target"] for record in self.storage.requests]
        self.assertEqual(len(requested), len(set(requested)), "a pending FILE was fetched twice")
        self.assertTrue(set(requested).issubset({asset.target for asset in assets[:2 * BATCH_ROWS]}),
                        "LIMIT admitted media beyond the bounded two-slot window")
        self.assert_work(before, first=[keys[selected]], downloads=requested)
        self.assertEqual(self.execute("SELECT 1"), ((1,),))


def cases():
    return sorted(name for name in SemanticFileRowAsyncContracts.__dict__ if name.startswith("test_"))


def main(arguments=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, help="Built seekdb; launch only a new private database")
    parser.add_argument("--log-dir", type=Path, help="Retain logs in a new directory, never DB files")
    parser.add_argument("--case", action="append", default=[], choices=cases())
    parser.add_argument("--list-cases", action="store_true", help="List cases without starting services")
    options = parser.parse_args(arguments)
    selected = list(dict.fromkeys(options.case)) or cases()
    if options.list_cases:
        print("\n".join(selected))
        return 0
    if options.binary is None:
        parser.error("--binary is required unless --list-cases is selected")
    binary = options.binary.resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        parser.error("--binary must be an executable file")
    if options.log_dir is not None and options.log_dir.exists():
        parser.error("--log-dir must be a directory that does not exist yet")
    database = files.PrivateDatabase(binary)
    try:
        database.start()
        SemanticFileRowAsyncContracts.database = database
        SemanticFileRowAsyncContracts.models_created = False
        suite = unittest.TestSuite(SemanticFileRowAsyncContracts(name) for name in selected)
        result = unittest.TextTestRunner(verbosity=2, failfast=True).run(suite)
        return 0 if result.wasSuccessful() else 1
    finally:
        try:
            if options.log_dir is not None:
                cache.retain_deployment_logs(database, options.log_dir)
        finally:
            database.close()
            SemanticFileRowAsyncContracts.database = None


if __name__ == "__main__":
    raise SystemExit(main())
