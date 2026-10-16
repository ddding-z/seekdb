"""Bounded AUTO contracts for ordinary serial nonspill INNER HASH JOIN.

The left/build input is purely relational and completes before semantic probe
work. Only a fresh right/probe fetch may suspend; duplicate bucket chains and
borrowed child datums must survive repeated parent pulls without reevaluation.
Positive cases assert actual ordinary INNER HASH JOINs, exact SQL bags and exact
loopback request windows. Async bag/ownership cases run with caching off and on.

Held providers exercise ready peers, downstream MAP/RANK, errors, cancellation,
deadlines and same-connection recovery. Final SQL order and projection demand
after ORDER BY/LIMIT remain mandatory. Unsupported algorithms, join types,
semantic build inputs, PX, rewrite boundaries and AI_JOIN expansion must reject
before any model call. Default/ordered execution and ordinary AI_JOIN retain
their existing behavior.

--list-cases starts no services. --binary owns a private database; --socket only
owns its connection and uniquely named SQL/model fixtures. No external provider
or build is used. Native capability tests separately cover forced/runtime spill,
shared/adaptive execution, rescans and scalar capability rejection.
"""

import argparse
from collections import Counter
import os
from pathlib import Path
import unittest

import test_semantic_cache as cache
import test_semantic_rank as rank
import test_semantic_runtime as semantic
from test_file_runtime import PrivateDatabase
from test_runtime_contracts import Reply, completion, sql_error


BATCH_ROWS = semantic.BATCH_ROWS
JOIN_HINTS = (
    "PARALLEL(1) LEADING(b a) USE_HASH(a) NO_SWAP_JOIN_INPUTS(a) "
    "OPT_PARAM('enable_topn_runtime_filter','false')"
)
JOIN_SOURCE = "cache_right b JOIN cache_inputs a ON b.join_id=a.guard_flag"


class SemanticRelationalJoinContracts(rank.SemanticRankContracts):
    def statement(self, fields, *, ordering="auto", enabled="true", caching="false",
                  suffix="", hints=JOIN_HINTS, source=JOIN_SOURCE, enabled_lift="false",
                  **kwargs):
        if enabled is not None:
            hints += f" OPT_PARAM('enable_semantic_row_async','{enabled}')"
        if ordering is not None:
            hints += f" OPT_PARAM('semantic_output_order','{ordering}')"
        return cache.sql_statement(
            fields, hints=hints, cache=caching, source=source, suffix=suffix,
            enabled_lift=enabled_lift, **kwargs)

    def retained_join_projection(self, fields, *, source=JOIN_SOURCE, where=None):
        inner = cache.sql_statement(
            fields, source=source, where=where, suffix="",
            hints=JOIN_HINTS + " NO_QUERY_TRANSFORMATION NO_MERGE(a)")
        return "(" + inner + ") j"

    def load_build(self, rows):
        """Rows are (row ID, join key, category[, keep flag[, carried text]])."""
        self.cursor.execute("TRUNCATE TABLE cache_right")
        if rows:
            self.cursor.executemany("INSERT INTO cache_right VALUES (%s,%s,%s,%s,%s)", [
                (row[0], row[1], row[3] if len(row) > 3 else 1,
                 row[4] if len(row) > 4 else f"build-{row[0]}", row[2])
                for row in rows
            ])

    def demanded(self, prompts, caching):
        return list(prompts) if caching == "false" else list(dict.fromkeys(prompts))

    def assert_auto(self, node, mode="UNORDERED_READY"):
        annotation = node.annotation.upper()
        self.assertIn("ROW_ASYNC=" + mode, annotation)
        self.assertIn("OUTPUT_POLICY=AUTO", annotation)

    def assert_relational_build(self, plan, join):
        children = sorted(
            (node for node in plan.nodes.values() if node.parent_id == join.operator_id),
            key=lambda node: node.operator_id)
        self.assertEqual(len(children), 2, plan.text)
        build, probe = children
        for node in plan.nodes.values():
            if node.operator.startswith("SEMANTIC "):
                self.assertFalse(node == build or plan.is_above(build, node),
                                 "semantic work entered the build subtree:\n" + plan.text)
        return probe

    def assert_inner_hash(self, plan, *, probe_label=None, probe_stage=None):
        joins = [node for node in plan.nodes.values()
                 if node.operator in ("HASH JOIN", "HASH INNER JOIN")]
        self.assertEqual(len(joins), 1, plan.text)
        join = joins[0]
        self.assertFalse(plan.find("SEMANTIC JOIN"), plan.text)
        self.assertFalse(plan.find("PX COORDINATOR"), plan.text)
        probe = self.assert_relational_build(plan, join)
        if probe_label is not None or probe_stage is not None:
            stage = probe_stage if probe_stage is not None else plan.one(probe_label)
            self.assertTrue(stage == probe or plan.is_above(probe, stage), plan.text)
            self.assertTrue(plan.is_above(join, stage), plan.text)
            self.assert_auto(stage)
        return join

    def assert_rejected(self, statement):
        before = self.snapshots()
        for prefix in ("EXPLAIN ", ""):
            with self.subTest(prefix=prefix, sql=statement):
                error = sql_error(self, 1235, lambda: self.execute(prefix + statement))
                self.assertTrue(error.args[1], "unsupported SQL needs a truthful diagnostic")
        self.assert_windows(before)

    def test_ready_probe_peer_joins_and_reaches_downstream_map_and_rank(self):
        self.load_inputs(["held-probe", "ready-probe"], flags=[0, 1])
        self.load_build([(10, 0, "slow"), (11, 1, "fast")])
        mapped = self.call("map", "CONCAT(b.category,'/',a.prompt)", provider=1)
        source = self.retained_join_projection(
            "b.row_id AS build_id,a.id AS probe_id," + mapped + " AS joined_value",
            where=self.call("filter"))
        scored = self.rank("CONCAT('rank:',j.joined_value)", provider=1)
        for caching in ("false", "true"):
            with self.subTest(caching=caching):
                gate = self.fixtures.gate()
                filter_keys = [
                    self.fixtures.reply(0, "held-probe", True, kind="filter", gate=gate),
                    self.fixtures.reply(0, "ready-probe", True, kind="filter"),
                ]
                map_keys = [
                    self.fixtures.reply(1, "slow/held-probe", "slow-value"),
                    self.fixtures.reply(1, "fast/ready-probe", "ready-value"),
                ]
                rank_keys = [
                    self.rank_reply("rank:slow-value", 7.25, provider=1),
                    self.rank_reply("rank:ready-value", 2.5, provider=1),
                ]
                statement = self.statement(
                    "j.build_id,j.probe_id,j.joined_value," + scored,
                    source=source, caching=caching,
                    hints="PARALLEL(1) NO_QUERY_TRANSFORMATION NO_MERGE(j)")
                plan = self.explain(statement)
                join = self.assert_inner_hash(plan, probe_label="SEMANTIC FILTER")
                mapper, scorer = plan.one("SEMANTIC MAP"), plan.one("SEMANTIC RANK")
                self.assertTrue(plan.is_above(mapper, join), plan.text)
                self.assertTrue(plan.is_above(scorer, mapper), plan.text)
                self.assert_auto(mapper)
                self.assert_auto(scorer)
                before = self.snapshots()
                pending, rows, first = self.stream(statement)
                self.assertTrue(first.wait(3), f"ready join peer stayed blocked: {pending.error!r}")
                self.assertEqual(rows, [(11, 1, "ready-value", 2.5)])
                self.assertFalse(gate.is_set())
                self.assertFalse(pending.done.is_set(), repr(pending.error))
                gate.set()
                self.assertCountEqual(
                    pending.result(),
                    ((10, 0, "slow-value", 7.25), (11, 1, "ready-value", 2.5)))
                self.assert_windows(before, first=filter_keys, second=map_keys + rank_keys)
                self.assert_no_model_sockets()

    def test_default_and_ordered_probe_keep_their_existing_head_barrier(self):
        self.load_inputs(["ordered-head", "ordered-peer"], flags=[0, 1])
        self.load_build([(10, 0, "head"), (11, 1, "peer")])
        for enabled, ordering in ((None, None), ("true", None), ("true", "ordered")):
            with self.subTest(enabled=enabled, ordering=ordering):
                gate = self.fixtures.gate()
                keys = [
                    self.fixtures.reply(0, "ordered-head", True, kind="filter", gate=gate),
                    self.fixtures.reply(0, "ordered-peer", True, kind="filter"),
                ]
                statement = self.statement(
                    "b.row_id,a.id", where=self.call("filter"), enabled=enabled,
                    ordering=ordering)
                plan = self.explain(statement)
                self.assertEqual(len(plan.find("HASH JOIN")), 1, plan.text)
                stage = plan.one("SEMANTIC FILTER")
                if enabled is not None:
                    self.assertIn("ROW_ASYNC=ORDERED_PREFIX", stage.annotation.upper())
                else:
                    self.assertNotIn("ROW_ASYNC=", stage.annotation.upper())
                self.assertNotIn("OUTPUT_POLICY=AUTO", stage.annotation.upper())
                before = self.snapshots()
                offset = len(self.first.requests)
                pending, rows, first = self.stream(statement)
                semantic.wait_evidence(
                    self, self.first,
                    lambda: any(record["prompt"] == "ordered-peer" and
                                record["response"] is not None
                                for record in semantic.audit(self.first)[offset:]),
                    pending, message="the ordered peer response did not complete")
                self.assertFalse(first.wait(0.15), "default/ordered bypassed the held probe head")
                self.assertEqual(rows, [])
                gate.set()
                self.assertCountEqual(pending.result(), ((10, 0), (11, 1)))
                self.assert_windows(before, first=keys)

    def test_empty_build_or_relationally_empty_build_never_speculates_probe_work(self):
        for caching in ("false", "true"):
            for mode in ("empty-build", "filtered-build", "empty-probe"):
                with self.subTest(caching=caching, mode=mode):
                    self.load_inputs([] if mode == "empty-probe" else ["never-demanded"],
                                     flags=[] if mode == "empty-probe" else [0])
                    self.load_build([] if mode == "empty-build" else [(10, 0, "build", 0)])
                    self.fixtures.reply(0, "never-demanded", True, kind="filter")
                    where = self.call("filter")
                    if mode == "filtered-build":
                        where += " AND b.keep_row=1"
                    statement = self.statement("b.row_id,a.id", where=where, caching=caching)
                    self.assert_inner_hash(self.explain(statement), probe_label="SEMANTIC FILTER")
                    self.check_query(statement, (), ordered=False)
                    self.assert_no_model_sockets()

    def test_many_to_many_duplicates_preserve_exact_bag_and_cache_call_counts(self):
        prompts, flags = ["same", "same", "other", "unmatched"], [0, 0, 1, 7]
        build = [(10, 0, "repeat"), (11, 0, "repeat"), (12, 0, "unique"),
                 (13, 1, "other"), (14, 1, "other"), (15, 9, "unused")]
        self.load_inputs(prompts, flags=flags)
        self.load_build(build)
        pairs = [(row_id, index, category, category + "/" + prompt)
                 for row_id, key, category in build
                 for index, (prompt, flag) in enumerate(zip(prompts, flags)) if key == flag]
        mapped = self.call("map", "CONCAT(b.category,'/',a.prompt)", provider=1)
        for caching in ("false", "true"):
            with self.subTest(caching=caching):
                filter_keys = {prompt: self.fixtures.reply(0, prompt, True, kind="filter")
                               for prompt in dict.fromkeys(prompts)}
                map_keys = {payload: self.fixtures.reply(1, payload, "value:" + payload)
                            for *_columns, payload in pairs}
                statement = self.statement(
                    "b.row_id,a.id,b.category," + mapped, where=self.call("filter"),
                    caching=caching)
                plan = self.explain(statement)
                join = self.assert_inner_hash(plan, probe_label="SEMANTIC FILTER")
                self.assertTrue(plan.is_above(plan.one("SEMANTIC MAP"), join), plan.text)
                self.check_query(
                    statement,
                    [(row_id, index, category, "value:" + payload)
                     for row_id, index, category, payload in pairs],
                    first=[filter_keys[prompt] for prompt in self.demanded(prompts, caching)],
                    second=[map_keys[payload]
                            for payload in self.demanded([row[3] for row in pairs], caching)],
                    ordered=False)

    def test_null_safe_equality_preserves_null_matches_and_duplicate_probe_bags(self):
        prompts = [None, "same", "same", "other"]
        self.load_inputs(prompts, flags=[0] * len(prompts))
        self.load_build([
            (10, 0, "null-key", 1, None),
            (11, 0, "same-key-0", 1, "same"),
            (12, 0, "same-key-1", 1, "same"),
            (13, 0, "other-key", 1, "other"),
        ])
        source = "cache_right b JOIN cache_inputs a ON b.prompt<=>a.prompt"
        predicate = self.call("filter", "COALESCE(a.prompt,'null-key-input')")
        expected = [(10, 0, None), (11, 1, "same"), (11, 2, "same"),
                    (12, 1, "same"), (12, 2, "same"), (13, 3, "other")]
        for caching in ("false", "true"):
            with self.subTest(caching=caching):
                gate = self.fixtures.gate()
                filter_keys = {
                    "null-key-input": self.fixtures.reply(
                        0, "null-key-input", True, kind="filter", gate=gate),
                    "same": self.fixtures.reply(0, "same", True, kind="filter"),
                    "other": self.fixtures.reply(0, "other", True, kind="filter"),
                }
                statement = self.statement(
                    "b.row_id,a.id,a.prompt", source=source, where=predicate, caching=caching)
                self.assert_inner_hash(self.explain(statement), probe_label="SEMANTIC FILTER")
                before = self.snapshots()
                pending, rows, first = self.stream(statement)
                self.assertTrue(first.wait(3), f"ready NSEQ probe stayed blocked: {pending.error!r}")
                self.assertTrue(all(row[1] > 0 for row in rows), rows)
                self.assertFalse(gate.is_set())
                gate.set()
                self.assertCountEqual(pending.result(), expected)
                demanded = ["null-key-input", "same", "same", "other"]
                self.assert_windows(
                    before, first=[filter_keys[prompt]
                                   for prompt in self.demanded(demanded, caching)])

    def test_projection_above_an_ordinary_join_preserves_multiplicity_and_calls(self):
        self.load_inputs(["same", "same", "other"], flags=[0, 0, 1])
        self.load_build([(10, 0, "same-key"), (11, 0, "same-key"), (12, 1, "other-key")])
        payloads = ["same-key/same"] * 4 + ["other-key/other"]
        expected = [(10, 0, "same-result"), (10, 1, "same-result"),
                    (11, 0, "same-result"), (11, 1, "same-result"),
                    (12, 2, "other-result")]
        mapped = self.call("map", "CONCAT(b.category,'/',a.prompt)")
        for caching in ("false", "true"):
            keys = {
                "same-key/same": self.fixtures.reply(0, "same-key/same", "same-result"),
                "other-key/other": self.fixtures.reply(0, "other-key/other", "other-result"),
            }
            statement = self.statement("b.row_id,a.id," + mapped, caching=caching)
            plan = self.explain(statement)
            join = self.assert_inner_hash(plan)
            self.assertTrue(plan.is_above(plan.one("SEMANTIC MAP"), join), plan.text)
            self.assert_auto(plan.one("SEMANTIC MAP"))
            self.check_query(
                statement, expected,
                first=[keys[prompt] for prompt in self.demanded(payloads, caching)],
                ordered=False)

    def test_two_retained_relational_branches_feed_a_real_hash_join(self):
        self.load_inputs(["a", "b", "c"], flags=[0, 1, 0])
        self.load_build([(10, 0, "left"), (11, 1, "right")])
        source = (
            "(SELECT row_id,join_id,category FROM cache_right) b JOIN "
            "(SELECT id,guard_flag,prompt FROM cache_inputs) a ON b.join_id=a.guard_flag"
        )
        mapped = self.call("map", "CONCAT(b.category,'/',a.prompt)")
        values = {(10, 0): "left/a", (10, 2): "left/c", (11, 1): "right/b"}
        for caching in ("false", "true"):
            keys = {payload: self.fixtures.reply(0, payload, "value:" + payload)
                    for payload in values.values()}
            statement = self.statement(
                "b.row_id,a.id," + mapped, source=source, caching=caching,
                hints=JOIN_HINTS + " NO_QUERY_TRANSFORMATION NO_MERGE(a) NO_MERGE(b)")
            plan = self.explain(statement)
            join = self.assert_inner_hash(plan)
            self.assertEqual(len(plan.find("SUBPLAN SCAN")), 2, plan.text)
            self.assertTrue(plan.is_above(plan.one("SEMANTIC MAP"), join), plan.text)
            self.check_query(
                statement, [(row_id, index, "value:" + payload)
                            for (row_id, index), payload in values.items()],
                first=list(keys.values()), ordered=False)

    def test_retained_semantic_probe_branch_suspends_without_rebuilding(self):
        self.load_inputs(["derived-held", "derived-ready"], flags=[0, 1])
        self.load_build([(10, 0, "head"), (11, 1, "peer")])
        predicate = self.call("filter", "prompt")
        source = (
            "cache_right b JOIN (SELECT id,guard_flag,prompt FROM cache_inputs WHERE " +
            predicate + ") a ON b.join_id=a.guard_flag"
        )
        for caching in ("false", "true"):
            gate = self.fixtures.gate()
            keys = [
                self.fixtures.reply(0, "derived-held", True, kind="filter", gate=gate),
                self.fixtures.reply(0, "derived-ready", True, kind="filter"),
            ]
            statement = self.statement(
                "b.row_id,a.id", source=source, caching=caching,
                hints=JOIN_HINTS + " NO_QUERY_TRANSFORMATION NO_MERGE(a)")
            plan = self.explain(statement)
            join = self.assert_inner_hash(plan, probe_label="SEMANTIC FILTER")
            scan = plan.one("SUBPLAN SCAN")
            self.assertTrue(plan.is_above(join, scan), plan.text)
            self.assertTrue(plan.is_above(scan, plan.one("SEMANTIC FILTER")), plan.text)
            before = self.snapshots()
            pending, rows, first = self.stream(statement)
            self.assertTrue(first.wait(3), f"retained probe did not resume: {pending.error!r}")
            self.assertEqual(rows, [(11, 1)])
            self.assertFalse(gate.is_set())
            gate.set()
            self.assertCountEqual(pending.result(), ((10, 0), (11, 1)))
            self.assert_windows(before, first=keys)

    def test_large_carried_datums_and_nested_results_survive_bucket_chain_pulls(self):
        prompts = [f"carried-{index}" for index in range(2 * BATCH_ROWS + 3)]
        self.load_inputs(prompts, flags=[0] * len(prompts))
        build = [(10 + index, 0, f"category-{index}", 1, f"build-{index}:" + "b" * 12001)
                 for index in range(3)]
        self.load_build(build)
        probe_values = {prompt: "probe:" + prompt + ":" + "p" * 19001 for prompt in prompts}
        payloads = {
            category + "/" + probe_values[prompt]: category + "/" + prompt + ":" + "m" * 19001
            for _row, _key, category, _keep, _carried in build for prompt in prompts
        }
        source = (
            "cache_right b JOIN (SELECT id,guard_flag,prompt," + self.call("map", "prompt") +
            " AS mapped FROM cache_inputs) a ON b.join_id=a.guard_flag"
        )
        mapped = self.call("map", "CONCAT(b.category,'/',a.mapped)", provider=1)
        source = self.retained_join_projection(
            "b.row_id AS build_id,a.id AS probe_id,b.prompt AS build_text,"
            "a.mapped AS probe_value," + mapped + " AS joined_value", source=source)
        scored = self.rank("CONCAT('rank:',j.joined_value)", provider=1)
        carried = "CONCAT('carried:',j.probe_value,REPEAT('c',10001))"
        fields = ("j.build_id,j.probe_id,j.build_text,j.probe_value," + carried +
                  ",j.joined_value,CONCAT('wrapped:',j.joined_value)," + scored +
                  ",LENGTH(j.joined_value)")
        for caching in ("false", "true"):
            with self.subTest(caching=caching):
                gate = self.fixtures.gate()
                probe_keys = [
                    self.fixtures.reply(0, prompt, probe_values[prompt],
                                        gate=gate if index < BATCH_ROWS else None)
                    for index, prompt in enumerate(prompts)
                ]
                map_keys = [self.fixtures.reply(1, prompt, value)
                            for prompt, value in payloads.items()]
                rank_keys = [self.rank_reply("rank:" + value, 4.25, provider=1)
                             for value in payloads.values()]
                statement = self.statement(
                    fields, source=source, caching=caching,
                    hints="PARALLEL(1) NO_QUERY_TRANSFORMATION NO_MERGE(j)")
                plan = self.explain(statement)
                maps = plan.find("SEMANTIC MAP")
                self.assertEqual(len(maps), 2, plan.text)
                join = self.assert_inner_hash(plan)
                probe_maps = [node for node in maps if plan.is_above(join, node)]
                self.assertEqual(len(probe_maps), 1, plan.text)
                self.assert_inner_hash(plan, probe_stage=probe_maps[0])
                outer_map = next(node for node in maps if node != probe_maps[0])
                self.assertTrue(plan.is_above(outer_map, join), plan.text)
                self.assertTrue(plan.is_above(plan.one("SEMANTIC RANK"), outer_map), plan.text)
                before = self.snapshots()
                pending, rows, first = self.stream(statement)
                self.assertTrue(first.wait(4), f"large ready output stayed blocked: {pending.error!r}")
                self.assertTrue(all(row[1] >= BATCH_ROWS for row in rows), rows[:1])
                self.assertFalse(gate.is_set())
                gate.set()
                expected = [
                    (row_id, index, build_text, probe_values[prompt],
                     "carried:" + probe_values[prompt] + "c" * 10001,
                     payloads[category + "/" + probe_values[prompt]],
                     "wrapped:" + payloads[category + "/" + probe_values[prompt]], 4.25,
                     len(payloads[category + "/" + probe_values[prompt]]))
                    for row_id, _key, category, _keep, build_text in build
                    for index, prompt in enumerate(prompts)
                ]
                self.assertEqual(Counter(pending.result(timeout=15)), Counter(expected))
                self.assert_windows(before, first=probe_keys, second=map_keys + rank_keys)
                self.assert_no_model_sockets()

    def test_nested_probe_hash_joins_preserve_each_borrowed_duplicate_chain(self):
        prompts = ["nested-head", "nested-peer"]
        self.load_inputs(prompts, flags=[0, 1])
        build = [(10, 0, "b0"), (11, 0, "b1"), (12, 1, "b0"), (13, 1, "b1")]
        self.load_build(build)
        chain = [(20, 0, 1, "c0"), (21, 0, 1, "c1"),
                 (22, 1, 1, "c0"), (23, 1, 1, "c1")]
        self.cursor.executemany("INSERT INTO cache_chain VALUES (%s,%s,%s,%s)", chain)
        source = (
            "cache_chain c JOIN (cache_right b JOIN cache_inputs a "
            "ON b.join_id=a.guard_flag) ON c.join_id=a.id"
        )
        hints = (
            "PARALLEL(1) LEADING(c (b a)) USE_HASH(a) USE_HASH(b) "
            "NO_SWAP_JOIN_INPUTS(a) NO_SWAP_JOIN_INPUTS(b) "
            "OPT_PARAM('enable_topn_runtime_filter','false')"
        )
        pairs = [
            (chain_id, row_id, index, chain_category + "/" + category + "/" + prompts[index])
            for chain_id, index, _keep, chain_category in chain
            for row_id, key, category in build if key == index
        ]
        mapped = self.call("map", "CONCAT(c.category,'/',b.category,'/',a.prompt)", provider=1)
        for caching in ("false", "true"):
            gate = self.fixtures.gate()
            filter_keys = [
                self.fixtures.reply(0, prompts[0], True, kind="filter", gate=gate),
                self.fixtures.reply(0, prompts[1], True, kind="filter"),
            ]
            map_keys = [self.fixtures.reply(1, payload, "value:" + payload)
                        for *_columns, payload in pairs]
            statement = self.statement(
                "c.row_id,b.row_id,a.id," + mapped, source=source, hints=hints,
                where=self.call("filter"), caching=caching)
            plan = self.explain(statement)
            joins = [node for node in plan.nodes.values()
                     if node.operator in ("HASH JOIN", "HASH INNER JOIN")]
            self.assertEqual(len(joins), 2, plan.text)
            stage, mapper = plan.one("SEMANTIC FILTER"), plan.one("SEMANTIC MAP")
            self.assert_auto(stage)
            for join in joins:
                probe = self.assert_relational_build(plan, join)
                self.assertTrue(stage == probe or plan.is_above(probe, stage), plan.text)
                self.assertTrue(plan.is_above(mapper, join), plan.text)
            before = self.snapshots()
            pending, rows, first = self.stream(statement)
            self.assertTrue(first.wait(3), f"nested ready probe stayed blocked: {pending.error!r}")
            self.assertTrue(all(row[2] == 1 for row in rows), rows)
            self.assertFalse(gate.is_set())
            gate.set()
            expected = [(chain_id, row_id, index, "value:" + payload)
                        for chain_id, row_id, index, payload in pairs]
            self.assertCountEqual(pending.result(), expected)
            self.assert_windows(before, first=filter_keys, second=map_keys)

    def test_projection_after_relational_order_and_limit_only_demands_selected_pairs(self):
        prompts = [f"selected-{index}" for index in range(7)]
        self.load_inputs(prompts, flags=list(range(len(prompts))))
        build = [(2 * key + copy, key, f"category-{key}-{copy}")
                 for key in range(len(prompts)) for copy in range(2)]
        self.load_build(build)
        pairs = sorted(
            [(row_id, key, category + "/" + prompts[key]) for row_id, key, category in build],
            key=lambda row: (row[1], row[0]), reverse=True)[2:5]
        mapped = self.call("map", "CONCAT(b.category,'/',a.prompt)")
        for caching in ("false", "true"):
            gate = self.fixtures.gate()
            keys = [
                self.fixtures.reply(0, payload, "value:" + payload,
                                    gate=gate if index == 0 else None)
                for index, (_row, _probe, payload) in enumerate(pairs)
            ]
            statement = self.statement(
                "b.row_id,a.id," + mapped, caching=caching,
                suffix="ORDER BY a.id DESC,b.row_id DESC LIMIT 3 OFFSET 2")
            plan = self.explain(statement)
            join = self.assert_inner_hash(plan)
            mapper = plan.one("SEMANTIC MAP")
            self.assert_auto(mapper, "ORDERED_PREFIX")
            self.assertTrue(plan.is_above(mapper, plan.one("LIMIT")), plan.text)
            self.assertTrue(plan.is_above(plan.one("LIMIT"), join), plan.text)
            before = self.snapshots()
            offset = len(self.first.requests)
            pending, rows, first = self.stream(statement)
            semantic.wait_evidence(
                self, self.first,
                lambda: any(record["prompt"] == pairs[1][2] and record["response"] is not None
                            for record in semantic.audit(self.first)[offset:]),
                pending, message="a later selected pair did not finish")
            self.assertFalse(first.wait(0.15), "projection broke the final SQL pair order")
            self.assertEqual(rows, [])
            self.assertFalse(gate.is_set())
            gate.set()
            self.assertEqual(
                pending.result(), tuple((row_id, index, "value:" + payload)
                                        for row_id, index, payload in pairs))
            self.assert_windows(before, first=keys)

    def test_hash_group_by_after_probe_join_scores_ready_peers_before_full_output(self):
        self.load_inputs(["group-head", "group-peer"], flags=[0, 1])
        self.load_build([(10, 0, "g0"), (11, 0, "g1"),
                         (12, 1, "g0"), (13, 1, "g1")])
        values = {"g0/group-head": 1, "g1/group-head": 3,
                  "g0/group-peer": 5, "g1/group-peer": 7}
        for caching in ("false", "true"):
            gate = self.fixtures.gate()
            filter_keys = [
                self.fixtures.reply(0, "group-head", True, kind="filter", gate=gate),
                self.fixtures.reply(0, "group-peer", True, kind="filter"),
            ]
            rank_keys = [self.rank_reply(prompt, value, provider=1)
                         for prompt, value in values.items()]
            statement = self.statement(
                "a.guard_flag,COUNT(*),AVG(" +
                self.rank("CONCAT(b.category,'/',a.prompt)", provider=1) + ")",
                where=self.call("filter"), caching=caching,
                hints=JOIN_HINTS + " USE_HASH_AGGREGATION",
                suffix="GROUP BY a.guard_flag ORDER BY a.guard_flag")
            plan = self.explain(statement)
            join = self.assert_inner_hash(plan, probe_label="SEMANTIC FILTER")
            group, scorer = plan.one("HASH GROUP BY"), plan.one("SEMANTIC RANK")
            self.assertTrue(plan.is_above(group, scorer), plan.text)
            self.assertTrue(plan.is_above(scorer, join), plan.text)
            self.assert_auto(scorer)
            before = self.snapshots()
            offset = len(self.second.requests)
            pending, rows, first = self.stream(statement)
            semantic.wait_evidence(
                self, self.second,
                lambda: any(record["prompt"] == "g1/group-peer" and
                            record["response"] is not None
                            for record in semantic.audit(self.second)[offset:]),
                pending, message="hash aggregation did not collect a ready join peer")
            self.assertFalse(first.wait(0.15), "a hash group escaped before its full input")
            self.assertEqual(rows, [])
            self.assertFalse(gate.is_set())
            gate.set()
            self.assertEqual(pending.result(), ((0, 2, 2.0), (1, 2, 6.0)))
            self.assert_windows(before, first=filter_keys, second=rank_keys)

    def exercise_probe_error(self, provider_error):
        self.load_inputs(["bad-head", "valid-peer"], flags=[0, 1])
        self.load_build([(10, 0, "head"), (11, 1, "peer")])
        for caching in ("false", "true"):
            with self.subTest(caching=caching, provider_error=provider_error):
                gate = self.fixtures.gate()
                reply = (Reply(status=400, body=b'{"error":"bad model"}', gate=gate)
                         if provider_error else
                         Reply(body=completion('{"value":"invalid"}', finish_reason="stop"),
                               gate=gate))
                keys = [
                    self.fixtures.replies(0, "bad-head", [reply], kind="filter"),
                    self.fixtures.reply(0, "valid-peer", True, kind="filter"),
                ]
                statement = self.statement(
                    "b.row_id,a.id", where=self.call("filter"), caching=caching)
                self.assert_inner_hash(self.explain(statement), probe_label="SEMANTIC FILTER")
                before = self.snapshots()
                pending, rows, first = self.stream(statement)
                self.assertTrue(first.wait(3), f"no joined peer before error: {pending.error!r}")
                self.assertEqual(rows, [(11, 1)])
                self.assertFalse(gate.is_set())
                gate.set()
                pending.expect_sql_error(self, 4216 if provider_error else 4070)
                self.assert_windows(before, first=keys)
                self.assert_no_model_sockets()
                recovered = self.statement(
                    "b.row_id,a.id", where="a.id=1 AND " + self.call("filter"),
                    caching=caching)
                self.check_query(recovered, ((11, 1),), first=[keys[1]], ordered=False)

    def test_invalid_probe_result_after_visible_joined_peer_is_a_final_sql_error(self):
        self.exercise_probe_error(False)

    def test_model_error_after_visible_joined_peer_is_a_final_sql_error(self):
        self.exercise_probe_error(True)

    def exercise_abort(self, cancel):
        self.load_inputs(["abort-head", "abort-peer"], flags=[0, 1])
        self.load_build([(10, 0, "head"), (11, 1, "peer")])
        for caching in ("false", "true"):
            with self.subTest(caching=caching, cancel=cancel):
                gate = self.fixtures.gate()
                self.cursor.execute("SET ob_query_timeout=%s",
                                    (20000000 if cancel else 3000000,))
                self.fixtures.reply(0, "abort-head", True, kind="filter", gate=gate)
                peer = self.fixtures.reply(0, "abort-peer", True, kind="filter")
                statement = self.statement(
                    "b.row_id,a.id", where=self.call("filter"), caching=caching)
                self.assert_inner_hash(self.explain(statement), probe_label="SEMANTIC FILTER")
                pending, rows, first = self.stream(statement)
                self.assertTrue(first.wait(2), f"no ready joined row before abort: {pending.error!r}")
                self.assertEqual(rows, [(11, 1)])
                if cancel:
                    self.kill_query()
                error = pending.expect_sql_error(self, 1317 if cancel else 4012)
                self.assertTrue(error.args[1])
                self.assertFalse(gate.is_set())
                self.assert_no_model_sockets()
                self.cursor.execute("SET ob_query_timeout=20000000")
                before = self.snapshots(settled=False)
                self.assertEqual(self.execute(self.statement(
                    "b.row_id,a.id", where="a.id=1 AND " + self.call("filter"),
                    caching=caching)), ((11, 1),))
                self.assert_windows(before, first=[peer], settled=False)
                self.assertFalse(gate.is_set())
                gate.set()

    def test_cancel_suspended_join_probe_recovers_on_the_same_connection(self):
        self.exercise_abort(True)

    def test_deadline_suspended_join_probe_recovers_on_the_same_connection(self):
        self.exercise_abort(False)

    def test_forced_merge_and_nested_loop_algorithms_reject_before_model_work(self):
        self.load_inputs(["algorithm-a", "algorithm-b"], flags=[0, 1])
        self.load_build([(10, 0, "left"), (11, 1, "right")])
        fields = "b.row_id,a.id," + self.call("map", "CONCAT(b.category,'/',a.prompt)")
        for algorithm, label in (("MERGE", "MERGE JOIN"), ("NL", "NESTED-LOOP JOIN")):
            with self.subTest(algorithm=algorithm):
                hints = f"PARALLEL(1) LEADING(b a) USE_{algorithm}(a)"
                control = self.statement(fields, ordering="ordered", hints=hints)
                self.explain(control).one(label)
                self.assert_rejected(self.statement(fields, hints=hints))

    def test_outer_semi_anti_and_cartesian_boundaries_reject_before_model_work(self):
        self.load_inputs(["unsafe-type"], flags=[0])
        self.load_build([(10, 0, "build")])
        mapped = self.call("map", "a.prompt")
        for source in (
                "cache_right b LEFT JOIN cache_inputs a ON b.join_id=a.guard_flag",
                "cache_right b RIGHT JOIN cache_inputs a ON b.join_id=a.guard_flag",
                "cache_right b JOIN cache_inputs a ON TRUE",
                "cache_right b JOIN cache_inputs a ON 1=1",
                "cache_right b,cache_inputs a"):
            self.assert_rejected(self.statement("b.row_id,a.id," + mapped, source=source))
        for negate in ("", "NOT "):
            statement = self.statement(
                "a.id," + mapped, source="cache_inputs a", hints="PARALLEL(1)",
                where=negate + "EXISTS (SELECT 1 FROM cache_right b "
                              "WHERE b.join_id=a.guard_flag)")
            self.assert_rejected(statement)

    def test_semantic_build_input_rejects_instead_of_claiming_probe_only_support(self):
        self.load_inputs(["unsafe-build-a", "unsafe-build-b"], flags=[0, 1])
        self.load_build([(10, 0, "left"), (11, 1, "right")])
        hints = "PARALLEL(1) LEADING(a b) USE_HASH(b) NO_SWAP_JOIN_INPUTS(b)"
        source = "cache_inputs a JOIN cache_right b ON a.guard_flag=b.join_id"
        control = self.statement(
            "b.row_id,a.id", source=source, hints=hints, ordering="ordered",
            where=self.call("filter"))
        plan = self.explain(control)
        join, stage = plan.one("HASH JOIN"), plan.one("SEMANTIC FILTER")
        children = sorted(
            (node for node in plan.nodes.values() if node.parent_id == join.operator_id),
            key=lambda node: node.operator_id)
        self.assertEqual(len(children), 2, plan.text)
        self.assertTrue(stage == children[0] or plan.is_above(children[0], stage), plan.text)
        self.assert_rejected(self.statement(
            "b.row_id,a.id", source=source, hints=hints, where=self.call("filter")))

    def test_two_semantic_input_branches_are_not_a_relational_build(self):
        self.load_inputs(["branch-a"], flags=[0])
        self.load_build([(10, 0, "branch-b")])
        predicate = self.call("filter") + " AND " + self.call("filter", "b.prompt", provider=1)
        self.assert_rejected(self.statement("b.row_id,a.id", where=predicate))

    def test_px_and_hidden_rewrite_boundaries_reject_before_model_work(self):
        self.load_inputs(["hidden-boundary"], flags=[0])
        self.load_build([(10, 0, "build")])
        fields = "b.row_id,a.id," + self.call("map", "CONCAT(b.category,'/',a.prompt)")
        for inner in (
                "SELECT id,guard_flag,prompt FROM cache_inputs LIMIT 1",
                "SELECT id,guard_flag,prompt FROM cache_inputs ORDER BY id",
                "SELECT DISTINCT id,guard_flag,prompt FROM cache_inputs LIMIT 1"):
            source = "cache_right b JOIN (" + inner + ") a ON b.join_id=a.guard_flag"
            self.assert_rejected(self.statement(fields, source=source))
        self.cursor.executemany(
            "INSERT INTO cache_px_inputs VALUES (%s,%s)",
            [(index, "parallel-boundary") for index in range(256)])
        self.assert_rejected(self.statement(
            fields, source="cache_right b JOIN cache_px_inputs a ON b.join_id=a.id",
            hints="PARALLEL(4) LEADING(b a) USE_HASH(a)"))

    def test_ai_join_expansion_is_outside_bounded_relational_auto_admission(self):
        self.load_inputs(["candidate"], flags=[0])
        self.load_build([(10, 0, "build")])
        source = JOIN_SOURCE + " AND " + self.join_call(left="b.prompt", right="a.prompt")
        self.assert_rejected(self.statement("b.row_id,a.id", source=source))

    def test_default_ai_join_keeps_its_candidate_bag_and_exact_calls(self):
        self.load_inputs(["candidate-a", "candidate-b"], flags=[0, 0])
        self.load_build([(10, 0, "left"), (11, 0, "right")])
        source = JOIN_SOURCE + " AND " + self.join_call(left="b.prompt", right="a.prompt")
        expected = [(10, 0), (10, 1), (11, 0)]
        keys = [
            self.fixtures.reply(
                1, cache.joins.pair_payload(cache.JOIN_RULE, f"build-{row_id}", prompt),
                (row_id, index) != (11, 1), kind="join")
            for row_id in (10, 11) for index, prompt in enumerate(("candidate-a", "candidate-b"))
        ]
        for caching in ("false", "true"):
            statement = self.statement(
                "b.row_id,a.id", source=source, caching=caching, enabled=None, ordering=None)
            plan = self.explain(statement)
            plan.one("HASH JOIN")
            candidate = plan.one("SEMANTIC JOIN")
            self.assertNotIn("OUTPUT_POLICY=AUTO", candidate.annotation.upper())
            self.check_query(statement, expected, second=keys, ordered=False)


def main(arguments=None):
    inventory = {name: SemanticRelationalJoinContracts
                 for name in SemanticRelationalJoinContracts.__dict__ if name.startswith("test_")}
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
        parser.error("--log-dir requires --binary and a new output directory")
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
        SemanticRelationalJoinContracts.database = database
        suite = unittest.TestSuite(SemanticRelationalJoinContracts(name) for name in selected)
        result = unittest.TextTestRunner(
            verbosity=2, failfast=True, resultclass=cache.SQLDiagnosticResult).run(suite)
        return 0 if result.wasSuccessful() else 1
    finally:
        try:
            if options.binary is not None:
                database.stop()
        finally:
            try:
                if options.log_dir is not None:
                    cache.retain_deployment_logs(database, options.log_dir)
            finally:
                database.close()
                SemanticRelationalJoinContracts.database = None


if __name__ == "__main__":
    raise SystemExit(main())
