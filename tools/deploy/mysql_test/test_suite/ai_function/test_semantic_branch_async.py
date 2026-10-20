"""Opt-in, single-worker materialized-CTE branch scheduling contracts.

Held loopback responses prove independent producers start before any sibling
finishes, while dependent producers wait for a complete materialization.
No real model, application-side query concurrency, or PX is used.
"""

import argparse
from collections import Counter
import os
from pathlib import Path
import unittest

import test_semantic_cache as cache
import test_semantic_runtime as semantic
from test_file_runtime import PrivateDatabase
from test_runtime_contracts import Reply, sql_error


class SemanticBranchContracts(cache.SemanticCacheContracts):
    def statement(self, *, enabled="true", dependent=False, shared=False,
                  empty=False, distinct=False, parallel=1, nullable=False, filtering=False,
                  mismatch=False):
        option = "" if enabled is None else (
            f"OPT_PARAM('enable_semantic_branch_async','{enabled}')")
        hints = (
            f"PARALLEL({parallel}) {option} OPT_PARAM('rowsets_max_rows',8) "
            "OPT_PARAM('enable_semantic_cache','false') "
            "OPT_PARAM('enable_semantic_row_async','false')"
        )
        definitions = []
        source = "cache_inputs" if parallel == 1 else "cache_px_inputs"
        if shared:
            definitions.append(
                "base AS (SELECT /*+ MATERIALIZE QB_NAME(base) */ "
                "id,prompt,guard_flag FROM cache_inputs WHERE guard_flag=1)")
            source = "base"
        for index in range(3):
            prompt = f"CONCAT('b{index}:',a.prompt)"
            if dependent and index == 2:
                branch_source = "b0 a"
                prompt = "CONCAT('after:',a.answer)"
            else:
                branch_source = source + " a"
            predicate = " WHERE a.id<0" if empty and index == 1 else ""
            if filtering:
                predicate += (" AND " if predicate else " WHERE ") + self.call(
                    "filter", prompt, provider=index % 2)
            dedup = "DISTINCT " if distinct and index == 0 else ""
            row_id = "0 AS id" if distinct and index == 0 else "a.id"
            extra = ",CASE WHEN MOD(a.id,2)=0 THEN NULL ELSE a.id END AS optional" if nullable else ""
            value = "'accepted'" if filtering else self.call("map", prompt, provider=index % 2)
            definitions.append(
                f"b{index} AS (SELECT /*+ MATERIALIZE QB_NAME(b{index}) */ "
                f"{dedup}{row_id},{value} AS answer{extra} "
                f"FROM {branch_source}{predicate})")
        fields = "id,answer,optional" if nullable else "id,answer"
        if mismatch:
            return (
                "WITH " + ",".join(definitions) + f" SELECT /*+ {hints} */ "
                "candidate.id,b0.id IS NOT NULL,b1.id IS NOT NULL,b2.id IS NOT NULL "
                "FROM base candidate LEFT JOIN b0 ON candidate.id=b0.id "
                "LEFT JOIN b1 ON candidate.id=b1.id LEFT JOIN b2 ON candidate.id=b2.id "
                "WHERE NOT ((b0.id IS NULL AND b1.id IS NULL AND b2.id IS NULL) OR "
                "(b0.id IS NOT NULL AND b1.id IS NOT NULL AND b2.id IS NOT NULL)) "
                "ORDER BY candidate.id")
        return (
            "WITH " + ",".join(definitions) + f" SELECT /*+ {hints} */ {fields} FROM ("
            f"SELECT {fields} FROM b0 UNION ALL SELECT {fields} FROM b1 "
            f"UNION ALL SELECT {fields} FROM b2) combined ORDER BY id,answer")

    def configure(self, prompts, *, gate=None, dependent=False):
        for index in range(3):
            for row, prompt in enumerate(prompts):
                text = f"after:v0:{row}" if dependent and index == 2 else f"b{index}:{prompt}"
                self.fixtures.reply(index % 2, text, f"v{index}:{row}", gate=gate)

    def wait_branch(self, pending, index, *, prefix=None):
        server = self.fixtures.mocks[index % 2]
        prefix = prefix or f"b{index}:"
        semantic.wait_evidence(
            self, server,
            lambda: any(record["prompt"].startswith(prefix)
                        for record in semantic.audit(server)), pending,
            message=f"materialized producer {index} never submitted its request")

    def assert_all_started(self, pending):
        for index in range(3):
            self.wait_branch(pending, index)

    def test_independent_producers_start_before_any_response(self):
        prompts = ["first", "second"]
        self.load_inputs(prompts)
        gate = self.fixtures.gate()
        self.configure(prompts, gate=gate)
        statement = self.statement()
        plan = self.explain(statement)
        self.assertIn("BRANCH_ASYNC=DEPENDENCY_READY", plan.text.upper())
        self.assertEqual(plan.text.upper().count("SEMANTIC MAP"), 3)
        pending = self.fixtures.start(lambda: self.execute(statement))
        self.assert_all_started(pending)
        for server in self.fixtures.mocks:
            semantic.assert_held(self, server, [gate])
        self.assertFalse(pending.done.is_set())
        gate.set()
        self.assertEqual(Counter(pending.result()),
                         Counter((row, f"v{branch}:{row}")
                                 for row in range(2) for branch in range(3)))
        self.assertEqual(sum(len(server.requests) for server in self.fixtures.mocks), 6)

    def test_default_and_false_remain_serial(self):
        self.load_inputs(["serial"])
        for enabled in (None, "false"):
            with self.subTest(enabled=enabled):
                gate = self.fixtures.gate()
                before = [len(server.requests) for server in self.fixtures.mocks]
                self.configure(["serial"], gate=gate)
                statement = self.statement(enabled=enabled)
                self.assertNotIn("BRANCH_ASYNC=", self.explain(statement).text.upper())
                pending = self.fixtures.start(lambda: self.execute(statement))
                semantic.wait_evidence(
                    self, self.first,
                    lambda: any(len(server.requests) > offset
                                for server, offset in zip(self.fixtures.mocks, before)),
                    pending, message="serial baseline submitted no model request")
                self.assertFalse(pending.done.wait(0.15))
                requests = [body for server, offset in zip(self.fixtures.mocks, before)
                            for body in server.requests[offset:]]
                prefixes = {body["messages"][0]["content"].split(":")[0] for body in requests}
                self.assertEqual(len(prefixes), 1, "default/off started a sibling producer")
                gate.set()
                self.assertEqual(len(pending.result()), 3)

    def test_dependent_producer_waits_for_upstream_eof(self):
        self.load_inputs(["dependency"])
        gate = self.fixtures.gate()
        self.configure(["dependency"], gate=gate, dependent=True)
        pending = self.fixtures.start(lambda: self.execute(self.statement(dependent=True)))
        self.wait_branch(pending, 0)
        self.wait_branch(pending, 1)
        self.assertFalse(any(record["prompt"].startswith("after:")
                             for record in semantic.audit(self.first)))
        self.assertFalse(pending.done.is_set())
        gate.set()
        self.assertEqual(Counter(pending.result()),
                         Counter((0, f"v{branch}:0") for branch in range(3)))
        records = semantic.audit(self.first)
        upstream = next(record for record in records if record["prompt"] == "b0:dependency")
        downstream = next(record for record in records if record["prompt"] == "after:v0:0")
        self.assertIsNotNone(upstream["response"])
        self.assertLessEqual(upstream["response"], downstream["received"])

    def test_shared_input_multibatch_duplicates_and_empty_producer(self):
        prompts = [f"duplicate-{row % 3}" for row in range(27)]
        self.load_inputs(prompts, flags=[int(row % 4 != 0) for row in range(len(prompts))])
        for branch in range(3):
            for prompt in set(prompts):
                self.fixtures.reply(branch % 2, f"b{branch}:{prompt}", f"value:{prompt}")
        expected = Counter(
            (row, f"value:{prompt}", None if row % 2 == 0 else row)
            for row, prompt in enumerate(prompts) if row % 4 != 0
            for branch in (0, 2))
        for enabled in ("false", "true"):
            with self.subTest(enabled=enabled):
                before = sum(len(server.requests) for server in self.fixtures.mocks)
                rows = self.execute(self.statement(
                    enabled=enabled, shared=True, empty=True, nullable=True))
                self.assertEqual(Counter(rows), expected)
                self.assertEqual(sum(len(server.requests) for server in self.fixtures.mocks) - before,
                                 sum(expected.values()))

    def test_cancel_cancels_all_started_branches_and_recovers(self):
        self.load_inputs(["cancel"])
        gate = self.fixtures.gate()
        self.configure(["cancel"], gate=gate)
        statement = self.statement()
        pending = self.fixtures.start(lambda: self.execute(statement))
        self.assert_all_started(pending)
        self.kill_query()
        pending.expect_sql_error(self, 1317, timeout=5)
        self.assertFalse(gate.is_set(), "cancellation waited for model completion")
        self.assert_no_model_sockets()
        self.configure(["cancel"])
        recovery = self.fixtures.start(lambda: self.execute(statement))
        self.assertEqual(len(recovery.result(timeout=5)), 3)
        self.assertFalse(gate.is_set(), "recovery required releasing canceled work")

    def test_model_error_aborts_siblings_without_releasing_them(self):
        self.load_inputs(["error"])
        held, error_gate = self.fixtures.gate(), self.fixtures.gate()
        self.configure(["error"], gate=held)
        self.fixtures.replies(0, "b0:error", [
            Reply(status=400, body=b'{"error":"permanent provider failure"}', gate=error_gate)])
        pending = self.fixtures.start(lambda: self.execute(self.statement()))
        self.assert_all_started(pending)
        error_gate.set()
        pending.expect_sql_error(self, 4216, timeout=5)
        self.assertFalse(held.is_set())
        self.assert_no_model_sockets()

    def test_different_per_cte_task_counts_share_one_query_budget(self):
        prompts = [f"task-count-{row}" for row in range(19)]
        self.load_inputs(prompts)
        self.configure(prompts)
        for row in range(len(prompts)):
            self.fixtures.reply(1, f"inner:v0:{row}", f"v0:{row}")
        inner = self.call("map", "CONCAT('b0:',a.prompt)")
        outer = self.call("map", "CONCAT('inner:'," + inner + ")", provider=1)
        expected = Counter((row, f"v{branch}:{row}")
                           for row in range(len(prompts)) for branch in range(3))
        for enabled in ("false", "true"):
            with self.subTest(enabled=enabled):
                query = self.statement(enabled=enabled).replace(inner, outer, 1)
                before = sum(len(server.requests) for server in self.fixtures.mocks)
                self.assertEqual(Counter(self.execute(query)), expected)
                self.assertEqual(sum(len(server.requests) for server in self.fixtures.mocks) - before,
                                 4 * len(prompts))

    def test_filter_branches_preserve_union_distinct_and_all_false_materialization(self):
        prompts = [f"filter-{row}" for row in range(25)]
        self.load_inputs(prompts)
        expected = Counter((row, "accepted") for row in range(len(prompts))
                           if row % 2 == 0 or row % 3 == 0)
        for enabled in ("true", "false"):
            with self.subTest(enabled=enabled):
                gate = self.fixtures.gate() if enabled == "true" else None
                for branch in range(3):
                    for row, prompt in enumerate(prompts):
                        matched = row % 2 == 0 if branch == 0 else branch == 2 and row % 3 == 0
                        self.fixtures.reply(branch % 2, f"b{branch}:{prompt}",
                                            matched, kind="filter", gate=gate)
                statement = self.statement(enabled=enabled, filtering=True).replace(
                    "UNION ALL", "UNION DISTINCT")
                before = sum(len(server.requests) for server in self.fixtures.mocks)
                pending = self.fixtures.start(lambda: self.execute(statement))
                if gate is not None:
                    self.assert_all_started(pending)
                    self.assertFalse(pending.done.is_set())
                    gate.set()
                self.assertEqual(Counter(pending.result()), expected)
                self.assertEqual(sum(len(server.requests) for server in self.fixtures.mocks) - before,
                                 3 * len(prompts))

    def test_shared_candidates_left_join_mismatch_consumer(self):
        prompts = [f"mismatch-{row}" for row in range(29)]
        self.load_inputs(prompts)
        expected = []
        for row, prompt in enumerate(prompts):
            matches = tuple(int(row % divisor == 0) for divisor in (2, 3, 4))
            if len(set(matches)) != 1:
                expected.append((row, *matches))
            for branch, matched in enumerate(matches):
                self.fixtures.reply(branch % 2, f"b{branch}:{prompt}",
                                    bool(matched), kind="filter")
        for enabled in ("false", "true"):
            with self.subTest(enabled=enabled):
                before = sum(len(server.requests) for server in self.fixtures.mocks)
                rows = self.execute(self.statement(
                    enabled=enabled, shared=True, filtering=True, mismatch=True))
                self.assertEqual(rows, tuple(expected))
                self.assertEqual(sum(len(server.requests) for server in self.fixtures.mocks) - before,
                                 3 * len(prompts))

    def test_deadline_cancels_all_started_branches(self):
        self.load_inputs(["timeout"])
        gate = self.fixtures.gate()
        self.configure(["timeout"], gate=gate)
        self.cursor.execute("SET ob_query_timeout=1000000")
        pending = self.fixtures.start(lambda: self.execute(self.statement()))
        self.assert_all_started(pending)
        pending.expect_sql_error(self, 4012, timeout=5)
        self.assertFalse(gate.is_set())
        self.assert_no_model_sockets()

    def test_explicit_invalid_or_unsupported_plans_fail_before_model_calls(self):
        self.load_inputs(["unsupported"])
        for enabled in ("not-boolean", "true,false", ""):
            with self.subTest(enabled=enabled):
                error = sql_error(self, (1210, 1235), lambda: self.execute(
                    self.statement(enabled=enabled)))
                self.assertTrue(error.args[1])
        sql_error(self, 1235, lambda: self.execute(
            self.statement(distinct=True)))
        self.cursor.execute("INSERT INTO cache_px_inputs VALUES (0,'unsupported')")
        sql_error(self, 1235, lambda: self.execute(
            self.statement(parallel=4)))
        sql_error(self, 1235, lambda: self.execute(
            "SELECT /*+ OPT_PARAM('enable_semantic_branch_async','true') */ id FROM cache_inputs"))
        self.assertEqual(sum(len(server.requests) for server in self.fixtures.mocks), 0)


def main(arguments=None):
    inventory = sorted(name for name in SemanticBranchContracts.__dict__ if name.startswith("test_"))
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--binary", type=Path)
    source.add_argument("--socket", type=Path)
    parser.add_argument("--log-dir", type=Path)
    parser.add_argument("--case", action="append", default=[], choices=inventory)
    options = parser.parse_args(arguments)
    if options.log_dir is not None and (options.binary is None or options.log_dir.exists()):
        parser.error("--log-dir requires --binary and a new directory")
    if options.binary is not None:
        binary = options.binary.resolve()
        if not binary.is_file() or not os.access(binary, os.X_OK):
            parser.error("--binary must be executable")
        database = PrivateDatabase(binary)
    else:
        sql_socket = options.socket.resolve()
        if not sql_socket.is_socket():
            parser.error("--socket must be a Unix socket")
        database = cache.SocketDatabase(sql_socket)
    try:
        database.start()
        SemanticBranchContracts.database = database
        selected = list(dict.fromkeys(options.case)) or inventory
        suite = unittest.TestSuite(SemanticBranchContracts(name) for name in selected)
        result = unittest.TextTestRunner(verbosity=2, failfast=True).run(suite)
        return 0 if result.wasSuccessful() else 1
    finally:
        try:
            if options.log_dir is not None:
                cache.retain_deployment_logs(database, options.log_dir)
        finally:
            database.close()
            SemanticBranchContracts.database = None


if __name__ == "__main__":
    raise SystemExit(main())
