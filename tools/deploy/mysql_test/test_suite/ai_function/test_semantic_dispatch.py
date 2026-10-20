"""Shared-transport grouped versus singleton model-dispatch SQL contracts.

SQL input batches stay at 16 rows in both modes. Held responses prove the
query-wide HTTP limit, incremental dependencies and rolling input admission.
Native log counters verify actual request-group sizes, not just hint parsing.
Only an owned private database is used; no real inference services are called.
"""

import argparse
from pathlib import Path
import re
import time
import unittest

import pymysql
from pymysql.constants import FIELD_TYPE

import test_semantic_cache as cache
import test_semantic_rank as rank
import test_semantic_row_async as row_async
import test_semantic_runtime as semantic
from test_file_runtime import PrivateDatabase
from test_runtime_contracts import Reply, completion, sql_error


class SemanticDispatchContracts(cache.SemanticCacheContracts):
    stream = row_async.SemanticRowAsyncContracts.stream
    wait_requests = row_async.SemanticRowAsyncContracts.wait_requests

    def setUp(self):
        self.cursor = None
        self.addCleanup(self.close_current_cursor)
        super().setUp()
        self.cursor.execute("SET ob_log_level='WDIAG'")

    def close_current_cursor(self):
        if self.cursor is not None:
            self.cursor.close()

    def statement(self, fields, *, group=1, limit=4, ordering="unordered", suffix="", **kwargs):
        return cache.sql_statement(
            fields, cache="false", hints=(
                "PARALLEL(1) OPT_PARAM('enable_semantic_row_async','true') "
                f"OPT_PARAM('semantic_output_order','{ordering}') "
                f"OPT_PARAM('semantic_dispatch_rows',{group}) "
                f"OPT_PARAM('semantic_request_limit',{limit})"),
            suffix=suffix, **kwargs)

    def log_offset(self):
        return self.database.server_log.stat().st_size

    def transport_stats(self, offset):
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            with self.database.server_log.open(errors="replace") as log:
                log.seek(offset)
                text = log.read()
            lines = [line for line in text.splitlines()
                     if "AI shared request transport" in line]
            if lines:
                self.assertEqual(len(lines), 1, text)
                fields = dict(re.findall(
                    r"\b(execution_id|request_limit|groups|requests|max_group_rows|peak_active|failure)_?=(-?\d+)",
                    lines[0]))
                self.assertEqual(len(fields), 7, lines[0])
                stats = {name: int(value) for name, value in fields.items()}
                self.assertEqual(stats["failure"], 0, lines[0])
                return stats, text
            time.sleep(0.01)
        self.fail("query teardown did not publish shared transport counters")

    def batch_stats(self, text):
        lines = [line for line in text.splitlines() if "semantic shared dispatch batches" in line]
        self.assertEqual(len(lines), 1, text)
        fields = dict(re.findall(
            r"\b(captured_batches|max_captured_rows|output_batches|output_rows|"
            r"max_output_rows|peak_pending_rows|peak_frames)_?=(\d+)", lines[0]))
        self.assertEqual(len(fields), 7, lines[0])
        return {name: int(value) for name, value in fields.items()}

    def test_group_sizes_input_batch_and_exact_http_cap(self):
        prompts = [f"window-{index}" for index in range(64)]
        self.load_inputs(prompts)
        for group in (1, 16):
            with self.subTest(group=group):
                gate = self.fixtures.gate()
                offset = len(self.first.requests)
                for prompt in prompts:
                    self.fixtures.reply(0, prompt, "value:" + prompt, gate=gate)
                statement = self.statement("a.id," + self.call("map"), group=group)
                plan = self.explain(statement).text.upper()
                self.assertIn(f"DISPATCH_ROWS={group}", plan)
                self.assertIn("REQUEST_LIMIT=4", plan)
                self.assertIn("TRANSPORT=SHARED_QUERY", plan)
                log_offset = self.log_offset()
                pending = self.fixtures.start(lambda: self.execute(statement))
                self.wait_requests(
                    self.first, lambda: len(self.first.requests) >= offset + 4,
                    pending, "four request credits were not filled")
                self.assertFalse(pending.done.wait(0.15), repr(pending.error))
                self.assertEqual(len(self.first.requests) - offset, 4,
                                 "HTTP concurrency exceeded the query request limit")
                gate.set()
                self.assertCountEqual(
                    pending.result(), ((index, "value:" + prompt)
                                       for index, prompt in enumerate(prompts)))
                stats, text = self.transport_stats(log_offset)
                self.assertEqual(stats["requests"], len(prompts))
                self.assertEqual(stats["max_group_rows"], group)
                self.assertEqual(stats["peak_active"], 4)
                if group == 1:
                    self.assertEqual(stats["groups"], len(prompts))
                self.assertRegex(text, r"semantic first prepare begin[^\n]*\brows=16\b")
                batches = self.batch_stats(text)
                self.assertEqual(batches["captured_batches"], 4)
                self.assertEqual(batches["max_captured_rows"], 16)
                self.assertEqual(batches["output_rows"], 64)
                self.assertLessEqual(batches["max_output_rows"], 16)
                self.assertLessEqual(batches["peak_pending_rows"], 2 * 4 + 16 - 1)
                self.assertLessEqual(batches["peak_frames"], 2 * 4 + 16 + 1)
                self.assert_no_model_sockets()

    def test_more_than_64_groups_do_not_exhaust_legacy_client_slots(self):
        prompts = [f"wide-{index}" for index in range(128)]
        self.load_inputs(prompts)
        for group in (1, 16):
            with self.subTest(group=group):
                gate = self.fixtures.gate()
                offset = len(self.first.requests)
                for prompt in prompts:
                    self.fixtures.reply(0, prompt, prompt, gate=gate)
                log_offset = self.log_offset()
                pending = self.fixtures.start(lambda: self.execute(self.statement(
                    "a.id," + self.call("map"), group=group, limit=80)))
                self.wait_requests(
                    self.first, lambda: len(self.first.requests) >= offset + 80,
                    pending, "shared transport stalled at the legacy 64-client boundary")
                self.assertFalse(pending.done.wait(0.15), repr(pending.error))
                self.assertEqual(len(self.first.requests) - offset, 80)
                gate.set()
                self.assertCountEqual(pending.result(), enumerate(prompts))
                stats, text = self.transport_stats(log_offset)
                self.assertEqual(stats["peak_active"], 80)
                self.assertEqual(stats["max_group_rows"], group)
                self.assertEqual(stats["groups"], 128 if group == 1 else 8)
                batches = self.batch_stats(text)
                self.assertEqual(batches["captured_batches"], 8)
                self.assertGreater(batches["max_output_rows"], 1)
                self.assertLessEqual(batches["max_output_rows"], 16)
                self.assert_no_model_sockets()

    def test_slow_tails_do_not_pin_the_input_window(self):
        prompts = [f"rolling-{index}" for index in range(96)]
        self.load_inputs(prompts)
        for group in (1, 16):
            with self.subTest(group=group):
                head = self.fixtures.gate()
                offset = len(self.first.requests)
                for index, prompt in enumerate(prompts):
                    self.fixtures.reply(0, prompt, "value:" + prompt,
                                        gate=head if index in (0, 16) else None)
                statement = self.statement("a.id," + self.call("map"), group=group)
                log_offset = self.log_offset()
                pending, rows, first = self.stream(statement)
                self.wait_requests(
                    self.first, lambda: len(self.first.requests) >= offset + 64,
                    pending, "two pinned source batches stopped rolling row admission")
                self.assertTrue(first.wait(3), repr(pending.error))
                self.assertFalse(head.is_set())
                self.assertFalse(pending.done.is_set(), repr(pending.error))
                self.assertTrue(rows)
                head.set()
                self.assertCountEqual(
                    pending.result(), ((index, "value:" + prompt)
                                       for index, prompt in enumerate(prompts)))
                stats, _ = self.transport_stats(log_offset)
                self.assertEqual(stats["requests"], len(prompts))
                self.assertLessEqual(stats["peak_active"], 4)

    def test_dependent_requests_share_one_query_http_limit(self):
        prompts = [f"inner-{index}" for index in range(32)]
        self.load_inputs(prompts)
        for group in (1, 16):
            with self.subTest(group=group):
                head = self.fixtures.gate()
                second_offset = len(self.second.requests)
                for index, prompt in enumerate(prompts):
                    self.fixtures.reply(0, prompt, f"middle-{index}",
                                        gate=head if index == 0 else None)
                    self.fixtures.reply(1, f"outer:middle-{index}", f"answer-{index}")
                expression = self.call(
                    "map", "CONCAT('outer:'," + self.call("map") + ")", provider=1)
                log_offset = self.log_offset()
                pending = self.fixtures.start(lambda: self.execute(
                    self.statement("a.id," + expression, group=group)))
                self.wait_requests(
                    self.second, lambda: len(self.second.requests) > second_offset,
                    pending, "ready inner rows did not advance to their dependent model")
                self.assertFalse(head.is_set())
                head.set()
                self.assertCountEqual(pending.result(),
                                      ((index, f"answer-{index}") for index in range(32)))
                stats, _ = self.transport_stats(log_offset)
                self.assertEqual(stats["requests"], 64)
                self.assertLessEqual(stats["peak_active"], 4)

    def test_q3_scalar_count_preserves_filter_and_empty_set(self):
        self.cursor.execute(
            "CREATE TABLE IF NOT EXISTS dispatch_reviews(id VARCHAR(64),reviewText TEXT)")
        self.cursor.execute("TRUNCATE TABLE dispatch_reviews")
        self.cursor.executemany("INSERT INTO dispatch_reviews VALUES(%s,%s)", [
            ("taken_3" if index < 19 else "excluded", f"review-{index}")
            for index in range(24)])
        for group in (1, 16):
            with self.subTest(group=group):
                offset = len(self.first.requests)
                for index in range(19):
                    self.fixtures.reply(0, f"review-{index}", index % 2 == 0, kind="filter")
                statement = self.statement(
                    "COUNT(*) AS positive_review_counts", group=group, ordering="auto",
                    source="dispatch_reviews",
                    where="id='taken_3' AND " + self.call("filter", "reviewText"))
                self.assertIn("SCALAR_COUNT_INPUT", self.explain(statement).text.upper())
                self.assertEqual(self.execute(statement), ((10,),))
                self.assertEqual(len(self.first.requests) - offset, 19)
                self.assertEqual(self.execute(statement.replace("'taken_3'", "'absent'")), ((0,),))
                self.assertEqual(len(self.first.requests) - offset, 19)
                self.assert_no_model_sockets()

    def test_limit_cancels_shared_transport_without_waiting_for_held_rows(self):
        prompts = [f"limit-{index}" for index in range(48)]
        self.load_inputs(prompts)
        for group in (1, 16):
            with self.subTest(group=group):
                head = self.fixtures.gate()
                for index, prompt in enumerate(prompts):
                    self.fixtures.reply(0, prompt, True, kind="filter",
                                        gate=None if index == 1 else head)
                pending = self.fixtures.start(lambda: self.execute(self.statement(
                    "a.id", group=group, where=self.call("filter"), suffix="LIMIT 1")))
                self.assertEqual(pending.result(timeout=5), ((1,),))
                self.assertFalse(head.is_set())
                self.assert_no_model_sockets()
                head.set()

    def test_invalid_model_result_errors_and_next_query_recovers(self):
        self.load_inputs(["bad"])
        for group in (1, 16):
            with self.subTest(group=group):
                self.fixtures.replies(
                    0, "bad", [Reply(body=completion('{"value":"not-a-boolean"}'))],
                    kind="filter")
                statement = self.statement("a.id", group=group, where=self.call("filter"))
                sql_error(self, 4070, lambda: self.execute(statement))
                self.assert_no_model_sockets()
                self.fixtures.reply(0, "bad", True, kind="filter")
                self.assertEqual(self.execute(statement), ((0,),))
                self.assert_no_model_sockets()

    def test_retry_releases_credit_without_duplicating_sql_rows(self):
        self.load_inputs(["retry", "retry", "fast"])
        for group in (1, 16):
            with self.subTest(group=group):
                gate = self.fixtures.gate()
                retry_offset = self.first.counts["retry"]
                self.fixtures.replies(0, "retry", [
                    Reply(status=429, body=b'{"error":"retry"}', headers={"Retry-After": "0"}),
                    Reply(body=completion("retried", finish_reason="stop"), gate=gate),
                ])
                self.fixtures.reply(0, "fast", "visible")
                log_offset = self.log_offset()
                pending, rows, first = self.stream(self.statement(
                    "a.id," + self.call("map"), group=group, limit=2))
                self.assertTrue(first.wait(3), repr(pending.error))
                self.assertEqual(rows, [(2, "visible")])
                self.wait_requests(
                    self.first, lambda: self.first.counts["retry"] >= retry_offset + 3,
                    pending, "the failed attempt did not retry while its peer was held")
                self.assertFalse(gate.is_set())
                gate.set()
                self.assertCountEqual(
                    pending.result(), ((0, "retried"), (1, "retried"), (2, "visible")))
                self.assertEqual(self.first.counts["retry"] - retry_offset, 3)
                stats, _ = self.transport_stats(log_offset)
                self.assertEqual(stats["requests"], 3)
                self.assertLessEqual(stats["peak_active"], 2)
                self.assert_no_model_sockets()

    def exercise_abort(self, cancel):
        self.load_inputs(["abort-head", "abort-peer"])
        for group in (1, 16):
            with self.subTest(group=group, cancel=cancel):
                gate = self.fixtures.gate()
                self.cursor.execute("SET ob_query_timeout=%s", (20000000 if cancel else 3000000,))
                self.fixtures.reply(0, "abort-head", "held", gate=gate)
                self.fixtures.reply(0, "abort-peer", "visible")
                pending, rows, first = self.stream(self.statement(
                    "a.id," + self.call("map"), group=group))
                self.assertTrue(first.wait(2), repr(pending.error))
                self.assertEqual(rows, [(1, "visible")])
                if cancel:
                    self.kill_query()
                pending.expect_sql_error(self, 1317 if cancel else 4012)
                self.assertFalse(gate.is_set())
                self.assert_no_model_sockets()
                self.cursor.execute("SET ob_query_timeout=20000000")
                self.fixtures.reply(0, "abort-peer", "recovered")
                self.assertEqual(self.execute(self.statement(
                    "a.id," + self.call("map"), group=group, where="a.id=1")),
                    ((1, "recovered"),))
                gate.set()

    def test_kill_cancels_shared_transport_and_recovers(self):
        self.exercise_abort(True)

    def test_deadline_cancels_shared_transport_and_recovers(self):
        self.exercise_abort(False)

    def test_server_shutdown_cancels_held_shared_requests(self):
        self.load_inputs([f"shutdown-{index}" for index in range(32)])
        for group in (1, 16):
            with self.subTest(group=group):
                gate = self.fixtures.gate()
                offset = len(self.first.requests)
                for index in range(32):
                    self.fixtures.reply(0, f"shutdown-{index}", "held", gate=gate)
                pending = self.fixtures.start(lambda: self.execute(self.statement(
                    "a.id," + self.call("map"), group=group)))
                self.wait_requests(
                    self.first, lambda: len(self.first.requests) >= offset + 4,
                    pending, "no active shared requests before shutdown")
                try:
                    self.database.stop(close_connection_first=False, timeout=9)
                    self.assertFalse(gate.is_set())
                    pending.thread.join(2)
                    self.assertFalse(pending.thread.is_alive())
                    self.assertIsInstance(pending.error, pymysql.MySQLError)
                finally:
                    gate.set()
                    self.database.start()
                    self.cursor = self.database.connection.cursor()
                    self.cursor.execute(f"USE {self.schema}")
                    self.process_id = self.database.process.pid
                self.assertEqual(self.execute("SELECT 1"), ((1,),))
                self.assert_no_model_sockets()

    def test_rank_remains_a_numeric_sql_result(self):
        scores = [0.0, -2.25, 4.5, 1e100] * 12
        prompts = [f"score-{index}" for index in range(len(scores))]
        self.load_inputs(prompts)
        for group in (1, 16):
            with self.subTest(group=group):
                for prompt, score in zip(prompts, scores):
                    self.fixtures.replies(0, prompt, [
                        Reply(body=completion(cache.json_text({"value": score}), finish_reason="stop"))
                    ], options={"response_format": rank.RANK_FORMAT})
                log_offset = self.log_offset()
                self.assertCountEqual(self.execute(self.statement(
                    "a.id," + self.call("rank"), group=group)), enumerate(scores))
                self.assertEqual(self.cursor.description[1][1], FIELD_TYPE.DOUBLE)
                stats, _ = self.transport_stats(log_offset)
                self.assertEqual(stats["requests"], len(prompts))
                self.assertLessEqual(stats["peak_active"], 4)

    def test_case_guards_skip_undemanded_invalid_inputs(self):
        prompts = [None, "", "safe", "safe", None, ""] * 8
        flags = [0, 0, 1, 1, 0, 0] * 8
        self.load_inputs(prompts, flags=flags)
        for group in (1, 16):
            with self.subTest(group=group):
                first_offset, second_offset = len(self.first.requests), len(self.second.requests)
                self.fixtures.reply(0, "safe", True, kind="filter")
                self.fixtures.reply(1, "safe", "mapped")
                predicate = "a.guard_flag=0 OR " + self.call("filter")
                fields = "a.id,CASE WHEN a.guard_flag=1 THEN " + self.call(
                    "map", provider=1) + " ELSE 'skip' END"
                self.assertCountEqual(self.execute(self.statement(
                    fields, group=group, where=predicate)),
                    ((index, "mapped" if flags[index] else "skip") for index in range(len(prompts))))
                self.assertEqual(len(self.first.requests) - first_offset, flags.count(1))
                self.assertEqual(len(self.second.requests) - second_offset, flags.count(1))

    def test_memory_quota_failure_is_explicit_and_recovers(self):
        prompts = [f"quota-{index}" + "x" * 40000 for index in range(32)]
        self.load_inputs(prompts)
        for group in (1, 16):
            with self.subTest(group=group):
                offset = len(self.first.requests)
                self.cursor.execute("SET ai_pipeline_memory_limit=%s", (cache.QUERY_MEMORY_BYTES,))
                statement = self.statement("a.id," + self.call("map"), group=group, batch_rows=64)
                sql_error(self, 4019, lambda: self.execute(statement))
                self.assertEqual(len(self.first.requests), offset)
                self.assert_no_model_sockets()
                self.cursor.execute("SET ai_pipeline_memory_limit=DEFAULT")
                for prompt in prompts:
                    self.fixtures.reply(0, prompt, "recovered")
                self.assertCountEqual(self.execute(statement),
                                      ((index, "recovered") for index in range(len(prompts))))
                self.assert_no_model_sockets()

    def test_unsupported_modes_fail_before_model_work(self):
        self.load_inputs(["unsupported"])
        base = self.statement("a.id," + self.call("map"))
        statements = [
            (base.replace("'enable_semantic_row_async','true'",
                          "'enable_semantic_row_async','false'"), 1235),
            (base.replace("'enable_semantic_cache', 'false'",
                          "'enable_semantic_cache', 'true'"), 1235),
            (base.replace("/*+", "/*+ OPT_PARAM('enable_semantic_proxy','true')", 1), 1235),
            (self.statement("a.id," + self.call("map"), ordering="ordered"), 1235),
            (self.statement(self.call("map", "TO_FILE('{\"uri\":\"file:///missing-dispatch.png\"}')"),
                            ordering="auto"), 1235),
            (self.statement(f"AI_AGG('{self.models[0]}',a.prompt,'Summarize.')"), 1235),
            (self.statement("a.id,b.id", source="cache_inputs a JOIN cache_inputs b ON " +
                            self.join_call()), 1235),
        ]
        self.cursor.executemany("INSERT INTO cache_px_inputs VALUES (%s,%s)",
                                [(index, "unsupported-px") for index in range(256)])
        statements.append((self.statement(
            "a.id," + self.call("map"), source="cache_px_inputs a", ordering="auto").replace(
                "PARALLEL(1)", "PARALLEL(4)"), 1235))
        for statement, code in statements:
            for prefix in ("EXPLAIN ", ""):
                with self.subTest(sql=statement, prefix=prefix):
                    sql_error(self, code, lambda: self.execute(prefix + statement))
        self.assertEqual(self.first.requests, [])
        self.assertEqual(self.second.requests, [])

    def test_option_validation_and_default_path(self):
        self.load_inputs(["default-control"])
        for name in ("semantic_dispatch_rows", "semantic_request_limit"):
            for value in ("0", "-1", "257", "'1'", "'bad'"):
                statement = self.statement("a.id," + self.call("map")).replace(
                    f"OPT_PARAM('{name}',{1 if name.endswith('rows') else 4})",
                    f"OPT_PARAM('{name}',{value})")
                sql_error(self, 1210, lambda: self.execute(statement))
        for name, value in (("semantic_dispatch_rows", 1), ("semantic_request_limit", 4)):
            statement = self.statement("a.id," + self.call("map")).replace(
                f"OPT_PARAM('{name}',{value})", "")
            sql_error(self, 1210, lambda: self.execute(statement))
        self.assertEqual(self.first.requests, [])
        self.fixtures.reply(0, "default-control", "unchanged")
        ordinary = cache.sql_statement("a.id," + self.call("map"), cache="false")
        self.assertNotIn("SHARED_QUERY", self.explain(ordinary).text.upper())
        self.assertEqual(self.execute(ordinary), ((0, "unchanged"),))


def main(arguments=None):
    cases = sorted(name for name in SemanticDispatchContracts.__dict__ if name.startswith("test_"))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--log-dir", type=Path)
    parser.add_argument("--case", action="append", default=[], choices=cases)
    parser.add_argument("--list-cases", action="store_true")
    options = parser.parse_args(arguments)
    selected = list(dict.fromkeys(options.case)) or cases
    if options.list_cases:
        print("\n".join(selected))
        return 0
    if options.binary is None or not options.binary.is_file():
        parser.error("--binary must name an existing seekdb executable")
    if options.log_dir is not None and options.log_dir.exists():
        parser.error("--log-dir must not exist")
    database = PrivateDatabase(options.binary.resolve())
    database.server_log = database.base / "log/seekdb.log"
    try:
        database.start()
        SemanticDispatchContracts.database = database
        suite = unittest.TestSuite(SemanticDispatchContracts(name) for name in selected)
        result = unittest.TextTestRunner(verbosity=2, failfast=True).run(suite)
        return 0 if result.wasSuccessful() else 1
    finally:
        try:
            if options.log_dir is not None:
                cache.retain_deployment_logs(database, options.log_dir)
        finally:
            database.close()
            SemanticDispatchContracts.database = None


if __name__ == "__main__":
    raise SystemExit(main())
