"""Opt-in, ordered row-asynchronous TEXT MAP/FILTER SQL contracts.

Uses fixed 16-row input batches, the existing two-slot window and held HTTP
responses. A fast sibling's downstream request, a completed LIMIT, or a small
row read through SSCursor must precede releasing the slow response. Merely
observing out-of-order HTTP responses is not sufficient.

--binary starts only an owned private database; --socket never stops its
server. Reuses the existing exact request-identity/cache and embedding mocks.
No real models, performance claims, coroutines, FILE, JOIN, AGG or PX support.
"""

import argparse
from collections import Counter
import os
from pathlib import Path
import threading
import unittest
from unittest import mock

import pymysql

import test_semantic_cache as cache
import test_semantic_proxy as proxy
import test_semantic_runtime as semantic
from test_file_runtime import PrivateDatabase
from test_runtime_contracts import Reply, completion, sql_error


BATCH_ROWS = semantic.BATCH_ROWS
ROW_HINT = "OPT_PARAM('enable_semantic_row_async', 'true')"


class SemanticRowAsyncContracts(cache.SemanticCacheContracts):
    def statement(self, fields, *, enabled="true", caching="false", suffix="", **kwargs):
        hint = "" if enabled is None else f"OPT_PARAM('enable_semantic_row_async', '{enabled}')"
        return cache.sql_statement(
            fields, cache=caching, suffix=suffix, hints="PARALLEL(1) " + hint, **kwargs)

    def wait_requests(self, server, predicate, pending, message):
        semantic.wait_evidence(self, server, predicate, pending, message=message)

    def wait_batch(self, pending, offset=0):
        self.wait_requests(self.first, lambda: len(self.first.requests) >= offset + BATCH_ROWS,
                           pending, "the unchanged full input batch did not reach the provider")

    def stream(self, statement):
        rows = []
        first_row = threading.Event()

        def consume():
            with self.database.connection.cursor(pymysql.cursors.SSCursor) as cursor:
                cursor.execute(statement)
                for row in cursor:
                    rows.append(row)
                    first_row.set()
            return tuple(rows)

        return self.fixtures.start(consume), rows, first_row

    def run_existing_with_row_async(self, method):
        execute = self.execute

        def opted_in(statement, parameters=None):
            if "OPT_PARAM('rowsets_max_rows'," in statement:
                statement = statement.replace("/*+", "/*+ " + ROW_HINT, 1)
            return execute(statement, parameters)

        with mock.patch.object(self, "execute", side_effect=opted_in):
            method()

    def test_default_and_false_keep_the_whole_batch_barrier(self):
        prompts = [f"legacy-{index}" for index in range(BATCH_ROWS)]
        self.load_inputs(prompts)
        for enabled in (None, "false"):
            with self.subTest(enabled=enabled):
                fast, tail = self.fixtures.gate(), self.fixtures.gate()
                offset = len(self.first.requests)
                for index, prompt in enumerate(prompts):
                    self.fixtures.reply(0, prompt, True, kind="filter",
                                        gate=fast if index == 0 else tail)
                statement = self.statement(
                    "a.id", where=self.call("filter"), enabled=enabled, suffix="LIMIT 1")
                self.assertNotIn("ROW_ASYNC=", self.explain(statement).text.upper())
                pending = self.fixtures.start(lambda: self.execute(statement))
                self.wait_batch(pending, offset)
                fast.set()
                self.wait_requests(
                    self.first, lambda: any(record["prompt"] == prompts[0] and
                                            record["response"] is not None
                                            for record in semantic.audit(self.first)[offset:]),
                    pending, "fast legacy response was not sent")
                self.assertFalse(pending.done.wait(0.15), "default/off bypassed the group barrier")
                self.assertFalse(tail.is_set())
                tail.set()
                self.assertEqual(pending.result(), ((0,),))

    def test_prefix_limit_cancels_held_tail_without_shrinking_input_batch(self):
        prompts = [f"limit-{index}" for index in range(3 * BATCH_ROWS + 5)]
        self.load_inputs(prompts)
        for caching in ("false", "true"):
            with self.subTest(caching=caching):
                fast, tail = self.fixtures.gate(), self.fixtures.gate()
                offset = len(self.first.requests)
                for index, prompt in enumerate(prompts):
                    self.fixtures.reply(0, prompt, True, kind="filter",
                                        gate=fast if index == 0 else tail)
                statement = self.statement(
                    "a.id", where=self.call("filter"), caching=caching, suffix="LIMIT 1")
                self.assertIn("ROW_ASYNC=ORDERED_PREFIX", self.explain(statement).text.upper())
                pending = self.fixtures.start(lambda: self.execute(statement))
                self.wait_batch(pending, offset)
                fast.set()
                self.assertEqual(pending.result(timeout=3), ((0,),))
                self.assertFalse(tail.is_set(), "LIMIT required the slow response")
                self.assertLessEqual(len(self.first.requests) - offset, 2 * BATCH_ROWS,
                                     "lookahead exceeded the existing retained-row window")
                self.assert_no_model_sockets()
                tail.set()

    def test_nested_rows_advance_while_a_sibling_inner_request_is_held(self):
        prompts = [f"nested-{index}" for index in range(BATCH_ROWS)]
        self.load_inputs(prompts)
        for caching in ("false", "true"):
            with self.subTest(caching=caching):
                head, outer = self.fixtures.gate(), self.fixtures.gate()
                before = len(self.second.requests)
                for index, prompt in enumerate(prompts):
                    self.fixtures.reply(0, prompt, f"inner-{index}", gate=head if index == 0 else None)
                    self.fixtures.reply(1, f"outer:inner-{index}", f"result-{index}", gate=outer)
                expression = self.call("map", "CONCAT('outer:'," + self.call("map") + ")", provider=1)
                pending = self.fixtures.start(lambda: self.execute(
                    self.statement("a.id," + expression, caching=caching)))
                self.wait_requests(
                    self.second,
                    lambda: any(body["messages"][0]["content"] == "outer:inner-1"
                                for body in self.second.requests[before:]),
                    pending, "a completed row did not advance to its dependent call")
                self.assertFalse(head.is_set())
                self.assertFalse(pending.done.is_set(), repr(pending.error))
                self.assertFalse(any(body["messages"][0]["content"] == "outer:inner-0"
                                     for body in self.second.requests[before:]))
                head.set()
                outer.set()
                self.assertEqual(pending.result(),
                                 tuple((index, f"result-{index}") for index in range(BATCH_ROWS)))
                semantic.assert_dependencies(
                    self, semantic.audit(self.first)[-BATCH_ROWS:],
                    semantic.audit(self.second)[before:],
                    {f"outer:inner-{index}": prompt for index, prompt in enumerate(prompts)})

    def test_filter_prefix_reaches_map_before_the_slow_filter_tail(self):
        prompts = [f"filter-{index}" for index in range(BATCH_ROWS)]
        self.load_inputs(prompts)
        for caching in ("false", "true"):
            with self.subTest(caching=caching):
                fast, tail, mapped = (self.fixtures.gate() for _ in range(3))
                first_offset, second_offset = len(self.first.requests), len(self.second.requests)
                for index, prompt in enumerate(prompts):
                    self.fixtures.reply(0, prompt, index % 2 == 0, kind="filter",
                                        gate=fast if index < 2 else tail)
                    if index % 2 == 0:
                        self.fixtures.reply(1, prompt, f"mapped-{index}", gate=mapped)
                statement = self.statement(
                    "a.id," + self.call("map", provider=1), caching=caching,
                    where=self.call("filter"))
                plan = self.explain(statement)
                plan.one("SEMANTIC MAP")
                plan.one("SEMANTIC FILTER")
                self.assertEqual(plan.text.upper().count("ROW_ASYNC=ORDERED_PREFIX"), 2)
                pending = self.fixtures.start(lambda: self.execute(statement))
                self.wait_batch(pending, first_offset)
                fast.set()
                self.wait_requests(
                    self.second, lambda: len(self.second.requests) > second_offset,
                    pending, "the true FILTER prefix did not reach the independent MAP")
                self.assertEqual([body["messages"][0]["content"]
                                  for body in self.second.requests[second_offset:]], [prompts[0]])
                self.assertFalse(tail.is_set())
                self.assertFalse(pending.done.is_set(), repr(pending.error))
                tail.set()
                mapped.set()
                self.assertEqual(pending.result(),
                                 tuple((index, f"mapped-{index}") for index in range(0, BATCH_ROWS, 2)))
                self.assertEqual(Counter(body["messages"][0]["content"]
                                         for body in self.second.requests[second_offset:]),
                                 Counter(prompts[::2]))

    def test_slow_filter_head_preserves_order_and_bounds_lookahead(self):
        prompts = [f"head-{index}" for index in range(3 * BATCH_ROWS + 5)]
        self.load_inputs(prompts)
        for caching in ("false", "true"):
            with self.subTest(caching=caching):
                head, mapped = self.fixtures.gate(), self.fixtures.gate()
                first_offset, second_offset = len(self.first.requests), len(self.second.requests)
                for index, prompt in enumerate(prompts):
                    self.fixtures.reply(0, prompt, True, kind="filter",
                                        gate=head if index == 0 else None)
                    self.fixtures.reply(1, prompt, f"mapped-{index}", gate=mapped)
                pending = self.fixtures.start(lambda: self.execute(self.statement(
                    "a.id," + self.call("map", provider=1), where=self.call("filter"),
                    caching=caching)))
                self.wait_requests(
                    self.first,
                    lambda: len([record for record in semantic.audit(self.first)[first_offset:]
                                 if record["response"] is not None]) >= 2 * BATCH_ROWS - 1,
                    pending, "the bounded lookahead window did not complete behind the slow head")
                self.assertFalse(head.is_set())
                self.assertEqual(len(self.first.requests) - first_offset, 2 * BATCH_ROWS)
                self.assertEqual(len(self.second.requests), second_offset,
                                 "a separate MAP operator overtook the blocked FILTER head")
                self.assertFalse(pending.done.is_set(), repr(pending.error))
                head.set()
                mapped.set()
                self.assertEqual(pending.result(),
                                 tuple((index, f"mapped-{index}") for index in range(len(prompts))))

    def test_all_rejected_prefixes_are_not_eof_and_duplicates_remain_rows(self):
        prompts = ["drop"] * (2 * BATCH_ROWS) + ["keep", "drop", "keep", "keep", "drop"]
        self.load_inputs(prompts)
        for caching in ("false", "true"):
            with self.subTest(caching=caching):
                offset = len(self.first.requests)
                self.fixtures.reply(0, "drop", False, kind="filter")
                self.fixtures.reply(0, "keep", True, kind="filter")
                predicate = self.call("filter")
                rows = self.execute(self.statement(
                    "a.id," + predicate, where=predicate, caching=caching))
                self.assertEqual(rows, tuple((index, 1) for index, prompt in enumerate(prompts)
                                             if prompt == "keep"))
                self.assertTrue(all(type(row[1]) is int for row in rows))
                # WHERE and SELECT are distinct demands without query caching.
                wanted = (prompts + [prompt for prompt in prompts if prompt == "keep"]
                          if caching == "false" else ["drop", "keep"])
                self.assertEqual(Counter(body["messages"][0]["content"]
                                         for body in self.first.requests[offset:]), Counter(wanted))

    def test_small_prefix_is_visible_to_an_unbuffered_client(self):
        self.load_inputs(["stream-fast", "stream-tail"])
        for caching in ("false", "true"):
            with self.subTest(caching=caching):
                fast, tail = self.fixtures.gate(), self.fixtures.gate()
                offset = len(self.first.requests)
                self.fixtures.reply(0, "stream-fast", "small", gate=fast)
                self.fixtures.reply(0, "stream-tail", "last", gate=tail)
                pending, rows, first = self.stream(self.statement(
                    "a.id," + self.call("map"), caching=caching))
                self.wait_requests(self.first, lambda: len(self.first.requests) >= offset + 2,
                                   pending, "both streaming requests must enter one group")
                fast.set()
                self.assertTrue(first.wait(3), f"small prefix remained buffered: {pending.error!r}")
                self.assertEqual(rows, [(0, "small")])
                self.assertFalse(tail.is_set())
                self.assertFalse(pending.done.is_set(), repr(pending.error))
                tail.set()
                self.assertEqual(pending.result(), ((0, "small"), (1, "last")))

    def test_error_after_a_visible_prefix_is_not_a_successful_partial_result(self):
        self.load_inputs(["valid", "invalid"])
        for caching in ("false", "true"):
            with self.subTest(caching=caching):
                tail = self.fixtures.gate()
                self.fixtures.reply(0, "valid", True, kind="filter")
                self.fixtures.replies(0, "invalid", [
                    Reply(body=completion('{"value":"not-a-boolean"}', finish_reason="stop"), gate=tail)
                ], kind="filter")
                pending, rows, first = self.stream(self.statement(
                    "a.id," + self.call("filter"), caching=caching))
                self.assertTrue(first.wait(3), f"no prefix before bad tail: {pending.error!r}")
                self.assertEqual(rows, [(0, 1)])
                self.assertFalse(tail.is_set())
                tail.set()
                pending.expect_sql_error(self, 4070)
                self.assert_no_model_sockets()
                self.fixtures.reply(0, "valid", True, kind="filter")
                self.assertEqual(self.execute(self.statement(
                    "a.id," + self.call("filter"), caching=caching, where="a.id=0")), ((0, 1),))

    def exercise_stream_abort(self, *, cancel):
        self.load_inputs(["abort-fast", "abort-tail"])
        for caching in ("false", "true"):
            with self.subTest(caching=caching):
                tail = self.fixtures.gate()
                self.cursor.execute("SET ob_query_timeout=%s", (20000000 if cancel else 3000000,))
                self.fixtures.reply(0, "abort-fast", "visible")
                self.fixtures.reply(0, "abort-tail", "abandoned", gate=tail)
                pending, rows, first = self.stream(self.statement(
                    "a.id," + self.call("map"), caching=caching))
                self.assertTrue(first.wait(2), f"no prefix before abort: {pending.error!r}")
                self.assertEqual(rows, [(0, "visible")])
                if cancel:
                    self.kill_query()
                pending.expect_sql_error(self, 1317 if cancel else 4012)
                self.assertFalse(tail.is_set(), "abort needed the provider to release its response")
                self.assert_no_model_sockets()
                self.cursor.execute("SET ob_query_timeout=20000000")
                self.fixtures.reply(0, "abort-fast", "fresh")
                self.assertEqual(self.execute(self.statement(
                    "a.id," + self.call("map"), caching=caching, where="a.id=0")), ((0, "fresh"),))
                tail.set()

    def test_kill_after_prefix_cancels_and_recovers(self):
        self.exercise_stream_abort(cancel=True)

    def test_deadline_after_prefix_cancels_and_recovers(self):
        self.exercise_stream_abort(cancel=False)

    def test_retry_is_not_a_completion_or_a_duplicate_row(self):
        prompts = ["retry", "retry", "fast"]
        self.load_inputs(prompts)
        retry = self.fixtures.gate()
        self.fixtures.replies(0, "retry", [
            Reply(status=429, body=b'{"error":"retry"}', headers={"Retry-After": "0"}),
            Reply(body=completion("retried", finish_reason="stop"), gate=retry),
        ])
        self.fixtures.reply(0, "fast", "fast-value")
        for text in ("retried", "fast-value"):
            self.fixtures.reply(1, "outer:" + text, text + "-mapped")
        nested = self.call("map", "CONCAT('outer:'," + self.call("map") + ")", provider=1)
        pending = self.fixtures.start(lambda: self.execute(self.statement(
            "a.id," + nested, caching="true")))
        self.wait_requests(self.first, lambda: self.first.counts["retry"] == 2, pending,
                           "retry did not reach its held successful attempt")
        self.wait_requests(self.second, lambda: self.second.counts["outer:fast-value"] == 1,
                           pending, "retry backoff blocked a different row")
        self.assertEqual(self.second.counts["outer:retried"], 0)
        self.assertFalse(retry.is_set())
        retry.set()
        self.assertEqual(pending.result(),
                         ((0, "retried-mapped"), (1, "retried-mapped"), (2, "fast-value-mapped")))
        self.assertEqual(self.first.counts, Counter({"retry": 2, "fast": 1}))
        self.assertEqual(self.second.counts, Counter({"outer:retried": 1, "outer:fast-value": 1}))

    def test_guards_keep_undemanded_invalid_inputs_unrequested(self):
        self.run_existing_with_row_async(
            super().test_guards_and_undemanded_calls_preserve_invalid_skipped_inputs)

    def test_cache_memory_quota_fails_explicitly_and_releases_resources(self):
        self.run_existing_with_row_async(
            super().test_bounded_non_evicting_cache_reports_quota_failure_and_releases_memory)

    def test_invalid_options_and_unsupported_shapes_fail_before_model_work(self):
        self.load_inputs(["shape"])
        map_call = self.call("map")
        before = self.snapshots()
        for value in ("", "yes", "true,false"):
            with self.subTest(value=value):
                statement = self.statement("a.id," + map_call, enabled=value)
                for prefix in ("EXPLAIN ", ""):
                    sql_error(self, 1210, lambda: self.execute(prefix + statement))
        for fields in (
                "a.id",
                f"AI_AGG('{self.models[0]}',a.prompt,'Summarize the input.')",
                f"AI_MAP('{self.models[0]}', AI_INPUT('file:',TO_FILE(a.prompt)))"):
            with self.subTest(fields=fields):
                statement = self.statement(fields)
                for prefix in ("EXPLAIN ", ""):
                    sql_error(self, 1235, lambda: self.execute(prefix + statement))
        join = self.statement(
            "a.id,b.id", source="cache_inputs a JOIN cache_inputs b ON " + self.join_call())
        for prefix in ("EXPLAIN ", ""):
            sql_error(self, 1235, lambda: self.execute(prefix + join))
        self.cursor.executemany(
            "INSERT INTO cache_px_inputs VALUES (%s,%s)", [(index, "px") for index in range(256)])
        px = cache.sql_statement(
            "a.id," + map_call, source="cache_px_inputs a", suffix="",
            hints="PARALLEL(4) " + ROW_HINT)
        sql_error(self, 1235, lambda: self.execute(px))
        aggregate = cache.sql_statement(
            self.call("map", "CAST(SUM(a.id) AS CHAR)"), source="cache_px_inputs a", suffix="",
            hints="PARALLEL(4)")
        self.explain(aggregate).one("PX COORDINATOR")
        for prefix in ("EXPLAIN ", ""):
            enabled = aggregate.replace("/*+", "/*+ " + ROW_HINT, 1)
            sql_error(self, 1235, lambda: self.execute(prefix + enabled))
        self.assert_windows(before)


class SemanticRowAsyncProxyContracts(proxy.SemanticProxySmoke):
    def statement(self, *, hints=None, **kwargs):
        options = {"enable_semantic_row_async": "true"}
        options.update(hints or {})
        return super().statement(hints=options, **kwargs)

    def test_partial_proxy_completion_reaches_map_while_sibling_is_held(self):
        prompts = ["near-0", "near-1", "far-0", "near-0"]
        self.load(prompts)
        fast, tail = self.fixtures.gate(), self.fixtures.gate()
        for text, gate in (("near-0", fast), ("near-1", tail)):
            self.main.scenarios[proxy.INSTRUCTION + text] = [
                Reply(body=completion('{"value":true}', finish_reason="stop"), gate=gate)]
        for text in ("near-0", "near-1"):
            self.main.scenarios["mapped:" + text] = [Reply(body=completion("value:" + text))]
        quote = self.database.connection.escape
        fields = (f"id,prompt,AI_MAP({quote(self.models[0])},CONCAT('mapped:',prompt),"
                  "'{\"temperature\":0}')")
        statement = self.statement(fields=fields)
        plan = proxy.render_explain(self.execute("EXPLAIN " + statement))
        self.assertEqual(plan.upper().count("ROW_ASYNC=ORDERED_PREFIX"), 2)
        pending = self.fixtures.start(lambda: self.execute(statement))
        semantic.wait_evidence(
            self, self.main, lambda: self.main.counts[proxy.INSTRUCTION + "near-1"] == 1,
            pending, message="the slow forwarded proxy request did not enter the same flight")
        fast.set()
        semantic.wait_evidence(
            self, self.main, lambda: self.main.counts["mapped:near-0"] == 1,
            pending, message="a completed proxy entry was held behind its sibling completion")
        self.assertFalse(tail.is_set())
        self.assertFalse(pending.done.is_set(), repr(pending.error))
        tail.set()
        self.assertEqual(pending.result(),
                         ((0, "near-0", "value:near-0"), (1, "near-1", "value:near-1"),
                          (3, "near-0", "value:near-0")))
        embeddings, completions, _requests = self.work(map_instruction="mapped:")
        self.assertEqual(embeddings, Counter([proxy.RETRIEVAL, "near-0", "near-1", "far-0"]))
        self.assertEqual(completions, Counter([
            proxy.INSTRUCTION + "near-0", proxy.INSTRUCTION + "near-1",
            "mapped:near-0", "mapped:near-1"]))


def cases():
    direct = {name: SemanticRowAsyncContracts for name in SemanticRowAsyncContracts.__dict__
              if name.startswith("test_")}
    direct.update({name: SemanticRowAsyncProxyContracts
                   for name in unittest.defaultTestLoader.getTestCaseNames(SemanticRowAsyncProxyContracts)})
    return direct


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
            parser.error("--binary must be an executable file")
        database = PrivateDatabase(binary)
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
        for test_class in (SemanticRowAsyncContracts, SemanticRowAsyncProxyContracts):
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
