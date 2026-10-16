"""Owned local-PX TEXT row-async contracts, including client-visible partial DTL output.

The SQL statement opts into AUTO; a parallel scan alone is not worker evidence.
Held replies prove task/thread preparation, unchanged query-wide wave admission,
quiescent suspended drivers, and one tiny client row before any task can finish.
Only --binary launches an owned database. No real-model speedup is asserted.
"""

import argparse
import json
from pathlib import Path
import sys
import threading
import time
from types import SimpleNamespace

import pymysql
from pymysql.constants import FIELD_TYPE

import test_semantic_runtime as semantic
from test_semantic_cache import retain_deployment_logs
from test_semantic_px import ADMITTED_ROWS, ROWS, SemanticPXContracts
from test_runtime_contracts import Reply, answer, completion, configure_mock, sql_error


class SemanticPXRowAsyncContracts(SemanticPXContracts):
    log_dir = None

    def setUp(self):
        super().setUp()
        self.cursor.executemany("INSERT INTO semantic_inputs (id,prompt) VALUES (%s,%s)",
                                list(enumerate(self.prompts)))

    @classmethod
    def tearDownClass(cls):
        try:
            super().tearDownClass()
        finally:
            if cls.log_dir is not None:
                retain_deployment_logs(
                    SimpleNamespace(base=Path(cls.sql_socket).parent.parent), cls.log_dir)

    def statement(self, model="semantic_a", *, dop=4, fields=None, suffix="",
                  caching="false", row_async="true", order="auto", hints=""):
        options = [f"PARALLEL({dop})", "OPT_PARAM('rowsets_max_rows', 16)",
                   f"OPT_PARAM('enable_semantic_cache', '{caching}')"]
        if row_async is not None:
            options.append(f"OPT_PARAM('enable_semantic_row_async', '{row_async}')")
        if order is not None:
            options.append(f"OPT_PARAM('semantic_output_order', '{order}')")
        if hints:
            options.append(hints)
        fields = fields or f"id, AI_MAP('{model}', prompt)"
        return (f"SELECT /*+ {' '.join(options)} */ {fields} "
                f"FROM semantic_px_inputs {suffix}")

    def execute(self, statement):
        self.cursor.execute(statement)
        return self.cursor.fetchall()

    def stream(self, statement):
        rows = []
        visible = threading.Event()

        def consume():
            with self.connection.cursor(pymysql.cursors.SSCursor) as cursor:
                cursor.execute(statement)
                for row in cursor:
                    rows.append(row)
                    visible.set()
            return tuple(rows)

        return self.fixtures.start(consume), rows, visible

    def reset_provider(self, server):
        server.reset()
        configure_mock(server)

    def wait_tasks(self, start, dop, pending):
        deadline = time.monotonic() + 5
        records = []
        while time.monotonic() < deadline:
            records = self.preparations(start)
            tasks = {record[2] for record in records if record[2] >= 0}
            if len(tasks) == dop:
                self.assertEqual(len({record[0] for record in records}), 1)
                self.assertGreaterEqual(len({record[3] for record in records}), min(dop, 2),
                                        "task IDs alone do not prove multi-threaded preparation")
                return records
            if pending.done.wait(0.01):
                break
        self.fail(f"missing {dop} actual worker preparations: {records!r}; error={pending.error!r}")

    def start_held(self, *, dop=4, caching="false", admitted_rows=ADMITTED_ROWS):
        statement = self.statement(dop=dop, caching=caching)
        labels = ["SEMANTIC MAP", "ROW_ASYNC=UNORDERED_READY", "OUTPUT_POLICY=AUTO"]
        if dop > 1:
            labels.extend(["PX COORDINATOR", f"DOP={dop}", "EXECUTION_SCOPE=LOCAL_PX_WORKER",
                           "ORDER_REASON=PX_GATHER_INPUT"])
        else:
            statement = statement.replace("FROM semantic_px_inputs", "FROM semantic_inputs")
            labels.append("EXECUTION_SCOPE=SERIAL")
        self.assert_plan_labels(statement, labels)
        gate = self.fixtures.gate()
        self.mock_a.default_reply = Reply(gate=gate)
        start = self.log.stat().st_size
        pending = self.fixtures.start(lambda: self.execute(statement))
        semantic.wait_evidence(
            self, self.mock_a, lambda: len(self.mock_a.requests) >= admitted_rows, pending,
            message="shared row-async admission window did not reach the model")
        self.assertFalse(pending.done.is_set(), repr(pending.error))
        records = self.wait_tasks(start, dop, pending) if dop > 1 else []
        with self.control_connection() as control:
            self.await_quiescence(control)
        with self.mock_a.condition:
            self.mock_a.condition.wait_for(
                lambda: len(self.mock_a.requests) > admitted_rows or pending.done.is_set(),
                timeout=0.15)
            self.assertEqual(len(self.mock_a.requests), admitted_rows,
                             "DOP multiplied the shared query task/wave budget")
        semantic.assert_held(self, self.mock_a, [gate])
        return pending, gate, records

    def start_tiny_prefix(self, *, row_async="true", order="auto"):
        gates = {prompt: self.fixtures.gate() for prompt in self.prompts}
        replies = {prompt: Reply(gate=gate) for prompt, gate in gates.items()}
        for prompt, reply in replies.items():
            self.mock_a.scenarios[prompt] = [reply]
        statement = self.statement(row_async=row_async, order=order)
        start = self.log.stat().st_size
        pending, rows, visible = self.stream(statement)
        semantic.wait_evidence(
            self, self.mock_a, lambda: len(self.mock_a.requests) >= ADMITTED_ROWS, pending,
            message="tiny-prefix query did not fill its unchanged input batches")
        self.wait_tasks(start, 4, pending)
        admitted = [record["prompt"] for record in semantic.audit(self.mock_a)]
        self.assertGreaterEqual(len(admitted), 2)
        return pending, rows, visible, gates, replies, admitted[:2]

    def recover_without_releasing_old_replies(self):
        self.assert_no_model_sockets()
        self.cursor.execute("SET ob_query_timeout=20000000, ai_pipeline_memory_limit=DEFAULT")
        self.cursor.execute(self.statement("semantic_b"))
        self.assert_map_rows(self.cursor.fetchall(), self.mock_b)

    def test_dop_1_2_4_preserve_bag_and_equal_query_admission(self):
        for dop in (1, 2, 4):
            with self.subTest(dop=dop):
                self.reset_provider(self.mock_a)
                pending, gate, records = self.start_held(dop=dop)
                peak_held_http = self.mock_a.peak
                gate.set()
                self.assert_map_rows(pending.result(timeout=20), self.mock_a)
                print("PX_ROW_EVIDENCE " + json.dumps({
                    "dop": dop, "rows": ROWS, "held_http": peak_held_http,
                    "semantic_tasks": len({record[2] for record in records}),
                    "prepare_threads": len({record[3] for record in records}),
                    "http_attempts": len(self.mock_a.requests),
                }), flush=True)

    def test_one_tiny_peer_row_reaches_client_before_all_other_replies(self):
        pending, rows, visible, gates, _replies, (head, peer) = self.start_tiny_prefix()
        gates[peer].set()
        self.assertTrue(visible.wait(3), f"one completed datum remained buffered in DTL: {pending.error!r}")
        peer_id = self.prompts.index(peer)
        self.assertEqual(rows, [(peer_id, answer(peer))])
        self.assertFalse(gates[head].is_set())
        self.assertFalse(pending.done.is_set(), "a worker falsely sent EOF while replies were held")
        self.assertTrue(all(gate.is_set() == (prompt == peer) for prompt, gate in gates.items()))
        self.fixtures.release()
        self.assert_map_rows(pending.result(timeout=20), self.mock_a)

    def test_default_batch_negative_control_keeps_the_same_tiny_peer_buffered(self):
        pending, rows, visible, _gates, _replies, (_head, peer) = self.start_tiny_prefix(
            row_async=None, order=None)
        self.mock_a.scenarios[peer][0].gate.set()
        semantic.wait_evidence(
            self, self.mock_a,
            lambda: any(record["prompt"] == peer and record["response"] is not None
                        for record in semantic.audit(self.mock_a)),
            pending, message="negative-control peer did not finish at the provider")
        self.assertFalse(visible.wait(0.15), "legacy batch execution bypassed its barrier")
        self.assertEqual(rows, [])
        self.fixtures.release()
        self.assert_map_rows(pending.result(timeout=20), self.mock_a)

    def test_cache_deduplicates_across_workers_and_survives_worker_gaps(self):
        self.prompts = [f"shared-px-{index % 8}" for index in range(ROWS)]
        self.cursor.executemany("UPDATE semantic_px_inputs SET prompt=%s WHERE id=%s",
                                [(prompt, index) for index, prompt in enumerate(self.prompts)])
        for caching, admitted in (("false", ADMITTED_ROWS), ("true", 2)):
            with self.subTest(caching=caching):
                self.reset_provider(self.mock_a)
                pending, gate, _records = self.start_held(
                    caching=caching, admitted_rows=admitted)
                gate.set()
                self.assertEqual(sorted(pending.result(timeout=20)),
                                 list(enumerate(answer(prompt) for prompt in self.prompts)))
                expected = self.prompts if caching == "false" else list(dict.fromkeys(self.prompts))
                self.assert_counts(self.mock_a, expected)
        self.reset_provider(self.mock_a)
        self.mock_a.default_reply = Reply(body=completion("fresh-query-result"))
        rows = self.execute(self.statement(caching="true"))
        self.assertEqual(sorted(rows), [(index, "fresh-query-result") for index in range(ROWS)])
        self.assert_counts(self.mock_a, list(dict.fromkeys(self.prompts)))

    def exercise_nested_maps(self, caching, depth):
        self.reset_provider(self.mock_a)
        self.reset_provider(self.mock_b)
        gates = {prompt: self.fixtures.gate() for prompt in self.prompts}
        for prompt, gate in gates.items():
            self.mock_a.scenarios[prompt] = [Reply(gate=gate)]
        mapped = "AI_MAP('semantic_b', AI_MAP('semantic_a', prompt))"
        if depth == 3:
            mapped = f"AI_MAP('semantic_a', {mapped})"
        statement = self.statement(fields=f"id, {mapped}", caching=caching)
        self.assert_plan_labels(statement, ("EXECUTION_SCOPE=LOCAL_PX_WORKER",))
        start = self.log.stat().st_size
        pending = self.fixtures.start(lambda: self.execute(statement))
        semantic.wait_evidence(
            self, self.mock_a,
            lambda: len(self.mock_a.requests) >= (ADMITTED_ROWS if caching == "false" else 2), pending,
            message="nested inner calls did not start")
        self.wait_tasks(start, 4, pending)
        head, peer = [record["prompt"] for record in semantic.audit(self.mock_a)[:2]]
        gates[peer].set()
        semantic.wait_evidence(
            self, self.mock_b, lambda: bool(self.mock_b.requests), pending,
            message="a completed inner row did not advance to its dependent model")
        self.assert_counts(self.mock_b, [answer(peer)])
        if depth == 3:
            semantic.wait_evidence(
                self, self.mock_a, lambda: self.mock_a.counts[answer(answer(peer))] == 1, pending,
                message="a third dependent call starved behind held inner waves")
        self.assertFalse(gates[head].is_set())
        self.fixtures.release()
        values = [answer(answer(prompt)) for prompt in self.prompts]
        if depth == 3:
            values = [answer(value) for value in values]
        self.assertEqual(sorted(pending.result(timeout=20)),
                         list(enumerate(values)))
        self.assert_counts(self.mock_a, self.prompts + (
            [answer(answer(prompt)) for prompt in self.prompts] if depth == 3 else []))
        self.assert_counts(self.mock_b, [answer(prompt) for prompt in self.prompts])
        semantic.assert_dependencies(
            self, semantic.audit(self.mock_a), semantic.audit(self.mock_b),
            {answer(prompt): prompt for prompt in self.prompts})

    def test_nested_maps_advance_per_row_across_the_worker_boundary(self):
        for caching in ("false", "true"):
            with self.subTest(caching=caching):
                self.exercise_nested_maps(caching, 2)

    def test_three_nested_maps_with_reused_model_preserve_dependency_capacity(self):
        for caching in ("false", "true"):
            with self.subTest(caching=caching):
                self.exercise_nested_maps(caching, 3)

    def test_guarded_invalid_inputs_remain_undemanded_in_each_worker(self):
        mapped = "AI_MAP('semantic_a', IF(id%2=0,prompt,NULL))"
        for caching in ("false", "true"):
            with self.subTest(caching=caching):
                self.reset_provider(self.mock_a)
                rows = self.execute(self.statement(
                    fields=f"id, CASE WHEN id%2=0 THEN {mapped} ELSE 'skip' END",
                    caching=caching))
                self.assertEqual(sorted(rows),
                                 [(index, answer(prompt) if index % 2 == 0 else "skip")
                                  for index, prompt in enumerate(self.prompts)])
                self.assert_counts(self.mock_a, self.prompts[::2])

    def test_filter_prunes_map_demand_with_shared_stage_budget(self):
        for index, prompt in enumerate(self.prompts):
            self.filter_reply(self.mock_a, prompt, index % 3 == 0)
        statement = self.statement("semantic_b", suffix="WHERE AI_FILTER('semantic_a', prompt)")
        self.assert_plan_labels(statement, ("SEMANTIC FILTER", "SEMANTIC MAP",
                                           "EXECUTION_SCOPE=LOCAL_PX_WORKER"))
        self.assertEqual(sorted(self.execute(statement)),
                         [(index, answer(prompt)) for index, prompt in enumerate(self.prompts)
                          if index % 3 == 0])
        self.assert_counts(self.mock_a, self.prompts)
        self.assert_counts(self.mock_b, [prompt for index, prompt in enumerate(self.prompts)
                                       if index % 3 == 0])

    def test_rank_is_double_and_preserves_exact_candidate_demand(self):
        scores = [index / 4 - 7 for index in range(ROWS)]
        for prompt, score in zip(self.prompts, scores):
            self.mock_a.scenarios[prompt] = [
                Reply(body=completion(json.dumps({"value": score}), finish_reason="stop"))]
        statement = self.statement(fields="id, AI_RANK('semantic_a', prompt)")
        self.assert_plan_labels(statement, ("SEMANTIC RANK", "EXECUTION_SCOPE=LOCAL_PX_WORKER"))
        self.assertEqual(sorted(self.execute(statement)), list(enumerate(scores)))
        self.assertEqual(self.cursor.description[1][1], FIELD_TYPE.DOUBLE)
        self.assert_counts(self.mock_a, self.prompts)

    def test_provider_failure_after_visible_prefix_cancels_peers_and_recovers(self):
        pending, rows, visible, gates, replies, (failed, peer) = self.start_tiny_prefix()
        replies[failed].status = 400
        gates[peer].set()
        self.assertTrue(visible.wait(3), f"prefix was not client-visible: {pending.error!r}")
        self.assertEqual(rows, [(self.prompts.index(peer), answer(peer))])
        gates[failed].set()
        pending.expect_sql_error(self, 4216, timeout=5)
        self.assertTrue(any(not gate.is_set() for gate in gates.values()))
        self.recover_without_releasing_old_replies()

    def test_kill_cancels_all_held_workers_without_releasing_replies(self):
        pending, gate, _records = self.start_held()
        self.kill_query()
        pending.expect_sql_error(self, 1317, timeout=5)
        self.assertFalse(gate.is_set())
        self.recover_without_releasing_old_replies()
        self.assertFalse(gate.is_set())

    def test_deadline_cancels_all_held_workers_without_releasing_replies(self):
        self.cursor.execute("SET ob_query_timeout=3000000")
        pending, gate, _records = self.start_held()
        pending.expect_sql_error(self, 4012, timeout=5)
        self.assertFalse(gate.is_set())
        self.recover_without_releasing_old_replies()
        self.assertFalse(gate.is_set())

    def test_query_memory_overflow_is_explicit_and_recovers(self):
        self.cursor.execute("UPDATE semantic_px_inputs SET prompt=REPEAT('x',1048576) WHERE id=0")
        self.cursor.execute("SET ai_pipeline_memory_limit=1048576")
        sql_error(self, 4019, lambda: self.execute(self.statement()))
        self.assert_no_model_sockets()
        self.cursor.execute("UPDATE semantic_px_inputs SET prompt=%s WHERE id=0", (self.prompts[0],))
        self.recover_without_releasing_old_replies()

    def test_unsupported_parallel_boundaries_fail_before_model_demand(self):
        statements = [
            self.statement(suffix="ORDER BY id"),
            self.statement(suffix="ORDER BY 1+0"),
            self.statement(suffix="LIMIT 999999999"),
            self.statement(suffix="GROUP BY id"),
            self.statement(fields="DISTINCT id, AI_MAP('semantic_a', prompt)"),
            self.statement(suffix="HAVING id>=0"),
            self.statement(suffix="WHERE id=(SELECT MIN(id) FROM semantic_px_inputs)"),
            self.statement(fields="a.id, AI_MAP('semantic_a',a.prompt)",
                           suffix="a JOIN semantic_px_inputs b ON a.id=b.id"),
            self.statement().replace(
                "FROM semantic_px_inputs",
                "FROM (SELECT id,prompt FROM semantic_px_inputs) merged"),
            self.statement(fields="id, AI_MAP('semantic_a',AI_INPUT('describe:',TO_FILE(prompt)))"),
            self.statement(fields="id, AI_MAP(IF(id=0,'semantic_a','semantic_b'),prompt)"),
            self.statement(fields="id, AI_MAP('semantic_a',prompt,CONCAT('{\"temperature\":',id,'}'))"),
            self.statement(fields="id, AI_JOIN('semantic_a','same',prompt,prompt)"),
            self.statement(fields="AI_AGG('semantic_a',prompt,'Summarize.')"),
            self.statement(order="ordered"),
            self.statement(order="unordered"),
            self.statement(dop=1),
            self.statement("semantic_b", caching="true",
                           suffix="WHERE AI_FILTER('semantic_a',prompt)",
                           hints="OPT_PARAM('enable_semantic_proxy','true')"),
        ]
        for statement in statements:
            with self.subTest(sql=statement):
                error = sql_error(self, 1235, lambda: self.execute(statement))
                self.assertTrue(error.args[1], "unsupported scope lacks an explicit diagnostic")
                self.assertEqual(self.mock_a.requests, [])
                self.assertEqual(self.mock_b.requests, [])


def main(arguments=None):
    arguments = list(sys.argv[1:] if arguments is None else arguments)
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--log-dir", type=Path)
    options, arguments = parser.parse_known_args(arguments)
    if options.log_dir is not None and "--binary" not in arguments:
        parser.error("--log-dir requires --binary")
    SemanticPXRowAsyncContracts.log_dir = options.log_dir
    if "--case" not in arguments and "--self-test" not in arguments:
        for name in sorted(SemanticPXRowAsyncContracts.__dict__):
            if name.startswith("test_"):
                arguments.extend(["--case", name])
    semantic.SemanticContracts = SemanticPXRowAsyncContracts
    semantic.main(arguments)


if __name__ == "__main__":
    main()
