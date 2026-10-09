"""Private SQL PX contracts with gated replies and actual task/thread evidence."""

import os
from pathlib import Path
import re
import signal
import sys
import threading
import time

import pymysql

import test_semantic_runtime as semantic
from test_runtime_contracts import Reply, answer


PREPARE = re.compile(
    r"semantic first prepare begin\(execution_id=(\d+), operator_id=(\d+), "
    r"px_task_id=(-?\d+), thread_id=(\d+), rows=(\d+)\)")
ROWS = 256
ADMITTED_ROWS = 32


class SemanticPXContracts(semantic.SemanticContracts):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        with cls.connection.cursor() as cursor:
            cursor.execute("CREATE TABLE semantic_px_inputs "
                           "(id INT PRIMARY KEY, prompt LONGTEXT) "
                           "PARTITION BY HASH(id) PARTITIONS 8")

    def setUp(self):
        super().setUp()
        self.prompts = [f"px-row-{index}" for index in range(ROWS)]
        self.cursor.execute("TRUNCATE TABLE semantic_px_inputs")
        self.cursor.executemany("INSERT INTO semantic_px_inputs VALUES (%s, %s)",
                                list(enumerate(self.prompts)))
        self.log = Path(self.sql_socket).parent.parent / "log/seekdb.log"

    def statement(self, model="semantic_a"):
        return ("SELECT /*+ PARALLEL(4) OPT_PARAM('rowsets_max_rows', 16) */ "
                f"id, AI_MAP('{model}', prompt) FROM semantic_px_inputs")

    def control_connection(self):
        return pymysql.connect(unix_socket=self.sql_socket, user="root", autocommit=True,
                               database="ai_semantic_contract_test",
                               read_timeout=10, write_timeout=10)

    def preparations(self, start):
        with self.log.open(errors="replace") as log:
            log.seek(start)
            return [tuple(map(int, match.groups())) for line in log
                    if (match := PREPARE.search(line))]

    def wait_for_preparations(self, start):
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            records = self.preparations(start)
            if len({record[2] for record in records if record[2] >= 0}) == 4:
                return records
            threading.Event().wait(0.01)
        self.fail(f"four actual PX preparation tasks were not observed: {records!r}")

    def await_quiescence(self, control):
        with control.cursor() as cursor:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                cursor.execute("SELECT COUNT(*) FROM oceanbase.__all_virtual_px_worker_stat")
                if cursor.fetchone() == (0,):
                    return
                threading.Event().wait(0.01)
        self.fail("PX task slices remained active while every reply was held")

    def start_held_map(self, *, status=200, statement=None,
                       admitted_rows=ADMITTED_ROWS, configure=None,
                       exact_admission=True,
                       labels=("PX COORDINATOR", "SEMANTIC MAP", "DOP=4")):
        if statement is None:
            statement = self.statement()
        self.assert_plan_labels(statement, labels)
        gate = self.fixtures.gate()
        self.mock_a.default_reply = Reply(status=status, gate=gate)
        if configure is not None:
            configure(gate)
        start = self.log.stat().st_size

        def execute():
            self.cursor.execute(statement)
            return self.cursor.fetchall()

        pending = self.fixtures.start(execute)
        minimum_rows = admitted_rows if exact_admission else 1
        with self.mock_a.condition:
            self.assertTrue(self.mock_a.condition.wait_for(
                lambda: len(self.mock_a.requests) >= minimum_rows or pending.done.is_set(),
                timeout=5), "semantic requests did not submit before any reply")
        self.assertFalse(pending.done.is_set(), repr(pending.error))
        records = self.wait_for_preparations(start)
        self.assertEqual(len({record[0] for record in records}), 1,
                         "preparation evidence must belong to one execution")
        self.assertGreaterEqual(len({record[3] for record in records}), 2,
                                "multiple task IDs do not prove multiple CPU threads")
        with self.control_connection() as control:
            self.await_quiescence(control)
        with self.mock_a.condition:
            self.mock_a.condition.wait_for(
                lambda: len(self.mock_a.requests) > admitted_rows or pending.done.is_set(),
                timeout=0.2)
            if exact_admission:
                self.assertEqual(len(self.mock_a.requests), admitted_rows,
                                 "DOP multiplied the query-wide slots=2 client-batch limit")
            else:
                # Guarded client-batches can contain fewer than sixteen demanded rows.
                self.assertLessEqual(len(self.mock_a.requests), admitted_rows,
                                     "guarded requests exceeded the query-wide admission ceiling")
        return pending, gate, records

    def assert_map_rows(self, rows, mock):
        self.assertEqual(sorted(rows),
                         list(enumerate(answer(prompt) for prompt in self.prompts)))
        self.assert_counts(mock, self.prompts)

    def assert_recovery(self):
        self.assert_no_model_sockets()
        self.fixtures.release()
        self.mock_a.reset()
        self.mock_a.default_reply = Reply()
        self.cursor.execute("SET ob_query_timeout=20000000")
        self.cursor.execute(self.statement())
        self.assert_map_rows(self.cursor.fetchall(), self.mock_a)

    def test_parallel_map_splits_input_and_shares_admission(self):
        pending, gate, _records = self.start_held_map()
        gate.set()
        self.assert_map_rows(pending.result(timeout=20), self.mock_a)

    def test_suspended_px_drivers_execute_another_parallel_query(self):
        pending, gate, records = self.start_held_map()
        original_threads = {record[3] for record in records}
        start = self.log.stat().st_size
        with self.control_connection() as control:
            with control.cursor() as cursor:
                cursor.execute("SET ai_pipeline_slots=2")
                cursor.execute(self.statement("semantic_b"))
                self.assert_map_rows(cursor.fetchall(), self.mock_b)
        later = self.wait_for_preparations(start)
        self.assertNotEqual(later[0][0], records[0][0])
        self.assertTrue({record[3] for record in later}.issubset(original_threads),
                        "the control query relied on additional PX driver threads")
        self.assertFalse(pending.done.is_set(), "held replies completed unexpectedly")
        gate.set()
        self.assert_map_rows(pending.result(timeout=20), self.mock_a)

    def test_parallel_filter_prunes_map_rows_with_shared_stage_budget(self):
        statement = (self.statement("semantic_b") +
                     " WHERE AI_FILTER('semantic_a', prompt)")
        accepted = [(index, prompt) for index, prompt in enumerate(self.prompts) if index % 3]

        def configure(gate):
            for index, prompt in enumerate(self.prompts):
                self.filter_reply(self.mock_a, prompt, index % 3 != 0, gate=gate)

        self.assert_plan_labels(statement, ("SEMANTIC FILTER",))
        pending, gate, _records = self.start_held_map(
            statement=statement, admitted_rows=64, configure=configure)
        self.assertEqual(self.mock_b.requests, [],
                         "dependent maps must not run before the filter replies")
        gate.set()
        self.assertEqual(sorted(pending.result(timeout=20)),
                         [(index, answer(prompt)) for index, prompt in accepted])
        self.assert_counts(self.mock_a, self.prompts)
        self.assert_counts(self.mock_b, [prompt for _index, prompt in accepted])

    def test_parallel_nested_maps_preserve_dependencies_and_row_mapping(self):
        statement = ("SELECT /*+ PARALLEL(4) OPT_PARAM('rowsets_max_rows', 16) */ "
                     "id, AI_MAP('semantic_b', AI_MAP('semantic_a', prompt)) "
                     "FROM semantic_px_inputs")
        pending, gate, _records = self.start_held_map(statement=statement, admitted_rows=64)
        self.assertEqual(self.mock_b.requests, [],
                         "dependent maps must not run before their input maps")
        gate.set()
        self.assertEqual(sorted(pending.result(timeout=20)),
                         [(index, answer(answer(prompt)))
                          for index, prompt in enumerate(self.prompts)])
        self.assert_counts(self.mock_a, self.prompts)
        self.assert_counts(self.mock_b, [answer(prompt) for prompt in self.prompts])

    def test_parallel_having_alias_materializes_skipped_projection_without_replay(self):
        statement = ("SELECT /*+ PARALLEL(4) OPT_PARAM('rowsets_max_rows', 16) */ "
                     "id, AI_FILTER('semantic_a', prompt) AS b FROM semantic_px_inputs "
                     "HAVING CASE WHEN MOD(id, 7) = 0 THEN TRUE ELSE b END")

        def configure(gate):
            for index, prompt in enumerate(self.prompts):
                self.filter_reply(self.mock_a, prompt, index % 3 != 0, gate=gate)

        pending, gate, _records = self.start_held_map(
            statement=statement, configure=configure, exact_admission=False,
            labels=("PX COORDINATOR", "SEMANTIC FILTER", "DOP=4"))
        gate.set()
        self.assertEqual(sorted(pending.result(timeout=20)),
                         [(index, int(index % 3 != 0)) for index in range(ROWS)
                          if index % 7 == 0 or index % 3 != 0])
        self.assert_counts(self.mock_a, self.prompts)

    def test_qc_cancel_cleans_all_suspended_px_tasks_and_recovers(self):
        pending, _gate, _records = self.start_held_map()
        self.kill_query()
        pending.expect_sql_error(self, 1317, timeout=5)
        self.assert_recovery()

    def test_deadline_cleans_all_suspended_px_tasks_and_recovers(self):
        self.cursor.execute("SET ob_query_timeout=3000000")
        pending, _gate, _records = self.start_held_map()
        pending.expect_sql_error(self, 4012, timeout=5)
        self.assert_recovery()

    def test_provider_failure_cleans_suspended_px_tasks_and_recovers(self):
        pending, gate, _records = self.start_held_map(status=400)
        gate.set()
        pending.expect_sql_error(self, 4216, timeout=5)
        self.assert_recovery()

    def check_shutdown_with_suspended_px_tasks(self):
        pending, _gate, _records = self.start_held_map()
        exit_waiter = self.fixtures.start(
            lambda: os.waitid(os.P_PID, self.process_id, os.WEXITED | os.WNOWAIT))
        os.kill(self.process_id, signal.SIGUSR1)
        status = exit_waiter.result(timeout=10)
        self.assertEqual((status.si_code, status.si_status), (os.CLD_EXITED, 0),
                         "shutdown crashed or required forced process termination")
        pending.wait(timeout=5)
        self.assertIsInstance(pending.error, pymysql.MySQLError)


def main(arguments=None):
    arguments = list(sys.argv[1:] if arguments is None else arguments)
    shutdown = "--shutdown-test" in arguments
    if shutdown:
        arguments.remove("--shutdown-test")
        SemanticPXContracts.test_shutdown = SemanticPXContracts.check_shutdown_with_suspended_px_tasks
        if "--case" not in arguments:
            arguments.extend(["--case", "test_shutdown"])
    elif "--case" not in arguments and "--self-test" not in arguments:
        for name in sorted(SemanticPXContracts.__dict__):
            if name.startswith("test_"):
                arguments.extend(["--case", name])
    semantic.SemanticContracts = SemanticPXContracts
    semantic.main(arguments)


if __name__ == "__main__":
    main()
