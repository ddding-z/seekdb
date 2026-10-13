"""Normal SIGUSR1 shutdown while object-storage GETs are held.

The private SQL client, storage gates and provider fixtures remain open through
shutdown. Exit must be zero within ten seconds, without forced termination.
"""

import argparse
import os
from pathlib import Path
import threading
import unittest

import pymysql

import test_file_runtime as files
from test_remote_file_runtime import MODEL_A, RemoteFileContracts


class RemoteFileShutdownContract(RemoteFileContracts):
    def close_case(self):
        self.storage.release()
        self.fixtures.release()
        for pending in self.fixtures.pending:
            pending.thread.join(5)
        self.assertFalse(any(pending.thread.is_alive() for pending in self.fixtures.pending),
                         "private shutdown query did not finish")
        self.assert_provider(self.first, [])
        self.assert_provider(self.second, [])

    def runTest(self):
        self.configure()
        assets = [self.storage.asset(seed=701 + index, gate=threading.Event())
                  for index in range(4)]
        self.insert([(index, "held-shutdown", asset.descriptor())
                     for index, asset in enumerate(assets)])
        pending = self.fixtures.start(lambda: self.execute(
            f"SELECT id, AI_MAP('{MODEL_A}', (text_value, asset)) "
            "FROM remote_values ORDER BY id"))
        self.storage.wait(lambda: len(self.storage.requests) == 4 and self.storage.active == 4,
                          "shutdown query did not start four held storage GETs")
        self.assertFalse(pending.done.is_set(), repr(pending.error))
        self.assert_provider(self.first, [])
        self.assert_provider(self.second, [])
        stops = self.database.normal_stops
        self.database.stop(close_connection_first=False, timeout=10)
        self.assertEqual(self.database.normal_stops, stops + 1)
        self.assertIsNone(self.database.process)
        self.assertIsNone(self.database.connection)
        pending.wait(timeout=5)
        self.assertIsInstance(pending.error, pymysql.MySQLError,
                              "held SQL client did not observe server shutdown")
        self.storage.wait(lambda: self.storage.active == 0 and
                          sum(record["disconnected"] for record in self.storage.requests) == 4,
                          "normal shutdown did not close every held storage socket")
        self.assertTrue(all(not self.storage.routes[asset.target].gate.is_set() for asset in assets))
        self.assert_storage([asset.target for asset in assets])


def main(arguments=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path,
                        help="Built seekdb binary; never an existing database")
    options = parser.parse_args(arguments)
    binary = options.binary.resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        parser.error(f"--binary is not an executable file: {binary}")
    database = files.PrivateDatabase(binary)
    try:
        database.start()
        RemoteFileShutdownContract.database = database
        RemoteFileShutdownContract.models_created = False
        result = unittest.TextTestRunner(verbosity=2).run(
            unittest.TestSuite([RemoteFileShutdownContract("runTest")]))
        if not result.wasSuccessful():
            raise SystemExit(1)
    finally:
        database.close()


if __name__ == "__main__":
    main()
