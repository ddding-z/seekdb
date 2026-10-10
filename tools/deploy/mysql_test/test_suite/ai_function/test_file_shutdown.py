"""Normal SIGUSR1 shutdown with held FILE HTTP requests in private SQL processes.

Each case owns a separate database. Its client and provider gates remain open
through server shutdown. Forced termination or a nonzero exit is a failure.
The sorted PX case also requires four real tasks and zero active worker slices.
"""

import argparse
import os
from pathlib import Path
import unittest

import pymysql

import test_semantic_runtime as semantic
from test_file_runtime import FileContracts, PrivateDatabase


CASES = ("coordinator", "sorted-px")


class FileShutdownContract(FileContracts):
    mode = None

    def stop_while_held(self, pending, gate):
        semantic.assert_held(self, self.first, [gate])
        stops = self.database.normal_stops
        self.media_configured = False
        self.database.stop(close_connection_first=False, timeout=10)
        self.assertEqual(self.database.normal_stops, stops + 1)
        self.assertIsNone(self.database.process)
        self.assertIsNone(self.database.connection)
        pending.wait(timeout=5)
        self.assertIsInstance(pending.error, pymysql.MySQLError,
                              "the held SQL client must observe server shutdown")
        semantic.assert_held(self, self.first, [gate])
        self.assertFalse(self.second.requests)

    def runTest(self):
        if self.mode == "coordinator":
            pending, gate, _contents = self.start_held_media("shutdown-media")
            self.stop_while_held(pending, gate)
        elif self.mode == "sorted-px":
            self.check_media_px_transport(ordered=True, on_held=self.stop_while_held)
        else:
            raise ValueError(f"unknown FILE shutdown mode: {self.mode!r}")


def main(arguments=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path,
                        help="Built seekdb binary; never an existing database")
    parser.add_argument("--case", action="append", choices=CASES, default=[])
    options = parser.parse_args(arguments)
    binary = options.binary.resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        parser.error(f"--binary is not an executable file: {binary}")
    for mode in options.case or CASES:
        database = PrivateDatabase(binary)
        try:
            database.start()
            FileShutdownContract.database = database
            FileShutdownContract.models_created = False
            FileShutdownContract.mode = mode
            print(f"FILE held shutdown case: {mode}", flush=True)
            result = unittest.TextTestRunner(verbosity=2).run(
                unittest.TestSuite([FileShutdownContract("runTest")]))
            if not result.wasSuccessful():
                raise SystemExit(1)
        finally:
            database.close()


if __name__ == "__main__":
    main()
