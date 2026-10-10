"""Focused native FILE INSERT SELECT contracts using a private UNIX-socket database."""

import argparse
import os
from pathlib import Path
import unittest

import pymysql

from test_file_runtime import (
    PrivateDatabase,
    decode_descriptor,
    descriptor,
    descriptor_text,
    run_suite,
)


class FileInsertSelectContracts(unittest.TestCase):
    database = None

    @classmethod
    def setUpClass(cls):
        with cls.database.connection.cursor() as cursor:
            cursor.execute("CREATE DATABASE file_insert_select_test")
            cursor.execute("USE file_insert_select_test")
            cursor.execute("CREATE TABLE t (id INT PRIMARY KEY, bucket INT, "
                           "text_value LONGTEXT, asset FILE, other_asset FILE)")
            cursor.execute("CREATE TABLE text_sink (value LONGTEXT)")

    def setUp(self):
        self.cursor = self.database.connection.cursor()
        self.addCleanup(self.cursor.close)
        self.cursor.execute("USE file_insert_select_test")
        self.cursor.execute("SET ob_query_timeout=30000000")
        self.cursor.execute("TRUNCATE TABLE t")
        self.cursor.execute("TRUNCATE TABLE text_sink")
        self.seed_path = self.database.base / "uncreated-media/seed.png"
        self.assertFalse(self.seed_path.exists())
        self.cursor.execute(
            "INSERT INTO t VALUES (1, 2, 'seed', TO_FILE(%s), NULL)", (str(self.seed_path),))

    def test_insert_select_stores_native_file_without_media_io(self):
        path = self.database.base / "uncreated-media/copied.jpeg"
        expected = descriptor(path, mime_type="image/jpeg", object_version="copied-v1", size_bytes=0)
        self.assertFalse(path.exists())
        self.cursor.execute(
            "INSERT INTO t SELECT 4, 8, %s, TO_FILE(%s), NULL FROM t WHERE id=1",
            ("copied", descriptor_text(expected)))
        self.assertEqual(self.cursor.rowcount, 1)
        self.cursor.execute(
            "INSERT INTO t SELECT 5, 10, 'column-copy', asset, other_asset FROM t WHERE id=4")
        self.assertEqual(self.cursor.rowcount, 1)
        self.cursor.execute(
            "SELECT id, bucket, text_value, asset, FILE_URI(asset), FILE_META(asset), other_asset "
            "FROM t WHERE id IN (4, 5) ORDER BY id")
        rows = self.cursor.fetchall()
        self.assertEqual(len(rows), 2)
        for row, scalars in zip(rows, ((4, 8, "copied"), (5, 10, "column-copy"))):
            self.assertEqual(row[:3], scalars)
            self.assertEqual(decode_descriptor(row[3]), expected)
            self.assertEqual(row[4], expected["uri"])
            self.assertEqual(decode_descriptor(row[5]), expected)
            self.assertIsNone(row[6])
        self.cursor.execute("DESCRIBE t")
        types = {row[0]: row[1].lower() for row in self.cursor.fetchall()}
        self.assertEqual(types["asset"], "file")
        self.assertEqual(types["other_asset"], "file")
        self.assertFalse(path.exists())
        self.assertFalse(self.seed_path.exists())

    def test_insert_select_does_not_enable_whole_file_operations_or_implicit_casts(self):
        statements = (
            ("INSERT INTO t SELECT 6, 12, 'comparison', asset=asset, NULL FROM t WHERE id=1", None),
            ("INSERT INTO t SELECT 6, 12, 'arithmetic', asset+1, NULL FROM t WHERE id=1", None),
            ("INSERT INTO t SELECT 6, 12, 'implicit', %s, NULL FROM t WHERE id=1",
             (str(self.seed_path),)),
            ("INSERT INTO text_sink SELECT asset FROM t WHERE id=1", None),
            ("SELECT asset FROM t ORDER BY asset", None),
        )
        for statement, parameters in statements:
            with self.subTest(statement=statement):
                with self.assertRaises(pymysql.MySQLError) as caught:
                    self.cursor.execute(statement, parameters)
                self.assertIsInstance(caught.exception.args[0], int)
                self.assertTrue(caught.exception.args[1])
                self.cursor.execute("SELECT COUNT(*) FROM t")
                self.assertEqual(self.cursor.fetchall(), ((1,),))
                self.cursor.execute("SELECT COUNT(*) FROM text_sink")
                self.assertEqual(self.cursor.fetchall(), ((0,),))
        self.cursor.execute("SELECT FILE_URI(asset), FILE_META(asset) FROM t WHERE id=1")
        uri, metadata = self.cursor.fetchone()
        self.assertEqual(uri, self.seed_path.as_uri())
        self.assertEqual(decode_descriptor(metadata), descriptor(self.seed_path))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path,
                        help="Built seekdb binary for a new private test database")
    options = parser.parse_args()
    binary = options.binary.resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        parser.error(f"--binary is not an executable file: {binary}")
    database = PrivateDatabase(binary)
    try:
        database.start()
        FileInsertSelectContracts.database = database
        run_suite(FileInsertSelectContracts, [])
    finally:
        database.close()


if __name__ == "__main__":
    main()
