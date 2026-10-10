"""Verify FILE catalog compatibility on private write-fenced standby restarts."""

import argparse
import json
import os
from pathlib import Path
import unittest

import pymysql

from test_file_runtime import PrivateDatabase


DATABASE = "ai_file_catalog_role_test"
MEDIA_NAMES = "'ai_media_root','ai_media_max_bytes','ai_media_parallel'"


class CatalogRoleContracts(unittest.TestCase):
    binary = None
    old_binary = None

    def execute(self, cursor, statement, parameters=None):
        cursor.execute(statement, parameters)
        return cursor.fetchall()

    def snapshot(self, cursor):
        return {
            "media": self.execute(
                cursor, "SELECT name, value, flags FROM oceanbase.__all_sys_variable "
                f"WHERE name IN ({MEDIA_NAMES}) ORDER BY name"),
            "media_history": self.execute(
                cursor, "SELECT name, value, schema_version, is_deleted "
                "FROM oceanbase.__all_sys_variable_history "
                f"WHERE name IN ({MEDIA_NAMES}) ORDER BY name, schema_version"),
            "columns": self.execute(
                cursor, "SELECT table_id, column_id, nullable, orig_default_value, cur_default_value "
                "FROM oceanbase.__all_column WHERE table_id IN (1128,1129) "
                "AND column_name='input_modalities' ORDER BY table_id"),
            "models": self.execute(
                cursor, "SELECT model_id, name, type, model_name FROM oceanbase.__all_ai_model "
                "WHERE name='role_model'"),
            "model_history": self.execute(
                cursor, "SELECT model_id, schema_version, is_deleted, name, type, model_name "
                "FROM oceanbase.__all_ai_model_history WHERE name='role_model' "
                "ORDER BY model_id, schema_version"),
        }

    def check_readonly_restart(self, legacy):
        database = PrivateDatabase(self.old_binary if legacy else self.binary)
        try:
            database.start()
            with database.connection.cursor() as cursor:
                cursor.execute("SET ob_query_timeout=30000000")
                cursor.execute(f"CREATE DATABASE {DATABASE}")
                cursor.execute(f"USE {DATABASE}")
                cursor.execute("CREATE TABLE write_probe (id INT PRIMARY KEY)")
                cursor.execute("INSERT INTO write_probe VALUES (0)")
                model = {"type": "completion", "model_name": "role-local-only"}
                if not legacy:
                    model["input_modalities"] = ["text", "image"]
                    cursor.execute("SET GLOBAL ai_media_root=%s", (str(database.base / "media"),))
                    cursor.execute("SET GLOBAL ai_media_max_bytes=1077")
                    cursor.execute("SET GLOBAL ai_media_parallel=2")
                    cursor.execute("CREATE TABLE file_probe (asset FILE)")
                    cursor.execute("INSERT INTO file_probe VALUES (TO_FILE('/not-loaded/role.png'))")
                cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL(%s,%s)",
                               ("role_model", json.dumps(model)))
                before = self.snapshot(cursor)
                self.assertEqual(len(before["media"]), 0 if legacy else 3)
                self.assertEqual(len(before["columns"]), 0 if legacy else 2)
                self.assertTrue(before["models"])
                self.assertTrue(before["model_history"])
                cursor.execute("ALTER SYSTEM SWITCHOVER TO STANDBY")
            database.restart(self.binary)
            with database.connection.cursor() as cursor:
                cursor.execute("SET ob_query_timeout=30000000")
                cursor.execute(f"USE {DATABASE}")
                self.assertEqual(self.snapshot(cursor), before,
                                 "write-fenced startup mutated persistent media/model catalogs")
                self.assertEqual(self.execute(
                    cursor, "SELECT role FROM oceanbase.__all_virtual_server_stat LIMIT 1"),
                    (("STANDBY",),))
                with self.assertRaises(pymysql.MySQLError) as caught:
                    cursor.execute("INSERT INTO write_probe VALUES (1)")
                self.assertEqual(caught.exception.args[0], 4688, caught.exception.args)
                self.assertEqual(self.execute(cursor, "SELECT * FROM write_probe"), ((0,),))
                self.assertEqual(self.execute(cursor, "SELECT 1"), ((1,),))
                if legacy:
                    self.assertEqual(self.execute(
                        cursor, "SELECT @@GLOBAL.ai_media_root, @@ai_media_max_bytes, @@ai_media_parallel"),
                        (("", 2097152, 4),))
                else:
                    self.assertEqual(self.execute(
                        cursor, "SELECT input_modalities FROM oceanbase.__all_ai_model "
                        "WHERE name='role_model'"), ((3,),))
                    self.assertEqual(self.execute(cursor, "SELECT FILE_URI(asset) FROM file_probe"),
                                     (("file:///not-loaded/role.png",),))
                self.assertEqual(self.snapshot(cursor), before)
            self.assertEqual((database.starts, database.normal_stops), (2, 1))
        finally:
            database.close()

    def test_legacy_standby_restart_loads_safe_defaults_without_catalog_writes(self):
        self.check_readonly_restart(True)

    def test_current_standby_restart_preserves_policy_masks_and_file_values(self):
        self.check_readonly_restart(False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--upgrade-binary", type=Path, required=True)
    parser.add_argument("--case", action="append", default=[])
    options = parser.parse_args()
    binary, old_binary = options.binary.resolve(), options.upgrade_binary.resolve()
    for path in (binary, old_binary):
        if not path.is_file() or not os.access(path, os.X_OK):
            parser.error(f"not an executable binary: {path}")
    if binary == old_binary:
        parser.error("--upgrade-binary must differ from --binary")
    names = unittest.defaultTestLoader.getTestCaseNames(CatalogRoleContracts)
    if any(name not in names for name in options.case):
        parser.error("unknown catalog role contract")
    CatalogRoleContracts.binary = binary
    CatalogRoleContracts.old_binary = old_binary
    result = unittest.TextTestRunner(verbosity=2).run(
        unittest.TestSuite(CatalogRoleContracts(name) for name in options.case or names))
    if not result.wasSuccessful():
        raise SystemExit(1)


if __name__ == "__main__":
    main()
