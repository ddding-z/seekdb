"""Verify FILE catalog compatibility on private write-fenced standby restarts."""

import argparse
import json
import os
from pathlib import Path
import unittest

import pymysql

from test_file_runtime import PrivateDatabase


DATABASE = "ai_file_catalog_role_test"
MEDIA_NAMES = "'ai_media_root','ai_media_max_bytes','ai_media_parallel','ai_media_remote_root'"


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
                    cursor.execute("SET GLOBAL ai_media_remote_root='https://role.example.invalid/media/'")
                    cursor.execute("SET GLOBAL ai_media_max_bytes=1077")
                    cursor.execute("SET GLOBAL ai_media_parallel=2")
                    cursor.execute("CREATE TABLE file_probe (asset FILE)")
                    cursor.execute("INSERT INTO file_probe VALUES (TO_FILE('/not-loaded/role.png'))")
                cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL(%s,%s)",
                               ("role_model", json.dumps(model)))
                before = self.snapshot(cursor)
                self.assertEqual(len(before["media"]), 0 if legacy else 4)
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
                    self.assertEqual(self.execute(cursor, "SELECT @@GLOBAL.ai_media_remote_root"), (("",),))
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

    def check_missing_remote_policy_restart(self, *, standby):
        database = PrivateDatabase(self.binary)
        try:
            database.start()
            with database.connection.cursor() as cursor:
                cursor.execute("SET ob_query_timeout=30000000")
                cursor.execute(f"CREATE DATABASE {DATABASE}")
                cursor.execute(f"USE {DATABASE}")
                cursor.execute("CREATE TABLE write_probe (id INT PRIMARY KEY)")
                cursor.execute("INSERT INTO write_probe VALUES (0)")
                cursor.execute("CREATE TABLE file_probe (asset FILE)")
                cursor.execute("INSERT INTO file_probe VALUES (TO_FILE('https://role.example.invalid/"
                               "media/image.png?signature=synthetic-only'))")
                cursor.execute("SET GLOBAL ai_media_root=%s", (str(database.base / "media"),))
                cursor.execute("SET GLOBAL ai_media_remote_root='https://role.example.invalid/media/'")
                cursor.execute("SET GLOBAL ai_media_max_bytes=1077")
                cursor.execute("SET GLOBAL ai_media_parallel=2")
                cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL(%s,%s)", (
                    "role_model", json.dumps({
                        "type": "completion", "model_name": "role-local-only",
                        "input_modalities": ["text", "image"],
                    })))
                original = self.snapshot(cursor)
                remote_flags = next(row[2] for row in original["media"]
                                    if row[0] == "ai_media_remote_root")
                for table in ("__all_sys_variable", "__all_sys_variable_history"):
                    cursor.execute(f"DELETE FROM oceanbase.{table} WHERE name='ai_media_remote_root'")
                    self.assertGreater(cursor.rowcount, 0, "private fixture did not remove remote policy")
                before = self.snapshot(cursor)
                self.assertEqual(len(before["media"]), 3)
                self.assertFalse(any(row[0] == "ai_media_remote_root"
                                     for row in before["media_history"]))
                if standby:
                    cursor.execute("ALTER SYSTEM SWITCHOVER TO STANDBY")
            database.restart(self.binary)
            with database.connection.cursor() as cursor:
                cursor.execute("SET ob_query_timeout=30000000")
                cursor.execute(f"USE {DATABASE}")
                self.assertEqual(self.execute(cursor, "SELECT @@GLOBAL.ai_media_remote_root"), (("",),))
                self.assertEqual(self.execute(
                    cursor, "SELECT role FROM oceanbase.__all_virtual_server_stat LIMIT 1"),
                    (("STANDBY" if standby else "PRIMARY",),))
                self.assertEqual(self.execute(cursor, "SELECT FILE_URI(asset) FROM file_probe"),
                                 (("https://role.example.invalid/media/image.png?signature=synthetic-only",),))
                after = self.snapshot(cursor)
                if standby:
                    self.assertEqual(after, before, "standby persisted an in-memory safe remote default")
                    with self.assertRaises(pymysql.MySQLError) as caught:
                        cursor.execute("INSERT INTO write_probe VALUES (1)")
                    self.assertEqual(caught.exception.args[0], 4688, caught.exception.args)
                else:
                    self.assertEqual(after["media"], tuple(sorted(
                        before["media"] + (("ai_media_remote_root", "", remote_flags),))))
                    remote_history = tuple(row for row in after["media_history"]
                                           if row[0] == "ai_media_remote_root")
                    self.assertEqual(len(remote_history), 1)
                    self.assertEqual(remote_history[0][:2], ("ai_media_remote_root", ""))
                    self.assertGreater(remote_history[0][2], 0)
                    self.assertEqual(remote_history[0][3], 0)
                    self.assertEqual(tuple(row for row in after["media_history"]
                                           if row[0] != "ai_media_remote_root"), before["media_history"])
                    for name in ("columns", "models", "model_history"):
                        self.assertEqual(after[name], before[name], f"remote upgrade changed {name}")
                    cursor.execute("INSERT INTO write_probe VALUES (1)")
                    cursor.execute("SET GLOBAL ai_media_remote_root='https://role.example.invalid/preserved/'")
                    after = self.snapshot(cursor)
            database.restart(self.binary)
            with database.connection.cursor() as cursor:
                self.assertEqual(self.snapshot(cursor), after, "second restart changed durable policies/history")
            self.assertEqual((database.starts, database.normal_stops), (3, 2))
        finally:
            database.close()

    def test_missing_remote_policy_primary_restart_persists_default_and_preserves_catalog(self):
        self.check_missing_remote_policy_restart(standby=False)

    def test_missing_remote_policy_standby_restart_keeps_safe_default_unpersisted(self):
        self.check_missing_remote_policy_restart(standby=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--upgrade-binary", type=Path)
    parser.add_argument("--case", action="append", default=[])
    options = parser.parse_args()
    names = unittest.defaultTestLoader.getTestCaseNames(CatalogRoleContracts)
    if any(name not in names for name in options.case):
        parser.error("unknown catalog role contract")
    selected = options.case or names
    if "test_legacy_standby_restart_loads_safe_defaults_without_catalog_writes" in selected \
            and options.upgrade_binary is None:
        parser.error("--upgrade-binary is required for the pre-FILE standby contract")
    binary = options.binary.resolve()
    old_binary = options.upgrade_binary.resolve() if options.upgrade_binary is not None else None
    for path in (binary,) if old_binary is None else (binary, old_binary):
        if not path.is_file() or not os.access(path, os.X_OK):
            parser.error(f"not an executable binary: {path}")
    if binary == old_binary:
        parser.error("--upgrade-binary must differ from --binary")
    CatalogRoleContracts.binary = binary
    CatalogRoleContracts.old_binary = old_binary
    result = unittest.TextTestRunner(verbosity=2).run(
        unittest.TestSuite(CatalogRoleContracts(name) for name in selected))
    if not result.wasSuccessful():
        raise SystemExit(1)


if __name__ == "__main__":
    main()
