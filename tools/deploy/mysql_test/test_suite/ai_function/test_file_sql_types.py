"""Compile and run focused native FILE SQL type-map regressions without building observer."""

import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess
import tempfile

from native_test_build import ROOT, compile_native_source


def run_parser_tests(directory, entries, cpp_entry, link_manifest):
    manifest = json.loads(link_manifest.read_text())
    if isinstance(manifest, list):
        actions = [{"arguments": manifest}]
    elif isinstance(manifest, dict) and "arguments" in manifest:
        actions = [manifest]
    elif isinstance(manifest, dict) and "actions" in manifest:
        actions = manifest["actions"]
    else:
        raise ValueError("Expected a cached link argv, link action, or Bazel aquery JSON")
    action = next(item for item in actions
                  if any("/_observer_main_ob_main_0_objects/" in arg
                         for arg in item.get("arguments", [])))
    libraries = dict.fromkeys(
        arg for arg in action["arguments"]
        if arg.endswith(".a") and ("/src/oblib/" in arg or "/src/sql/parser/" in arg
                                  or "libunwind" in arg))
    c_entry = next(item for item in entries
                   if item["file"].endswith("/sql_parser_mysql_mode_tab.c"))
    parser_directory = ROOT / "src/sql/parser"
    subprocess.run(["bash", str(parser_directory / "gen_parser.sh")],
                   cwd=parser_directory, env={**os.environ, "NEED_PARSER_CACHE": "OFF"}, check=True)
    objects = []
    sources = [(ROOT / "unittest/sql/engine/test_file_parser.cpp", cpp_entry)]
    sources.extend((parser_directory / name, c_entry) for name in (
        "sql_parser_mysql_mode_tab.c", "sql_parser_mysql_mode_lex.c", "type_name.c"))
    for source, entry in sources:
        object_file = directory / (source.name + ".o")
        compile_native_source(source, object_file, entry)
        objects.append(str(object_file))
    compiler = (cpp_entry.get("arguments") or shlex.split(cpp_entry["command"]))[0]
    output = directory / "file_parser"
    subprocess.run([compiler, *objects, "-Wl,--gc-sections", "-Wl,--start-group",
                    *libraries, "-Wl,--end-group", "-lstdc++", "-lpthread", "-ldl",
                    "-lrt", "-lm", "-o", str(output)], cwd=cpp_entry["directory"], check=True)
    subprocess.run([str(output)], check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parser", action="store_true",
                        help="also test freshly regenerated native parser ASTs")
    parser.add_argument("--link-manifest", type=Path,
                        help="cached Debug observer link action (no observer build is performed)")
    options = parser.parse_args()
    if options.parser and options.link_manifest is None:
        parser.error("--parser requires --link-manifest with cached Debug parser/core libraries")
    entries = json.loads((ROOT / "compile_commands.json").read_text())
    entry = next(item for item in entries
                 if item["file"].endswith("/ob_raw_expr_deduce_type.cpp"))
    source = ROOT / "unittest/sql/engine/test_file_type_maps.cpp"
    original = entry.get("arguments") or shlex.split(entry["command"])
    with tempfile.TemporaryDirectory(prefix="seekdb-file-sql-types-") as directory:
        output = Path(directory) / "file_sql_types"
        object_file = output.with_suffix(".o")
        compile_native_source(source, object_file, entry)
        subprocess.run([original[0], str(object_file), "-Wl,--gc-sections",
                        "-lstdc++", "-o", str(output)],
                       cwd=entry["directory"], check=True)
        subprocess.run([str(output)], check=True)
        if options.parser:
            run_parser_tests(Path(directory), entries, entry, options.link_manifest.resolve())


if __name__ == "__main__":
    main()
