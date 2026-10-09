"""Build a standalone native test using cached seekdb Debug objects, without rebuilding seekdb."""

import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[5]
MAIN_OBJECT = "/_observer_main_ob_main_0_objects/"


def build_native_test(build_dir, output, source, *, wrapped=(), link_manifest=None, bazel=None):
    source = Path(source).resolve()
    output = Path(output).resolve()
    if not source.is_file():
        raise FileNotFoundError(f"Native test source does not exist: {source}")
    entries = json.loads((ROOT / "compile_commands.json").read_text())
    entry = next((item for item in entries if item["file"].endswith("/ob_ai_func_op.cpp")), None)
    if entry is None:
        raise RuntimeError("Native tests require existing Debug compile metadata for ob_ai_func_op.cpp")
    directory = Path(entry["directory"])
    if link_manifest is not None and bazel is not None:
        raise ValueError("Choose either a cached link manifest or a Bazel executable")
    if link_manifest is not None:
        manifest = json.loads(Path(link_manifest).read_text())
        if isinstance(manifest, list) and all(isinstance(value, str) for value in manifest):
            actions = [{"arguments": manifest}]
        elif isinstance(manifest, dict) and "arguments" in manifest:
            actions = [manifest]
        elif isinstance(manifest, dict) and "actions" in manifest:
            actions = manifest["actions"]
        else:
            raise ValueError("Expected a link argv array, a link action, or Bazel aquery JSON")
    else:
        query_args = [sys.executable, str(ROOT / "bazel.py"), f"--build-dir={build_dir}"]
        if bazel is not None:
            query_args.extend(["--bazel", str(Path(bazel).resolve())])
        query_args.extend([
            "aquery", 'mnemonic(".*Link.*", deps(//src/observer:seekdb))', "-c", "dbg",
            "--action_env=CARGO_NET_OFFLINE=true", "--action_env=CC=clang", "--output=jsonproto",
        ])
        query = subprocess.run(query_args, cwd=ROOT, check=True, stdout=subprocess.PIPE, text=True)
        actions = json.loads(query.stdout)["actions"]
    action = next((item for item in actions
                   if any(MAIN_OBJECT in value for value in item.get("arguments", []))), None)
    if action is None:
        raise RuntimeError("No cached seekdb Debug entry-point link action; complete the Debug build first")

    original = entry.get("arguments") or shlex.split(entry["command"])
    arguments = iter(original[1:])
    compile_args = [original[0]]
    for value in arguments:
        if value in ("-o", "-MF", "-MT", "-MQ"):
            next(arguments)
        elif value not in ("-MD", "-MMD", "-MP") and not value.startswith("-frandom-seed="):
            compile_args.append(str(source) if value == entry["file"] else value)
    object_file = output.with_suffix(".o")
    subprocess.run(compile_args + ["-pthread", "-o", str(object_file)],
                   cwd=directory, check=True)
    link_args = list(action["arguments"])
    replacements = [index for index, value in enumerate(link_args) if MAIN_OBJECT in value]
    if len(replacements) != 1:
        raise RuntimeError("Expected exactly one seekdb entry-point object")
    link_args[replacements[0]] = str(object_file)
    link_args[link_args.index("-o") + 1] = str(output)
    link_args.extend(f"-Wl,--wrap={symbol}" for symbol in wrapped)
    subprocess.run(link_args, cwd=directory, check=True)
    library_dirs = dict.fromkeys(str((directory / value).resolve().parent)
                                for value in action["arguments"]
                                if re.search(r"\.so(?:\.\d+)*$", value))
    environment = dict(os.environ)
    environment["LD_LIBRARY_PATH"] = ":".join(
        [*library_dirs, environment.get("LD_LIBRARY_PATH", "")])
    return environment
