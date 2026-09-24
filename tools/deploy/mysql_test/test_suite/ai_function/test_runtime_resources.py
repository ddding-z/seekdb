"""Compile resource fault tests against the existing Linux Debug build."""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import threading

import test_runtime as runtime
from test_runtime_contracts import configure_mock


ROOT = Path(__file__).resolve().parents[5]
WRAPPED = ("curl_easy_init", "curl_easy_cleanup", "curl_multi_init", "curl_multi_cleanup",
           "curl_multi_add_handle", "curl_multi_remove_handle", "curl_multi_perform",
           "curl_multi_poll", "curl_slist_append", "curl_slist_free_all")


def build_test(build_dir, output):
    entries = json.loads((ROOT / "compile_commands.json").read_text())
    entry = next(item for item in entries if item["file"].endswith("/ob_ai_func_op.cpp"))
    directory = Path(entry["directory"])
    query = subprocess.run([
        sys.executable, str(ROOT / "bazel.py"), f"--build-dir={build_dir}", "aquery",
        'mnemonic(".*Link.*", deps(//src/observer:seekdb))', "-c", "dbg",
        "--action_env=CARGO_NET_OFFLINE=true", "--action_env=CC=clang", "--output=jsonproto",
    ], cwd=ROOT, check=True, stdout=subprocess.PIPE, text=True)
    actions = json.loads(query.stdout)["actions"]
    main_object = "/_observer_main_ob_main_0_objects/"
    action = next(item for item in actions
                  if any(main_object in value for value in item.get("arguments", [])))
    arguments = iter(entry["arguments"][1:])
    compile_args = [entry["arguments"][0]]
    for value in arguments:
        if value in ("-o", "-MF", "-MT", "-MQ"):
            next(arguments)
        elif value not in ("-MD", "-MMD", "-MP") and not value.startswith("-frandom-seed="):
            compile_args.append(str(Path(__file__).with_suffix(".cpp"))
                                if value == entry["file"] else value)
    object_file = output.with_suffix(".o")
    subprocess.run(compile_args + ["-o", str(object_file)], cwd=directory, check=True)
    link_args = list(action["arguments"])
    replacements = [index for index, value in enumerate(link_args) if main_object in value]
    if len(replacements) != 1:
        raise RuntimeError("Expected exactly one seekdb entry-point object")
    link_args[replacements[0]] = str(object_file)
    link_args[link_args.index("-o") + 1] = str(output)
    link_args.extend(f"-Wl,--wrap={symbol}" for symbol in WRAPPED)
    subprocess.run(link_args, cwd=directory, check=True)
    library_dirs = dict.fromkeys(str((directory / value).resolve().parent)
                                for value in action["arguments"]
                                if re.search(r"\.so(?:\.\d+)*$", value))
    environment = dict(os.environ)
    environment["LD_LIBRARY_PATH"] = ":".join([*library_dirs, environment.get("LD_LIBRARY_PATH", "")])
    return environment


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, default=ROOT / "build_debug")
    options = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="seekdb-ai-resources-") as directory:
        output = Path(directory) / "resource_tests"
        environment = build_test(options.build_dir.resolve(), output)
        environment.update(no_proxy="127.0.0.1,localhost", NO_PROXY="127.0.0.1,localhost")
        with runtime.MockServer() as server:
            configure_mock(server)
            worker = threading.Thread(target=server.serve_forever)
            worker.start()
            try:
                command = [str(output), f"http://127.0.0.1:{server.server_port}/"]
                for control, diagnostic in (("allocation", "initialization rollback leaked an allocation"),
                                            ("curl", "curl easy handles were not released")):
                    negative = subprocess.run(command + [f"--negative-control={control}"], cwd=directory,
                                              env=environment, capture_output=True, text=True, timeout=30)
                    if negative.returncode != 1 or diagnostic not in negative.stderr:
                        raise AssertionError(f"{control} negative control failed: {negative.stderr[-2000:]}")
                    print(f"PASS negative control: omitted {control} release is detected", flush=True)
                subprocess.run(command, cwd=directory, env=environment, check=True, timeout=60)
            finally:
                server.shutdown()
                worker.join()
            if server.fixture_errors:
                raise AssertionError(server.fixture_errors)


if __name__ == "__main__":
    main()