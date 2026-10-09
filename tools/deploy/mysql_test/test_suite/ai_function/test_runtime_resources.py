"""Compile resource fault tests against the existing Linux Debug build."""

import argparse
from pathlib import Path
import subprocess
import tempfile
import threading

from native_test_build import ROOT, build_native_test
import test_runtime as runtime
from test_runtime_contracts import Reply, configure_mock


WRAPPED = ("curl_easy_init", "curl_easy_cleanup", "curl_multi_init", "curl_multi_cleanup",
           "curl_multi_add_handle", "curl_multi_remove_handle", "curl_multi_perform",
           "curl_multi_poll", "curl_slist_append", "curl_slist_free_all")


def build_test(build_dir, output):
    return build_native_test(build_dir, output, Path(__file__).with_suffix(".cpp"), wrapped=WRAPPED)


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
            held = threading.Event()
            server.scenarios["native-hold"] = [Reply(gate=held)]
            server.scenarios["native-admission"] = [Reply(gate=held)]
            worker = threading.Thread(target=server.serve_forever)
            worker.start()
            try:
                command = [str(output), f"http://127.0.0.1:{server.server_port}/"]
                for control, diagnostic in (("allocation", "initialization rollback leaked an allocation"),
                                            ("curl", "curl easy handles were not released"),
                                            ("progress", "requests must complete while the SQL thread does not poll")):
                    negative = subprocess.run(command + [f"--negative-control={control}"], cwd=directory,
                                              env=environment, capture_output=True, text=True, timeout=30)
                    if negative.returncode != 1 or diagnostic not in negative.stderr:
                        raise AssertionError(f"{control} negative control failed: {negative.stderr[-2000:]}")
                    print(f"PASS negative control: omitted {control} action is detected", flush=True)
                subprocess.run(command, cwd=directory, env=environment, check=True, timeout=60)
            finally:
                held.set()
                server.shutdown()
                worker.join()
            if server.fixture_errors:
                raise AssertionError(server.fixture_errors)


if __name__ == "__main__":
    main()