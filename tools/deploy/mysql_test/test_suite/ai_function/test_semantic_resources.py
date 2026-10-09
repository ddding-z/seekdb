"""Build and run native semantic resource contracts without SQL or HTTP services."""

import argparse
from pathlib import Path
import subprocess
import tempfile

from native_test_build import ROOT, build_native_test


SOURCE = ROOT / "unittest/sql/engine/test_semantic_resources.cpp"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, default=ROOT / "build_debug")
    build_source = parser.add_mutually_exclusive_group()
    build_source.add_argument("--link-manifest", type=Path,
                              help="cached Debug link argv/action/aquery JSON; does not invoke Bazel")
    build_source.add_argument("--bazel", type=Path, help="existing Bazel executable when not on PATH")
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--case", help="run one named native contract")
    parser.add_argument("--timeout", type=float, default=120)
    options = parser.parse_args()
    if not 1 <= options.repeat <= 100:
        parser.error("--repeat must be in [1, 100]")
    if options.timeout <= 0:
        parser.error("--timeout must be positive")
    with tempfile.TemporaryDirectory(prefix="seekdb-semantic-resources-") as directory:
        output = Path(directory) / "semantic_resource_tests"
        environment = build_native_test(options.build_dir.resolve(), output, SOURCE,
                                        link_manifest=options.link_manifest, bazel=options.bazel)
        command = [str(output), "--repeat", str(options.repeat)]
        if options.case is not None:
            command.extend(["--case", options.case])
        subprocess.run(command, cwd=directory,
                       env=environment, check=True, timeout=options.timeout)


if __name__ == "__main__":
    main()
