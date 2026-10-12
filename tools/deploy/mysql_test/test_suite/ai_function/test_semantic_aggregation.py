"""Build and run engine-independent semantic aggregation contracts without SQL or HTTP."""

import argparse
from pathlib import Path
import subprocess
import tempfile

from native_test_build import ROOT, build_native_test


SOURCE = ROOT / "unittest/sql/engine/test_semantic_aggregation.cpp"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, default=ROOT / "build_debug")
    build_source = parser.add_mutually_exclusive_group()
    build_source.add_argument("--link-manifest", type=Path)
    build_source.add_argument("--bazel", type=Path)
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--case")
    parser.add_argument("--timeout", type=float, default=120)
    options = parser.parse_args()
    if not 1 <= options.repeat <= 100:
        parser.error("--repeat must be in [1, 100]")
    if options.timeout <= 0:
        parser.error("--timeout must be positive")
    with tempfile.TemporaryDirectory(prefix="seekdb-semantic-aggregation-") as directory:
        output = Path(directory) / "semantic_aggregation_tests"
        environment = build_native_test(
            options.build_dir.resolve(), output, SOURCE,
            link_manifest=options.link_manifest, bazel=options.bazel,
        )
        command = [str(output), "--repeat", str(options.repeat)]
        if options.case is not None:
            command.extend(["--case", options.case])
        subprocess.run(command, cwd=directory, env=environment, check=True, timeout=options.timeout)


if __name__ == "__main__":
    main()
