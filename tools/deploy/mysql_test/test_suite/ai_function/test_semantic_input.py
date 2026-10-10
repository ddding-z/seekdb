"""Run native ordered-input/provider contracts against current Debug objects."""

import argparse
from pathlib import Path
import subprocess
import tempfile

from native_test_build import ROOT, build_native_test


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", default="build_debug")
    parser.add_argument("--link-manifest", type=Path)
    parser.add_argument("--bazel", type=Path)
    options = parser.parse_args()
    if options.link_manifest is not None and options.bazel is not None:
        parser.error("choose --link-manifest or --bazel, not both")
    with tempfile.TemporaryDirectory(prefix="seekdb-semantic-input-") as directory:
        output = Path(directory) / "test-semantic-input"
        environment = build_native_test(
            options.build_dir,
            output,
            ROOT / "unittest/sql/engine/test_semantic_input.cpp",
            link_manifest=options.link_manifest,
            bazel=options.bazel,
        )
        subprocess.run([str(output)], env=environment, check=True)


if __name__ == "__main__":
    main()
