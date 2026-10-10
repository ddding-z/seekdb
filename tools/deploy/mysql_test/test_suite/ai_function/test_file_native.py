"""Run native FILE codec, storage/wire and asynchronous media contracts."""

import argparse
from pathlib import Path
import subprocess
import tempfile

from native_test_build import ROOT, build_native_test


SOURCES = {
    "codec": ROOT / "unittest/oblib/test_file_ref.cpp",
    "storage": ROOT / "unittest/oblib/test_file_storage.cpp",
    "media": ROOT / "unittest/sql/engine/test_semantic_media.cpp",
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, default=ROOT / "build_debug")
    build_source = parser.add_mutually_exclusive_group()
    build_source.add_argument("--link-manifest", type=Path)
    build_source.add_argument("--bazel", type=Path)
    parser.add_argument("--suite", action="append", choices=tuple(SOURCES), default=[])
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=120)
    options = parser.parse_args()
    if not 1 <= options.repeat <= 100:
        parser.error("--repeat must be in [1, 100]")
    if options.timeout <= 0:
        parser.error("--timeout must be positive")
    for name in dict.fromkeys(options.suite or SOURCES):
        with tempfile.TemporaryDirectory(prefix=f"seekdb-file-{name}-") as temporary:
            directory = Path(temporary)
            output = directory / f"test-file-{name}"
            environment = build_native_test(
                options.build_dir.resolve(), output, SOURCES[name],
                link_manifest=options.link_manifest, bazel=options.bazel)
            for repeat in range(options.repeat):
                print(f"Native FILE suite={name} repeat={repeat + 1}", flush=True)
                subprocess.run([str(output)], cwd=directory, env=environment,
                               check=True, timeout=options.timeout)


if __name__ == "__main__":
    main()
