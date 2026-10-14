"""Build an experimental scorer using the same Debug objects as seekdb."""

import argparse
import hashlib
import json
import platform
from pathlib import Path
import subprocess

from native_test_build import ROOT, build_native_test


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def cpu_contract():
    flags = set()
    for line in Path("/proc/cpuinfo").read_text().splitlines():
        if line.startswith("flags"):
            flags.update(line.split(":", 1)[1].split())
    return {
        "architecture": platform.machine(),
        "vector_flags": sorted(flag for flag in flags if flag.startswith(("sse", "avx", "fma"))),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", default="build_debug")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--link-manifest", type=Path)
    parser.add_argument("--bazel", type=Path)
    options = parser.parse_args()
    output = options.output.resolve()
    manifest_path = output.with_suffix(output.suffix + ".json")
    if output.exists() or manifest_path.exists() or output.with_suffix(".o").exists():
        parser.error("Scorer output already exists; choose a new path")
    if options.link_manifest is not None and options.bazel is not None:
        parser.error("Choose --link-manifest or --bazel, not both")
    seekdb = ROOT / options.build_dir / "bin/src/observer/seekdb"
    if not seekdb.is_file():
        parser.error("Complete the matching seekdb Debug build first")
    output.parent.mkdir(parents=True, exist_ok=True)
    seekdb_hash = sha256_file(seekdb)
    environment = build_native_test(
        options.build_dir, output, Path(__file__).with_name("semantic_proxy_score.cpp"),
        link_manifest=options.link_manifest, bazel=options.bazel,
    )
    result = subprocess.run(
        [str(output), "--contract"], env=environment, check=True,
        text=True, stdout=subprocess.PIPE,
    )
    if sha256_file(seekdb) != seekdb_hash:
        raise RuntimeError("seekdb changed while building the native scorer")
    manifest = {
        "schema_version": 1,
        "executable": str(output),
        "executable_sha256": sha256_file(output),
        "seekdb_sha256": seekdb_hash,
        "cpu": cpu_contract(),
        "kernel": json.loads(result.stdout),
        "ld_library_path": environment["LD_LIBRARY_PATH"],
    }
    with manifest_path.open("x") as stream:
        json.dump(manifest, stream, indent=2, allow_nan=False)
        stream.write("\n")
    print(manifest_path)


if __name__ == "__main__":
    main()
