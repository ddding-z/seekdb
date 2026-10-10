"""Compile and run declared-model modality contracts without rebuilding or starting seekdb."""

import argparse
import json
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile

from native_test_build import ROOT, build_native_test, compile_native_source


SOURCE = ROOT / "unittest/share/schema/test_ai_model_modalities.cpp"
MODEL_SOURCE = ROOT / "src/share/ai_service/ob_ai_service_struct.cpp"
GENERATOR = ROOT / "src/share/inner_table/generate_inner_table_schema.py"
SCHEMA_DEFINITION = ROOT / "src/share/inner_table/ob_inner_table_schema_def.py"
GENERATED_MODEL_SOURCE = "ob_inner_table_schema.1101_1150.cpp"


def compile_output(entry):
    arguments = entry.get("arguments") or shlex.split(entry["command"])
    return Path(arguments[arguments.index("-o") + 1])


def archive_input(entry):
    output = compile_output(entry)
    return str(output.parents[2] / f"lib{output.parent.name}.a")


def binary_tool(llvm_name, standard_name):
    executable = shutil.which(llvm_name) or shutil.which(standard_name)
    if executable is None:
        raise FileNotFoundError(f"Native modality tests require {llvm_name} or {standard_name}")
    return executable


def retain_cached_schema_factories(directory, schema_object, entry):
    nm = binary_tool("llvm-nm", "nm")
    ar = binary_tool("llvm-ar", "ar")
    objcopy = binary_tool("llvm-objcopy", "objcopy")

    def factory_symbols(path):
        result = subprocess.run([nm, "-g", "--defined-only", str(path)],
                                check=True, stdout=subprocess.PIPE, text=True)
        return {parts[-1] for line in result.stdout.splitlines()
                if len(parts := line.split()) >= 3
                and parts[-2] in ("T", "W")
                and parts[-1].startswith("_ZN9oceanbase5share18ObInnerTableSchema")}

    archive = Path(entry["directory"]) / archive_input(entry)
    symbols = factory_symbols(schema_object)
    if not symbols or not symbols.issubset(factory_symbols(archive)):
        raise RuntimeError("Fresh model-table factories do not match the cached schema archive")
    members = subprocess.run([ar, "t", str(archive)], check=True,
                             stdout=subprocess.PIPE, text=True).stdout.splitlines()
    if not members:
        raise RuntimeError("Cached schema archive is empty")
    copied_objects = []
    for index, member in enumerate(members):
        copied = directory / f"cached_schema_{index}.o"
        with copied.open("wb") as output:
            subprocess.run([ar, "p", str(archive), member], check=True, stdout=output)
        subprocess.run([objcopy, *(f"--weaken-symbol={symbol}" for symbol in sorted(symbols)),
                        str(copied)], check=True)
        copied_objects.append(str(copied))
    copied_archive = directory / "cached_schema_factories.a"
    subprocess.run([ar, "rcs", str(copied_archive), *copied_objects], check=True)
    return copied_archive


def build_fresh_model_objects(directory):
    entries = json.loads((ROOT / "compile_commands.json").read_text())
    model_entry = next((entry for entry in entries if Path(entry["file"]) == MODEL_SOURCE), None)
    generated_entry = next((entry for entry in entries
                            if Path(entry["file"]).name == GENERATED_MODEL_SOURCE), None)
    if model_entry is None or generated_entry is None:
        raise RuntimeError("Model modality tests require existing Debug model/schema compile metadata")
    model_output = compile_output(model_entry)
    sources = dict.fromkeys(entry["file"] for entry in entries
                            if compile_output(entry) == model_output)
    unity = directory / "model_metadata.unity.cpp"
    unity.write_text("".join(f'#undef USING_LOG_PREFIX\n#include "{source}"\n'
                             for source in sources))
    model_object = directory / "model_metadata.o"
    compile_native_source(unity, model_object, model_entry)

    share_output = directory / "generated/share/inner_table"
    observer_output = directory / "generated/observer/virtual_table"
    subprocess.run([
        sys.executable, str(GENERATOR), "--def-file", str(SCHEMA_DEFINITION),
        "--share-output-dir", str(share_output), "--observer-output-dir", str(observer_output),
        "--quiet",
    ], cwd=ROOT, check=True)
    schema_object = directory / "model_tables.o"
    # Temporary generated headers are not listed in the cached Bazel module map.
    generated_entry = dict(generated_entry)
    arguments = generated_entry.get("arguments") or shlex.split(generated_entry["command"])
    generated_entry["arguments"] = [argument for argument in arguments
                                    if argument != "-fmodules-strict-decluse"]
    compile_native_source(share_output / GENERATED_MODEL_SOURCE, schema_object, generated_entry)
    # Keep unrelated cached factories, but let the explicitly linked fresh definitions win.
    cached_schema = retain_cached_schema_factories(directory, schema_object, generated_entry)
    return {
        archive_input(model_entry): model_object,
        archive_input(generated_entry): cached_schema,
    }, schema_object


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
    with tempfile.TemporaryDirectory(prefix="seekdb-model-modalities-") as temporary:
        directory = Path(temporary)
        replacements, schema_object = build_fresh_model_objects(directory)
        output = directory / "ai_model_modality_tests"
        environment = build_native_test(
            options.build_dir.resolve(), output, SOURCE,
            link_manifest=options.link_manifest, bazel=options.bazel,
            link_replacements=replacements, extra_link_args=[str(schema_object), "-Wl,--gc-sections"])
        command = [str(output), "--repeat", str(options.repeat)]
        if options.case is not None:
            command.extend(["--case", options.case])
        result = subprocess.run(command, cwd=directory, env=environment,
                                check=False, timeout=options.timeout)
        if result.returncode != 0:
            log = directory / "model_modalities.log"
            if log.is_file():
                print("\n".join(log.read_text().splitlines()[-20:]), file=sys.stderr)
            result.check_returncode()


if __name__ == "__main__":
    main()
