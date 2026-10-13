"""Deterministic native FILE and loopback-only multimodal SQL contracts.

--self-test runs synthetic media, protocol/assertion and launcher fixtures only.
--binary /path/to/seekdb starts a private unix-socket database. Repeat --case
test_name to select contracts; --list-cases never starts a database. There is no
external-socket or live-model mode.

The private launcher follows test_runtime's bootstrap parameters, but uses
SIGUSR1 for every normal stop and can restart the same data directory. Native
persistence is checked after committed DML and an actual process restart, not
with SQL DUMP or a serialized type enum. Compaction-specific cases require
successful native freeze commands and per-table completed compaction evidence;
an explicitly unsupported private-launcher facility is reported as a skip.

All media bytes are generated here, with the Python standard library. Held HTTP
responses prove cross-batch/dependency behavior, not asynchronous file-loading
overlap or unrestricted AI_JOIN worker release. Unordered and full-sort PX cases
check actual tasks and FILE values transported through an exchange; the sorted
case also requires worker release while every provider reply remains held.

CREATE_AI_MODEL input_modalities metadata enables image/audio inputs; omitted
metadata remains text-only. Explicit modalities are distinct lowercase
text/image/audio names, with text required; non-completion models stay text-only.
Catalog and history masks are checked before and after a native restart.
ai_media_root is configured with admin SET GLOBAL, never a per-session policy
override. The only media provider is loopback openai. Ordinary path/URI strings
intentionally remain text; only typed FILE is loaded.
Nullable ordered parts must fail only on semantic demand. Skipped-call SQL cases
use disabled-root/missing-path sentinels and require zero HTTP; direct loader
counter assertions remain in the parent's native contracts.
"""

import argparse
import base64
import binascii
from collections import Counter
from contextlib import redirect_stderr
from dataclasses import dataclass
from http.client import HTTPConnection
import io
import json
import os
from pathlib import Path
import re
import signal
import socket
import struct
import subprocess
import tempfile
import threading
import time
import unittest
from unittest import mock
from urllib.parse import urlsplit
import wave
import zlib

import pymysql
from pymysql.constants import FIELD_TYPE

import test_runtime as runtime
import test_semantic_runtime as semantic
from test_runtime_contracts import Reply, completion, model_socket_fds, sql_error
from test_semantic_join import (
    assert_unmaterialized_join_rescan,
    reject_json_constant,
    unique_object,
)
from test_semantic_px import PREPARE


BATCH_ROWS = semantic.BATCH_ROWS
ROW_COUNT = 2 * BATCH_ROWS + 3
PX_ROWS = 128
DATABASE = "ai_file_contract_test"
UPGRADE_DATABASE = "ai_file_upgrade_contract_test"
UPGRADE_CASE = "test_old_catalog_upgrade_preserves_model_masks_and_is_idempotent"
MEDIA_MAX_BYTES = 2097152
MEDIA_PARALLEL = 4
JOIN_RULE = "Match the left media to the right input."
TEXT_WIRE_TYPES = {
    FIELD_TYPE.JSON, FIELD_TYPE.STRING, FIELD_TYPE.VAR_STRING, FIELD_TYPE.VARCHAR,
    FIELD_TYPE.TINY_BLOB, FIELD_TYPE.MEDIUM_BLOB, FIELD_TYPE.LONG_BLOB, FIELD_TYPE.BLOB,
}
OPTIONAL_DESCRIPTOR_FIELDS = {"mime_type", "object_version", "size_bytes"}
UNSUPPORTED_FACILITY_CODES = {1064, 1142, 1227, 1235}


def strict_json(value):
    return json.loads(value, object_pairs_hook=unique_object,
                      parse_constant=reject_json_constant)


def descriptor(path, **metadata):
    return {"format_version": 1, "uri": Path(path).as_uri(), **metadata}


def descriptor_text(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def decode_descriptor(value):
    if not isinstance(value, (str, bytes)):
        raise AssertionError("FILE protocol values must be JSON text, not a private object")
    try:
        result = strict_json(value)
    except (ValueError, UnicodeDecodeError) as error:
        raise AssertionError(f"FILE value is not strict UTF-8 JSON: {error}") from error
    if not isinstance(result, dict):
        raise AssertionError("FILE descriptor must be an object")
    if set(result) - {"format_version", "uri", *OPTIONAL_DESCRIPTOR_FIELDS}:
        raise AssertionError("FILE descriptor contains an unknown field")
    if type(result.get("format_version")) is not int or result["format_version"] != 1:
        raise AssertionError("FILE descriptor format_version must be integer 1")
    uri = result.get("uri")
    if not isinstance(uri, str):
        raise AssertionError("FILE descriptor uri must be a string")
    parsed = urlsplit(uri)
    local = (parsed.scheme == "file" and not parsed.netloc and parsed.path.startswith("/")
             and not parsed.query and not parsed.fragment and uri.startswith("file:///"))
    remote = (parsed.scheme in {"https", "http"} and uri.startswith(parsed.scheme + "://")
              and parsed.netloc and parsed.path.startswith("/") and not parsed.fragment
              and parsed.username is None and parsed.password is None
              and (parsed.scheme == "https" or parsed.hostname in {"127.0.0.1", "::1"}))
    if not local and not remote:
        raise AssertionError("FILE descriptor must contain a supported file or remote URI")
    for name in ("mime_type", "object_version"):
        if name in result and not isinstance(result[name], str):
            raise AssertionError(f"FILE descriptor {name} must be a string")
    if "size_bytes" in result and (
            type(result["size_bytes"]) is not int or result["size_bytes"] < 0):
        raise AssertionError("FILE descriptor size_bytes must be a nonnegative integer")
    return result


def assert_file_value(test, value, expected, field=None):
    if field is not None:
        test.assertIn(field[1], TEXT_WIRE_TYPES,
                      "FILE used an unknown/private or non-text MySQL wire type")
    if expected is None:
        test.assertIsNone(value)
    else:
        test.assertEqual(decode_descriptor(value), expected)


def png_bytes(seed=0):
    def chunk(kind, data):
        return (struct.pack(">I", len(data)) + kind + data +
                struct.pack(">I", binascii.crc32(kind + data) & 0xffffffff))

    pixel = bytes((seed % 256, (seed * 17 + 31) % 256, (seed * 29 + 7) % 256))
    return (b"\x89PNG\r\n\x1a\n" +
            chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 2, 0, 0, 0)) +
            chunk(b"IDAT", zlib.compress(b"\x00" + pixel)) + chunk(b"IEND", b""))


def jpeg_bytes(seed=0):
    def segment(marker, data):
        return b"\xff" + bytes((marker,)) + struct.pack(">H", len(data) + 2) + data

    # One grayscale block: DC=0, AC=EOB, then all-one entropy padding.
    huffman_lengths = b"\x01" + b"\x00" * 15
    return (b"\xff\xd8" +
            segment(0xe0, b"JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00") +
            segment(0xfe, f"seekdb-synthetic-{seed}".encode("ascii")) +
            segment(0xdb, b"\x00" + b"\x01" * 64) +
            segment(0xc0, b"\x08\x00\x01\x00\x01\x01\x01\x11\x00") +
            segment(0xc4, b"\x00" + huffman_lengths + b"\x00") +
            segment(0xc4, b"\x10" + huffman_lengths + b"\x00") +
            segment(0xda, b"\x01\x01\x00\x00\x3f\x00") + b"\x3f\xff\xd9")


def wav_bytes(seed=0):
    samples = tuple(((seed * 97 + index * 101) % 2048) - 1024 for index in range(32))
    destination = io.BytesIO()
    with wave.open(destination, "wb") as output:
        output.setnchannels(1)
        output.setsampwidth(2)
        output.setframerate(8000)
        output.writeframes(struct.pack("<32h", *samples))
    return destination.getvalue()


def mp3_bytes(seed=0):
    def synchsafe(size):
        return bytes((size >> 21 & 0x7f, size >> 14 & 0x7f,
                      size >> 7 & 0x7f, size & 0x7f))

    tag_value = b"\x03fixture\x00" + str(seed).encode("ascii")
    tag = b"TXXX" + synchsafe(len(tag_value)) + b"\x00\x00" + tag_value
    # MPEG-1 Layer III, 32 kbit/s, 44.1 kHz mono; empty spectral data is silence.
    frame = b"\xff\xfb\x10\xc4" + b"\x00" * 100
    return b"ID3\x04\x00\x00" + synchsafe(len(tag)) + tag + frame * 3


@dataclass(frozen=True)
class MediaAsset:
    path: Path
    raw: bytes
    mime_type: str

    def descriptor(self, *, include_mime=True, include_size=False):
        metadata = {}
        if include_mime:
            metadata["mime_type"] = self.mime_type
        if include_size:
            metadata["size_bytes"] = len(self.raw)
        return descriptor(self.path, **metadata)

    def block(self):
        encoded = base64.b64encode(self.raw).decode("ascii")
        if self.mime_type.startswith("image/"):
            return {"type": "image_url",
                    "image_url": {"url": f"data:{self.mime_type};base64,{encoded}"}}
        formats = {"audio/wav": "wav", "audio/mpeg": "mp3"}
        return {"type": "input_audio",
                "input_audio": {"data": encoded, "format": formats[self.mime_type]}}


class MediaFiles:
    def __init__(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="seekdb-file-media-")
        self.base = Path(self.temporary.name)
        self.root = self.base / "media"
        self.outside = self.base / "media-other"
        self.root.mkdir()
        self.outside.mkdir()

    def asset(self, kind="png", seed=0, *, outside=False, name=None):
        generators = {
            "png": (png_bytes, "image/png"),
            "jpeg": (jpeg_bytes, "image/jpeg"),
            "wav": (wav_bytes, "audio/wav"),
            "mp3": (mp3_bytes, "audio/mpeg"),
        }
        generator, mime_type = generators[kind]
        path = (self.outside if outside else self.root) / (name or f"{kind}-{seed}.{kind}")
        raw = generator(seed)
        path.write_bytes(raw)
        return MediaAsset(path, raw, mime_type)

    def corrupt(self, kind="png"):
        valid = self.asset(kind, 900)
        raw = valid.raw[:8]
        valid.path.write_bytes(raw)
        return MediaAsset(valid.path, raw, valid.mime_type)

    def close(self):
        self.temporary.cleanup()


def text_block(value):
    return {"type": "text", "text": value}


def content_key(content):
    return json.dumps(content, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def join_content(rule, left, right):
    return [text_block("AI_JOIN rule:\n"), text_block(rule),
            text_block("\nLEFT INPUT:\n"),
            left.block() if isinstance(left, MediaAsset) else text_block(left),
            text_block("\nRIGHT INPUT:\n"),
            right.block() if isinstance(right, MediaAsset) else text_block(right)]


def assert_content(test, body, expected, *, assets=(), forbidden_paths=()):
    test.assertIn("messages", body)
    test.assertTrue(body["messages"])
    test.assertEqual(body["messages"][-1]["role"], "user")
    content = body["messages"][-1]["content"]
    test.assertEqual(content, expected, "provider content blocks changed type, order or row identity")
    test.assertNotIn("input_modalities", body, "model capability metadata leaked to the provider")
    if assets:
        test.assertIsInstance(content, list, "typed FILE was sent as ordinary prompt text")
        blocks = [block for block in content if block["type"] != "text"]
        test.assertEqual(len(blocks), len(assets), "a FILE was dropped or duplicated")
        for block, asset in zip(blocks, assets):
            if asset.mime_type.startswith("image/"):
                test.assertEqual(set(block), {"type", "image_url"})
                test.assertEqual(set(block["image_url"]), {"url"})
                prefix = f"data:{asset.mime_type};base64,"
                uri = block["image_url"]["url"]
                test.assertIsInstance(uri, str)
                test.assertTrue(uri.startswith(prefix), "image_url did not carry inline media bytes")
                encoded = uri[len(prefix):]
            else:
                test.assertEqual(set(block), {"type", "input_audio"})
                test.assertEqual(set(block["input_audio"]), {"data", "format"})
                test.assertEqual(block["input_audio"]["format"],
                                 "wav" if asset.mime_type == "audio/wav" else "mp3")
                encoded = block["input_audio"]["data"]
            test.assertIsInstance(encoded, str)
            try:
                decoded = base64.b64decode(encoded, validate=True)
            except (binascii.Error, ValueError) as error:
                raise AssertionError("media content is not strict base64") from error
            test.assertEqual(decoded, asset.raw,
                             "media bytes belong to the wrong FILE/row or were corrupted")
    wire = descriptor_text(body)
    for path in forbidden_paths:
        test.assertNotIn(str(path), wire, "a raw local path leaked into the provider request")
        test.assertNotIn(Path(path).as_uri(), wire, "a local FILE URI leaked into the provider request")


class FileHandler(runtime.MockHandler):
    def handle(self):
        try:
            super().handle()
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_POST(self):
        server = self.server
        record = None
        try:
            raw_body = self.rfile.read(int(self.headers["Content-Length"]))
            body = strict_json(raw_body)
            key = content_key(body["messages"][-1]["content"])
            with server.condition:
                server.active += 1
                server.peak = max(server.peak, server.active)
                server.counts[key] += 1
                server.requests.append(body)
                record = {"prompt": key, "body": body, "raw_body": raw_body,
                          "received": time.monotonic(), "response": None}
                server.audit.append(record)
                replies = server.scenarios.get(key, [server.default_reply])
                reply = replies[min(server.counts[key] - 1, len(replies) - 1)]
                server.condition.notify_all()
            if reply.gate is not None and not reply.gate.wait(timeout=20):
                raise AssertionError("the test did not release its media response gate")
            if reply.body is None:
                raise AssertionError("media fixtures require an explicit deterministic response")
            self.send_response(reply.status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(reply.body)))
            for name, value in reply.headers.items():
                self.send_header(name, value)
            self.end_headers()
            with server.condition:
                record["response"] = time.monotonic()
                server.condition.notify_all()
            self.wfile.write(reply.body)
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as error:
            with server.condition:
                server.fixture_errors.append(repr(error))
                server.condition.notify_all()
            self.close_connection = True
        finally:
            if record is not None:
                with server.condition:
                    server.active -= 1
                    server.finished.append(record["prompt"])
                    server.condition.notify_all()


class FileFixtures(semantic.MockFixtures):
    def __init__(self):
        super().__init__()
        for server in self.mocks:
            server.RequestHandlerClass = FileHandler
            server.default_reply = Reply(
                400, b'{"error":"unexpected FILE contract request"}')

    def reply(self, server, content, result, *, boolean=False, gate=None):
        raw = (descriptor_text({"value": result}) if boolean else result)
        reply = Reply(body=completion(raw, finish_reason="stop"), gate=gate)
        server.scenarios[content_key(content)] = [reply]
        return reply


def post_body(server, body):
    client = HTTPConnection("127.0.0.1", server.server_port, timeout=8)
    try:
        client.request("POST", "/v1/chat/completions", descriptor_text(body),
                       {"Content-Type": "application/json"})
        response = client.getresponse()
        return response.status, response.read()
    finally:
        client.close()


def assert_model_catalog(test, execute, expected):
    names = sorted(expected)
    placeholders = ",".join(["%s"] * len(names))
    current = execute(
        "SELECT model_id, name, input_modalities FROM oceanbase.__all_ai_model "
        f"WHERE name IN ({placeholders}) ORDER BY name", tuple(names))
    test.assertEqual(len(current), len(expected), "registered models are missing/duplicated in native catalog")
    test.assertEqual(len({model_id for model_id, _name, _mask in current}), len(expected),
                     "native catalog model IDs were duplicated")
    test.assertTrue(all(type(mask) is int for _model_id, _name, mask in current),
                    "native capability masks were not returned as SQL integers")
    test.assertEqual({name: mask for _model_id, name, mask in current}, expected,
                    "native model masks differ from CREATE_AI_MODEL metadata")
    for model_id, name, mask in current:
        latest = execute(
            "SELECT model_id, name, input_modalities, is_deleted "
            "FROM oceanbase.__all_ai_model_history WHERE model_id=%s "
            "ORDER BY schema_version DESC LIMIT 1", (model_id,))
        test.assertEqual(len(latest), 1, f"native history is missing current model ID {model_id}")
        test.assertIs(type(latest[0][2]), int)
        test.assertEqual(latest, ((model_id, name, mask, 0),),
                         f"latest native history did not persist live model {name}'s identity/mask")


def assert_modality_column(test, rows, *, table, column_id, nullable):
    test.assertEqual(rows, ((column_id, int(nullable), "1", "1"),),
                     f"{table} has missing/duplicate/wrong input_modalities IDs, nullability or defaults")


class PrivateDatabase:
    def __init__(self, binary):
        self.binary = str(binary)
        self.temporary = tempfile.TemporaryDirectory(prefix="seekdb-file-runtime-")
        self.base = Path(self.temporary.name)
        self.sql_socket = str(self.base / "run/sql.sock")
        self.process = None
        self.connection = None
        self.log = None
        self.starts = 0
        self.normal_stops = 0

    def command(self):
        return [self.binary, "--nodaemon", "--base-dir", str(self.base),
                "--parameter", "mysql_port_mode=disabled",
                "--parameter", "enable_rpc_service=false",
                "--parameter", "memory_budget=2G",
                "--parameter", "datafile_size=1G",
                "--parameter", "datafile_maxsize=4G",
                "--parameter", "log_disk_size=2G"]

    def start(self):
        if self.process is not None:
            raise RuntimeError("the private FILE database is already running")
        self.log = (self.base / f"startup-{self.starts}.log").open("w+")
        environment = dict(os.environ, no_proxy="127.0.0.1,localhost",
                           NO_PROXY="127.0.0.1,localhost")
        self.process = subprocess.Popen(self.command(), stdout=self.log,
                                        stderr=subprocess.STDOUT, env=environment)
        self.starts += 1
        deadline = time.monotonic() + 180
        last_error = None
        while time.monotonic() < deadline and self.process.poll() is None:
            try:
                self.connection = pymysql.connect(
                    unix_socket=self.sql_socket, user="root", autocommit=True,
                    read_timeout=100, write_timeout=100, charset="utf8mb4")
                return
            except pymysql.MySQLError as error:
                last_error = error
                threading.Event().wait(0.2)
        self.log.seek(0)
        raise RuntimeError(
            f"private FILE database failed to start ({last_error!r}): " + self.log.read()[-4000:])

    def connect(self, *, database=DATABASE):
        return pymysql.connect(
            unix_socket=self.sql_socket, user="root", autocommit=True, database=database,
            read_timeout=10, write_timeout=10, charset="utf8mb4")

    def stop(self, *, close_connection_first=True, timeout=30):
        if timeout <= 0:
            raise ValueError("normal database shutdown timeout must be positive")
        if close_connection_first and self.connection is not None:
            self.connection.close()
            self.connection = None
        if self.process is None:
            if self.connection is not None:
                self.connection.close()
                self.connection = None
            if self.log is not None:
                self.log.close()
                self.log = None
            return
        process = self.process
        try:
            if process.poll() is not None:
                raise RuntimeError(f"private FILE database exited unexpectedly: {process.returncode}")
            process.send_signal(signal.SIGUSR1)
            try:
                status = process.wait(timeout=timeout)
            except subprocess.TimeoutExpired as error:
                process.kill()
                process.wait(timeout=10)
                raise RuntimeError("SIGUSR1 normal database shutdown timed out; process killed") from error
            if status != 0:
                raise RuntimeError(f"SIGUSR1 normal database shutdown returned {status}")
            self.normal_stops += 1
        finally:
            if self.connection is not None:
                self.connection.close()
                self.connection = None
            self.process = None
            if self.log is not None:
                self.log.close()
                self.log = None

    def restart(self, binary=None):
        self.stop()
        if binary is not None:
            self.binary = str(binary)
        self.start()

    def close(self):
        try:
            self.stop()
        finally:
            self.temporary.cleanup()


class FileHarnessTests(unittest.TestCase):
    def setUp(self):
        self.media = MediaFiles()
        self.addCleanup(self.media.close)
        self.fixtures = FileFixtures()
        self.addCleanup(self.fixtures.close)
        self.first, self.second = self.fixtures.mocks

    def test_synthetic_png_has_valid_crc_pixels_and_no_external_data(self):
        for seed in (0, 1, 34):
            raw = png_bytes(seed)
            self.assertEqual(raw[:8], b"\x89PNG\r\n\x1a\n")
            position = 8
            chunks = []
            while position < len(raw):
                size = struct.unpack(">I", raw[position:position + 4])[0]
                kind = raw[position + 4:position + 8]
                data = raw[position + 8:position + 8 + size]
                crc = struct.unpack(">I", raw[position + 8 + size:position + 12 + size])[0]
                self.assertEqual(crc, binascii.crc32(kind + data) & 0xffffffff)
                chunks.append((kind, data))
                position += size + 12
            self.assertEqual(position, len(raw))
            self.assertEqual([kind for kind, _ in chunks], [b"IHDR", b"IDAT", b"IEND"])
            self.assertEqual(struct.unpack(">IIBBBBB", chunks[0][1]), (1, 1, 8, 2, 0, 0, 0))
            self.assertEqual(zlib.decompress(chunks[1][1]),
                             bytes((0, seed % 256, (seed * 17 + 31) % 256,
                                    (seed * 29 + 7) % 256)))

    def test_synthetic_jpeg_is_complete_baseline_grayscale(self):
        raw = jpeg_bytes(7)
        self.assertEqual(raw[:2], b"\xff\xd8")
        position = 2
        segments = []
        while raw[position:position + 2] != b"\xff\xda":
            self.assertEqual(raw[position], 0xff)
            marker = raw[position + 1]
            size = struct.unpack(">H", raw[position + 2:position + 4])[0]
            segments.append((marker, raw[position + 4:position + 2 + size]))
            position += size + 2
        self.assertEqual([marker for marker, _ in segments], [0xe0, 0xfe, 0xdb, 0xc0, 0xc4, 0xc4])
        self.assertEqual(segments[0][1][:5], b"JFIF\x00")
        self.assertEqual(segments[1][1], b"seekdb-synthetic-7")
        self.assertEqual(segments[2][1], b"\x00" + b"\x01" * 64)
        self.assertEqual(segments[3][1], b"\x08\x00\x01\x00\x01\x01\x01\x11\x00")
        for table, (_, data) in zip((0, 0x10), segments[4:]):
            self.assertEqual(data, bytes((table, 1)) + b"\x00" * 16)
        self.assertEqual(raw[position:], b"\xff\xda\x00\x08\x01\x01\x00\x00\x3f\x00\x3f\xff\xd9")
        self.assertNotEqual(jpeg_bytes(7), jpeg_bytes(8))

    def test_synthetic_wav_roundtrips_exact_pcm_samples(self):
        for seed in (0, 9, 34):
            with wave.open(io.BytesIO(wav_bytes(seed)), "rb") as source:
                self.assertEqual((source.getnchannels(), source.getsampwidth(),
                                  source.getframerate(), source.getnframes()), (1, 2, 8000, 32))
                samples = struct.unpack("<32h", source.readframes(32))
                self.assertEqual(samples, tuple(((seed * 97 + index * 101) % 2048) - 1024
                                               for index in range(32)))

    def test_synthetic_mp3_has_three_complete_silent_frames(self):
        raw = mp3_bytes(3)
        self.assertEqual(raw[:6], b"ID3\x04\x00\x00")
        self.assertTrue(all(value < 128 for value in raw[6:10]))
        tag_size = sum(value << shift for value, shift in zip(raw[6:10], (21, 14, 7, 0)))
        self.assertEqual(raw[10:14], b"TXXX")
        frames = raw[10 + tag_size:]
        self.assertEqual(len(frames), 3 * (144000 * 32 // 44100))
        for start in range(0, len(frames), 104):
            self.assertEqual(frames[start:start + 4], b"\xff\xfb\x10\xc4")
            self.assertEqual(frames[start + 4:start + 104], b"\x00" * 100)
        self.assertNotEqual(mp3_bytes(3), mp3_bytes(4))

    def test_descriptor_oracle_preserves_optional_fields_and_null(self):
        expected = descriptor(self.media.root / "space #?.png",
                              mime_type="image/png", object_version="opaque-v1", size_bytes=71)
        self.assertIn("%20%23%3F", expected["uri"])
        for value in (descriptor_text(expected), descriptor_text(expected).encode("utf-8")):
            assert_file_value(self, value, expected, ("asset", FIELD_TYPE.JSON))
        assert_file_value(self, None, None, ("asset", FIELD_TYPE.VAR_STRING))
        with self.assertRaises(AssertionError):
            assert_file_value(self, descriptor_text(expected), expected, ("asset", 244))
        with self.assertRaises(AssertionError):
            assert_file_value(self, descriptor_text(expected), {**expected, "size_bytes": 72})

    def test_descriptor_oracle_rejects_nonobjects_duplicates_and_invalid_fields(self):
        valid = descriptor(self.media.root / "not-loaded")
        values = ["null", "[]", "1", '{"format_version":1,"format_version":1,"uri":"file:///a"}']
        values.extend(descriptor_text(value) for value in (
            {"uri": valid["uri"]}, {**valid, "format_version": True},
            {**valid, "format_version": 2}, {**valid, "uri": "/not-a-uri"},
            {**valid, "uri": "s3://bucket/a"}, {**valid, "mime_type": 1},
            {**valid, "size_bytes": -1}, {**valid, "size_bytes": 1.5},
            {**valid, "size_bytes": True}, {**valid, "object_version": {}},
            {**valid, "unknown": "field"},
        ))
        for value in values:
            with self.subTest(value=value), self.assertRaises(AssertionError):
                decode_descriptor(value)

    def test_content_oracle_decodes_png_jpeg_wav_and_mp3_bytes_in_order(self):
        assets = [self.media.asset(kind, index)
                  for index, kind in enumerate(("png", "wav", "jpeg", "mp3"))]
        content = [text_block("inspect")] + [asset.block() for asset in assets]
        body = {"messages": [{"role": "user", "content": content}]}
        assert_content(self, body, content, assets=assets,
                       forbidden_paths=[asset.path for asset in assets])
        self.assertEqual([block["type"] for block in content],
                         ["text", "image_url", "input_audio", "image_url", "input_audio"])

    def test_content_oracle_rejects_paths_instead_of_media_and_wrong_row_bytes(self):
        first, second = self.media.asset("png", 1), self.media.asset("png", 2)
        expected = [text_block("row-1"), first.block()]
        bad_contents = [
            str(first.path), [text_block("row-1"), text_block(str(first.path))],
            [text_block("row-1"), second.block()], [first.block(), text_block("row-1")],
            [text_block("row-2"), first.block()], [text_block("row-1")],
            expected + [first.block()],
        ]
        for content in bad_contents:
            with self.subTest(content=content), self.assertRaises(AssertionError):
                assert_content(self, {"messages": [{"role": "user", "content": content}]},
                               expected, assets=[first], forbidden_paths=[first.path])

    def test_content_oracle_rejects_hidden_path_leaks_and_internal_capabilities(self):
        asset = self.media.asset()
        content = [text_block("inspect"), asset.block()]
        body = {"messages": [{"role": "user", "content": content}]}
        for extra in ({"metadata": str(asset.path)},
                      {"metadata": asset.path.as_uri()},
                      {"input_modalities": ["text", "image"]}):
            with self.subTest(extra=extra), self.assertRaises(AssertionError):
                assert_content(self, {**body, **extra}, content, assets=[asset],
                               forbidden_paths=[asset.path])

    def test_content_oracle_checks_decoded_bytes_even_if_expected_blocks_are_wrong(self):
        first, second = self.media.asset("png", 1), self.media.asset("png", 2)
        for block in (second.block(), {"type": "image_url", "image_url": {
                "url": "data:image/png;base64,not base64!"}}):
            content = [text_block("same-row"), block]
            with self.subTest(block=block), self.assertRaises(AssertionError):
                assert_content(self, {"messages": [{"role": "user", "content": content}]},
                               content, assets=[first])

    def test_join_content_oracle_locks_rule_left_right_markers_and_media_order(self):
        left, right = self.media.asset("jpeg", 1), self.media.asset("wav", 2)
        content = join_content(JOIN_RULE, left, right)
        self.assertEqual(content[:3], [text_block("AI_JOIN rule:\n"), text_block(JOIN_RULE),
                                      text_block("\nLEFT INPUT:\n")])
        self.assertEqual(content[4], text_block("\nRIGHT INPUT:\n"))
        body = {"messages": [{"role": "user", "content": content}],
                "response_format": semantic.MATCHING_FILTER_FORMAT}
        assert_content(self, body, content, assets=[left, right],
                       forbidden_paths=[left.path, right.path])
        semantic.assert_filter_request(self, body)
        swapped = join_content(JOIN_RULE, right, left)
        with self.assertRaises(AssertionError):
            assert_content(self, {**body, "messages": [{"role": "user", "content": swapped}]},
                           content, assets=[left, right])
        ordinary_path = str(self.media.outside / "ordinary-path.png")
        mixed = join_content(JOIN_RULE, left, ordinary_path)
        self.assertEqual(mixed[-1], text_block(ordinary_path))
        assert_content(self, {"messages": [{"role": "user", "content": mixed}]}, mixed, assets=[left])

    def test_model_registration_uses_parent_metadata_and_global_root_not_call_options(self):
        contract = FileContracts("test_media_sysvar_defaults_and_global_only_root_policy")
        contract.media = self.media
        contract.media_configured = False
        contract.first, contract.second = self.fixtures.mocks
        contract.cursor = mock.Mock()
        contract.execute = mock.Mock(return_value=(("",),))
        with mock.patch.object(FileContracts, "models_created", False):
            contract.prepare_media()
        calls = contract.cursor.execute.call_args_list
        root_calls = [call for call in calls if "ai_media_root" in call.args[0]]
        self.assertEqual(len(root_calls), 1)
        self.assertEqual(root_calls[0].args, ("SET GLOBAL ai_media_root=%s", (str(self.media.root),)))
        registrations = {call.args[1][0]: strict_json(call.args[1][1])
                         for call in calls if "CREATE_AI_MODEL(" in call.args[0]}
        self.assertEqual(registrations["file_media_a"]["input_modalities"], ["text", "image", "audio"])
        self.assertEqual(registrations["file_media_b"]["input_modalities"], ["text", "image", "audio"])
        self.assertEqual(registrations["file_media_image"]["input_modalities"], ["text", "image"])
        self.assertEqual(registrations["file_media_audio"]["input_modalities"], ["text", "audio"])
        self.assertNotIn("input_modalities", registrations["file_media_text"])
        self.assertTrue(all("options" not in config for config in registrations.values()))

    def test_capability_catalog_oracle_checks_current_history_and_exact_integer_masks(self):
        contract = FileContracts(
            "test_media_registration_completion_masks_preserve_default_explicit_and_reordered_arrays")
        expected = {"audio": 5, "image": 3, "text": 1, "all": 7}
        rows = [(1000 + index, name, mask) for index, (name, mask) in enumerate(sorted(expected.items()))]
        history = [((model_id, name, mask, 0),) for model_id, name, mask in rows]
        contract.execute = mock.Mock(side_effect=[rows, *history])
        contract.assert_catalog_modalities(expected)
        calls = contract.execute.call_args_list
        self.assertIn("oceanbase.__all_ai_model ", calls[0].args[0])
        self.assertEqual(calls[0].args[1], tuple(sorted(expected)))
        for call, (model_id, _name, _mask) in zip(calls[1:], rows):
            self.assertIn("oceanbase.__all_ai_model_history ", call.args[0])
            self.assertIn("WHERE model_id=%s", call.args[0])
            self.assertIn("ORDER BY schema_version DESC LIMIT 1", call.args[0])
            self.assertNotIn("name IN", call.args[0])
            self.assertEqual(call.args[1], (model_id,))
        model_id, name, mask = rows[0]
        for current in (
                rows[:-1], rows + [rows[0]],
                [(model_id, name, 1), *rows[1:]],
                [(model_id, name, "7"), *rows[1:]]):
            with self.subTest(current=current), self.assertRaises(AssertionError):
                contract.execute = mock.Mock(side_effect=[current, *history])
                contract.assert_catalog_modalities(expected)
        for latest in (
                [], [(model_id + 9000, name, mask, 0)],
                [(model_id, "wrong-name", mask, 0)], [(model_id, name, 1, 0)],
                [(model_id, name, None, 0)], [(model_id, name, "7", 0)],
                [(model_id, name, mask, 1)], history[0] * 2):
            with self.subTest(latest=latest), self.assertRaises(AssertionError):
                contract.execute = mock.Mock(side_effect=[rows, latest, *history[1:]])
                contract.assert_catalog_modalities(expected)

    def test_loopback_records_exact_blocks_schema_and_provider_stop(self):
        asset = self.media.asset("wav", 2)
        content = [text_block("fixture"), asset.block(), text_block("ordinary text")]
        body = {"messages": [{"role": "user", "content": content}],
                "response_format": semantic.MATCHING_FILTER_FORMAT, "temperature": 0.25}
        reply = self.fixtures.reply(self.first, content, False, boolean=True)
        self.assertEqual(post_body(self.first, body), (200, reply.body))
        self.assertEqual(self.first.requests, [body])
        record = semantic.audit(self.first)[0]
        self.assertEqual(strict_json(record["raw_body"]), body)
        self.assertEqual(record["prompt"], content_key(content))
        semantic.assert_filter_request(self, body)
        payload = strict_json(reply.body)
        self.assertEqual(payload["choices"][0]["finish_reason"], "stop")
        self.assertIs(strict_json(payload["choices"][0]["message"]["content"])["value"], False)

    def test_loopback_gates_release_independently_without_timing_assumptions(self):
        assets = [self.media.asset("png", 5), self.media.asset("jpeg", 6)]
        contents = [[text_block(f"fixture-{index}"), asset.block()]
                    for index, asset in enumerate(assets)]
        gates = [self.fixtures.gate(), self.fixtures.gate()]
        pending = []
        for server, content, gate in zip(self.fixtures.mocks, contents, gates):
            self.fixtures.reply(server, content, "fixture-result", gate=gate)
            pending.append(self.fixtures.start(lambda server=server, content=content: post_body(
                server, {"messages": [{"role": "user", "content": content}]})))
        for server, operation, gate in zip(self.fixtures.mocks, pending, gates):
            semantic.wait_evidence(self, server, lambda server=server: len(server.audit) == 1,
                                   operation, message="media fixture request did not arrive")
            semantic.assert_held(self, server, [gate])
        gates[1].set()
        self.assertEqual(pending[1].result()[0], 200)
        self.assertFalse(pending[0].done.is_set())
        semantic.assert_held(self, self.first, [gates[0]])
        gates[0].set()
        self.assertEqual(pending[0].result()[0], 200)

    def test_loopback_error_gate_does_not_release_other_media_requests(self):
        assets = [self.media.asset("png", 9), self.media.asset("wav", 10)]
        contents = [[text_block(f"error-gate-{index}"), asset.block()]
                    for index, asset in enumerate(assets)]
        gates = [self.fixtures.gate(), self.fixtures.gate()]
        replies, pending = [], []
        for content, gate in zip(contents, gates):
            replies.append(self.fixtures.reply(self.first, content, "held-result", gate=gate))
            pending.append(self.fixtures.start(lambda content=content: post_body(
                self.first, {"messages": [{"role": "user", "content": content}]})))
        semantic.wait_evidence(self, self.first, lambda: len(self.first.audit) == 2, pending[0],
                               message="both independently gated media requests did not arrive")
        replies[0].status = 400
        replies[0].body = b'{"error":"fixture failure"}'
        gates[0].set()
        self.assertEqual(pending[0].result(), (400, replies[0].body))
        semantic.assert_held(self, self.first, [gates[1]], {content_key(contents[1])})
        self.assertFalse(pending[1].done.is_set())
        gates[1].set()
        self.assertEqual(pending[1].result()[0], 200)

    def test_duplicate_block_requests_keep_independent_ordinals(self):
        asset = self.media.asset("png", 4)
        content = [text_block("duplicate"), asset.block()]
        gate = self.fixtures.gate()
        bodies = [completion(f"ordinal-{index}", finish_reason="stop") for index in range(3)]
        self.first.scenarios[content_key(content)] = [
            Reply(body=body, gate=gate if index == 0 else None)
            for index, body in enumerate(bodies)]
        body = {"messages": [{"role": "user", "content": content}]}
        first = self.fixtures.start(lambda: post_body(self.first, body))
        semantic.wait_evidence(self, self.first, lambda: len(self.first.audit) == 1,
                               first, message="first duplicate request did not arrive")
        for response in bodies[1:]:
            self.assertEqual(post_body(self.first, body), (200, response))
        self.assertFalse(gate.is_set())
        self.assertIsNone(semantic.audit(self.first)[0]["response"])
        self.assertTrue(all(record["response"] is not None
                            for record in semantic.audit(self.first)[1:]))
        self.assertFalse(first.done.is_set())
        gate.set()
        self.assertEqual(first.result(), (200, bodies[0]))
        self.assertEqual(self.first.counts, Counter({content_key(content): 3}))

    def test_dependency_oracle_rejects_unready_and_wrong_row_sources(self):
        inner = [{"prompt": "inner-0", "received": 1, "response": 3}]
        outer = [{"prompt": "outer-0", "received": 4, "response": None}]
        semantic.assert_dependencies(self, inner, outer, {"outer-0": "inner-0"})
        for records, dependencies in (
                ([{**inner[0], "response": None}], {"outer-0": "inner-0"}),
                (inner, {"outer-0": "inner-1"}),
                ([{**inner[0], "response": 5}], {"outer-0": "inner-0"})):
            with self.subTest(records=records), self.assertRaises(AssertionError):
                semantic.assert_dependencies(self, records, outer, dependencies)

    def test_private_restart_reuses_directory_and_only_signals_sigusr1(self):
        database = PrivateDatabase("/not-executed/seekdb")
        self.addCleanup(database.close)
        processes = [mock.Mock(), mock.Mock()]
        for process in processes:
            process.poll.return_value = None
            process.wait.return_value = 0
        connections = [mock.Mock(), mock.Mock()]
        with mock.patch.object(subprocess, "Popen", side_effect=processes) as launch, \
                mock.patch.object(pymysql, "connect", side_effect=connections) as connect:
            database.start()
            original_base = database.base
            database.restart()
            self.assertEqual(database.base, original_base)
            self.assertEqual(launch.call_args_list[0].args[0], launch.call_args_list[1].args[0])
            self.assertEqual(database.command()[2:4], ["--base-dir", str(original_base)])
            self.assertEqual(connect.call_args.kwargs["unix_socket"], database.sql_socket)
            database.stop()
        self.assertEqual((database.starts, database.normal_stops), (2, 2))
        for process, connection in zip(processes, connections):
            process.send_signal.assert_called_once_with(signal.SIGUSR1)
            process.terminate.assert_not_called()
            process.kill.assert_not_called()
            connection.close.assert_called_once()

    def test_private_upgrade_switches_only_binary_and_restarts_same_directory_with_sigusr1(self):
        database = PrivateDatabase("/not-executed/old-seekdb")
        self.addCleanup(database.close)
        processes = [mock.Mock() for _ in range(3)]
        connections = [mock.Mock() for _ in range(3)]
        for process in processes:
            process.poll.return_value = None
            process.wait.return_value = 0
        with mock.patch.object(subprocess, "Popen", side_effect=processes) as launch, \
                mock.patch.object(pymysql, "connect", side_effect=connections):
            database.start()
            base = database.base
            database.restart("/not-executed/new-seekdb")
            database.restart()
            database.stop()
        commands = [call.args[0] for call in launch.call_args_list]
        self.assertEqual([command[0] for command in commands],
                         ["/not-executed/old-seekdb", "/not-executed/new-seekdb", "/not-executed/new-seekdb"])
        self.assertTrue(all(command[1:] == commands[0][1:] for command in commands))
        self.assertEqual(database.base, base)
        self.assertEqual((database.starts, database.normal_stops), (3, 3))
        for process in processes:
            process.send_signal.assert_called_once_with(signal.SIGUSR1)
            process.terminate.assert_not_called()
            process.kill.assert_not_called()

    def test_upgrade_column_oracle_rejects_missing_duplicates_ids_defaults_and_nullability(self):
        for table, column_id, nullable in (
                ("__all_ai_model", 22, False), ("__all_ai_model_history", 24, True)):
            correct = ((column_id, int(nullable), "1", "1"),)
            assert_modality_column(self, correct, table=table, column_id=column_id, nullable=nullable)
            for rows in (
                    (), correct * 2, ((column_id + 1, int(nullable), "1", "1"),),
                    ((column_id, int(not nullable), "1", "1"),),
                    ((column_id, int(nullable), "0", "1"),),
                    ((column_id, int(nullable), "1", "0"),),
                    ((column_id, int(nullable), None, "1"),)):
                with self.subTest(table=table, rows=rows), self.assertRaises(AssertionError):
                    assert_modality_column(self, rows, table=table, column_id=column_id, nullable=nullable)

    def test_upgrade_cli_requires_explicit_sole_case_and_never_uses_old_binary_by_default(self):
        new_binary, old_binary = "/not-executed/new-seekdb", "/not-executed/old-seekdb"
        ordinary = "test_native_restart_preserves_committed_file_values_and_all_schema_surfaces"
        options = parse_options(["--binary", new_binary])
        self.assertIsNone(options.upgrade_binary)
        self.assertNotIn(UPGRADE_CASE, case_names(FileContracts))
        self.assertEqual(case_names(CatalogUpgradeContracts), [UPGRADE_CASE])
        selected = parse_options(["--binary", new_binary, "--upgrade-binary", old_binary,
                                  "--case", UPGRADE_CASE])
        self.assertEqual(selected.case, [UPGRADE_CASE])
        self.assertEqual(selected.upgrade_binary, Path(old_binary))
        self.assertEqual(parse_options(["--list-cases", "--case", UPGRADE_CASE]).case, [UPGRADE_CASE])
        for arguments in (
                ["--binary", new_binary, "--case", UPGRADE_CASE],
                ["--binary", new_binary, "--upgrade-binary", old_binary],
                ["--binary", new_binary, "--upgrade-binary", old_binary, "--case", ordinary],
                ["--binary", new_binary, "--upgrade-binary", old_binary, "--case", UPGRADE_CASE,
                 "--case", ordinary],
                ["--self-test", "--upgrade-binary", old_binary],
                ["--binary", new_binary, "--upgrade-binary", old_binary, "--case", UPGRADE_CASE,
                 "--case", UPGRADE_CASE]):
            with self.subTest(arguments=arguments), redirect_stderr(io.StringIO()), \
                    self.assertRaises(SystemExit) as caught:
                parse_options(arguments)
            self.assertEqual(caught.exception.code, 2)
        with mock.patch.object(Path, "is_file", return_value=True), \
                mock.patch.object(os, "access", return_value=True), \
                mock.patch(__name__ + ".PrivateDatabase") as launcher, \
                mock.patch(__name__ + ".run_suite") as runner, \
                mock.patch.object(FileContracts, "database", None):
            main(["--binary", new_binary])
            launcher.assert_called_once_with(Path(new_binary).resolve())
            launcher.return_value.start.assert_called_once()
            launcher.return_value.restart.assert_not_called()
            runner.assert_called_once_with(FileContracts, [])
            launcher.return_value.close.assert_called_once()
        with mock.patch.object(Path, "is_file", return_value=True), \
                mock.patch.object(os, "access", return_value=True), \
                mock.patch(__name__ + ".PrivateDatabase") as launcher, \
                mock.patch(__name__ + ".run_suite") as runner, \
                mock.patch.object(CatalogUpgradeContracts, "database", None), \
                mock.patch.object(CatalogUpgradeContracts, "old_binary", None), \
                mock.patch.object(CatalogUpgradeContracts, "new_binary", None):
            main(["--binary", new_binary, "--upgrade-binary", old_binary, "--case", UPGRADE_CASE])
            launcher.assert_called_once_with(Path(old_binary).resolve())
            launcher.return_value.start.assert_not_called()
            runner.assert_called_once_with(CatalogUpgradeContracts, [UPGRADE_CASE])
            launcher.return_value.close.assert_called_once()

    def test_private_launcher_does_not_call_a_failed_shutdown_persistent(self):
        database = PrivateDatabase("/not-executed/seekdb")
        self.addCleanup(database.close)
        database.process = mock.Mock()
        database.process.poll.return_value = None
        database.process.wait.return_value = 1
        with self.assertRaisesRegex(RuntimeError, "shutdown returned 1"):
            database.stop()
        self.assertEqual(database.normal_stops, 0)

    def test_private_launcher_timeout_kills_only_owned_process_and_fails_explicitly(self):
        database = PrivateDatabase("/not-executed/seekdb")
        self.addCleanup(database.close)
        process = mock.Mock()
        process.poll.return_value = None
        process.wait.side_effect = [subprocess.TimeoutExpired("private-seekdb", 30), 0]
        database.process = process
        with self.assertRaisesRegex(RuntimeError, "normal database shutdown timed out"):
            database.stop()
        process.send_signal.assert_called_once_with(signal.SIGUSR1)
        process.kill.assert_called_once()
        process.terminate.assert_not_called()
        self.assertEqual(database.normal_stops, 0)

    def test_private_shutdown_can_hold_the_client_open_until_sigusr1_exit(self):
        database = PrivateDatabase("/not-executed/seekdb")
        self.addCleanup(database.close)
        events = []
        connection = mock.Mock()
        connection.close.side_effect = lambda: events.append("close")
        process = mock.Mock()
        process.poll.return_value = None
        process.send_signal.side_effect = lambda _signal: events.append("signal")
        process.wait.side_effect = lambda **_options: events.append("wait") or 0
        database.connection = connection
        database.process = process
        database.stop(close_connection_first=False, timeout=10)
        self.assertEqual(events, ["signal", "wait", "close"])
        process.send_signal.assert_called_once_with(signal.SIGUSR1)
        process.wait.assert_called_once_with(timeout=10)
        process.kill.assert_not_called()
        self.assertEqual(database.normal_stops, 1)
        self.assertIsNone(database.connection)
        self.assertIsNone(database.process)

    def test_private_shutdown_rejects_invalid_timeout_without_touching_the_process(self):
        database = PrivateDatabase("/not-executed/seekdb")
        self.addCleanup(database.close)
        process = mock.Mock()
        connection = mock.Mock()
        with mock.patch.object(database, "process", process), \
                mock.patch.object(database, "connection", connection):
            for timeout in (0, -1):
                with self.subTest(timeout=timeout), self.assertRaisesRegex(ValueError, "must be positive"):
                    database.stop(timeout=timeout)
            process.send_signal.assert_not_called()
            process.wait.assert_not_called()
            connection.close.assert_not_called()
            self.assertIs(database.process, process)
            self.assertIs(database.connection, connection)
        self.assertEqual(database.normal_stops, 0)

    def test_cli_selects_cases_and_forbids_live_or_external_databases(self):
        selected = "test_synthetic_wav_roundtrips_exact_pcm_samples"
        options = parse_options(["--self-test", "--case", selected])
        self.assertEqual(options.case, [selected])
        for arguments in (
                ["--self-test", "--binary", "/not-executed/seekdb"],
                ["--self-test", "--socket", "/some/sql.sock"],
                ["--self-test", "--allow-live"],
                ["--self-test", "--case", "test_missing"],
                ["--binary", "/not-executed/seekdb", "--case", selected],
                []):
            with self.subTest(arguments=arguments), redirect_stderr(io.StringIO()), \
                    self.assertRaises(SystemExit) as caught:
                parse_options(arguments)
            self.assertEqual(caught.exception.code, 2)


class FileContracts(unittest.TestCase):
    database = None
    models_created = False

    @classmethod
    def setUpClass(cls):
        with cls.database.connection.cursor() as cursor:
            cursor.execute("SET ob_query_timeout = 30000000")
            cursor.execute(f"CREATE DATABASE {DATABASE}")
            cursor.execute(f"USE {DATABASE}")
            cursor.execute("CREATE TABLE file_values (id INT PRIMARY KEY, bucket INT, "
                           "text_value LONGTEXT, asset FILE, other_asset FILE)")
            cursor.execute("CREATE TABLE file_right (id INT PRIMARY KEY, text_value LONGTEXT, asset FILE)")
            cursor.execute("CREATE TABLE file_drivers (id INT PRIMARY KEY)")

    @property
    def connection(self):
        return self.database.connection

    def setUp(self):
        self.media = MediaFiles()
        self.addCleanup(self.media.close)
        self.fixtures = FileFixtures()
        self.first, self.second = self.fixtures.mocks
        self.cursor = self.connection.cursor()
        self.media_configured = False
        self.original_media_root = None
        self.expected_requests = {server: {} for server in self.fixtures.mocks}
        self.addCleanup(self.close_case)
        self.cursor.execute(f"USE {DATABASE}")
        self.cursor.execute("SET ob_query_timeout = 30000000, ai_pipeline_slots = 2, "
                            "ai_pipeline_memory_limit = DEFAULT")
        for table in ("file_values", "file_right", "file_drivers"):
            self.cursor.execute(f"TRUNCATE TABLE {table}")
        self.connection_id = self.connection.thread_id()
        self.process_id = struct.unpack(
            "3i", self.connection._sock.getsockopt(
                socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i")))[0]

    def close_case(self):
        try:
            self.fixtures.release()
            for pending in self.fixtures.pending:
                pending.thread.join(5)
            if any(pending.thread.is_alive() for pending in self.fixtures.pending):
                self.kill_query()
        finally:
            try:
                self.fixtures.close()
            finally:
                try:
                    if self.media_configured:
                        self.cursor.execute("SET GLOBAL ai_media_root=%s", (self.original_media_root,))
                        self.cursor.execute("SET ai_media_max_bytes = DEFAULT, ai_media_parallel = DEFAULT")
                finally:
                    self.cursor.close()
        for server, expectations in self.expected_requests.items():
            for body in server.requests:
                key = content_key(body["messages"][-1]["content"])
                self.assertIn(key, expectations, "provider received an unplanned row/candidate")
                expected, assets, boolean, caller_format = expectations[key]
                assert_content(self, body, expected, assets=assets,
                               forbidden_paths=[asset.path for asset in assets])
                if boolean:
                    semantic.assert_filter_request(self, body, caller_format)

    def execute(self, statement, parameters=None):
        self.cursor.execute(statement, parameters)
        return self.cursor.fetchall()

    def reject(self, statement, parameters=None, *, code=None):
        before = [len(server.requests) for server in self.fixtures.mocks]
        with self.assertRaises(pymysql.MySQLError) as caught:
            self.cursor.execute(statement, parameters)
        self.assertIsInstance(caught.exception.args[0], int)
        self.assertTrue(caught.exception.args[1], "SQL error did not explain the rejected operation")
        if code is not None:
            self.assertEqual(caught.exception.args[0], code, caught.exception.args)
        self.assertEqual([len(server.requests) for server in self.fixtures.mocks], before,
                         "rejected FILE SQL unexpectedly made a model request")
        self.assertEqual(self.execute("SELECT 1"), ((1,),), "rejection poisoned the SQL connection")
        return caught.exception

    def insert_values(self, records):
        self.cursor.executemany(
            "INSERT INTO file_values VALUES (%s, %s, %s, TO_FILE(%s), TO_FILE(%s))",
            [(row_id, bucket, text, descriptor_text(asset) if asset is not None else None,
              descriptor_text(other) if other is not None else None)
             for row_id, bucket, text, asset, other in records])

    def schema(self, table="file_values", columns=("asset", "other_asset")):
        create = self.execute(f"SHOW CREATE TABLE {table}")[0][1]
        for column in columns:
            self.assertRegex(create.lower(), rf"`{column}`\s+file\b")
        described = {row[0]: row[1].lower() for row in self.execute(f"DESCRIBE {table}")}
        for column in columns:
            self.assertEqual(described[column], "file")
        information = self.execute(
            "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s ORDER BY ORDINAL_POSITION",
            (DATABASE, table))
        types = {name: (data_type.lower(), column_type.lower())
                 for name, data_type, column_type in information}
        for column in columns:
            self.assertEqual(types[column], ("file", "file"))

    def assert_rows(self, expected, table="file_values"):
        rows = self.execute(
            f"SELECT id, bucket, text_value, asset, other_asset FROM {table} ORDER BY id")
        self.assertEqual(len(rows), len(expected))
        for row, wanted in zip(rows, expected):
            self.assertEqual(row[:3], wanted[:3])
            for column in (3, 4):
                assert_file_value(self, row[column], wanted[column], self.cursor.description[column])

    def restart(self):
        self.cursor.close()
        before = self.database.normal_stops
        base = self.database.base
        self.database.restart()
        self.assertEqual(self.database.base, base, "persistence test restarted a fresh data directory")
        self.assertEqual(self.database.normal_stops, before + 1,
                         "persistence test did not complete a graceful normal stop")
        self.cursor = self.connection.cursor()
        self.cursor.execute(f"USE {DATABASE}")
        self.cursor.execute("SET ob_query_timeout = 30000000")
        self.process_id = self.database.process.pid

    def kill_query(self):
        with self.database.connect() as control, control.cursor() as cursor:
            cursor.execute(f"KILL QUERY {self.connection.thread_id()}")

    def assert_no_model_sockets(self):
        for server in self.fixtures.mocks:
            self.assertFalse(model_socket_fds(self.process_id, server.server_port),
                             "a completed/cancelled media query retained a model socket")

    def assert_plan(self, statement, labels):
        plan = "\n".join(" ".join(map(str, row)) for row in self.execute("EXPLAIN " + statement)).upper()
        for label in labels:
            self.assertIn(label, plan)
        return plan

    def prepare_media(self, *, root=None):
        if not self.media_configured:
            self.original_media_root = self.execute("SELECT @@GLOBAL.ai_media_root")[0][0]
            self.media_configured = True
        self.cursor.execute("SET GLOBAL ai_media_root=%s",
                            (str(self.media.root) if root is None else root,))
        self.cursor.execute("SET ai_media_max_bytes=DEFAULT, ai_media_parallel=DEFAULT")
        if not type(self).models_created:
            for suffix, modalities in (
                    ("a", ["text", "image", "audio"]), ("b", ["text", "image", "audio"]),
                    ("image", ["text", "image"]), ("audio", ["text", "audio"]), ("text", None)):
                config = {"type": "completion", "model_name": f"mock-file-{suffix}"}
                if modalities is not None:
                    config["input_modalities"] = modalities
                model = f"file_media_{suffix}"
                self.cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL(%s, %s)",
                                    (model, descriptor_text(config)))
                endpoint = {"ai_model_name": model, "access_key": "file-contract-local-only",
                            "provider": "openai", "request_model_name": f"mock-file-{suffix}",
                            "url": f"http://127.0.0.1:{self.first.server_port}/v1/chat/completions"}
                self.cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT(%s, %s)",
                                    (f"{model}_endpoint", descriptor_text(endpoint)))
            type(self).models_created = True
        for suffix in ("a", "b", "image", "audio", "text"):
            server = self.second if suffix == "b" else self.first
            self.cursor.execute("CALL DBMS_AI_SERVICE.ALTER_AI_MODEL_ENDPOINT(%s, %s)",
                                (f"file_media_{suffix}_endpoint", descriptor_text({
                                    "url": f"http://127.0.0.1:{server.server_port}/v1/chat/completions",
                                    "provider": "openai",
                                })))

    def assert_catalog_modalities(self, expected):
        assert_model_catalog(self, self.execute, expected)

    def load_media(self, count=ROW_COUNT, *, prefix="row", kinds=("png", "jpeg", "wav", "mp3")):
        assets = [self.media.asset(kinds[index % len(kinds)], index) for index in range(count)]
        others = [self.media.asset("png", 1000 + index) for index in range(count)]
        self.insert_values([(index, index % 3, f"{prefix}-{index}", asset.descriptor(), other.descriptor())
                            for index, (asset, other) in enumerate(zip(assets, others))])
        return assets, others

    def load_nullable_inputs(self):
        missing = descriptor(self.media.root / "skipped-null-instruction.png", mime_type="image/png")
        self.insert_values([
            (0, 0, "question", None, None),
            (1, 0, None, missing, None),
            (2, 0, None, None, None),
        ])

    def seed(self, server, content, result, *, assets=(), boolean=False, gate=None, caller_format=None):
        self.expected_requests[server][content_key(content)] = (
            content, tuple(assets), boolean, caller_format)
        return self.fixtures.reply(server, content, result, boolean=boolean, gate=gate)

    def assert_counts(self, server, contents):
        with server.condition:
            self.assertEqual(server.counts, Counter(content_key(content) for content in contents))

    def media_statement(self, expression, *, suffix="ORDER BY id", table="file_values"):
        return (f"SELECT /*+ OPT_PARAM('rowsets_max_rows', {BATCH_ROWS}) */ "
                f"id, {expression} FROM {table} {suffix}")

    def select_media(self, expression, *, suffix="ORDER BY id", parameters=None):
        return self.execute(self.media_statement(expression, suffix=suffix), parameters)

    def reject_media(self, statement, parameters=None, *, code=None):
        error = self.reject(statement, parameters, code=code)
        self.assertNotIn(error.args[0], (1317, 4012),
                         "invalid media only failed because the query was cancelled/timed out")
        self.assert_no_model_sockets()
        return error

    def wait_two_batches(self, server, pending, gates):
        semantic.wait_evidence(
            self, server, lambda: len(server.audit) == 2 * BATCH_ROWS, pending,
            message="FILE inputs did not fill two semantic batches while every reply was held")
        self.assertFalse(pending.done.is_set(), repr(pending.error))
        self.assertLessEqual(len(server.audit), 2 * BATCH_ROWS,
                             "media escaped the slots=2 semantic admission bound")
        semantic.assert_held(self, server, gates)

    def recover_while_held(self, old_gate):
        self.assertFalse(old_gate.is_set())
        self.assertEqual(self.connection.thread_id(), self.connection_id)
        asset = self.media.asset("png", 777)
        content = [text_block("recovered"), asset.block()]
        self.seed(self.first, content, "recovered-row", assets=[asset])
        pending = self.fixtures.start(lambda: self.execute(
            "SELECT AI_MAP('file_media_a', ('recovered', TO_FILE(%s)))",
            (descriptor_text(asset.descriptor()),)))
        self.assertEqual(pending.result(timeout=3), (("recovered-row",),))
        self.assertFalse(old_gate.is_set(), "media recovery required releasing abandoned HTTP replies")

    def test_to_file_paths_uris_json_and_null_do_not_load_files(self):
        path = self.media.outside / "does not exist #?.png"
        self.assertFalse(path.exists())
        expected = descriptor(path)
        enriched = {**expected, "mime_type": "image/png", "object_version": "opaque-v1", "size_bytes": 987}
        for value, wanted in ((str(path), expected), (path.as_uri(), expected),
                              (descriptor_text(enriched), enriched)):
            with self.subTest(value=value):
                rows = self.execute(
                    "SELECT TO_FILE(%s), FILE_URI(TO_FILE(%s)), FILE_META(TO_FILE(%s))",
                    (value, value, value))
                self.assertEqual(len(rows), 1)
                assert_file_value(self, rows[0][0], wanted, self.cursor.description[0])
                self.assertEqual(rows[0][1], wanted["uri"])
                self.assertEqual(strict_json(rows[0][2]), wanted)
        rows = self.execute(
            "SELECT TO_FILE(NULL), FILE_URI(TO_FILE(NULL)), FILE_META(TO_FILE(NULL))")
        self.assertEqual(rows, ((None, None, None),))
        self.assertFalse(path.exists(), "descriptor construction unexpectedly touched the filesystem")

    def test_to_file_accepts_json_typed_descriptor_and_preserves_optional_fields(self):
        expected = descriptor(self.media.outside / "metadata.wav",
                              mime_type="audio/wav", object_version="version-002", size_bytes=0)
        row = self.execute("SELECT TO_FILE(CAST(%s AS JSON)), "
                           "FILE_META(TO_FILE(CAST(%s AS JSON)))",
                           (descriptor_text(expected), descriptor_text(expected)))[0]
        assert_file_value(self, row[0], expected, self.cursor.description[0])
        self.assertEqual(strict_json(row[1]), expected)
        minimal = descriptor(self.media.outside / "metadata-only")
        row = self.execute("SELECT FILE_META(TO_FILE(%s))", (descriptor_text(minimal),))[0]
        self.assertEqual(strict_json(row[0]), minimal, "omitted metadata became fabricated defaults")

    def test_to_file_normalizes_path_segments_and_uri_percent_encoding_without_io(self):
        final = self.media.root / "canonical #?.png"
        expected = descriptor(final)
        raw_path = str(self.media.root) + "//not-created/.././canonical #?.png"
        raw_uri = self.media.root.as_uri() + "/not-created/.././canonical%20%23%3f.png"
        for value in (raw_path, raw_uri):
            with self.subTest(value=value):
                row = self.execute("SELECT TO_FILE(%s), FILE_URI(TO_FILE(%s))", (value, value))[0]
                assert_file_value(self, row[0], expected, self.cursor.description[0])
                self.assertEqual(row[1], expected["uri"])
        self.assertFalse(final.exists())
        for value in ("file:///tmp/bad%", "file:///tmp/bad%0", "file:///tmp/bad%GG",
                      "file:///../escape.png", "/../../escape.png"):
            with self.subTest(value=value):
                self.reject("SELECT TO_FILE(%s)", (value,))

    def test_to_file_rejects_invalid_descriptor_shape_types_and_duplicate_fields(self):
        valid = descriptor(self.media.root / "valid.png")
        invalid = [
            "", "relative.png", "[]", "null", "true", "123", '"file:///tmp/image.png"',
            '{"format_version":1,"format_version":1,"uri":"file:///tmp/image.png"}',
            '{"format_version":1,"uri":"file:///a","uri":"file:///b"}',
            '{"format_version":1,"uri":',
        ]
        invalid.extend(descriptor_text(value) for value in (
            {}, {"uri": valid["uri"]}, {"format_version": 1},
            {**valid, "format_version": 0}, {**valid, "format_version": 2},
            {**valid, "format_version": True}, {**valid, "format_version": "1"},
            {**valid, "uri": None}, {**valid, "uri": 4}, {**valid, "uri": "relative.png"},
            {**valid, "mime_type": 4}, {**valid, "mime_type": None},
            {**valid, "object_version": 4}, {**valid, "object_version": []},
            {**valid, "size_bytes": -1}, {**valid, "size_bytes": True},
            {**valid, "size_bytes": 1.5}, {**valid, "size_bytes": "1"},
            {**valid, "unexpected_field": "not in FILE v1"},
        ))
        for value in invalid:
            with self.subTest(value=value):
                self.reject("SELECT TO_FILE(%s)", (value,))

    def test_to_file_rejects_remote_authority_uri_and_nontext_scalars(self):
        for value in ("s3://bucket/image.png", "http://example.invalid/image.png",
                      "file://localhost/tmp/image.png",
                      "file://server/share/image.png", "file:///tmp/image.png?query=1",
                      "file:///tmp/image.png#fragment"):
            with self.subTest(value=value):
                self.reject("SELECT TO_FILE(%s)", (value,))
        for expression in ("TO_FILE(1)", "TO_FILE(TRUE)", "TO_FILE(1.25)",
                           "TO_FILE(JSON_ARRAY('/tmp/image.png'))"):
            with self.subTest(expression=expression):
                self.reject("SELECT " + expression)

    def test_schema_and_standard_wire_type_are_native_file_not_json_or_string(self):
        self.schema()
        expected = descriptor(self.media.root / "not-opened.png", mime_type="image/png")
        self.insert_values([(1, 7, "native", expected, None)])
        self.assert_rows([(1, 7, "native", expected, None)])
        row = self.execute("SELECT asset, FILE_URI(asset), FILE_META(asset) FROM file_values")[0]
        assert_file_value(self, row[0], expected, self.cursor.description[0])
        self.assertEqual(row[1], expected["uri"])
        self.assertEqual(strict_json(row[2]), expected)

    def test_alter_add_and_rename_file_columns_keep_native_schema_and_values(self):
        self.cursor.execute("CREATE TABLE file_alter (id INT PRIMARY KEY)")
        try:
            self.cursor.execute("ALTER TABLE file_alter ADD COLUMN asset FILE")
            expected = descriptor(self.media.root / "alter.png", object_version="ddl-v1")
            self.cursor.execute("INSERT INTO file_alter VALUES (1, TO_FILE(%s))",
                                (descriptor_text(expected),))
            self.schema("file_alter", ("asset",))
            self.cursor.execute("ALTER TABLE file_alter CHANGE COLUMN asset renamed_asset FILE")
            self.schema("file_alter", ("renamed_asset",))
            row = self.execute("SELECT id, renamed_asset FROM file_alter")[0]
            self.assertEqual(row[0], 1)
            assert_file_value(self, row[1], expected, self.cursor.description[1])
        finally:
            self.cursor.execute("DROP TABLE file_alter")

    def test_file_is_nonreserved_as_table_column_alias_and_cte_identifier(self):
        self.cursor.execute("CREATE TABLE file (id INT PRIMARY KEY, file FILE)")
        try:
            assets = [descriptor(self.media.root / f"identifier-{index}.png") for index in range(3)]
            self.cursor.executemany("INSERT INTO file (id, file) VALUES (%s, TO_FILE(%s))",
                                    [(index, descriptor_text(asset)) for index, asset in enumerate(assets)])
            self.schema("file", ("file",))
            rows = self.execute(
                "SELECT file.id, file.file, FILE_URI(file.file) FROM file file ORDER BY file.id DESC")
            self.assertEqual([row[0] for row in rows], [2, 1, 0])
            for row in rows:
                assert_file_value(self, row[1], assets[row[0]], self.cursor.description[1])
                self.assertEqual(row[2], assets[row[0]]["uri"])
            rows = self.execute(
                f"WITH file AS (SELECT id, file AS value FROM {DATABASE}.file) "
                "SELECT id, value AS file FROM file ORDER BY id")
            self.assertEqual([row[0] for row in rows], [0, 1, 2])
            for row in rows:
                assert_file_value(self, row[1], assets[row[0]], self.cursor.description[1])
            self.cursor.execute("UPDATE file SET file=NULL WHERE id=1")
            self.assertEqual(self.execute("SELECT COUNT(file), SUM(file IS NULL) FROM file"), ((2, 1),))
        finally:
            self.cursor.execute("DROP TABLE file")

    def test_string_and_json_do_not_implicitly_become_file(self):
        path = str(self.media.root / "explicit-only.png")
        raw = descriptor_text(descriptor(Path(path)))
        for statement, values in (
                ("INSERT INTO file_values (id, asset) VALUES (1, %s)", (path,)),
                ("INSERT INTO file_values (id, asset) VALUES (1, %s)", (raw,)),
                ("INSERT INTO file_values (id, asset) VALUES (1, CAST(%s AS JSON))", (raw,)),
                ("SELECT FILE_URI(%s)", (path,)),
                ("SELECT FILE_META(CAST(%s AS JSON))", (raw,))):
            with self.subTest(statement=statement, values=values):
                self.reject(statement, values)
                self.assertEqual(self.execute("SELECT COUNT(*) FROM file_values"), ((0,),))
        expected = descriptor(Path(path))
        self.insert_values([(1, 0, "explicit", expected, None)])
        self.assert_rows([(1, 0, "explicit", expected, None)])

    def test_native_insert_update_delete_and_count_null_in_memtable(self):
        first = descriptor(self.media.root / "first.png", mime_type="image/png", size_bytes=71)
        second = descriptor(self.media.root / "second.wav",
                            mime_type="audio/wav", object_version="v2", size_bytes=108)
        self.insert_values([(1, 4, "first", first, None), (2, 4, "second", second, first),
                            (3, 8, "null", None, None)])
        self.assertEqual(self.execute(
            "SELECT COUNT(*), COUNT(asset), SUM(asset IS NULL), SUM(asset IS NOT NULL) "
            "FROM file_values"), ((3, 2, 1, 2),))
        self.cursor.execute(
            "UPDATE file_values SET asset=TO_FILE(%s), other_asset=TO_FILE(%s) WHERE id=1",
            (descriptor_text(second), descriptor_text(second)))
        self.cursor.execute("UPDATE file_values SET asset=NULL WHERE id=2")
        self.cursor.execute("DELETE FROM file_values WHERE id=3")
        self.assert_rows([(1, 4, "first", second, second), (2, 4, "second", None, first)])
        self.schema()

    def test_native_file_transactions_commit_and_rollback(self):
        first = descriptor(self.media.root / "committed.png")
        second = descriptor(self.media.root / "rolled-back.wav")
        self.cursor.execute("START TRANSACTION")
        try:
            self.insert_values([(1, 0, "committed", first, None)])
            self.cursor.execute("COMMIT")
            self.cursor.execute("START TRANSACTION")
            self.cursor.execute("UPDATE file_values SET asset=TO_FILE(%s) WHERE id=1",
                                (descriptor_text(second),))
            self.insert_values([(2, 0, "rolled-back", second, None)])
            self.cursor.execute("DELETE FROM file_values WHERE id=1")
            self.cursor.execute("ROLLBACK")
        finally:
            self.cursor.execute("ROLLBACK")
        self.assert_rows([(1, 0, "committed", first, None)])
        with self.database.connect() as reader, reader.cursor() as cursor:
            cursor.execute("SELECT id, asset FROM file_values ORDER BY id")
            row = cursor.fetchone()
            self.assertEqual(row[0], 1)
            assert_file_value(self, row[1], first, cursor.description[1])

    def test_file_case_coalesce_count_and_null_preserve_type(self):
        assets = [descriptor(self.media.root / f"case-{index}.png") for index in range(3)]
        self.insert_values([(0, 0, "zero", assets[0], assets[1]),
                            (1, 0, "one", None, assets[2]), (2, 0, "two", None, None)])
        rows = self.execute(
            "SELECT id, CASE WHEN MOD(id, 2)=0 THEN asset ELSE other_asset END, "
            "COALESCE(asset, other_asset), asset IS NULL, other_asset IS NOT NULL "
            "FROM file_values ORDER BY id")
        self.assertEqual([row[0] for row in rows], [0, 1, 2])
        for row, case_value, coalesced in zip(rows, (assets[0], assets[2], None),
                                             (assets[0], assets[2], None)):
            assert_file_value(self, row[1], case_value, self.cursor.description[1])
            assert_file_value(self, row[2], coalesced, self.cursor.description[2])
        self.assertEqual([row[3:] for row in rows], [(0, 1), (1, 1), (1, 0)])
        row = self.execute(
            "SELECT CASE WHEN 1=1 THEN TO_FILE(%s) ELSE NULL END, COALESCE(NULL, TO_FILE(%s))",
            (descriptor_text(assets[0]), descriptor_text(assets[1])))[0]
        for index, expected in enumerate(assets[:2]):
            assert_file_value(self, row[index], expected, self.cursor.description[index])
        for function in ("IFNULL", "NVL"):
            rows = self.execute(
                f"SELECT id, {function}(asset, other_asset) FROM file_values ORDER BY id")
            self.assertEqual([row[0] for row in rows], [0, 1, 2])
            for row, expected in zip(rows, (assets[0], assets[2], None)):
                assert_file_value(self, row[1], expected, self.cursor.description[1])
        for expression in (
                "CASE WHEN id=1 THEN asset ELSE text_value END",
                "CASE WHEN id=1 THEN asset ELSE JSON_OBJECT('uri', text_value) END",
                "COALESCE(asset, text_value)", "COALESCE(asset, JSON_OBJECT('uri', text_value))",
                "IFNULL(asset, text_value)", "NVL(asset, text_value)"):
            with self.subTest(expression=expression):
                self.reject(f"SELECT {expression} FROM file_values")

    def test_file_projection_derived_cte_and_insert_select_preserve_descriptors(self):
        values = [(index, index % 3, f"projection-{index}",
                   descriptor(self.media.root / f"projection-{index}.png", object_version=f"v{index}"),
                   None) for index in range(ROW_COUNT)]
        self.insert_values(values)
        for statement in (
                "SELECT /*+ NO_MERGE(d) */ id, asset FROM "
                "(SELECT id, asset FROM file_values WHERE asset IS NOT NULL) d ORDER BY id DESC",
                "WITH file_cte AS (SELECT id, asset FROM file_values) "
                "SELECT id, asset FROM file_cte ORDER BY id DESC"):
            with self.subTest(statement=statement):
                rows = self.execute(statement)
                for row, wanted in zip(rows, reversed(values)):
                    self.assertEqual(row[0], wanted[0])
                    assert_file_value(self, row[1], wanted[3], self.cursor.description[1])
                self.assertEqual(len(rows), ROW_COUNT)
        self.cursor.execute("CREATE TABLE file_insert_select LIKE file_values")
        try:
            self.cursor.execute(
                "INSERT INTO file_insert_select SELECT id, bucket, text_value, asset, other_asset "
                "FROM file_values WHERE MOD(id, 2)=0")
            self.schema("file_insert_select")
            self.assert_rows(values[::2], "file_insert_select")
        finally:
            self.cursor.execute("DROP TABLE file_insert_select")

    def test_create_table_as_select_keeps_native_file_schema(self):
        expected = descriptor(self.media.root / "ctas.png", object_version="ctas-version")
        self.insert_values([(1, 0, "ctas", expected, None)])
        self.cursor.execute("CREATE TABLE file_ctas AS SELECT id, asset FROM file_values")
        try:
            self.schema("file_ctas", ("asset",))
            row = self.execute("SELECT id, asset FROM file_ctas")[0]
            self.assertEqual(row[0], 1)
            assert_file_value(self, row[1], expected, self.cursor.description[1])
        finally:
            self.cursor.execute("DROP TABLE file_ctas")

    def test_sort_and_group_on_ordinary_keys_carry_file_values(self):
        values = [(index, ROW_COUNT - index, f"keys-{index}",
                   descriptor(self.media.root / f"keys-{index}.png", size_bytes=index), None)
                  for index in range(ROW_COUNT)]
        self.insert_values(values)
        for suffix in ("ORDER BY bucket, id", "ORDER BY id DESC LIMIT 20 OFFSET 3"):
            rows = self.execute("SELECT id, asset FROM file_values " + suffix)
            wanted = list(reversed(values)) if suffix.startswith("ORDER BY bucket") else list(reversed(values))[3:23]
            self.assertEqual(len(rows), len(wanted))
            for row, expected in zip(rows, wanted):
                self.assertEqual(row[0], expected[0])
                assert_file_value(self, row[1], expected[3], self.cursor.description[1])
        old_mode = self.execute("SELECT @@sql_mode")[0][0]
        mode = ",".join(value for value in old_mode.split(",") if value != "ONLY_FULL_GROUP_BY")
        self.cursor.execute("SET sql_mode=%s", (mode,))
        try:
            for hint in ("USE_HASH_AGGREGATION", "NO_USE_HASH_AGGREGATION"):
                rows = self.execute(
                    f"SELECT /*+ {hint} */ id, asset, COUNT(*) FROM file_values GROUP BY id ORDER BY id")
                self.assertEqual(len(rows), len(values))
                for row, expected in zip(rows, values):
                    self.assertEqual((row[0], row[2]), (expected[0], 1))
                    assert_file_value(self, row[1], expected[3], self.cursor.description[1])
        finally:
            self.cursor.execute("SET sql_mode=%s", (old_mode,))
        grouped = [descriptor(self.media.root / f"group-payload-{bucket}.png",
                              object_version=f"group-{bucket}") for bucket in range(3)]
        for bucket, asset in enumerate(grouped):
            self.cursor.execute(
                "UPDATE file_values SET bucket=MOD(id, 3), asset=TO_FILE(%s) WHERE MOD(id, 3)=%s",
                (descriptor_text(asset), bucket))
        for hint in ("USE_HASH_AGGREGATION", "NO_USE_HASH_AGGREGATION"):
            statement = (
                f"SELECT /*+ {hint} */ bucket, ANY_VALUE(asset), COUNT(asset) "
                "FROM file_values GROUP BY bucket ORDER BY bucket")
            self.assert_plan(statement, ("GROUP BY",))
            rows = self.execute(statement)
            self.assertEqual([row[0] for row in rows], [0, 1, 2])
            for row in rows:
                self.assertEqual(row[2], sum(index % 3 == row[0] for index in range(ROW_COUNT)))
                assert_file_value(self, row[1], grouped[row[0]], self.cursor.description[1])

    def test_ordinary_hash_and_nested_loop_joins_preserve_file_columns(self):
        values = [(index, index % 2, f"join-{index}",
                   descriptor(self.media.root / f"join-{index}.png"), None) for index in range(6)]
        self.insert_values(values)
        self.cursor.executemany("INSERT INTO file_right VALUES (%s, %s, TO_FILE(%s))",
                                [(row[0], row[2], descriptor_text(row[3])) for row in values])
        for hint in ("USE_HASH(r)", "USE_NL(r)"):
            rows = self.execute(
                f"SELECT /*+ LEADING(l r) {hint} */ l.id, l.asset, r.asset FROM file_values l "
                "INNER JOIN file_right r ON l.id=r.id ORDER BY l.id DESC")
            self.assertEqual(len(rows), len(values))
            for row, expected in zip(rows, reversed(values)):
                self.assertEqual(row[0], expected[0])
                for column in (1, 2):
                    assert_file_value(self, row[column], expected[3], self.cursor.description[column])

    def test_whole_file_arithmetic_concat_and_comparisons_are_rejected(self):
        expected = descriptor(self.media.root / "operations.png")
        self.insert_values([(1, 0, "not implicitly a path", expected, expected)])
        expressions = (
            "asset+1", "asset-other_asset", "asset*2", "asset/2", "-asset",
            "CONCAT(asset, 'suffix')", "CONCAT_WS('|', asset, text_value)",
            "asset=other_asset", "asset<>other_asset", "asset<other_asset",
            "asset<=>other_asset", "asset IN (other_asset)",
            "asset BETWEEN other_asset AND other_asset", "asset LIKE '%png'",
        )
        for expression in expressions:
            with self.subTest(expression=expression):
                self.reject(f"SELECT {expression} FROM file_values")
        self.assert_rows([(1, 0, "not implicitly a path", expected, expected)])

    def test_whole_file_sort_group_and_distinct_keys_are_rejected(self):
        expected = descriptor(self.media.root / "keys.png")
        self.insert_values([(1, 0, "same", expected, None), (2, 0, "same", expected, None)])
        statements = (
            "SELECT asset FROM file_values ORDER BY asset",
            "SELECT id, asset AS projected FROM file_values ORDER BY projected",
            "SELECT id, asset FROM file_values ORDER BY 2",
            "SELECT asset FROM (SELECT asset FROM file_values) d ORDER BY asset",
            "SELECT asset, COUNT(*) FROM file_values GROUP BY asset",
            "SELECT DISTINCT asset FROM file_values",
            "SELECT DISTINCT id, asset FROM file_values",
            "SELECT COUNT(DISTINCT asset) FROM file_values",
            "SELECT MIN(asset) FROM file_values", "SELECT MAX(asset) FROM file_values",
        )
        for statement in statements:
            with self.subTest(statement=statement):
                self.reject(statement)

    def test_file_uri_and_extracted_metadata_are_ordinary_sql_keys(self):
        assets = [descriptor(self.media.root / f"extracted-{index}.png", size_bytes=index % 2)
                  for index in range(4)]
        self.insert_values([(index, 0, f"extracted-{index}", asset, None)
                            for index, asset in enumerate(assets)])
        self.assertEqual(self.execute(
            "SELECT id FROM file_values WHERE FILE_URI(asset)=%s", (assets[2]["uri"],)), ((2,),))
        rows = self.execute(
            "SELECT id, asset FROM file_values ORDER BY FILE_URI(asset) DESC")
        self.assertEqual([row[0] for row in rows], [3, 2, 1, 0])
        for row in rows:
            assert_file_value(self, row[1], assets[row[0]], self.cursor.description[1])
        self.assertEqual(self.execute(
            "SELECT CAST(JSON_UNQUOTE(JSON_EXTRACT(FILE_META(asset), '$.size_bytes')) AS SIGNED), "
            "COUNT(*) FROM file_values "
            "GROUP BY CAST(JSON_UNQUOTE(JSON_EXTRACT(FILE_META(asset), '$.size_bytes')) AS SIGNED) "
            "ORDER BY 1"), ((0, 2), (1, 2)))

    def test_file_primary_unique_secondary_and_partition_keys_are_rejected(self):
        for definition in (
                "asset FILE PRIMARY KEY", "id INT, asset FILE, PRIMARY KEY(id, asset)",
                "id INT PRIMARY KEY, asset FILE, UNIQUE KEY file_unique(asset)",
                "id INT PRIMARY KEY, asset FILE, KEY file_key(asset)",
                "id INT PRIMARY KEY, asset FILE, KEY file_key(id, asset)"):
            with self.subTest(definition=definition):
                self.reject(f"CREATE TABLE file_bad_key ({definition})")
                self.assertEqual(self.execute("SHOW TABLES LIKE 'file_bad_key'"), ())
        for statement in (
                "CREATE INDEX file_bad_index ON file_values(asset)",
                "CREATE UNIQUE INDEX file_bad_index ON file_values(asset)",
                "ALTER TABLE file_values ADD KEY file_bad_index(id, asset)",
                "CREATE TABLE file_bad_key (id INT, asset FILE) PARTITION BY HASH(asset) PARTITIONS 2"):
            with self.subTest(statement=statement):
                self.reject(statement)
        self.schema()

    def test_sql_prepared_to_file_reexecution_keeps_values_and_schema(self):
        paths = [self.media.root / "prepared-first.png", self.media.root / "prepared-second.wav"]
        self.cursor.execute("PREPARE file_stmt FROM 'SELECT TO_FILE(?) AS asset'")
        try:
            for path in paths:
                self.cursor.execute("SET @file_path=%s", (str(path),))
                row = self.execute("EXECUTE file_stmt USING @file_path")[0]
                assert_file_value(self, row[0], descriptor(path), self.cursor.description[0])
        finally:
            self.cursor.execute("DEALLOCATE PREPARE file_stmt")

    def persistence_values(self, prefix):
        return [(1, 2, f"{prefix}-updated",
                 descriptor(self.media.outside / f"{prefix}-new.png",
                            mime_type="image/png", object_version="persisted-v2", size_bytes=123), None),
                (3, 6, f"{prefix}-null", None,
                 descriptor(self.media.outside / f"{prefix}-audio.wav", mime_type="audio/wav")),
                (4, 8, f"{prefix}-copied",
                 descriptor(self.media.outside / f"{prefix}-copied.jpeg", size_bytes=0), None)]

    def load_persistence_dml(self, prefix, table="file_values"):
        final = self.persistence_values(prefix)
        original = descriptor(self.media.outside / f"{prefix}-old.png", object_version="old")
        self.cursor.executemany(
            f"INSERT INTO {table} VALUES (%s, %s, %s, TO_FILE(%s), TO_FILE(%s))",
            [(1, 2, f"{prefix}-old", descriptor_text(original), None),
             (2, 4, f"{prefix}-deleted", descriptor_text(original), None),
             (3, 6, final[1][2], None, descriptor_text(final[1][4]))])
        self.cursor.execute(
            f"UPDATE {table} SET text_value=%s, asset=TO_FILE(%s) WHERE id=1",
            (final[0][2], descriptor_text(final[0][3])))
        self.cursor.execute(f"DELETE FROM {table} WHERE id=2")
        self.cursor.execute(
            f"INSERT INTO {table} SELECT 4, 8, %s, TO_FILE(%s), NULL FROM {table} WHERE id=1",
            (final[2][2], descriptor_text(final[2][3])))
        self.assert_rows(final, table)
        self.schema(table)
        return final

    def test_native_restart_preserves_committed_file_values_and_all_schema_surfaces(self):
        final = self.load_persistence_dml("restart")
        self.assertTrue(all(not Path(urlsplit(value["uri"]).path).exists()
                            for row in final for value in row[3:] if value is not None))
        self.restart()
        self.schema()
        self.assert_rows(final)
        self.assertEqual(self.execute("SELECT COUNT(asset), COUNT(other_asset) FROM file_values"),
                         ((2, 1),))
        self.cursor.execute("UPDATE file_values SET asset=other_asset, other_asset=NULL WHERE id=3")
        self.cursor.execute("DELETE FROM file_values WHERE id=4")
        expected = [final[0], (3, 6, final[1][2], final[1][4], None)]
        self.assert_rows(expected)
        self.restart()
        self.schema()
        self.assert_rows(expected)

    def compaction_snapshot(self, tablets, types):
        placeholders = ",".join(["%s"] * len(tablets))
        type_placeholders = ",".join(["%s"] * len(types))
        return dict(self.execute(
            "SELECT TABLET_ID, MAX(FINISH_TIME) "
            "FROM oceanbase.__all_virtual_tablet_compaction_history "
            f"WHERE TABLET_ID IN ({placeholders}) AND TYPE IN ({type_placeholders}) GROUP BY TABLET_ID",
            tuple(tablets) + tuple(types)))

    def force_compaction_restart(self, kind):
        command = f"ALTER SYSTEM {kind.upper()} FREEZE"
        try:
            self.cursor.execute(
                "SELECT TABLET_ID FROM oceanbase.__all_tablet_to_table WHERE TABLE_ID IN "
                "(SELECT TABLE_ID FROM oceanbase.__all_table WHERE TABLE_NAME='file_values')")
            self.cursor.fetchall()
            self.cursor.execute(
                "SELECT TABLET_ID, FINISH_TIME FROM oceanbase.__all_virtual_tablet_compaction_history LIMIT 0")
            self.cursor.fetchall()
        except pymysql.MySQLError as error:
            if error.args[0] in UNSUPPORTED_FACILITY_CODES or error.args[0] == 1146:
                self.skipTest(f"private launcher lacks {kind} freeze/compaction evidence: {error.args}")
            raise
        for name, value in (("merger_check_interval", "10s"),
                            ("ob_compaction_schedule_interval", "3s")):
            try:
                self.cursor.execute(f"ALTER SYSTEM SET {name}=%s", (value,))
            except pymysql.MySQLError as error:
                if error.args[0] not in UNSUPPORTED_FACILITY_CODES:
                    raise
                print(f"Optional private compaction speed setting unavailable: {error.args}", flush=True)
        final = self.load_persistence_dml(kind)
        tablets = [row[0] for row in self.execute(
            "SELECT TABLET_ID FROM oceanbase.__all_tablet_to_table WHERE TABLE_ID IN "
            "(SELECT TABLE_ID FROM oceanbase.__all_table WHERE TABLE_NAME='file_values')")]
        self.assertTrue(tablets, "no FILE table tablet was found for native compaction evidence")
        types = ("MAJOR_MERGE",) if kind == "major" else ("MINI_MERGE", "MINOR_MERGE")
        baseline = self.compaction_snapshot(tablets, types)
        if kind == "minor":
            for tablet in tablets:
                self.cursor.execute("ALTER SYSTEM MINOR FREEZE TABLET_ID=%s", (tablet,))
        else:
            self.cursor.execute(command)
        deadline = time.monotonic() + 90
        latest = {}
        while time.monotonic() < deadline:
            latest = self.compaction_snapshot(tablets, types)
            if all(tablet in latest and (tablet not in baseline or latest[tablet] > baseline[tablet])
                   for tablet in tablets):
                break
            threading.Event().wait(0.1)
        else:
            observed = self.execute(
                "SELECT TABLET_ID, TYPE, FINISH_TIME "
                "FROM oceanbase.__all_virtual_tablet_compaction_history "
                f"WHERE TABLET_ID IN ({','.join(['%s'] * len(tablets))})",
                tuple(tablets))
            self.fail(f"{kind} freeze did not complete a new FILE tablet compaction: "
                      f"tablets={tablets}, before={baseline}, after={latest}, observed={observed}")
        self.schema()
        self.assert_rows(final)
        self.restart()
        self.schema()
        self.assert_rows(final)

    def test_native_minor_freeze_flush_and_restart_preserve_file(self):
        self.force_compaction_restart("minor")

    def test_native_major_freeze_and_restart_preserve_file(self):
        self.force_compaction_restart("major")

    def test_media_sysvar_defaults_and_global_only_root_policy(self):
        self.assertEqual(self.execute(
            "SELECT @@GLOBAL.ai_media_root, @@ai_media_max_bytes, @@ai_media_parallel"),
            (("", MEDIA_MAX_BYTES, MEDIA_PARALLEL),))
        self.reject("SET SESSION ai_media_root=%s", (str(self.media.root),), code=1229)
        self.reject("SET ai_media_root=%s", (str(self.media.root),), code=1229)
        self.assertEqual(self.execute("SELECT @@GLOBAL.ai_media_root"), (("",),))
        self.prepare_media()
        self.assertEqual(self.execute("SELECT @@GLOBAL.ai_media_root"), ((str(self.media.root),),))
        for variable in ("ai_media_max_bytes", "ai_media_parallel"):
            for value in (0, -1):
                with self.subTest(variable=variable, value=value):
                    self.reject(f"SET {variable}=%s", (value,))
        self.assertEqual(self.execute("SELECT @@ai_media_max_bytes, @@ai_media_parallel"),
                         ((MEDIA_MAX_BYTES, MEDIA_PARALLEL),))

    def test_media_nonadmin_cannot_widen_global_root_or_override_session_policy(self):
        self.prepare_media()
        account = "'file_media_no_alter_system'@'%'"
        password = "local-file-privilege-fixture"
        self.cursor.execute("CREATE USER %s@'%%' IDENTIFIED BY %s",
                            ("file_media_no_alter_system", password))
        try:
            self.cursor.execute(f"GRANT SELECT ON {DATABASE}.* TO {account}")
            with pymysql.connect(
                    unix_socket=self.database.sql_socket, user="file_media_no_alter_system",
                    password=password, database=DATABASE, autocommit=True,
                    read_timeout=10, write_timeout=10, charset="utf8mb4") as guest:
                with guest.cursor() as cursor:
                    cursor.execute("SELECT 1, @@GLOBAL.ai_media_root")
                    self.assertEqual(cursor.fetchall(), ((1, str(self.media.root)),))
                    with self.assertRaises(pymysql.MySQLError) as caught:
                        cursor.execute("SET GLOBAL ai_media_root='/'")
                    self.assertRegex(str(caught.exception).lower(),
                                     r"privilege|access denied|permission|not authorized|denied")
                    for statement in ("SET SESSION ai_media_root='/'", "SET ai_media_root='/'"):
                        sql_error(self, 1229, lambda statement=statement: cursor.execute(statement))
                    cursor.execute("SELECT 1, @@GLOBAL.ai_media_root")
                    self.assertEqual(cursor.fetchall(), ((1, str(self.media.root)),))
            self.assertEqual(self.execute("SELECT @@GLOBAL.ai_media_root"), ((str(self.media.root),),))
            self.assertFalse(self.first.requests)
            self.assertFalse(self.second.requests)
        finally:
            self.cursor.execute(f"DROP USER {account}")

    def test_media_secure_file_priv_root_never_enables_or_widens_media_policy(self):
        self.prepare_media(root="")
        original = self.execute("SELECT @@GLOBAL.secure_file_priv")[0][0]
        self.cursor.execute("SET GLOBAL secure_file_priv='/'")
        try:
            self.assertEqual(self.execute("SELECT @@GLOBAL.secure_file_priv, @@GLOBAL.ai_media_root"),
                             (("/", ""),))
            allowed = self.media.asset("png", 601)
            outside = self.media.asset("png", 602, outside=True)
            for asset in (allowed, outside):
                self.reject_media("SELECT AI_MAP('file_media_a', TO_FILE(%s))",
                                  (descriptor_text(asset.descriptor()),))
            self.cursor.execute("SET GLOBAL ai_media_root=%s", (str(self.media.root),))
            self.reject_media("SELECT AI_MAP('file_media_a', TO_FILE(%s))",
                              (descriptor_text(outside.descriptor()),))
            content = [text_block("explicit-private-root"), allowed.block()]
            self.seed(self.first, content, "explicit-root-ok", assets=[allowed])
            self.assertEqual(self.execute(
                "SELECT AI_MAP('file_media_a', ('explicit-private-root', TO_FILE(%s)))",
                (descriptor_text(allowed.descriptor()),)), (("explicit-root-ok",),))
            self.assert_counts(self.first, [content])
        finally:
            self.cursor.execute("SET GLOBAL secure_file_priv=%s", (original,))

    def test_media_dynamic_global_root_reaches_existing_sessions_without_changing_session_limits(self):
        self.prepare_media()
        old_asset = self.media.asset("png", 701)
        new_root = self.media.base / "media-next"
        new_root.mkdir()
        source = self.media.asset("jpeg", 702)
        new_path = new_root / "new-root-image.jpeg"
        new_path.write_bytes(source.raw)
        new_asset = MediaAsset(new_path, source.raw, source.mime_type)
        successful = []

        def query(cursor, asset, instruction, *, allowed):
            before = len(self.first.requests)
            statement = "SELECT AI_MAP('file_media_a', (%s, TO_FILE(%s)))"
            arguments = (instruction, descriptor_text(asset.descriptor()))
            if allowed:
                content = [text_block(instruction), asset.block()]
                successful.append(content)
                self.seed(self.first, content, f"result-{instruction}", assets=[asset])
                cursor.execute(statement, arguments)
                self.assertEqual(cursor.fetchall(), ((f"result-{instruction}",),))
            else:
                with self.assertRaises(pymysql.MySQLError) as caught:
                    cursor.execute(statement, arguments)
                self.assertNotIn(caught.exception.args[0], (1317, 4012),
                                 "root policy failed only through cancellation/timeout")
                self.assertEqual(len(self.first.requests), before, "denied root still submitted HTTP")
                cursor.execute("SELECT 1")
                self.assertEqual(cursor.fetchall(), ((1,),))

        with self.database.connect() as peer, self.database.connect() as admin:
            with peer.cursor() as peer_cursor, admin.cursor() as admin_cursor:
                peer_cursor.execute("SET ob_query_timeout=30000000, ai_pipeline_slots=2")
                sessions = ((self.connection, self.cursor), (peer, peer_cursor))
                thread_ids = [connection.thread_id() for connection, _cursor in sessions]
                self.assertEqual(len(set(thread_ids)), 2)
                self.assertNotIn(admin.thread_id(), thread_ids,
                                 "policy setter reused one of the existing query sessions")
                snapshots = []
                for index, (_connection, cursor) in enumerate(sessions):
                    cursor.execute("SELECT @@ai_media_max_bytes, @@ai_media_parallel")
                    snapshots.append(cursor.fetchone())
                    query(cursor, old_asset, f"old-root-session-{index}", allowed=True)
                admin_cursor.execute("SELECT @@GLOBAL.ai_media_max_bytes, @@GLOBAL.ai_media_parallel")
                original_limits = admin_cursor.fetchone()
                try:
                    admin_cursor.execute("SET GLOBAL ai_media_root=%s", (str(new_root),))
                    admin_cursor.execute("SET GLOBAL ai_media_max_bytes=1048576")
                    admin_cursor.execute("SET GLOBAL ai_media_parallel=3")
                    admin_cursor.execute(
                        "SELECT @@GLOBAL.ai_media_root, @@GLOBAL.ai_media_max_bytes, @@GLOBAL.ai_media_parallel")
                    self.assertEqual(admin_cursor.fetchone(), (str(new_root), 1048576, 3))
                    for index, (connection, cursor) in enumerate(sessions):
                        self.assertEqual(connection.thread_id(), thread_ids[index])
                        query(cursor, old_asset, f"old-root-denied-{index}", allowed=False)
                        query(cursor, new_asset, f"new-root-session-{index}", allowed=True)
                        cursor.execute("SELECT @@ai_media_max_bytes, @@ai_media_parallel")
                        self.assertEqual(cursor.fetchone(), snapshots[index],
                                         "ordinary session limits became dynamic global policy")
                        for statement in ("SET SESSION ai_media_root='/'", "SET ai_media_root='/'"):
                            sql_error(self, 1229, lambda statement=statement, cursor=cursor:
                                      cursor.execute(statement))
                    admin_cursor.execute("SET GLOBAL ai_media_root=''")
                    for index, (connection, cursor) in enumerate(sessions):
                        self.assertEqual(connection.thread_id(), thread_ids[index])
                        query(cursor, new_asset, f"empty-root-new-{index}", allowed=False)
                        query(cursor, old_asset, f"empty-root-old-{index}", allowed=False)
                    admin_cursor.execute("SET GLOBAL ai_media_root=%s", (str(self.media.root),))
                    for index, (connection, cursor) in enumerate(sessions):
                        self.assertEqual(connection.thread_id(), thread_ids[index])
                        query(cursor, old_asset, f"restored-root-session-{index}", allowed=True)
                        query(cursor, new_asset, f"restored-root-denied-{index}", allowed=False)
                        cursor.execute("SELECT @@ai_media_max_bytes, @@ai_media_parallel")
                        self.assertEqual(cursor.fetchone(), snapshots[index])
                finally:
                    admin_cursor.execute("SET GLOBAL ai_media_root=%s", (str(self.media.root),))
                    admin_cursor.execute("SET GLOBAL ai_media_max_bytes=%s", (original_limits[0],))
                    admin_cursor.execute("SET GLOBAL ai_media_parallel=%s", (original_limits[1],))
        self.assertEqual(self.connection.thread_id(), self.connection_id)
        self.assert_counts(self.first, successful)
        self.assertEqual(len(successful), 6)
        self.assertFalse(self.second.requests)

    def test_media_registration_rejects_invalid_modalities_without_catalog_or_history_rows(self):
        self.prepare_media()
        self.assert_catalog_modalities({
            "file_media_text": 1, "file_media_image": 3, "file_media_audio": 5,
            "file_media_a": 7, "file_media_b": 7,
        })
        invalid = [
            None, True, False, 0, 1, "", "text", {}, [],
            ["image"], ["audio"], ["image", "audio"],
            ["TEXT"], ["text", "Image"], ["text", "audio", "IMAGE"],
            ["text", "text"], ["text", "image", "image"], ["text", "audio", "audio"],
            ["text", "image", "audio", "text"],
            ["text", "video"], ["text", ""], ["text", None], ["text", True], ["text", False],
            ["text", 0], ["text", 1], ["text", {}], ["text", ["image"]],
        ]
        for index, value in enumerate(invalid):
            name = f"file_invalid_modalities_{index:02d}"
            config = {"type": "completion", "model_name": "mock-invalid",
                      "input_modalities": value}
            with self.subTest(value=value):
                self.reject("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL(%s, %s)",
                            (name, descriptor_text(config)), code=11116)
                self.assertEqual(self.execute(
                    "SELECT COUNT(*) FROM oceanbase.__all_ai_model WHERE name=%s", (name,)), ((0,),))
                self.assertEqual(self.execute(
                    "SELECT COUNT(*) FROM oceanbase.__all_ai_model_history WHERE name=%s", (name,)),
                    ((0,),))

    def test_media_registration_completion_masks_preserve_default_explicit_and_reordered_arrays(self):
        self.prepare_media()
        cases = (
            ("default", None, 1), ("text", ["text"], 1),
            ("image", ["text", "image"], 3), ("audio", ["text", "audio"], 5),
            ("all", ["text", "image", "audio"], 7),
            ("image_first", ["image", "text"], 3),
            ("audio_first", ["audio", "text"], 5),
            ("reordered", ["audio", "text", "image"], 7),
        )
        expected = {}
        for suffix, modalities, mask in cases:
            name = f"file_registered_cap_{suffix}"
            config = {"type": "completion", "model_name": "mock-registered-cap"}
            if modalities is not None:
                config["input_modalities"] = modalities
            self.cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL(%s, %s)",
                                (name, descriptor_text(config)))
            expected[name] = mask
        self.assert_catalog_modalities(expected)
        self.restart()
        self.assert_catalog_modalities(expected)
        self.assertFalse(self.first.requests)
        self.assertFalse(self.second.requests)

    def test_media_registration_noncompletion_models_are_text_only(self):
        self.prepare_media()
        expected = {}
        for suffix, modalities in (("default", None), ("explicit", ["text"])):
            name = f"file_embedding_cap_{suffix}"
            config = {"type": "dense_embedding", "model_name": "mock-embedding"}
            if modalities is not None:
                config["input_modalities"] = modalities
            self.cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL(%s, %s)",
                                (name, descriptor_text(config)))
            expected[name] = 1
        self.assert_catalog_modalities(expected)
        for index, modalities in enumerate(
                (["text", "image"], ["text", "audio"], ["text", "image", "audio"])):
            name = f"file_embedding_bad_cap_{index}"
            config = {"type": "dense_embedding", "model_name": "mock-embedding",
                      "input_modalities": modalities}
            with self.subTest(modalities=modalities):
                self.reject("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL(%s, %s)",
                            (name, descriptor_text(config)), code=11116)
                self.assertEqual(self.execute(
                    "SELECT COUNT(*) FROM oceanbase.__all_ai_model WHERE name=%s", (name,)), ((0,),))
                self.assertEqual(self.execute(
                    "SELECT COUNT(*) FROM oceanbase.__all_ai_model_history WHERE name=%s", (name,)),
                    ((0,),))
        self.assert_catalog_modalities(expected)

    def test_media_direct_file_map_and_filter_encode_all_four_formats(self):
        self.prepare_media()
        assets, _others = self.load_media(4)
        contents = [[asset.block()] for asset in assets]
        for index, (content, asset) in enumerate(zip(contents, assets)):
            self.seed(self.first, content, f"direct-{index}", assets=[asset])
            self.seed(self.second, content, index % 2 == 0, assets=[asset], boolean=True)
        rows = self.select_media(
            "AI_MAP('file_media_a', asset), AI_FILTER('file_media_b', asset)")
        self.assertEqual(rows, tuple((index, f"direct-{index}", int(index % 2 == 0))
                                     for index in range(len(assets))))
        for row in rows:
            self.assertIs(type(row[2]), int, "FILE filter returned JSON/text rather than SQL Boolean")
        self.assert_counts(self.first, contents)
        self.assert_counts(self.second, contents)
        self.assert_plan(self.media_statement("AI_MAP('file_media_a', asset)"), ("SEMANTIC MAP",))

    def test_media_tuple_keeps_instruction_text_and_multiple_file_blocks_in_order(self):
        self.prepare_media()
        assets, others = self.load_media(4, prefix="ordered")
        literal = 'ordinary text: "quoted" \\ slash\n{"not":"a FILE"}'
        contents = []
        for index, (asset, other) in enumerate(zip(assets, others)):
            content = [text_block(f"ordered-{index}"), asset.block(),
                       text_block(literal), other.block()]
            contents.append(content)
            self.seed(self.first, content, f"ordered-result-{index}", assets=[asset, other])
        rows = self.select_media(
            "asset, other_asset, AI_MAP('file_media_a', (text_value, asset, %s, other_asset))",
            parameters=(literal,))
        self.assertEqual(len(rows), len(assets))
        for index, row in enumerate(rows):
            self.assertEqual((row[0], row[3]), (index, f"ordered-result-{index}"))
            assert_file_value(self, row[1], assets[index].descriptor(), self.cursor.description[1])
            assert_file_value(self, row[2], others[index].descriptor(), self.cursor.description[2])
        self.assert_counts(self.first, contents)
        self.assertFalse(self.second.requests)

    def test_media_case_skips_null_file_and_null_instruction_without_http(self):
        self.prepare_media(root="")
        self.load_nullable_inputs()
        guard = "text_value IS NOT NULL AND asset IS NOT NULL"
        rows = self.select_media(
            f"CASE WHEN {guard} THEN AI_MAP('file_media_a', (text_value, asset)) ELSE 'skipped' END, "
            f"CASE WHEN {guard} THEN AI_FILTER('file_media_b', (text_value, asset)) ELSE 0 END")
        self.assertEqual(rows, ((0, "skipped", 0), (1, "skipped", 0), (2, "skipped", 0)))
        self.assertFalse(self.first.requests)
        self.assertFalse(self.second.requests)
        self.assert_no_model_sockets()
        self.assertEqual(self.execute("SELECT @@GLOBAL.ai_media_root"), (("",),))

    def test_media_and_or_skip_null_tuple_parts_in_projection_and_where_without_http(self):
        self.prepare_media(root="")
        self.load_nullable_inputs()
        call = "AI_FILTER('file_media_a', (text_value, asset))"
        guarded_and = f"text_value IS NOT NULL AND asset IS NOT NULL AND {call}"
        guarded_or = f"text_value IS NULL OR asset IS NULL OR {call}"
        self.assertEqual(self.select_media(f"({guarded_and}), ({guarded_or})"),
                         ((0, 0, 1), (1, 0, 1), (2, 0, 1)))
        self.assertEqual(self.execute(
            f"SELECT id FROM file_values WHERE {guarded_and} ORDER BY id"), ())
        self.assertEqual(self.execute(
            f"SELECT id FROM file_values WHERE {guarded_or} ORDER BY id"), ((0,), (1,), (2,)))
        self.assertFalse(self.first.requests)
        self.assertFalse(self.second.requests)
        self.assert_no_model_sockets()

    def test_media_image_not_null_guard_skips_nulls_and_keeps_filter_rows_aligned(self):
        self.prepare_media()
        image = self.media.asset("png", 501)
        rejected = self.media.asset("jpeg", 502)
        self.insert_values([
            (0, 0, None, None, None), (1, 0, None, image.descriptor(), None),
            (2, 0, None, None, None), (3, 0, None, rejected.descriptor(), None),
            (4, 0, None, image.descriptor(), None),
        ])
        accepted_content = [text_block("question"), image.block()]
        rejected_content = [text_block("question"), rejected.block()]
        self.seed(self.first, accepted_content, True, assets=[image], boolean=True)
        self.seed(self.first, rejected_content, False, assets=[rejected], boolean=True)
        statement = (
            f"SELECT /*+ OPT_PARAM('rowsets_max_rows', {BATCH_ROWS}) */ id, image "
            "FROM (SELECT id, asset AS image FROM file_values) inputs "
            "WHERE image IS NOT NULL AND AI_FILTER('file_media_a', ('question', image)) ORDER BY id")
        self.assert_plan(statement, ("SEMANTIC FILTER",))
        rows = self.execute(statement)
        self.assertEqual([row[0] for row in rows], [1, 4])
        for row in rows:
            assert_file_value(self, row[1], image.descriptor(), self.cursor.description[1])
        self.assert_counts(self.first, [accepted_content, rejected_content, accepted_content])
        self.assertFalse(self.second.requests)

    def test_media_demanded_null_parts_fail_before_missing_file_or_http_and_recover(self):
        self.prepare_media()
        missing = descriptor_text(descriptor(self.media.root / "null-demand-missing.png",
                                            mime_type="image/png"))
        cases = (
            ("('question', TO_FILE(NULL))", None),
            ("(CAST(NULL AS CHAR), TO_FILE(%s))", (missing,)),
            ("('question', TO_FILE(%s), CAST(NULL AS CHAR))", (missing,)),
            ("(NULL, TO_FILE(NULL))", None),
            ("TO_FILE(NULL)", None),
        )
        asset = self.media.asset("png", 503)
        recovered = []
        for function in ("AI_MAP", "AI_FILTER"):
            for index, (expression, parameters) in enumerate(cases):
                with self.subTest(function=function, expression=expression):
                    self.reject_media(
                        f"SELECT {function}('file_media_a', {expression})", parameters, code=1210)
                    self.assertEqual(self.connection.thread_id(), self.connection_id)
                    instruction = f"null-recovery-{function}-{index}"
                    content = [text_block(instruction), asset.block()]
                    recovered.append(content)
                    boolean = function == "AI_FILTER"
                    result = index % 2 == 0 if boolean else f"recovered-{index}"
                    self.seed(self.first, content, result, assets=[asset], boolean=boolean)
                    rows = self.execute(
                        f"SELECT {function}('file_media_a', (%s, TO_FILE(%s)))",
                        (instruction, descriptor_text(asset.descriptor())))
                    self.assertEqual(rows, ((int(result) if boolean else result,),))
        self.assert_counts(self.first, recovered)
        self.assertFalse(self.second.requests)

    def test_media_optional_mime_is_detected_from_actual_png_jpeg_wav_mp3_bytes(self):
        self.prepare_media()
        assets = [self.media.asset(kind, index + 100)
                  for index, kind in enumerate(("png", "jpeg", "wav", "mp3"))]
        for index, asset in enumerate(assets):
            content = [text_block(f"detected-{index}"), asset.block()]
            self.seed(self.first, content, f"detected-result-{index}", assets=[asset])
            value = asset.descriptor(include_mime=False, include_size=True)
            self.assertEqual(self.execute(
                "SELECT AI_MAP('file_media_a', (%s, TO_FILE(%s)))",
                (f"detected-{index}", descriptor_text(value))),
                ((f"detected-result-{index}",),))
        self.assertEqual(len(self.first.requests), len(assets))

    def test_media_filter_false_projection_and_where_keep_native_file_rows_aligned(self):
        self.prepare_media()
        assets, _others = self.load_media(7, prefix="filter")
        contents = []
        for index, asset in enumerate(assets):
            content = [text_block(f"filter-{index}"), asset.block()]
            contents.append(content)
            self.seed(self.first, content, index % 3 != 1, assets=[asset], boolean=True)
        expression = "AI_FILTER('file_media_a', (text_value, asset))"
        self.assertEqual(self.select_media(expression),
                         tuple((index, int(index % 3 != 1)) for index in range(len(assets))))
        statement = self.media_statement("asset", suffix=f"WHERE {expression} ORDER BY id")
        self.assert_plan(statement, ("SEMANTIC FILTER",))
        rows = self.execute(statement)
        accepted = [index for index in range(len(assets)) if index % 3 != 1]
        self.assertEqual([row[0] for row in rows], accepted)
        for row in rows:
            assert_file_value(self, row[1], assets[row[0]].descriptor(), self.cursor.description[1])
        self.assert_counts(self.first, contents * 2)

    def test_media_join_file_text_preserves_roles_candidates_and_plain_path_text(self):
        self.prepare_media()
        left = [self.media.asset("png", 11), self.media.asset("jpeg", 12)]
        right_texts = ["caption-10", str(self.media.outside / "ordinary-path.wav"),
                       (self.media.outside / "ordinary-uri.jpeg").as_uri()]
        self.insert_values([(index, index, f"left-{index}", asset.descriptor(), None)
                            for index, asset in enumerate(left)])
        right_descriptors = [descriptor(self.media.outside / f"not-loaded-right-{index}.wav")
                             for index in range(len(right_texts))]
        self.cursor.executemany(
            "INSERT INTO file_right VALUES (%s, %s, TO_FILE(%s))",
            [(10 + index, text, descriptor_text(value))
             for index, (text, value) in enumerate(zip(right_texts, right_descriptors))])
        contents = []
        for left_id, asset in enumerate(left):
            for right_index, text in enumerate(right_texts):
                content = join_content(JOIN_RULE, asset, text)
                contents.append(content)
                self.seed(self.first, content, (left_id + right_index) % 2 == 0,
                          assets=[asset], boolean=True)
        statement = (
            f"SELECT /*+ OPT_PARAM('rowsets_max_rows', {BATCH_ROWS}) */ "
            "l.id, r.id, l.asset, r.asset FROM file_values l INNER JOIN file_right r "
            "ON AI_JOIN('file_media_a', %s, l.asset, r.text_value) ORDER BY l.id, r.id")
        plan = "\n".join(map(str, self.execute("EXPLAIN " + statement, (JOIN_RULE,)))).upper()
        self.assertIn("SEMANTIC JOIN", plan)
        rows = self.execute(statement, (JOIN_RULE,))
        expected = [(left_id, 10 + right_index)
                    for left_id in range(len(left)) for right_index in range(len(right_texts))
                    if (left_id + right_index) % 2 == 0]
        self.assertEqual([row[:2] for row in rows], expected)
        for row in rows:
            assert_file_value(self, row[2], left[row[0]].descriptor(), self.cursor.description[2])
            assert_file_value(self, row[3], right_descriptors[row[1] - 10], self.cursor.description[3])
        self.assert_counts(self.first, contents)
        self.assertFalse(self.second.requests)

    def test_media_join_file_file_keeps_image_left_and_audio_right_blocks(self):
        self.prepare_media()
        left = [self.media.asset("png", 21), self.media.asset("jpeg", 22)]
        right = [self.media.asset("wav", 31), self.media.asset("mp3", 32)]
        self.insert_values([(index, 0, f"left-{index}", asset.descriptor(), None)
                            for index, asset in enumerate(left)])
        self.cursor.executemany(
            "INSERT INTO file_right VALUES (%s, %s, TO_FILE(%s))",
            [(10 + index, f"right-{index}", descriptor_text(asset.descriptor()))
             for index, asset in enumerate(right)])
        contents = []
        for left_id, left_asset in enumerate(left):
            for right_index, right_asset in enumerate(right):
                content = join_content(JOIN_RULE, left_asset, right_asset)
                contents.append(content)
                self.seed(self.first, content, left_id == right_index,
                          assets=[left_asset, right_asset], boolean=True)
        rows = self.execute(
            "SELECT l.id, r.id, l.asset, r.asset FROM file_values l INNER JOIN file_right r "
            "ON AI_JOIN('file_media_a', %s, l.asset, r.asset) ORDER BY l.id, r.id", (JOIN_RULE,))
        self.assertEqual([row[:2] for row in rows], [(0, 10), (1, 11)])
        for row in rows:
            assert_file_value(self, row[2], left[row[0]].descriptor(), self.cursor.description[2])
            assert_file_value(self, row[3], right[row[1] - 10].descriptor(), self.cursor.description[3])
        self.assert_counts(self.first, contents)
        for body in self.first.requests:
            self.assertEqual(body["response_format"]["json_schema"]["name"], "ai_join")

    def test_media_disabled_root_denies_demand_not_to_file_and_recovers_on_same_connection(self):
        self.prepare_media(root="")
        asset = self.media.asset("png", 41)
        value = descriptor_text(asset.descriptor())
        row = self.execute("SELECT TO_FILE(%s), FILE_URI(TO_FILE(%s))", (value, value))[0]
        assert_file_value(self, row[0], asset.descriptor(), self.cursor.description[0])
        self.assertEqual(row[1], asset.path.as_uri())
        denied = self.reject_media("SELECT AI_MAP('file_media_a', TO_FILE(%s))", (value,))
        option_denied = self.reject_media(
            "SELECT AI_MAP('file_media_a', TO_FILE(%s), %s)",
            (value, descriptor_text({"ai_media_root": str(self.media.root)})))
        self.assertEqual(option_denied.args, denied.args, "call options bypassed the GLOBAL root policy")
        self.cursor.execute("SET GLOBAL ai_media_root=%s", (str(self.media.root),))
        content = [asset.block()]
        self.seed(self.first, content, "root-enabled", assets=[asset])
        self.assertEqual(self.execute("SELECT AI_MAP('file_media_a', TO_FILE(%s))", (value,)),
                         (("root-enabled",),))
        self.assertEqual(self.connection.thread_id(), self.connection_id)

    def test_media_root_rejects_prefix_collision_and_symlink_escape_without_http(self):
        self.prepare_media()
        outside = self.media.asset("png", 51, outside=True)
        symlink = self.media.root / "escape.png"
        symlink.symlink_to(outside.path)
        linked_directory = self.media.root / "escape-directory"
        linked_directory.symlink_to(self.media.outside, target_is_directory=True)
        for path in (outside.path, symlink, linked_directory / outside.path.name):
            with self.subTest(path=path):
                value = descriptor_text(descriptor(path, mime_type="image/png"))
                assert_file_value(self, self.execute("SELECT TO_FILE(%s)", (value,))[0][0],
                                  descriptor(path, mime_type="image/png"), self.cursor.description[0])
                self.reject_media("SELECT AI_MAP('file_media_a', ('must-not-escape', TO_FILE(%s)))",
                                  (value,))
        self.assertFalse(self.first.requests)

    def test_media_missing_and_truncated_files_fail_at_demand_without_http(self):
        self.prepare_media()
        missing = descriptor(self.media.root / "missing.png", mime_type="image/png")
        assert_file_value(self, self.execute("SELECT TO_FILE(%s)", (descriptor_text(missing),))[0][0],
                          missing, self.cursor.description[0])
        self.reject_media("SELECT AI_MAP('file_media_a', TO_FILE(%s))", (descriptor_text(missing),))
        for kind in ("png", "jpeg", "wav", "mp3"):
            asset = self.media.corrupt(kind)
            with self.subTest(kind=kind):
                self.reject_media("SELECT AI_MAP('file_media_a', ('corrupt', TO_FILE(%s)))",
                                  (descriptor_text(asset.descriptor()),))
        garbage = self.media.root / "invalid-signature.png"
        garbage.write_bytes(b"not an image or audio container")
        self.reject_media(
            "SELECT AI_FILTER('file_media_a', ('invalid-signature', TO_FILE(%s)))",
            (descriptor_text(descriptor(garbage, mime_type="image/png")),))

    def test_media_raw_byte_limit_accepts_exact_size_and_rejects_one_byte_over(self):
        self.prepare_media()
        asset = self.media.asset("png", 61)
        content = [text_block("exact-size"), asset.block()]
        self.seed(self.first, content, "exact-size-ok", assets=[asset])
        self.cursor.execute("SET ai_media_max_bytes=%s", (len(asset.raw),))
        self.assertGreater(len(base64.b64encode(asset.raw)), len(asset.raw))
        self.assertEqual(self.execute(
            "SELECT AI_MAP('file_media_a', ('exact-size', TO_FILE(%s)))",
            (descriptor_text(asset.descriptor(include_size=True)),)), (("exact-size-ok",),))
        self.cursor.execute("SET ai_media_max_bytes=%s", (len(asset.raw) - 1,))
        for value in (asset.descriptor(), {**asset.descriptor(), "size_bytes": 0}):
            with self.subTest(value=value):
                self.reject_media("SELECT AI_MAP('file_media_a', ('oversize', TO_FILE(%s)))",
                                  (descriptor_text(value),))
        self.assert_counts(self.first, [content])

    def test_media_text_only_capability_rejects_before_missing_file_or_http(self):
        self.prepare_media()
        asset = self.media.asset("png", 71)
        values = [asset.descriptor(), descriptor(self.media.root / "missing-capability.png",
                                                mime_type="image/png")]
        errors = []
        for value in values:
            errors.append(self.reject_media(
                "SELECT AI_MAP('file_media_text', TO_FILE(%s))",
                (descriptor_text(value),), code=1235))
        for function in ("AI_MAP", "AI_FILTER"):
            errors.append(self.reject_media(
                f"SELECT {function}('file_media_text', TO_FILE(%s), %s)",
                (descriptor_text(values[0]),
                 descriptor_text({"input_modalities": ["text", "image", "audio"]})), code=1235))
        for error in errors:
            self.assertIn("text-only", str(error).lower(),
                          "missing-file/root failure masked the model capability gate")
        self.assertTrue(all(error.args == errors[0].args for error in errors[1:]),
                         "text-only rejection depended on filesystem availability")
        self.assertFalse(self.first.requests)

    def test_media_image_only_capability_accepts_images_but_denies_audio_before_http(self):
        self.prepare_media()
        image = self.media.asset("jpeg", 81)
        content = [image.block()]
        self.seed(self.first, content, "image-capability-ok", assets=[image])
        self.assertEqual(self.execute(
            "SELECT AI_MAP('file_media_image', TO_FILE(%s))",
            (descriptor_text(image.descriptor()),)), (("image-capability-ok",),))
        for kind in ("wav", "mp3"):
            audio = self.media.asset(kind, 82)
            error = self.reject_media(
                "SELECT AI_MAP('file_media_image', TO_FILE(%s))",
                (descriptor_text(audio.descriptor()),), code=1235)
            self.assertIn("modality", str(error).lower())
        self.assert_counts(self.first, [content])

    def test_media_audio_only_capability_accepts_audio_but_denies_images_before_http(self):
        self.prepare_media()
        contents = []
        for index, kind in enumerate(("wav", "mp3")):
            audio = self.media.asset(kind, 83 + index)
            content = [audio.block()]
            contents.append(content)
            self.seed(self.first, content, f"audio-capability-{index}", assets=[audio])
            self.assertEqual(self.execute(
                "SELECT AI_MAP('file_media_audio', TO_FILE(%s))",
                (descriptor_text(audio.descriptor()),)), ((f"audio-capability-{index}",),))
        for kind in ("png", "jpeg"):
            image = self.media.asset(kind, 85)
            error = self.reject_media(
                "SELECT AI_MAP('file_media_audio', TO_FILE(%s))",
                (descriptor_text(image.descriptor()),), code=1235)
            self.assertIn("modality", str(error).lower())
        self.assert_counts(self.first, contents)

    def test_media_non_openai_provider_is_rejected_before_loading_missing_file(self):
        self.prepare_media()
        self.cursor.execute("CALL DBMS_AI_SERVICE.ALTER_AI_MODEL_ENDPOINT(%s, %s)",
                            ("file_media_a_endpoint", descriptor_text({"provider": "aliyun-dashscope"})))
        try:
            missing = descriptor(self.media.root / "provider-missing.png", mime_type="image/png")
            error = self.reject_media(
                "SELECT AI_MAP('file_media_a', ('provider-gate', TO_FILE(%s)))",
                (descriptor_text(missing),), code=1235)
            self.assertIn("openai", str(error).lower())
        finally:
            self.cursor.execute("CALL DBMS_AI_SERVICE.ALTER_AI_MODEL_ENDPOINT(%s, %s)",
                                ("file_media_a_endpoint", descriptor_text({"provider": "openai"})))

    def test_media_path_uri_and_descriptor_strings_remain_plain_text_with_root_disabled(self):
        self.prepare_media(root="")
        path = self.media.outside / "plain-text-not-loaded.png"
        prompts = [str(path), path.as_uri(), descriptor_text(descriptor(path, mime_type="image/png"))]
        for index, prompt in enumerate(prompts):
            if index == 1:
                self.seed(self.first, prompt, False, boolean=True)
                rows = self.execute("SELECT AI_FILTER('file_media_text', %s)", (prompt,))
                self.assertEqual(rows, ((0,),))
            else:
                self.seed(self.first, prompt, f"plain-text-{index}")
                self.assertEqual(self.execute("SELECT AI_MAP('file_media_text', %s)", (prompt,)),
                                 ((f"plain-text-{index}",),))
        self.assert_counts(self.first, prompts)
        self.assertTrue(all(isinstance(body["messages"][-1]["content"], str)
                            for body in self.first.requests))
        self.assertEqual(self.execute("SELECT @@GLOBAL.ai_media_root"), (("",),))

    def test_media_options_pass_through_but_cannot_replace_model_or_messages(self):
        self.prepare_media()
        asset = self.media.asset("png", 91)
        value = descriptor_text(asset.descriptor())
        for config in ({"model": "not-the-registered-model"},
                       {"messages": [{"role": "user", "content": "discard the FILE"}]}):
            with self.subTest(config=config):
                self.reject_media("SELECT AI_MAP('file_media_a', TO_FILE(%s), %s)",
                                  (value, descriptor_text(config)), code=1210)
        content = [text_block("options"), asset.block()]
        self.seed(self.first, content, "options-ok", assets=[asset])
        options = {"temperature": 0.25, "max_tokens": 64}
        self.assertEqual(self.execute(
            "SELECT AI_MAP('file_media_a', ('options', TO_FILE(%s)), %s)",
            (value, descriptor_text(options))), (("options-ok",),))
        self.assertEqual({name: self.first.requests[0][name] for name in options}, options)

    def test_media_filter_enforces_boolean_schema_and_finish_reason_stop(self):
        self.prepare_media()
        asset = self.media.asset("png", 101)
        cases = [
            (completion('{"value":"false"}', finish_reason="stop"), 4070),
            (completion('{"value":null}', finish_reason="stop"), 4070),
            (completion('{"value":true,"extra":1}', finish_reason="stop"), 4070),
            (completion("true", finish_reason="stop"), 4070),
            (completion('```json\n{"value":true}\n```', finish_reason="stop"), (3140, 5447)),
            (completion('{"value":true}', finish_reason="length"), 4070),
            (completion('{"value":true}'), 4070),
        ]
        contents = []
        for index, (raw, error_code) in enumerate(cases):
            instruction = f"strict-media-{index}"
            content = [text_block(instruction), asset.block()]
            contents.append(content)
            reply = self.seed(self.first, content, True, assets=[asset], boolean=True)
            reply.body = raw
            sql_error(self, error_code, lambda instruction=instruction: self.execute(
                "SELECT AI_FILTER('file_media_a', (%s, TO_FILE(%s)))",
                (instruction, descriptor_text(asset.descriptor()))))
            self.assert_no_model_sockets()
            self.assertEqual(self.execute("SELECT 1"), ((1,),))
        self.assert_counts(self.first, contents)

    def test_media_filter_preserves_matching_caller_schema_and_rejects_conflicts_before_http(self):
        self.prepare_media()
        asset = self.media.asset("wav", 111)
        value = descriptor_text(asset.descriptor())
        for config in ({"Schema": semantic.FILTER_SCHEMA},
                       {"response_format": {"type": "json_object"}},
                       {"response_format": {"type": "json_schema", "json_schema": {
                           "name": "conflict", "strict": False, "schema": semantic.FILTER_SCHEMA}}}):
            with self.subTest(config=config):
                self.reject_media(
                    "SELECT AI_FILTER('file_media_a', ('caller-schema', TO_FILE(%s)), %s)",
                    (value, descriptor_text(config)), code=1210)
        content = [text_block("caller-schema"), asset.block()]
        response_format = {**semantic.MATCHING_FILTER_FORMAT, "json_schema": {
            **semantic.MATCHING_FILTER_FORMAT["json_schema"], "name": "file_caller_boolean"}}
        self.seed(self.first, content, False, assets=[asset], boolean=True,
                  caller_format=response_format)
        self.assertEqual(self.execute(
            "SELECT AI_FILTER('file_media_a', ('caller-schema', TO_FILE(%s)), %s)",
            (value, descriptor_text({"response_format": response_format, "temperature": 0.25}))),
            ((0,),))
        self.assertEqual(self.first.requests[0]["response_format"], response_format)
        self.assertEqual(self.first.requests[0]["temperature"], 0.25)

    def test_media_duplicate_file_refs_do_not_deduplicate_model_calls_or_rows(self):
        self.prepare_media()
        asset = self.media.asset("png", 121)
        self.insert_values([(index, 0, f"duplicate-file-{index}", asset.descriptor(), None)
                            for index in range(9)])
        contents = []
        for index in range(9):
            content = [text_block(f"duplicate-file-{index}"), asset.block()]
            contents.append(content)
            self.seed(self.first, content, f"independent-result-{index}", assets=[asset])
        self.assertEqual(self.select_media("AI_MAP('file_media_a', (text_value, asset))"),
                         tuple((index, f"independent-result-{index}") for index in range(9)))
        self.assert_counts(self.first, contents)

    def test_media_held_later_batch_can_finish_before_first_and_output_stays_aligned(self):
        self.prepare_media()
        assets, _others = self.load_media(prefix="held")
        first_gate, later_gate = self.fixtures.gate(), self.fixtures.gate()
        contents = []
        for index, asset in enumerate(assets):
            content = [text_block(f"held-{index}"), asset.block()]
            contents.append(content)
            self.seed(self.first, content, f"held-result-{index}", assets=[asset],
                      gate=first_gate if index < BATCH_ROWS else later_gate)
        pending = self.fixtures.start(lambda: self.select_media(
            "asset, AI_MAP('file_media_a', (text_value, asset))"))
        self.wait_two_batches(self.first, pending, [first_gate, later_gate])
        later_gate.set()
        later_keys = {content_key(content) for content in contents[BATCH_ROWS:]}
        semantic.wait_evidence(
            self, self.first,
            lambda: any(record["prompt"] in later_keys and record["response"] is not None
                        for record in self.first.audit),
            pending, message="later FILE results could not finish while the first batch was held")
        semantic.assert_held(self, self.first, [first_gate],
                             {content_key(content) for content in contents[:BATCH_ROWS]})
        self.assertFalse(pending.done.is_set())
        first_gate.set()
        rows = pending.result(timeout=10)
        self.assertEqual(len(rows), ROW_COUNT)
        for index, row in enumerate(rows):
            self.assertEqual((row[0], row[2]), (index, f"held-result-{index}"))
            assert_file_value(self, row[1], assets[index].descriptor())
        self.assert_counts(self.first, contents)

    def test_media_independent_maps_hold_both_services_and_keep_distinct_file_buffers(self):
        self.prepare_media()
        assets, others = self.load_media(prefix="independent")
        gates = [self.fixtures.gate(), self.fixtures.gate()]
        all_contents = [[], []]
        for index, (asset, other) in enumerate(zip(assets, others)):
            for position, (server, media_asset) in enumerate(((self.first, asset), (self.second, other))):
                content = [text_block(f"{'A' if position == 0 else 'B'}:{index}"), media_asset.block()]
                all_contents[position].append(content)
                self.seed(server, content, f"{'A' if position == 0 else 'B'}-result-{index}",
                          assets=[media_asset], gate=gates[position])
        expression = (
            "AI_MAP('file_media_a', (CONCAT('A:', id), asset)), "
            "AI_MAP('file_media_b', (CONCAT('B:', id), other_asset))")
        pending = self.fixtures.start(lambda: self.select_media(expression))
        for server, gate in zip(self.fixtures.mocks, gates):
            self.wait_two_batches(server, pending, [gate])
        gates[1].set()
        semantic.wait_evidence(
            self, self.second, lambda: len(self.second.finished) >= 2 * BATCH_ROWS, pending,
            message="independent B media responses were not released before A")
        semantic.assert_held(self, self.first, [gates[0]])
        self.assertFalse(pending.done.is_set())
        gates[0].set()
        self.assertEqual(pending.result(timeout=10),
                         tuple((index, f"A-result-{index}", f"B-result-{index}")
                               for index in range(ROW_COUNT)))
        for server, contents in zip(self.fixtures.mocks, all_contents):
            self.assert_counts(server, contents)

    def test_media_nested_maps_wait_for_own_row_and_overlap_outer_with_later_inner_batch(self):
        self.prepare_media()
        assets, others = self.load_media(prefix="nested-media")
        first_gate, later_gate, outer_gate = (self.fixtures.gate() for _ in range(3))
        inner_contents, outer_contents = [], []
        for index, (asset, other) in enumerate(zip(assets, others)):
            inner = [text_block(f"inner:{index}"), asset.block()]
            outer = [text_block(f"outer:{index}"), other.block(), text_block(f"mapped-{index}")]
            inner_contents.append(inner)
            outer_contents.append(outer)
            self.seed(self.first, inner, f"mapped-{index}", assets=[asset],
                      gate=first_gate if index < BATCH_ROWS else later_gate)
            self.seed(self.second, outer, f"outer-result-{index}", assets=[other], gate=outer_gate)
        expression = (
            "AI_MAP('file_media_b', (CONCAT('outer:', id), other_asset, "
            "AI_MAP('file_media_a', (CONCAT('inner:', id), asset))))")
        pending = self.fixtures.start(lambda: self.select_media(expression))
        self.wait_two_batches(self.first, pending, [first_gate, later_gate])
        self.assertFalse(self.second.requests, "outer FILE inputs were sent before their nested results")
        first_gate.set()
        first_outer = [content_key(content) for content in outer_contents[:BATCH_ROWS]]
        semantic.wait_evidence(
            self, self.second, lambda: all(self.second.counts[key] == 1 for key in first_outer),
            pending, message="outer media batch 1 did not overlap held inner media batch 2")
        semantic.assert_held(self, self.first, [later_gate],
                             {content_key(content) for content in inner_contents[BATCH_ROWS:]})
        semantic.assert_held(self, self.second, [outer_gate])
        self.assert_counts(self.second, outer_contents[:BATCH_ROWS])
        dependencies = {content_key(outer): content_key(inner)
                        for inner, outer in zip(inner_contents, outer_contents)}
        semantic.assert_dependencies(self, semantic.audit(self.first), semantic.audit(self.second), dependencies)
        later_gate.set()
        outer_gate.set()
        self.assertEqual(pending.result(timeout=10),
                         tuple((index, f"outer-result-{index}") for index in range(ROW_COUNT)))
        self.assert_counts(self.first, inner_contents)
        self.assert_counts(self.second, outer_contents)
        semantic.assert_dependencies(self, semantic.audit(self.first), semantic.audit(self.second), dependencies)

    def test_media_filter_prunes_missing_downstream_files_and_overlaps_next_batch(self):
        self.prepare_media()
        assets, others = self.load_media(prefix="pruned")
        first_gate, later_gate, map_gate = (self.fixtures.gate() for _ in range(3))
        filter_contents, map_contents = [], []
        accepted = [index for index in range(ROW_COUNT) if index % 3 != 1]
        for index, (asset, other) in enumerate(zip(assets, others)):
            filtered = [text_block(f"filter:{index}"), asset.block()]
            filter_contents.append(filtered)
            self.seed(self.first, filtered, index in accepted, assets=[asset], boolean=True,
                      gate=first_gate if index < BATCH_ROWS else later_gate)
            if index in accepted:
                mapped = [text_block(f"kept:{index}"), other.block()]
                map_contents.append(mapped)
                self.seed(self.second, mapped, f"kept-result-{index}", assets=[other], gate=map_gate)
            else:
                missing = descriptor(self.media.root / f"undemanded-{index}.png", mime_type="image/png")
                self.cursor.execute("UPDATE file_values SET other_asset=TO_FILE(%s) WHERE id=%s",
                                    (descriptor_text(missing), index))
        expression = "AI_MAP('file_media_b', (CONCAT('kept:', id), other_asset))"
        statement = self.media_statement(
            expression, suffix="WHERE AI_FILTER('file_media_a', (CONCAT('filter:', id), asset)) ORDER BY id")
        pending = self.fixtures.start(lambda: self.execute(statement))
        self.wait_two_batches(self.first, pending, [first_gate, later_gate])
        self.assertFalse(self.second.requests, "downstream FILE was demanded before the filter result")
        first_gate.set()
        first_map = [content for index, content in zip(accepted, map_contents) if index < BATCH_ROWS]
        semantic.wait_evidence(
            self, self.second,
            lambda: all(self.second.counts[content_key(content)] == 1 for content in first_map),
            pending, message="kept media batch 1 did not overlap held filter batch 2")
        semantic.assert_held(self, self.first, [later_gate],
                             {content_key(content) for content in filter_contents[BATCH_ROWS:]})
        semantic.assert_held(self, self.second, [map_gate])
        self.assert_counts(self.second, first_map)
        later_gate.set()
        map_gate.set()
        self.assertEqual(pending.result(timeout=10),
                         tuple((index, f"kept-result-{index}") for index in accepted))
        self.assert_counts(self.first, filter_contents)
        self.assert_counts(self.second, map_contents)

    def start_held_media(self, prefix):
        self.prepare_media()
        assets, _others = self.load_media(prefix=prefix)
        gate = self.fixtures.gate()
        contents = []
        for index, asset in enumerate(assets):
            content = [text_block(f"{prefix}-{index}"), asset.block()]
            contents.append(content)
            self.seed(self.first, content, f"{prefix}-result-{index}", assets=[asset], gate=gate)
        pending = self.fixtures.start(lambda: self.select_media(
            "AI_MAP('file_media_a', (text_value, asset))"))
        self.wait_two_batches(self.first, pending, [gate])
        return pending, gate, contents

    def test_media_cancel_held_cross_batch_closes_sockets_and_reuses_same_connection(self):
        pending, gate, _contents = self.start_held_media("cancel-media")
        admitted = [record["prompt"] for record in semantic.audit(self.first)]
        self.kill_query()
        pending.expect_sql_error(self, 1317, timeout=3)
        semantic.assert_held(self, self.first, [gate])
        self.assert_no_model_sockets()
        self.recover_while_held(gate)
        self.assertEqual(sum(self.first.counts.values()), len(admitted) + 1)

    def test_media_deadline_held_cross_batch_closes_sockets_and_recovers_without_old_replies(self):
        self.cursor.execute("SET ob_query_timeout=3000000")
        pending, gate, _contents = self.start_held_media("deadline-media")
        pending.expect_sql_error(self, 4012, timeout=5)
        semantic.assert_held(self, self.first, [gate])
        self.assert_no_model_sockets()
        self.cursor.execute("SET ob_query_timeout=30000000")
        self.recover_while_held(gate)

    def test_media_permanent_provider_error_cancels_held_peers_and_recovers(self):
        self.prepare_media()
        assets, _others = self.load_media(prefix="provider-error-media")
        gates = {}
        for index, asset in enumerate(assets):
            content = [text_block(f"provider-error-media-{index}"), asset.block()]
            gate = self.fixtures.gate()
            gates[content_key(content)] = gate
            self.seed(self.first, content, f"provider-error-result-{index}", assets=[asset], gate=gate)
        pending = self.fixtures.start(lambda: self.select_media(
            "AI_MAP('file_media_a', (text_value, asset))"))
        self.wait_two_batches(self.first, pending, list(gates.values()))
        admitted = semantic.audit(self.first)
        rejected = admitted[-1]["prompt"]
        reply = self.first.scenarios[rejected][0]
        reply.status = 400
        reply.body = b'{"error":"permanent media fixture failure"}'
        gates[rejected].set()
        pending.expect_sql_error(self, 4216, timeout=5)
        other_keys = {record["prompt"] for record in admitted} - {rejected}
        other_gates = [gates[key] for key in other_keys]
        semantic.assert_held(self, self.first, other_gates, other_keys)
        self.assert_no_model_sockets()
        self.assertEqual(self.first.counts[rejected], 1, "permanent media failure was retried")
        self.recover_while_held(other_gates[0])

    def test_media_nested_loop_rescan_preserves_file_roles_without_cached_semantic_results(self):
        self.prepare_media()
        left = [self.media.asset("png", 131), self.media.asset("jpeg", 132)]
        right = [self.media.asset("wav", 141), self.media.asset("mp3", 142)]
        self.insert_values([(index, 0, f"left-{index}", asset.descriptor(), None)
                            for index, asset in enumerate(left)])
        self.cursor.executemany(
            "INSERT INTO file_right VALUES (%s, %s, TO_FILE(%s))",
            [(10 + index, f"right-{index}", descriptor_text(asset.descriptor()))
             for index, asset in enumerate(right)])
        self.cursor.executemany("INSERT INTO file_drivers VALUES (%s)", [(3,), (7,), (9,)])
        contents = []
        for left_id, left_asset in enumerate(left):
            for right_index, right_asset in enumerate(right):
                content = join_content(JOIN_RULE, left_asset, right_asset)
                contents.append(content)
                self.seed(self.first, content, left_id == right_index,
                          assets=[left_asset, right_asset], boolean=True)
        inner = (
            "SELECT l.id AS left_id, r.id AS right_id, l.asset AS left_file, r.asset AS right_file "
            "FROM file_values l INNER JOIN file_right r "
            f"ON AI_JOIN('file_media_a', '{JOIN_RULE}', l.asset, r.asset)")
        statement = (
            f"SELECT /*+ OPT_PARAM('rowsets_max_rows', {BATCH_ROWS}) "
            "NO_MERGE(j) LEADING(d j) USE_NL(j) NO_USE_NL_MATERIALIZATION(j) */ "
            "d.id, j.left_id, j.right_id, j.left_file, j.right_file FROM file_drivers d "
            f"INNER JOIN ({inner}) j ON 1=1 ORDER BY d.id, j.left_id, j.right_id")
        plan = self.assert_plan(statement, ("NESTED-LOOP JOIN", "SEMANTIC JOIN"))
        assert_unmaterialized_join_rescan(self, plan)
        rows = self.execute(statement)
        self.assertEqual([row[:3] for row in rows],
                         [(driver, left_id, 10 + left_id)
                          for driver in (3, 7, 9) for left_id in range(2)])
        for row in rows:
            assert_file_value(self, row[3], left[row[1]].descriptor(), self.cursor.description[3])
            assert_file_value(self, row[4], right[row[2] - 10].descriptor(), self.cursor.description[4])
        self.assert_counts(self.first, contents * 3)
        self.assertEqual(len(self.first.requests), 12)

    def test_media_model_capability_and_file_values_survive_private_restart(self):
        self.prepare_media()
        assets, _others = self.load_media(2, kinds=("png", "wav"), prefix="persisted-media")
        masks = {
            "file_media_text": 1, "file_media_image": 3, "file_media_audio": 5,
            "file_media_a": 7, "file_media_b": 7,
        }
        self.assert_catalog_modalities(masks)
        self.restart()
        self.assert_catalog_modalities(masks)
        self.prepare_media()
        contents = []
        for index, asset in enumerate(assets):
            content = [text_block(f"persisted-media-{index}"), asset.block()]
            contents.append(content)
            self.seed(self.first, content, f"persisted-result-{index}", assets=[asset])
        self.schema()
        self.assertEqual(self.select_media("AI_MAP('file_media_a', (text_value, asset))"),
                         ((0, "persisted-result-0"), (1, "persisted-result-1")))
        missing = descriptor(self.media.root / "persisted-text-only.png", mime_type="image/png")
        error = self.reject_media("SELECT AI_MAP('file_media_text', TO_FILE(%s))",
                                  (descriptor_text(missing),), code=1235)
        self.assertIn("text-only", str(error).lower())
        self.reject_media("SELECT AI_MAP('file_media_image', TO_FILE(%s))",
                          (descriptor_text(assets[1].descriptor()),), code=1235)
        audio_content = [assets[1].block()]
        self.seed(self.first, audio_content, "persisted-audio-mask-five", assets=[assets[1]])
        self.assertEqual(self.execute(
            "SELECT AI_MAP('file_media_audio', TO_FILE(%s))",
            (descriptor_text(assets[1].descriptor()),)), (("persisted-audio-mask-five",),))
        self.reject_media("SELECT AI_MAP('file_media_audio', TO_FILE(%s))",
                          (descriptor_text(assets[0].descriptor()),), code=1235)
        self.assert_counts(self.first, contents + [audio_content])

    def test_media_actual_px_tasks_transport_file_values_through_exchange(self):
        self.check_media_px_transport()

    def test_media_sorted_projection_runs_on_actual_px_tasks(self):
        self.check_media_px_transport(ordered=True)

    def check_media_px_transport(self, *, ordered=False, on_held=None):
        self.prepare_media()
        self.cursor.execute("DROP TABLE IF EXISTS file_px")
        self.cursor.execute(
            "CREATE TABLE file_px (id INT PRIMARY KEY, sort_key INT, text_value LONGTEXT, asset FILE) "
            "PARTITION BY HASH(id) PARTITIONS 8")
        assets = [self.media.asset(("png", "jpeg", "wav", "mp3")[index % 4], 2000 + index)
                  for index in range(PX_ROWS)]
        self.cursor.executemany(
            "INSERT INTO file_px VALUES (%s, %s, %s, TO_FILE(%s))",
            [(index, index % 17, f"px-file-{index}", descriptor_text(asset.descriptor()))
             for index, asset in enumerate(assets)])
        gate = self.fixtures.gate()
        contents = []
        for index, asset in enumerate(assets):
            content = [text_block(f"px-file-{index}"), asset.block()]
            contents.append(content)
            self.seed(self.first, content, f"px-result-{index}", assets=[asset], gate=gate)
        statement = (
            f"SELECT /*+ PARALLEL(4) OPT_PARAM('rowsets_max_rows', {BATCH_ROWS}) */ "
            "id, asset, AI_MAP('file_media_a', (text_value, asset)) FROM file_px")
        if ordered:
            statement += " ORDER BY sort_key DESC, id"
        self.assert_plan(statement, ("PX COORDINATOR", "EXCHANGE", "SEMANTIC MAP", "DOP=4"))
        log_path = self.database.base / "log/seekdb.log"
        start = log_path.stat().st_size
        pending = self.fixtures.start(lambda: self.execute(statement))
        self.wait_two_batches(self.first, pending, [gate])
        records = []
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            with log_path.open(errors="replace") as log:
                log.seek(start)
                records = [tuple(map(int, match.groups())) for line in log
                           if (match := PREPARE.search(line))]
            if len({record[2] for record in records if record[2] >= 0}) == 4:
                break
            if pending.done.is_set():
                self.fail(f"PX FILE query finished unexpectedly: {pending.error!r}")
            threading.Event().wait(0.02)
        self.assertEqual(len({record[2] for record in records if record[2] >= 0}), 4,
                         f"four actual semantic PX tasks were not observed: {records}")
        self.assertEqual(len({record[0] for record in records}), 1,
                         "PX evidence mixed multiple SQL executions")
        self.assertGreaterEqual(len({record[3] for record in records}), 2,
                                "PX task IDs did not correspond to multiple CPU threads")
        if ordered:
            with self.database.connect() as control:
                with control.cursor() as cursor:
                    deadline = time.monotonic() + 5
                    while time.monotonic() < deadline:
                        cursor.execute("SELECT COUNT(*) FROM oceanbase.__all_virtual_px_worker_stat")
                        if cursor.fetchone() == (0,):
                            break
                        threading.Event().wait(0.01)
                    else:
                        self.fail("sorted FILE tasks retained CPU workers while replies were held")
        semantic.assert_held(self, self.first, [gate])
        if on_held is not None:
            on_held(pending, gate)
            return
        gate.set()
        rows = pending.result(timeout=20)
        if ordered:
            expected_order = sorted(range(PX_ROWS), key=lambda index: (-(index % 17), index))
            self.assertEqual([row[0] for row in rows], expected_order,
                             "SQL must retain the requested compound order")
        rows = sorted(rows, key=lambda row: row[0])
        self.assertEqual([row[0] for row in rows], list(range(PX_ROWS)))
        for index, row in enumerate(rows):
            assert_file_value(self, row[1], assets[index].descriptor())
            self.assertEqual(row[2], f"px-result-{index}")
        self.assert_counts(self.first, contents)
        self.assertFalse(self.second.requests)

    def test_media_sorted_limit_does_not_load_discarded_files(self):
        self.prepare_media()
        self.cursor.execute(
            "CREATE TABLE file_limited_px (id INT PRIMARY KEY, asset FILE) "
            "PARTITION BY HASH(id) PARTITIONS 8")
        retained = [8, 7, 6]
        assets = {index: self.media.asset("png", 2500 + index) for index in retained}
        self.cursor.executemany(
            "INSERT INTO file_limited_px VALUES (%s, TO_FILE(%s))",
            [(index, descriptor_text(assets[index].descriptor() if index in assets else
                                    descriptor(self.media.root / f"not-demanded-{index}.png")))
             for index in range(11)])
        contents = [[text_block("limit demand"), assets[index].block()] for index in retained]
        for index, content in zip(retained, contents):
            self.seed(self.first, content, f"limited-result-{index}", assets=[assets[index]])
        statement = (
            f"SELECT /*+ PARALLEL(4) OPT_PARAM('rowsets_max_rows', {BATCH_ROWS}) */ "
            "id, AI_MAP('file_media_a', ('limit demand', asset)) "
            "FROM file_limited_px ORDER BY id DESC LIMIT 3 OFFSET 2")
        self.assertEqual(self.execute(statement),
                         tuple((index, f"limited-result-{index}") for index in retained))
        self.assert_counts(self.first, contents)
        self.assertFalse(self.second.requests)

    def test_media_computed_sort_error_precedes_projection_file_demand(self):
        self.prepare_media()
        missing = descriptor(self.media.root / "computed-sort-must-not-load.png")
        self.insert_values([(1, 0, "{", missing, None)])
        error = self.reject_media(
            "SELECT id, AI_MAP('file_media_a', ('not demanded', asset)) "
            "FROM file_values ORDER BY JSON_EXTRACT(text_value, '$.sort')", code=3141)
        self.assertIn("json", str(error).lower())

    def test_media_json_map_projection_waits_for_completed_results(self):
        self.prepare_media()
        assets, _others = self.load_media(prefix="json-media")
        contents = []
        for index, asset in enumerate(assets):
            content = [text_block(f"json-media-{index}"), asset.block()]
            contents.append(content)
            self.seed(self.first, content, descriptor_text({"label": f"media-label-{index}"}),
                      assets=[asset])
        rows = self.select_media(
            "JSON_UNQUOTE(JSON_EXTRACT(AI_MAP('file_media_a', (text_value, asset)), '$.label'))")
        self.assertEqual(rows, tuple((index, f"media-label-{index}") for index in range(ROW_COUNT)))
        self.assert_counts(self.first, contents)
        self.assertFalse(self.second.requests)


class CatalogUpgradeContracts(unittest.TestCase):
    database = None
    old_binary = None
    new_binary = None

    def setUp(self):
        self.cursor = None
        self.fixtures = FileFixtures()
        self.server = self.fixtures.mocks[0]
        self.addCleanup(self.close_case)

    def close_case(self):
        try:
            self.fixtures.close()
        finally:
            if self.cursor is not None:
                self.cursor.close()

    def execute(self, statement, parameters=None):
        self.cursor.execute(statement, parameters)
        return self.cursor.fetchall()

    def open_cursor(self):
        self.cursor = self.database.connection.cursor()
        self.cursor.execute("SET ob_query_timeout=30000000")
        self.cursor.execute(f"USE {UPGRADE_DATABASE}")

    def restart_as(self, binary):
        self.cursor.close()
        self.cursor = None
        base = self.database.base
        before = self.database.normal_stops
        self.database.restart(binary)
        self.assertEqual(self.database.base, base, "catalog upgrade used a fresh data directory")
        self.assertEqual(self.database.normal_stops, before + 1,
                         "catalog upgrade did not finish a SIGUSR1 normal stop")
        self.assertEqual(self.database.binary, str(binary))
        self.open_cursor()

    def assert_legacy_columns_absent(self):
        for table, table_id in (("__all_ai_model", 1128), ("__all_ai_model_history", 1129)):
            columns = self.execute(f"SHOW COLUMNS FROM oceanbase.{table}")
            self.assertTrue(columns, f"legacy system table {table} was not visible")
            self.assertNotIn("input_modalities", {row[0] for row in columns},
                             "the supplied old binary already has the capability column")
            self.assertEqual(self.execute(
                "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='oceanbase' "
                "AND TABLE_NAME=%s AND COLUMN_NAME='input_modalities'", (table,)), ((0,),))
            self.assertEqual(self.execute(
                "SELECT COUNT(*) FROM oceanbase.__all_column "
                "WHERE table_id=%s AND column_name='input_modalities'", (table_id,)), ((0,),))

    def upgraded_schema(self):
        for table, table_id, column_id, nullable in (
                ("__all_ai_model", 1128, 22, False),
                ("__all_ai_model_history", 1129, 24, True)):
            metadata = self.execute(
                "SELECT column_id, nullable, CAST(orig_default_value_v2 AS CHAR), "
                "CAST(cur_default_value_v2 AS CHAR) FROM oceanbase.__all_column "
                "WHERE table_id=%s AND column_name='input_modalities' ORDER BY column_id", (table_id,))
            assert_modality_column(self, metadata, table=table, column_id=column_id, nullable=nullable)
            public = self.execute(
                "SELECT DATA_TYPE, IS_NULLABLE, CAST(COLUMN_DEFAULT AS CHAR) "
                "FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='oceanbase' "
                "AND TABLE_NAME=%s AND COLUMN_NAME='input_modalities'", (table,))
            self.assertEqual(public, (("bigint", "YES" if nullable else "NO", "1"),))
        signature = self.execute(
            "SELECT table_id, column_id, column_name FROM oceanbase.__all_column "
            "WHERE table_id IN (1128,1129) ORDER BY table_id, column_id")
        self.assertTrue(signature, "upgraded system column metadata was empty")
        self.assertEqual(len(signature), len({(row[0], row[1]) for row in signature}),
                         "system catalog contains duplicate column IDs")
        self.assertEqual(len(signature), len({(row[0], row[2]) for row in signature}),
                         "system catalog contains duplicate column names")
        return signature

    def live_legacy_model(self, phase):
        prompt = f"legacy-text-{phase}"
        reply = self.fixtures.reply(self.server, prompt, f"legacy-result-{phase}")
        self.assertEqual(self.execute(
            "SELECT AI_MAP('upgrade_legacy_text', %s)", (prompt,)), ((f"legacy-result-{phase}",),))
        body = self.server.requests[-1]
        assert_content(self, body, prompt)
        self.assertNotIn("response_format", body)
        self.assertIsNotNone(reply.body)

    def test_old_catalog_upgrade_preserves_model_masks_and_is_idempotent(self):
        self.assertIsNotNone(self.old_binary, "the upgrade contract requires an explicit old binary")
        self.assertIsNotNone(self.new_binary, "the upgrade contract requires an explicit new binary")
        self.assertNotEqual(self.old_binary, self.new_binary, "old/new upgrade binaries must be distinct")
        self.assertIsNone(self.database.process, "the old catalog must be bootstrapped before any new binary")
        self.assertEqual(self.database.binary, str(self.old_binary))
        self.database.start()
        self.cursor = self.database.connection.cursor()
        self.cursor.execute("SET ob_query_timeout=30000000")
        self.cursor.execute(f"CREATE DATABASE {UPGRADE_DATABASE}")
        self.cursor.execute(f"USE {UPGRADE_DATABASE}")
        self.assert_legacy_columns_absent()
        media_query = (
            "SELECT name, value FROM oceanbase.__all_sys_variable "
            "WHERE name IN ('ai_media_root','ai_media_max_bytes','ai_media_parallel',"
            "'ai_media_remote_root') ORDER BY name")
        self.assertEqual(self.execute(media_query), (),
                         "old-binary bootstrap unexpectedly persisted new media variables")
        self.cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL(%s, %s)", (
            "upgrade_legacy_text",
            descriptor_text({"type": "completion", "model_name": "mock-upgrade-legacy"})))
        self.cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT(%s, %s)", (
            "upgrade_legacy_endpoint", descriptor_text({
                "ai_model_name": "upgrade_legacy_text", "provider": "openai",
                "url": f"http://127.0.0.1:{self.server.server_port}/v1/chat/completions",
                "access_key": "catalog-upgrade-local-only", "request_model_name": "mock-upgrade-legacy",
            })))
        legacy = self.execute(
            "SELECT model_id, name, type, model_name FROM oceanbase.__all_ai_model "
            "WHERE name='upgrade_legacy_text'")
        self.assertEqual(len(legacy), 1)
        legacy_id, name, model_type, provider_name = legacy[0]
        self.assertEqual((name, model_type, provider_name),
                         ("upgrade_legacy_text", 3, "mock-upgrade-legacy"))
        old_history = self.execute(
            "SELECT model_id, schema_version, is_deleted, name, type, model_name "
            "FROM oceanbase.__all_ai_model_history WHERE model_id=%s "
            "ORDER BY schema_version DESC LIMIT 1", (legacy_id,))
        self.assertEqual(len(old_history), 1)
        self.assertEqual(old_history[0][0], legacy_id)
        self.assertEqual(old_history[0][2:], (0, "upgrade_legacy_text", 3, "mock-upgrade-legacy"))
        self.assert_legacy_columns_absent()
        self.live_legacy_model("old")
        self.restart_as(self.new_binary)
        upgraded = self.upgraded_schema()
        self.assertEqual(self.execute(media_query),
                         (("ai_media_max_bytes", "2097152"), ("ai_media_parallel", "4"),
                          ("ai_media_remote_root", ""), ("ai_media_root", "")),
                         "upgrade did not durably add exactly the four safe media defaults")
        self.assertEqual(self.execute(
            "SELECT model_id, name, type, model_name, input_modalities "
            "FROM oceanbase.__all_ai_model WHERE model_id=%s", (legacy_id,)),
            ((legacy_id, "upgrade_legacy_text", 3, "mock-upgrade-legacy", 1),))
        self.assertEqual(self.execute(
            "SELECT model_id, schema_version, is_deleted, name, type, model_name, input_modalities "
            "FROM oceanbase.__all_ai_model_history WHERE model_id=%s "
            "ORDER BY schema_version DESC LIMIT 1", (legacy_id,)), (old_history[0] + (1,),))
        assert_model_catalog(self, self.execute, {"upgrade_legacy_text": 1})
        self.live_legacy_model("upgraded")
        masks = {"upgrade_legacy_text": 1}
        for suffix, modalities, mask in (
                ("image", ["text", "image"], 3), ("audio", ["text", "audio"], 5),
                ("all", ["text", "image", "audio"], 7)):
            name = f"upgrade_new_{suffix}"
            self.cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL(%s, %s)", (
                name, descriptor_text({"type": "completion", "model_name": f"mock-upgrade-{suffix}",
                                       "input_modalities": modalities})))
            masks[name] = mask
        assert_model_catalog(self, self.execute, masks)
        identities = self.execute(
            "SELECT model_id, name, type, model_name, input_modalities "
            "FROM oceanbase.__all_ai_model WHERE name LIKE 'upgrade_%' ORDER BY model_id")
        self.assertEqual(len(identities), 4)
        media_root = str(self.database.base / "upgrade-media-policy")
        remote_root = "https://upgrade.example.invalid/media/"
        self.cursor.execute("SET GLOBAL ai_media_root=%s", (media_root,))
        self.cursor.execute("SET GLOBAL ai_media_remote_root=%s", (remote_root,))
        self.cursor.execute("SET GLOBAL ai_media_max_bytes=1077")
        self.cursor.execute("SET GLOBAL ai_media_parallel=2")
        media_policy = self.execute(media_query)
        self.assertEqual(media_policy,
                         (("ai_media_max_bytes", "1077"), ("ai_media_parallel", "2"),
                          ("ai_media_remote_root", remote_root), ("ai_media_root", media_root)))
        self.restart_as(self.new_binary)
        self.assertEqual(self.upgraded_schema(), upgraded,
                         "second new-binary startup changed system columns or added duplicates")
        self.assertEqual(self.execute(
            "SELECT model_id, name, type, model_name, input_modalities "
            "FROM oceanbase.__all_ai_model WHERE name LIKE 'upgrade_%' ORDER BY model_id"), identities)
        assert_model_catalog(self, self.execute, masks)
        self.assertEqual(self.execute(media_query), media_policy,
                         "second startup replaced an existing durable media policy")
        for name, value in media_policy:
            self.assertEqual(self.execute(
                "SELECT value FROM oceanbase.__all_sys_variable_history WHERE name=%s "
                "ORDER BY schema_version DESC LIMIT 1", (name,)), ((value,),),
                f"latest history did not durably preserve {name}")
        self.live_legacy_model("idempotent")
        self.assertEqual((self.database.starts, self.database.normal_stops), (3, 2))
        self.assertEqual(self.server.counts, Counter(content_key(f"legacy-text-{phase}")
                                                    for phase in ("old", "upgraded", "idempotent")))
        self.assertFalse(self.fixtures.mocks[1].requests)


def case_names(test_class):
    return unittest.defaultTestLoader.getTestCaseNames(test_class)


def available_case_names(self_test=False):
    if self_test:
        return case_names(FileHarnessTests)
    return case_names(FileContracts) + case_names(CatalogUpgradeContracts)


def parse_options(arguments=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, help="Built seekdb binary for a new private test database")
    parser.add_argument("--upgrade-binary", type=Path,
                        help=f"Old binary, only with --case {UPGRADE_CASE}; never used by default")
    parser.add_argument("--case", action="append", default=[], help="Test method name; repeat to select")
    parser.add_argument("--self-test", action="store_true", help="Run deterministic fixtures without SQL")
    parser.add_argument("--list-cases", action="store_true", help="List cases without starting a database")
    options = parser.parse_args(arguments)
    unknown = [name for name in options.case if name not in available_case_names(options.self_test)]
    if unknown:
        parser.error(f"unknown case(s): {', '.join(unknown)}")
    if options.self_test and options.binary is not None:
        parser.error("--self-test cannot be combined with --binary")
    if options.self_test and options.upgrade_binary is not None:
        parser.error("--self-test cannot be combined with --upgrade-binary")
    upgrade_selected = UPGRADE_CASE in options.case
    if upgrade_selected and options.case != [UPGRADE_CASE]:
        parser.error("the optional upgrade contract must be selected alone")
    if options.upgrade_binary is not None and options.case != [UPGRADE_CASE]:
        parser.error(f"--upgrade-binary requires --case {UPGRADE_CASE} alone")
    if upgrade_selected and not options.list_cases and options.upgrade_binary is None:
        parser.error("the optional upgrade contract requires --upgrade-binary")
    if not options.self_test and not options.list_cases and options.binary is None:
        parser.error("--binary is required unless --self-test or --list-cases is selected")
    return options


def run_suite(test_class, selected):
    names = selected or case_names(test_class)
    result = unittest.TextTestRunner(verbosity=2).run(
        unittest.TestSuite(test_class(name) for name in names))
    if not result.wasSuccessful():
        raise SystemExit(1)


def main(arguments=None):
    options = parse_options(arguments)
    if options.list_cases:
        for name in options.case or available_case_names(options.self_test):
            print(name)
    elif options.self_test:
        run_suite(FileHarnessTests, options.case)
    else:
        binary = options.binary.resolve()
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise SystemExit(f"--binary is not an executable file: {binary}")
        old_binary = None
        if options.upgrade_binary is not None:
            old_binary = options.upgrade_binary.resolve()
            if not old_binary.is_file() or not os.access(old_binary, os.X_OK):
                raise SystemExit(f"--upgrade-binary is not an executable file: {old_binary}")
            if old_binary == binary:
                raise SystemExit("--upgrade-binary must differ from --binary")
        database = PrivateDatabase(old_binary if old_binary is not None else binary)
        try:
            if old_binary is not None:
                CatalogUpgradeContracts.database = database
                CatalogUpgradeContracts.old_binary = old_binary
                CatalogUpgradeContracts.new_binary = binary
                run_suite(CatalogUpgradeContracts, options.case)
            else:
                database.start()
                FileContracts.database = database
                run_suite(FileContracts, options.case)
        finally:
            database.close()


if __name__ == "__main__":
    main()
