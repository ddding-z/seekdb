"""Private, loopback-only contracts for pre-signed HTTP(S) FILE media.

--self-test exercises storage, generated media, provider and CLI fixtures only.
--binary /path/to/seekdb uses the existing private unix-socket launcher; repeat
--case test_name to select this suite's ten SQL contracts. --list-cases never
starts a database or server. No base unittest suite is inherited or discovered.

Storage serves numeric-loopback HTTP only and records raw escaped request
targets, including signed query bytes. Model endpoints are deterministic
loopback mocks. All media is synthetic; no external services, credentials or
GPU are required. Runtime roots are restored and every owned server is stopped.
Native remote_media contracts separately prove HTTP does not occupy the four
local/CPU workers and that fetch and encoding share query media credits.
"""

import argparse
import base64
from collections import Counter, deque
from contextlib import redirect_stderr
from dataclasses import dataclass, field
import hashlib
from http.client import HTTPConnection, IncompleteRead
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import os
from pathlib import Path
import select
import socket
import struct
import sys
import threading
import time
import unittest
from unittest import mock
import wave

import pymysql

import test_file_runtime as files
from test_semantic_aggregate import DEFAULT_OPTIONS, decode_request
from test_runtime_contracts import model_socket_fds, sql_error


DATABASE = "ai_remote_file_contract_test"
MODEL_A = "remote_file_media_a"
MODEL_B = "remote_file_media_b"
GENERATORS = {
    "png": (files.png_bytes, "image/png"),
    "jpeg": (files.jpeg_bytes, "image/jpeg"),
    "wav": (files.wav_bytes, "audio/wav"),
    "mp3": (files.mp3_bytes, "audio/mpeg"),
}


def streaming_wav_bytes():
    destination = io.BytesIO()
    with wave.open(destination, "wb") as output:
        output.setnchannels(1)
        output.setsampwidth(2)
        output.setframerate(8000)
        output.writeframes(b"\x00\x00" * 20000)
    return destination.getvalue()


@dataclass
class RemoteAsset:
    uri: str
    target: str
    raw: bytes
    mime_type: str

    def descriptor(self, *, include_mime=True, include_size=False, version=None):
        value = {"format_version": 1, "uri": self.uri}
        if include_mime:
            value["mime_type"] = self.mime_type
        if include_size:
            value["size_bytes"] = len(self.raw)
        if version is not None:
            value["object_version"] = version
        return value

    def block(self):
        return files.MediaAsset.block(self)

    def version(self):
        return "sha256:" + hashlib.sha256(self.raw).hexdigest()


@dataclass
class StorageReply:
    body: bytes = b""
    status: int = 200
    framing: str = "length"
    headers: dict = field(default_factory=dict)
    content_length: int | None = None
    gate: threading.Event | None = None
    before_gate: int = 0


class StorageHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_arguments):
        pass

    def do_GET(self):
        self.respond("GET")

    def do_HEAD(self):
        self.respond("HEAD")

    def wait_gate(self, reply, record):
        if reply.gate is None:
            return True
        deadline = time.monotonic() + 20
        while not reply.gate.is_set() and not self.server.stopping.is_set():
            readable, _, _ = select.select([self.connection], [], [], 0.02)
            if readable:
                try:
                    closed = not self.connection.recv(1, socket.MSG_PEEK)
                except ConnectionResetError:
                    closed = True
                if closed:
                    with self.server.condition:
                        record["disconnected"] = True
                        self.server.condition.notify_all()
                    return False
            if time.monotonic() >= deadline:
                raise AssertionError("storage fixture gate was not released within 20 seconds")
        return not self.server.stopping.is_set()

    def write_body(self, reply, begin, end):
        for offset in range(begin, end, 16384):
            part = reply.body[offset:min(offset + 16384, end)]
            if reply.framing == "chunked":
                self.wfile.write(f"{len(part):x}\r\n".encode("ascii"))
            self.wfile.write(part)
            if reply.framing == "chunked":
                self.wfile.write(b"\r\n")
        self.wfile.flush()

    def respond(self, method):
        server = self.server
        record = None
        self.close_connection = True
        try:
            target = self.raw_requestline.split(b" ", 2)[1].decode("ascii")
            with server.condition:
                reply = server.routes.get(target)
                if reply is None:
                    server.fixture_errors.append("storage received an unregistered raw target")
                    reply = StorageReply(status=404)
                record = {
                    "method": method, "target": target,
                    "received": time.monotonic(), "finished": None,
                    "disconnected": False, "before_gate": 0,
                }
                server.requests.append(record)
                server.active += 1
                server.peak = max(server.peak, server.active)
                server.condition.notify_all()
            self.send_response(reply.status)
            self.send_header("Connection", "close")
            if reply.framing == "length":
                length = len(reply.body) if reply.content_length is None else reply.content_length
                self.send_header("Content-Length", str(length))
            elif reply.framing == "chunked":
                self.send_header("Transfer-Encoding", "chunked")
            elif reply.framing != "close":
                raise AssertionError("unknown storage fixture framing")
            for name, value in reply.headers.items():
                self.send_header(name, value)
            self.end_headers()
            if method == "HEAD":
                return
            before = min(reply.before_gate, len(reply.body))
            self.write_body(reply, 0, before)
            with server.condition:
                record["before_gate"] = before
                server.condition.notify_all()
            if not self.wait_gate(reply, record):
                return
            self.write_body(reply, before, len(reply.body))
            if reply.framing == "chunked":
                self.wfile.write(b"0\r\n\r\n")
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            if record is not None:
                with server.condition:
                    record["disconnected"] = True
                    server.condition.notify_all()
        except Exception as error:
            with server.condition:
                server.fixture_errors.append(repr(error))
                server.condition.notify_all()
        finally:
            if record is not None:
                with server.condition:
                    record["finished"] = time.monotonic()
                    server.active -= 1
                    server.condition.notify_all()


class StorageServer(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 128

    def __init__(self):
        super().__init__(("127.0.0.1", 0), StorageHandler)
        self.condition = threading.Condition()
        self.routes = {}
        self.requests = []
        self.fixture_errors = []
        self.active = 0
        self.peak = 0
        self.stopping = threading.Event()
        self.thread = threading.Thread(
            target=lambda: self.serve_forever(poll_interval=0.02),
            name="remote-file-storage", daemon=True)
        self.thread.start()

    @property
    def origin(self):
        return f"http://127.0.0.1:{self.server_port}"

    @property
    def root(self):
        return self.origin + "/media/"

    def add(self, target, reply):
        with self.condition:
            if target in self.routes:
                raise ValueError("storage targets must be unique")
            if not target.startswith("/") or not target.isascii() or " " in target:
                raise ValueError("storage targets must be escaped ASCII origin-form targets")
            if reply.framing not in {"length", "chunked", "close"}:
                raise ValueError("unsupported storage fixture framing")
            self.routes[target] = reply

    def asset(self, kind="png", seed=0, *, target=None, framing="length", gate=None, raw=None):
        generator, mime_type = GENERATORS[kind]
        target = target or f"/media/{kind}-{seed}.bin"
        raw = generator(seed) if raw is None else raw
        self.add(target, StorageReply(
            raw, framing=framing, gate=gate,
            headers={"Content-Type": "application/octet-stream", "ETag": '"not-the-sha256"'}))
        return RemoteAsset(self.origin + target, target, raw, mime_type)

    def wait(self, predicate, message, timeout=8):
        with self.condition:
            if not self.condition.wait_for(predicate, timeout=timeout):
                raise AssertionError(message)
            if self.fixture_errors:
                raise AssertionError(self.fixture_errors)

    def counts(self):
        with self.condition:
            return Counter((record["method"], record["target"]) for record in self.requests)

    def release(self):
        with self.condition:
            for reply in self.routes.values():
                if reply.gate is not None:
                    reply.gate.set()

    def close(self):
        if self.stopping.is_set():
            return
        self.stopping.set()
        self.release()
        self.shutdown()
        self.thread.join(5)
        self.server_close()
        self.wait(lambda: self.active == 0, "owned storage handlers did not stop", timeout=5)
        if self.thread.is_alive():
            raise AssertionError("owned storage listener did not stop")

    def __enter__(self):
        return self

    def __exit__(self, *_arguments):
        self.close()


def storage_get(server, target, *, method="GET"):
    connection = HTTPConnection("127.0.0.1", server.server_port, timeout=5)
    try:
        connection.request(method, target)
        response = connection.getresponse()
        return response.status, dict(response.getheaders()), response.read()
    finally:
        connection.close()


class RemoteFileHarnessTests(unittest.TestCase):
    def test_generated_media_descriptors_hashes_and_inline_blocks(self):
        with StorageServer() as server:
            for index, kind in enumerate(GENERATORS):
                asset = server.asset(kind, index)
                descriptor = asset.descriptor(include_size=True, version=asset.version())
                self.assertEqual(files.decode_descriptor(files.descriptor_text(descriptor)), descriptor)
                self.assertEqual(descriptor["object_version"],
                                 "sha256:" + hashlib.sha256(asset.raw).hexdigest())
                block = asset.block()
                if kind in {"png", "jpeg"}:
                    prefix = f"data:{asset.mime_type};base64,"
                    self.assertTrue(block["image_url"]["url"].startswith(prefix))
                    encoded = block["image_url"]["url"][len(prefix):]
                else:
                    self.assertEqual(block["input_audio"]["format"], "wav" if kind == "wav" else "mp3")
                    encoded = block["input_audio"]["data"]
                self.assertEqual(base64.b64decode(encoded, validate=True), asset.raw)
            self.assertEqual(server.counts(), Counter(), "descriptor construction must not perform I/O")

    def test_storage_records_exact_get_query_bytes_and_head(self):
        with StorageServer() as server:
            target = "/media/a%20b%2A.bin?sig=test%2B%2f&repeat=a+b&repeat=%252F&blank="
            asset = server.asset(target=target)
            self.assertEqual(storage_get(server, target)[2], asset.raw)
            self.assertEqual(storage_get(server, target, method="HEAD")[2], b"")
            self.assertEqual(server.counts(), Counter({("GET", target): 1, ("HEAD", target): 1}))

    def test_storage_fixed_chunked_and_unknown_length_responses(self):
        with StorageServer() as server:
            for index, mode in enumerate(("length", "chunked", "close")):
                asset = server.asset("wav", index, framing=mode, raw=streaming_wav_bytes())
                status, headers, raw = storage_get(server, asset.target)
                self.assertEqual((status, raw), (200, asset.raw))
                if mode == "chunked":
                    self.assertEqual(headers["Transfer-Encoding"], "chunked")
                    self.assertNotIn("Content-Length", headers)
                elif mode == "close":
                    self.assertNotIn("Content-Length", headers)
                    self.assertNotIn("Transfer-Encoding", headers)
                else:
                    self.assertEqual(int(headers["Content-Length"]), len(asset.raw))

    def test_storage_error_redirect_and_truncated_size_fixtures(self):
        with StorageServer() as server:
            asset = server.asset(seed=10)
            server.add("/media/redirect", StorageReply(
                status=302, headers={"Location": asset.uri}))
            server.add("/media/error", StorageReply(
                b"denied", status=429, headers={"Retry-After": "1"}))
            server.add("/media/truncated", StorageReply(
                asset.raw, content_length=len(asset.raw) + 3))
            self.assertEqual(storage_get(server, "/media/redirect")[0], 302)
            self.assertEqual(storage_get(server, "/media/error")[0], 429)
            with self.assertRaises(IncompleteRead):
                storage_get(server, "/media/truncated")
            self.assertNotIn(("GET", asset.target), server.counts(),
                             "storage fixture helper must not follow redirects")

    def test_held_storage_does_not_block_an_independent_get(self):
        with StorageServer() as server:
            gate = threading.Event()
            held = server.asset(seed=20, gate=gate)
            fast = server.asset(seed=21)
            results, errors = [], []

            def fetch():
                try:
                    results.append(storage_get(server, held.target)[2])
                except Exception as error:
                    errors.append(error)

            worker = threading.Thread(target=fetch, name="held-storage-client", daemon=True)
            worker.start()
            try:
                server.wait(lambda: len(server.requests) == 1 and server.active == 1,
                            "held storage GET was not admitted")
                self.assertEqual(storage_get(server, fast.target)[2], fast.raw)
                self.assertTrue(worker.is_alive(), "held response completed without release")
                self.assertFalse(results)
            finally:
                gate.set()
                worker.join(6)
            self.assertFalse(worker.is_alive())
            self.assertFalse(errors, errors)
            self.assertEqual(results, [held.raw])

    def test_held_storage_detects_client_socket_cleanup(self):
        with StorageServer() as server:
            asset = server.asset(seed=30, gate=threading.Event())
            client = socket.create_connection(("127.0.0.1", server.server_port), timeout=5)
            try:
                client.sendall(f"GET {asset.target} HTTP/1.1\r\nHost: fixture\r\n\r\n".encode("ascii"))
                server.wait(lambda: len(server.requests) == 1, "client request was not recorded")
                header = b""
                while b"\r\n\r\n" not in header:
                    header += client.recv(1024)
                self.assertIn(b"200 OK", header)
            finally:
                client.close()
            server.wait(lambda: server.active == 0 and server.requests[0]["disconnected"],
                        "held fixture did not observe client-side socket cleanup")
            self.assertFalse(server.routes[asset.target].gate.is_set(),
                             "socket cleanup evidence must precede gate release")

    def test_provider_fixture_preserves_remote_inline_content_without_uri(self):
        with StorageServer() as storage:
            fixtures = files.FileFixtures()
            try:
                asset = storage.asset(seed=40)
                content = [files.text_block("inspect synthetic media"), asset.block()]
                fixtures.reply(fixtures.mocks[0], content, "mock-description")
                body = {"model": "mock", "messages": [{"role": "user", "content": content}]}
                status, raw = files.post_body(fixtures.mocks[0], body)
                self.assertEqual(status, 200)
                self.assertEqual(files.strict_json(raw)["choices"][0]["message"]["content"],
                                 "mock-description")
                files.assert_content(self, fixtures.mocks[0].requests[0], content, assets=[asset])
                self.assertNotIn(asset.uri, files.descriptor_text(body))
                self.assertEqual(storage.counts(), Counter())
            finally:
                fixtures.close()

    def test_case_selection_is_focused_and_listing_never_starts_services(self):
        sql_cases = case_names(RemoteFileContracts)
        self.assertEqual(len(sql_cases), 10)
        self.assertFalse(set(sql_cases) & set(case_names(files.FileContracts)))
        self.assertEqual(RemoteFileContracts.__bases__, (unittest.TestCase,))
        selected = case_names(type(self))[0]
        self.assertEqual(parse_options(["--self-test", "--case", selected]).case, [selected])
        with mock.patch.object(files.PrivateDatabase, "start") as start, \
                mock.patch.object(StorageServer, "__init__") as storage, \
                unittest.mock.patch("sys.stdout", new=io.StringIO()) as output:
            main(["--list-cases"])
            self.assertEqual(output.getvalue().splitlines(), sql_cases)
            start.assert_not_called()
            storage.assert_not_called()
        for arguments in (["--self-test", "--binary", "/unused"],
                          ["--case", "test_media_valid_formats"], []):
            with self.subTest(arguments=arguments), redirect_stderr(io.StringIO()), \
                    self.assertRaises(SystemExit):
                parse_options(arguments)

    def test_storage_rejects_bad_fixture_registration(self):
        with StorageServer() as server:
            server.asset(seed=50)
            for target, reply in (
                    ("/media/png-50.bin", StorageReply()),
                    ("http://127.0.0.1/absolute-target", StorageReply()),
                    ("/media/unescaped space", StorageReply()),
                    ("/media/invalid-mode", StorageReply(framing="invalid"))):
                with self.subTest(target=target), self.assertRaises(ValueError):
                    server.add(target, reply)


class RemoteFileContracts(unittest.TestCase):
    database = None
    models_created = False

    @classmethod
    def setUpClass(cls):
        with cls.database.connection.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE {DATABASE}")
            cursor.execute(f"USE {DATABASE}")
            cursor.execute("CREATE TABLE remote_values "
                           "(id INT PRIMARY KEY, text_value LONGTEXT, asset FILE)")
            cursor.execute("CREATE TABLE remote_right "
                           "(id INT PRIMARY KEY, text_value LONGTEXT, asset FILE)")

    def setUp(self):
        self.media = files.MediaFiles()
        self.addCleanup(self.media.close)
        self.storage = StorageServer()
        self.addCleanup(self.storage.close)
        self.fixtures = files.FileFixtures()
        self.addCleanup(self.fixtures.close)
        self.first, self.second = self.fixtures.mocks
        self.cursor = self.database.connection.cursor()
        self.addCleanup(self.cursor.close)
        self.cursor.execute(f"USE {DATABASE}")
        self.original_roots = self.execute(
            "SELECT @@GLOBAL.ai_media_root, @@GLOBAL.ai_media_remote_root")[0]
        self.expected = {server: {} for server in self.fixtures.mocks}
        self.addCleanup(self.close_case)
        self.cursor.execute("SET ob_query_timeout=20000000, ai_pipeline_slots=2, "
                            "ai_media_parallel=4, ai_media_max_bytes=DEFAULT")
        for table in ("remote_values", "remote_right"):
            self.cursor.execute(f"TRUNCATE TABLE {table}")
        self.prepare_models()
        self.process_id = struct.unpack(
            "3i", self.database.connection._sock.getsockopt(
                socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i")))[0]

    def close_case(self):
        self.storage.release()
        self.fixtures.release()
        for pending in self.fixtures.pending:
            pending.thread.join(5)
        if any(pending.thread.is_alive() for pending in self.fixtures.pending):
            self.kill_query()
            for pending in self.fixtures.pending:
                pending.thread.join(5)
            self.assertFalse(any(pending.thread.is_alive() for pending in self.fixtures.pending),
                             "private remote SQL query did not stop")
        self.cursor.execute("SET GLOBAL ai_media_root=%s", (self.original_roots[0],))
        self.cursor.execute("SET GLOBAL ai_media_remote_root=%s", (self.original_roots[1],))
        self.cursor.execute("SET ob_query_timeout=20000000, ai_media_parallel=DEFAULT, "
                            "ai_media_max_bytes=DEFAULT")
        for server in self.fixtures.mocks:
            for body in server.requests:
                key = files.content_key(body["messages"][-1]["content"])
                self.assertIn(key, self.expected[server], "provider received an unplanned input")
                content, assets = self.expected[server][key]
                files.assert_content(self, body, content, assets=assets)
                wire = files.descriptor_text(body)
                for asset in assets:
                    if isinstance(asset, RemoteAsset):
                        self.assertNotIn(asset.uri, wire, "pre-signed URL leaked to the model provider")
                    else:
                        self.assertNotIn(str(asset.path), wire, "local path leaked to the provider")
                        self.assertNotIn(asset.path.as_uri(), wire, "local FILE URI leaked to the provider")

    def execute(self, statement, parameters=None):
        self.cursor.execute(statement, parameters)
        return self.cursor.fetchall()

    def prepare_models(self):
        if not type(self).models_created:
            for model in (MODEL_A, MODEL_B):
                self.cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL(%s,%s)", (
                    model, files.descriptor_text({
                        "type": "completion", "model_name": "mock-remote-media",
                        "input_modalities": ["text", "image", "audio"],
                    })))
                self.cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT(%s,%s)", (
                    model + "_endpoint", files.descriptor_text({
                        "ai_model_name": model, "access_key": "local-remote-file-fixture-only",
                        "provider": "openai", "request_model_name": "mock-remote-media",
                        "url": f"http://127.0.0.1:{self.first.server_port}/v1/chat/completions",
                    })))
            type(self).models_created = True
        for model, server in ((MODEL_A, self.first), (MODEL_B, self.second)):
            self.cursor.execute("CALL DBMS_AI_SERVICE.ALTER_AI_MODEL_ENDPOINT(%s,%s)", (
                model + "_endpoint", files.descriptor_text({
                    "url": f"http://127.0.0.1:{server.server_port}/v1/chat/completions",
                    "provider": "openai",
                })))

    def configure(self, *, remote=True, local=False):
        self.cursor.execute("SET GLOBAL ai_media_remote_root=%s",
                            (self.storage.root if remote else "",))
        self.cursor.execute("SET GLOBAL ai_media_root=%s", (str(self.media.root) if local else "",))

    def seed(self, server, content, result, *, assets=(), boolean=False):
        self.expected[server][files.content_key(content)] = (content, tuple(assets))
        return self.fixtures.reply(server, content, result, boolean=boolean)

    def insert(self, records, table="remote_values"):
        self.cursor.executemany(
            f"INSERT INTO {table} VALUES (%s,%s,TO_FILE(%s))",
            [(index, text, files.descriptor_text(value)) for index, text, value in records])

    def reject(self, statement, parameters=None, *, code=None, uri=None, runtime_error=False):
        before = [len(server.requests) for server in self.fixtures.mocks]
        with self.assertRaises(pymysql.MySQLError) as caught:
            self.cursor.execute(statement, parameters)
        error = caught.exception
        self.assertIsInstance(error.args[0], int)
        self.assertTrue(error.args[1], "rejection must contain an explicit diagnostic")
        if code is not None:
            self.assertEqual(error.args[0], code, error.args)
        if runtime_error:
            self.assertNotIn(error.args[0], (1317, 4012),
                             "invalid remote FILE failed only because it timed out or was cancelled")
        if uri is not None:
            self.assertNotIn(uri, str(error), "runtime diagnostic leaked the pre-signed URL")
            if "?" in uri:
                self.assertNotIn(uri.split("?", 1)[1], str(error),
                                 "runtime diagnostic leaked the signature query")
        self.assertEqual([len(server.requests) for server in self.fixtures.mocks], before,
                         "rejected remote FILE made a model request")
        self.assertEqual(self.execute("SELECT 1"), ((1,),), "rejection poisoned the SQL connection")
        return error

    def reject_asset(self, value):
        return self.reject(
            f"SELECT AI_MAP('{MODEL_A}', TO_FILE(%s))",
            (files.descriptor_text(value),), uri=value["uri"], runtime_error=True)

    def map_asset(self, asset, prompt, result, *, value=None):
        content = [files.text_block(prompt), asset.block()]
        self.seed(self.first, content, result, assets=[asset])
        descriptor = asset.descriptor() if value is None else value
        self.assertEqual(self.execute(
            f"SELECT AI_MAP('{MODEL_A}', (%s, TO_FILE(%s)))",
            (prompt, files.descriptor_text(descriptor))), ((result,),))
        return content

    def assert_storage(self, targets):
        self.assertEqual(self.storage.counts(), Counter(("GET", target) for target in targets),
                         "wrong GET count, HEAD/retry, or changed escaped signature bytes")
        self.assertFalse(self.storage.fixture_errors, self.storage.fixture_errors)

    def assert_provider(self, server, contents):
        with server.condition:
            self.assertEqual(server.counts, Counter(files.content_key(value) for value in contents))
            self.assertFalse(server.fixture_errors, server.fixture_errors)

    def kill_query(self):
        with self.database.connect(database=DATABASE) as connection, connection.cursor() as cursor:
            cursor.execute(f"KILL QUERY {self.database.connection.thread_id()}")

    def assert_no_sockets(self):
        for server in (self.storage, *self.fixtures.mocks):
            self.assertFalse(model_socket_fds(self.process_id, server.server_port),
                             "remote query retained a storage/model socket after completion")

    def test_remote_root_default_canonical_and_global_only_privileges(self):
        self.assertEqual(self.original_roots, ("", ""), "private database must start with deny defaults")
        self.reject("SET SESSION ai_media_remote_root=%s", (self.storage.root,), code=1229)
        self.reject("SET ai_media_remote_root=%s", (self.storage.root,), code=1229)
        for root in (
                "https://objects.example.invalid/prefix/",
                "https://objects.example.invalid:8443/prefix/",
                "http://127.0.0.1:12345/prefix/", "http://[::1]:12345/prefix/",
                self.storage.root, ""):
            self.cursor.execute("SET GLOBAL ai_media_remote_root=%s", (root,))
            self.assertEqual(self.execute("SELECT @@GLOBAL.ai_media_remote_root"), ((root,),))
        for root in (
                "HTTPS://objects.example.invalid/prefix/",
                "https://Objects.example.invalid/prefix/",
                "https://objects.example.invalid/prefix",
                "https://objects.example.invalid/prefix/?signature=not-a-root",
                "https://objects.example.invalid/prefix/#fragment",
                "https://user@objects.example.invalid/prefix/",
                "http://objects.example.invalid/prefix/", "http://localhost/prefix/",
                "http://127.0.0.1:12345/prefix/../outside/",
                "http://127.0.0.1:12345/prefix/%2e%2e/outside/"):
            with self.subTest(root=root):
                self.reject("SET GLOBAL ai_media_remote_root=%s", (root,))
                self.assertEqual(self.execute("SELECT @@GLOBAL.ai_media_remote_root"), (("",),))
        self.configure()
        account = "'remote_file_no_admin'@'%'"
        self.cursor.execute("CREATE USER %s@'%%' IDENTIFIED BY %s",
                            ("remote_file_no_admin", "local-remote-privilege-fixture"))
        try:
            self.cursor.execute(f"GRANT SELECT ON {DATABASE}.* TO {account}")
            with pymysql.connect(
                    unix_socket=self.database.sql_socket, user="remote_file_no_admin",
                    password="local-remote-privilege-fixture", database=DATABASE,
                    autocommit=True, read_timeout=10, write_timeout=10, charset="utf8mb4") as guest:
                with guest.cursor() as cursor:
                    cursor.execute("SELECT @@GLOBAL.ai_media_remote_root")
                    self.assertEqual(cursor.fetchall(), ((self.storage.root,),))
                    with self.assertRaises(pymysql.MySQLError) as caught:
                        cursor.execute("SET GLOBAL ai_media_remote_root='https://example.invalid/'")
                    self.assertRegex(str(caught.exception).lower(),
                                     r"privilege|access denied|permission|not authorized|denied")
                    for statement in (
                            "SET SESSION ai_media_remote_root='https://example.invalid/'",
                            "SET ai_media_remote_root='https://example.invalid/'"):
                        sql_error(self, 1229, lambda statement=statement: cursor.execute(statement))
        finally:
            self.cursor.execute(f"DROP USER {account}")
        self.assertEqual(self.execute("SELECT @@GLOBAL.ai_media_remote_root"), ((self.storage.root,),))
        self.assert_storage([])

    def test_remote_map_exact_get_query_and_all_media_formats(self):
        self.configure()
        self.assertEqual(self.execute("SELECT @@GLOBAL.ai_media_root"), (("",),))
        assets = []
        for index, kind in enumerate(GENERATORS):
            target = (f"/media/a%20b-{index}%2A.bin"
                      "?signature=private-test%2B%2f&repeat=a+b&repeat=%252F&blank=")
            assets.append(self.storage.asset(kind, index, target=target))
        descriptors = [asset.descriptor(include_size=True, version=asset.version()) for asset in assets]
        self.insert([(index, f"row-{index}", value) for index, value in enumerate(descriptors)])
        projected = self.execute("SELECT id, FILE_URI(asset), asset FROM remote_values ORDER BY id")
        self.assert_storage([])
        for index, row in enumerate(projected):
            self.assertEqual(row[:2], (index, assets[index].uri))
            files.assert_file_value(self, row[2], descriptors[index], self.cursor.description[2])
        contents = []
        for index, asset in enumerate(assets):
            content = [files.text_block(f"row-{index}"), asset.block()]
            contents.append(content)
            self.seed(self.first, content, f"mapped-{index}", assets=[asset])
        self.assertEqual(self.execute(
            f"SELECT id, AI_MAP('{MODEL_A}', (text_value, asset)) FROM remote_values ORDER BY id"),
            tuple((index, f"mapped-{index}") for index in range(4)))
        self.assert_storage([asset.target for asset in assets])
        self.assert_provider(self.first, contents)
        self.assert_provider(self.second, [])
        self.assert_no_sockets()

    def test_remote_filter_join_and_mixed_text_vision_chain(self):
        self.configure(local=True)
        assets = [self.storage.asset("png", 101), self.storage.asset("jpeg", 102)]
        audio = self.media.asset("wav", 103)
        self.insert([(index, f"row-{index}", asset.descriptor()) for index, asset in enumerate(assets)])
        self.insert([(10, "right-audio", audio.descriptor())], table="remote_right")
        first_contents, second_contents = [], []
        for index, asset in enumerate(assets):
            inner = [files.text_block("inner"), asset.block()]
            outer = [files.text_block("outer"), asset.block(), files.text_block(f"inner-{index}")]
            self.seed(self.first, inner, f"inner-{index}", assets=[asset])
            self.seed(self.second, outer, f"outer-{index}", assets=[asset])
            first_contents.append(inner)
            second_contents.append(outer)
        self.assertEqual(self.execute(
            f"SELECT id, AI_MAP('{MODEL_B}', ('outer', asset, "
            f"AI_MAP('{MODEL_A}', ('inner', asset)))) FROM remote_values ORDER BY id"),
            ((0, "outer-0"), (1, "outer-1")))
        self.assert_storage([asset.target for asset in assets])
        inner_audit = {record["prompt"]: record for record in self.first.audit}
        for index, record in enumerate(self.second.audit):
            content = record["body"]["messages"][-1]["content"]
            asset = next(asset for asset in assets if asset.block() == content[1])
            inner = inner_audit[files.content_key([files.text_block("inner"), asset.block()])]
            self.assertIsNotNone(inner["response"])
            self.assertGreaterEqual(record["received"], inner["response"],
                                    "outer vision request preceded its own inner text result")
        for index, asset in enumerate(assets):
            content = [files.text_block("keep"), asset.block()]
            self.seed(self.first, content, index == 0, assets=[asset], boolean=True)
            first_contents.append(content)
        self.assertEqual(self.execute(
            f"SELECT id FROM remote_values WHERE AI_FILTER('{MODEL_A}', ('keep', asset)) ORDER BY id"),
            ((0,),))
        rule = "Match the left image to the right audio."
        for index, asset in enumerate(assets):
            content = [
                files.text_block("AI_JOIN rule:\n"), files.text_block(rule),
                files.text_block("\nLEFT INPUT:\n"), asset.block(),
                files.text_block("\nRIGHT INPUT:\n"), audio.block(),
            ]
            self.seed(self.first, content, index == 1, assets=[asset, audio], boolean=True)
            first_contents.append(content)
        self.assertEqual(self.execute(
            f"SELECT l.id, r.id FROM remote_values l JOIN remote_right r "
            f"ON AI_JOIN('{MODEL_A}', %s, l.asset, r.asset) ORDER BY l.id, r.id", (rule,)),
            ((1, 10),))
        self.assert_storage([asset.target for asset in assets] * 3)
        self.assert_provider(self.first, first_contents)
        self.assert_provider(self.second, second_contents)

    def test_long_signed_urls_and_metadata_survive_sql_materialization(self):
        self.configure()
        assets, descriptors = [], []
        for index, uri_size in enumerate((128, 256, 1024, 4096)):
            prefix = f"/media/long-{index}.png?signature="
            target = prefix + "a" * (uri_size - len(self.storage.origin) - len(prefix))
            asset = self.storage.asset(seed=140 + index, target=target)
            self.assertEqual(len(asset.uri), uri_size)
            value = asset.descriptor(include_size=True, version=asset.version())
            self.assertEqual(self.execute("SELECT FILE_URI(TO_FILE(%s))",
                                         (files.descriptor_text(value),)), ((asset.uri,),))
            assets.append(asset)
            descriptors.append(value)
        self.insert([(index, f"long-{index}", value)
                     for index, value in enumerate(descriptors)])
        projected = self.execute("SELECT id, FILE_URI(asset), asset FROM remote_values ORDER BY id")
        for index, row in enumerate(projected):
            self.assertEqual(row[:2], (index, assets[index].uri))
            files.assert_file_value(self, row[2], descriptors[index], self.cursor.description[2])
        self.reject("SELECT TO_FILE(%s)",
                    (files.descriptor_text({**descriptors[-1], "uri": assets[-1].uri + "a"}),))
        self.assert_storage([])
        contents = []
        for index, asset in enumerate(assets):
            content = [files.text_block(f"long-{index}"), asset.block()]
            self.seed(self.first, content, f"mapped-long-{index}", assets=[asset])
            contents.append(content)
        self.assertEqual(self.execute(
            f"SELECT id, AI_MAP('{MODEL_A}', (text_value, asset)) FROM remote_values ORDER BY id"),
            tuple((index, f"mapped-long-{index}") for index in range(len(assets))))
        self.assert_storage([asset.target for asset in assets])
        self.assert_provider(self.first, contents)
        self.assert_provider(self.second, [])
        self.assert_no_sockets()

    def test_remote_vision_map_aggregate_and_post_map_preserve_dependencies(self):
        self.configure()
        assets = [self.storage.asset("png", 131), self.storage.asset("jpeg", 132)]
        captions = ["caption-0", "caption-1"]
        contents = [[files.text_block("caption"), asset.block()] for asset in assets]
        self.insert([(index, "caption", asset.descriptor())
                     for index, asset in enumerate(assets)])
        for content, caption, asset in zip(contents, captions, assets):
            self.seed(self.first, content, caption, assets=[asset])
        request = {
            "operation": "ai_agg", "phase": "direct", "instruction": "summarize captions",
            "inputs": captions, "source_count": 2,
        }
        prompt = files.descriptor_text(request)
        summary = "combined captions"
        self.seed(self.second, prompt, summary)
        self.seed(self.first, summary, "checked summary")
        self.assertEqual(self.execute(
            f"SELECT AI_MAP('{MODEL_A}', AI_AGG('{MODEL_B}', "
            f"AI_MAP('{MODEL_A}', ('caption', asset)), 'summarize captions', %s)), COUNT(*) "
            "FROM (SELECT id, asset FROM remote_values ORDER BY id LIMIT 2) source",
            (files.descriptor_text(DEFAULT_OPTIONS),)), (("checked summary", 2),))
        self.assert_storage([asset.target for asset in assets])
        self.assert_provider(self.first, contents + [summary])
        self.assert_provider(self.second, [prompt])
        aggregate = self.second.requests[0]
        self.assertEqual(decode_request(aggregate["messages"][-1]["content"]), request)
        self.assertEqual(aggregate["messages"][0]["role"], "system")
        self.assertIn("For extract", aggregate["messages"][0]["content"])
        self.assertEqual(aggregate["max_tokens"], DEFAULT_OPTIONS["max_output_tokens"])
        self.assertLessEqual(
            sum(len(message["content"].encode("utf-8")) for message in aggregate["messages"]) +
            DEFAULT_OPTIONS["template_tokens"] + DEFAULT_OPTIONS["max_output_tokens"],
            DEFAULT_OPTIONS["context_window"])
        records = {record["prompt"]: record for record in self.first.audit}
        aggregate_record = self.second.audit[0]
        for content in contents:
            response = records[files.content_key(content)]["response"]
            self.assertIsNotNone(response)
            self.assertGreaterEqual(aggregate_record["received"], response)
        self.assertGreaterEqual(records[files.content_key(summary)]["received"],
                                aggregate_record["response"])
        for server in self.fixtures.mocks:
            for body in server.requests:
                for asset in assets:
                    self.assertNotIn(asset.uri, files.descriptor_text(body))

    def test_policy_prefix_and_local_denial_precede_storage_io(self):
        remote = self.storage.asset(seed=201)
        outside = self.storage.asset(seed=202, target="/media-sibling/image?signature=private-denial")
        local = self.media.asset("png", 203)
        self.configure(remote=False, local=True)
        self.reject_asset(remote.descriptor())
        self.configure()
        self.reject_asset(outside.descriptor())
        self.reject_asset(local.descriptor())
        with StorageServer() as other_origin:
            other = other_origin.asset(seed=204)
            self.reject_asset(other.descriptor())
            self.assertEqual(other_origin.counts(), Counter(), "origin/port denial occurred after I/O")
        self.assert_storage([])
        content = self.map_asset(remote, "recover-prefix", "allowed")
        self.assert_storage([remote.target])
        self.assert_provider(self.first, [content])

    def test_skipped_case_and_limit_zero_fetch(self):
        asset = self.storage.asset(seed=301, target="/media/skipped?signature=never-fetch-this")
        self.configure(remote=False)
        self.insert([(0, "skip", asset.descriptor())])
        self.assertEqual(self.execute(
            f"SELECT id, CASE WHEN id < 0 THEN AI_MAP('{MODEL_A}', asset) ELSE 'skipped' END, "
            f"CASE WHEN id < 0 THEN AI_FILTER('{MODEL_A}', asset) ELSE 0 END "
            "FROM remote_values ORDER BY id"), ((0, "skipped", 0),))
        for suffix in ("LIMIT 0", "ORDER BY id DESC LIMIT 0", "WHERE 1=0"):
            self.assertEqual(self.execute(
                f"SELECT AI_MAP('{MODEL_A}', asset) FROM remote_values {suffix}"), ())
        self.assertEqual(self.execute("SELECT FILE_URI(asset) FROM remote_values"), ((asset.uri,),))
        self.assert_storage([])
        self.assert_provider(self.first, [])
        self.assert_provider(self.second, [])

    def test_transfer_framing_size_caps_and_error_statuses(self):
        self.configure()
        targets, contents = [], []
        for index, mode in enumerate(("length", "chunked", "close")):
            asset = self.storage.asset(
                "wav", 401 + index, framing=mode, raw=streaming_wav_bytes())
            self.cursor.execute("SET ai_media_max_bytes=%s", (len(asset.raw),))
            contents.append(self.map_asset(
                asset, f"framing-{mode}", mode, value=asset.descriptor(include_size=True)))
            targets.append(asset.target)
            too_large = asset.descriptor(include_size=True)
            too_large["size_bytes"] += 1
            self.reject_asset(too_large)
            self.cursor.execute("SET ai_media_max_bytes=%s", (len(asset.raw) - 1,))
            self.reject_asset(asset.descriptor(include_mime=False))
            targets.append(asset.target)
        raw = files.png_bytes(410)
        self.cursor.execute("SET ai_media_max_bytes=%s", (len(raw),))
        self.storage.add("/media/large-header?sig=private-header", StorageReply(
            raw, content_length=len(raw) + 1))
        self.reject_asset({"format_version": 1, "uri": self.storage.origin +
                           "/media/large-header?sig=private-header"})
        targets.append("/media/large-header?sig=private-header")
        self.cursor.execute("SET ai_media_max_bytes=DEFAULT")
        self.storage.add("/media/truncated?sig=private-truncated", StorageReply(
            raw, content_length=len(raw) + 5))
        self.reject_asset({"format_version": 1, "uri": self.storage.origin +
                           "/media/truncated?sig=private-truncated"})
        targets.append("/media/truncated?sig=private-truncated")
        redirect_target = self.storage.asset(seed=411)
        for status in (302, 403, 404, 429, 503):
            target = f"/media/status-{status}?sig=private-status-{status}"
            self.storage.add(target, StorageReply(
                raw, status=status, headers={"Retry-After": "1", "Location": redirect_target.uri}))
            self.reject_asset({"format_version": 1, "uri": self.storage.origin + target})
            targets.append(target)
        self.assert_storage(targets)
        self.assert_provider(self.first, contents)

    def test_metadata_hash_and_container_errors_do_not_reach_provider(self):
        self.configure()
        valid = self.storage.asset(seed=501)
        content = self.map_asset(valid, "verified-content", "verified",
                                 value=valid.descriptor(include_size=True, version=valid.version()))
        targets = [valid.target]
        for index, field_name in enumerate(("mime_type", "size_bytes", "object_version")):
            asset = self.storage.asset(seed=510 + index)
            value = asset.descriptor(include_size=True, version=asset.version())
            if field_name == "mime_type":
                value[field_name] = "image/jpeg"
            elif field_name == "size_bytes":
                value[field_name] += 1
            else:
                value[field_name] = "sha256:" + "0" * 64
                self.storage.routes[asset.target].headers["ETag"] = '"' + value[field_name] + '"'
            self.reject_asset(value)
            targets.append(asset.target)
        for index, kind in enumerate(GENERATORS):
            generator, mime_type = GENERATORS[kind]
            target = f"/media/corrupt-{kind}?sig=private-container-{index}"
            self.storage.add(target, StorageReply(generator(520 + index)[:8]))
            self.reject_asset({"format_version": 1, "uri": self.storage.origin + target,
                               "mime_type": mime_type})
            targets.append(target)
        unsupported = self.storage.asset(seed=530)
        for metadata in ({"mime_type": "application/octet-stream"},
                         {"object_version": "etag:must-not-be-trusted"}):
            self.reject_asset({**unsupported.descriptor(include_mime=False), **metadata})
        self.assert_storage(targets)
        self.assert_provider(self.first, [content])
        self.assert_no_sockets()

    def test_held_downloads_cancel_timeout_and_socket_cleanup(self):
        self.configure()
        assets = [self.storage.asset(seed=601 + index, gate=threading.Event()) for index in range(4)]
        self.insert([(index, "held", asset.descriptor()) for index, asset in enumerate(assets)])
        pending = self.fixtures.start(lambda: self.execute(
            f"SELECT id, AI_MAP('{MODEL_A}', (text_value, asset)) FROM remote_values ORDER BY id"))
        self.storage.wait(lambda: len(self.storage.requests) == 4 and self.storage.active == 4,
                          "query did not start four held media GETs")
        self.assertFalse(pending.done.is_set(), repr(pending.error))
        self.assert_provider(self.first, [])
        self.kill_query()
        with self.assertRaises(pymysql.MySQLError) as caught:
            pending.result(timeout=8)
        self.assertEqual(caught.exception.args[0], 1317, caught.exception.args)
        self.storage.wait(
            lambda: self.storage.active == 0 and
            sum(record["disconnected"] for record in self.storage.requests) == 4,
            "KILL QUERY did not close held storage sockets before gate release")
        self.assertTrue(all(not self.storage.routes[asset.target].gate.is_set() for asset in assets))
        self.assert_no_sockets()
        self.assertEqual(self.execute("SELECT 1"), ((1,),))
        self.cursor.execute("TRUNCATE TABLE remote_values")
        timed = self.storage.asset(seed=610, gate=threading.Event())
        self.insert([(0, "timeout", timed.descriptor())])
        self.cursor.execute("SET ob_query_timeout=1500000")
        timed_query = self.fixtures.start(lambda: self.execute(
            f"SELECT AI_MAP('{MODEL_A}', asset) FROM remote_values"))
        self.storage.wait(lambda: len(self.storage.requests) == 5,
                          "deadline test never started its held GET")
        with self.assertRaises(pymysql.MySQLError) as caught:
            timed_query.result(timeout=8)
        self.assertEqual(caught.exception.args[0], 4012, caught.exception.args)
        self.storage.wait(lambda: self.storage.active == 0 and self.storage.requests[-1]["disconnected"],
                          "query deadline did not close the held storage socket")
        self.assertFalse(self.storage.routes[timed.target].gate.is_set())
        self.assert_no_sockets()
        self.cursor.execute("SET ob_query_timeout=20000000")
        recovered = self.storage.asset(seed=620)
        content = self.map_asset(recovered, "post-cancel-recovery", "recovered")
        self.assert_storage([asset.target for asset in assets] + [timed.target, recovered.target])
        self.assert_provider(self.first, [content])


def case_names(test_class):
    return unittest.defaultTestLoader.getTestCaseNames(test_class)


def parse_options(arguments=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, help="Executable seekdb for a new private database")
    parser.add_argument("--case", action="append", default=[], help="Test method; repeat to select")
    parser.add_argument("--self-test", action="store_true", help="Run fixtures without SQL")
    parser.add_argument("--list-cases", action="store_true", help="List without starting any service")
    options = parser.parse_args(arguments)
    test_class = RemoteFileHarnessTests if options.self_test else RemoteFileContracts
    unknown = [name for name in options.case if name not in case_names(test_class)]
    if unknown:
        parser.error("unknown case(s): " + ", ".join(unknown))
    if options.self_test and options.binary is not None:
        parser.error("--self-test cannot be combined with --binary")
    if not options.self_test and not options.list_cases and options.binary is None:
        parser.error("--binary is required unless --self-test or --list-cases is selected")
    return options


def run_suite(test_class, selected):
    suite = unittest.TestSuite(test_class(name) for name in dict.fromkeys(selected or case_names(test_class)))
    if not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful():
        raise SystemExit(1)


def print_engine_diagnostics(database):
    diagnostics = deque(maxlen=24)
    for log_file in (database.base / "log").rglob("*.log*"):
        if not log_file.is_file():
            continue
        with log_file.open(errors="replace") as engine_log:
            for line in engine_log:
                if any(f"] {level} " in line for level in ("WARN", "ERROR", "WDIAG", "EDIAG")) and (
                        "[SQL" in line or "semantic" in line.lower() or "-4013" in line):
                    diagnostics.append(line.rstrip()[:2000])
    if diagnostics:
        print("Private remote FILE engine diagnostics:", file=sys.stderr)
        print("\n".join(diagnostics), file=sys.stderr)


def main(arguments=None):
    options = parse_options(arguments)
    test_class = RemoteFileHarnessTests if options.self_test else RemoteFileContracts
    if options.list_cases:
        for name in dict.fromkeys(options.case or case_names(test_class)):
            print(name)
    elif options.self_test:
        run_suite(test_class, options.case)
    else:
        binary = options.binary.resolve()
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise SystemExit(f"--binary is not an executable file: {binary}")
        database = files.PrivateDatabase(binary)
        checks_completed = False
        try:
            database.start()
            RemoteFileContracts.database = database
            RemoteFileContracts.models_created = False
            run_suite(RemoteFileContracts, options.case)
            checks_completed = True
        finally:
            try:
                if not checks_completed:
                    print_engine_diagnostics(database)
            finally:
                database.close()


if __name__ == "__main__":
    main()
