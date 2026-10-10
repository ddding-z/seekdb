"""Opt-in, loopback-only real-provider semantic and native FILE contracts.

--endpoint and --model are mandatory for any provider traffic. --preflight-only
does not start SQL; --self-test and --list-cases do not contact a real provider.
SQL uses test_file_runtime.PrivateDatabase, never an existing database.
No model service, weights, environment, or production files are managed here.
"""

import argparse
import base64
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from http.client import HTTPConnection, HTTPSConnection, HTTPException, IncompleteRead
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import socket
import stat
import struct
import sys
import tempfile
import threading
import time
from urllib.parse import quote, quote_plus, urlsplit
import zlib

import pymysql

from test_file_runtime import (
    MEDIA_MAX_BYTES, MediaAsset, PrivateDatabase, content_key, decode_descriptor,
    descriptor_text, join_content, strict_json, text_block,
)
from test_semantic_runtime import FILTER_SCHEMA


MODEL_NAME = "semantic_live_text"
IMAGE_MODEL_NAME = "semantic_live_image"
DATABASE = "ai_semantic_live_test"
REGISTRATION_KEY = "semantic-live-loopback-only"
MAX_REQUEST_BYTES = 4 * 1024 * 1024
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
DIAGNOSTIC_RECORDS = 16
DIAGNOSTIC_CHARS = 2048
DIAGNOSTIC_OUTPUT_CHARS = 48 * 1024
HTTP_TIMEOUT_SECONDS = 15.0
QUERY_TIMEOUT_SECONDS = 60
OPTIONS = {"temperature": 0, "max_tokens": 32, "stream": False}
LABELS = ("ALPHA", "BETA", "DROP")
LEFT_ROWS = ((11, "ALPHA"), (22, "BETA"), (33, "ALPHA"), (44, "DROP"))
RIGHT_ROWS = ((101, "ALPHA"), (102, "BETA"))
LABEL_SCHEMA = {
    "type": "object",
    "properties": {"label": {"type": "string", "enum": list(LABELS)}},
    "required": ["label"],
    "additionalProperties": False,
}
MAP_PREFIX = (
    'Copy the single label after LABEL= into {"label": <label>}. '
    "The label is ALPHA, BETA, or DROP. Return only that JSON object. LABEL="
)
FILTER_PREFIX = (
    'Return only {"value":true} if the single label after LABEL= is ALPHA, '
    'otherwise return only {"value":false}. LABEL='
)
JOIN_RULE = (
    'Return only {"value":true} when left and right are the same exact label, '
    'otherwise return only {"value":false}. Each input is ALPHA, BETA, or DROP.'
)
TEXT_CASES = (
    "text-map", "text-filter-projection", "text-filter-where", "text-join",
    "composed-group", "composed-join-map",
)
IMAGE_CASES = ("file-map", "file-filter-projection", "file-filter-where", "file-join")


class RunFailure(Exception):
    category = "runner"
    exit_code = 6


class SafetyFailure(RunFailure):
    category = "safety"
    exit_code = 2


class ProtocolFailure(RunFailure):
    category = "provider-protocol"
    exit_code = 4


class ModelQualityFailure(RunFailure):
    category = "model-quality"
    exit_code = 3


class SqlFailure(RunFailure):
    category = "sql-integration"
    exit_code = 5


class LifecycleFailure(RunFailure):
    category = "private-runner-lifecycle"


@dataclass(frozen=True)
class Endpoint:
    scheme: str
    host: str
    port: int
    connect_host: str

    @property
    def authority(self):
        host = f"[{self.host}]" if ":" in self.host else self.host
        return f"{host}:{self.port}"

    def connection(self, timeout):
        if self.scheme == "https":
            return LoopbackHTTPSConnection(self, timeout)
        return HTTPConnection(self.connect_host, self.port, timeout=timeout)


class LoopbackHTTPSConnection(HTTPSConnection):
    def __init__(self, endpoint, timeout):
        super().__init__(endpoint.connect_host, endpoint.port, timeout=timeout)
        self.tls_name = endpoint.host

    def connect(self):
        # Pin localhost to a numeric loopback address without changing TLS identity.
        deadline = time.monotonic() + self.timeout
        HTTPConnection.connect(self)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("HTTPS connection deadline expired")
        self.sock.settimeout(remaining)
        self.sock = self._context.wrap_socket(self.sock, server_hostname=self.tls_name)


def parse_endpoint(value):
    message = ("use an HTTP(S) loopback origin, /v1, or /v1/chat/completions URL "
               "without credentials, query, fragment, or non-loopback host")
    try:
        if not value or any(character.isspace() or ord(character) < 32 or ord(character) == 127
                            for character in value):
            raise ValueError(message)
        parsed = urlsplit(value)
        if (parsed.scheme not in ("http", "https") or not parsed.hostname
                or parsed.username is not None or parsed.password is not None
                or parsed.query or parsed.fragment or "?" in value or "#" in value
                or parsed.path not in ("", "/", "/v1", "/v1/", "/v1/chat/completions")):
            raise ValueError(message)
        host = parsed.hostname
        if host == "localhost":
            address = "127.0.0.1"
        else:
            parsed_address = ipaddress.ip_address(host)
            address = str(parsed_address)
            if (not parsed_address.is_loopback or "%" in host
                    or isinstance(parsed_address, ipaddress.IPv6Address)
                    and parsed_address.ipv4_mapped is not None):
                raise ValueError(message)
        port = parsed.port if parsed.port is not None else (443 if parsed.scheme == "https" else 80)
        if not 1 <= port <= 65535:
            raise ValueError(message)
        return Endpoint(parsed.scheme, host, port, address)
    except ValueError:
        raise argparse.ArgumentTypeError(message) from None


def bounded_timeout(value):
    try:
        result = float(value)
        if not math.isfinite(result) or not 0 < result <= 60:
            raise ValueError
        return result
    except ValueError:
        raise argparse.ArgumentTypeError("--timeout must be finite and in (0, 60] seconds") from None


def parse_options(arguments=None):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--endpoint", type=parse_endpoint,
                        help="Explicit local origin, /v1 base, or chat endpoint; remote URLs are rejected")
    parser.add_argument("--model", help="Exact served model ID, checked against /v1/models and replies")
    parser.add_argument("--binary", type=Path, help="Built seekdb executable for a new private database")
    parser.add_argument("--api-key-env", help="Name of the environment variable holding a credential")
    parser.add_argument("--timeout", type=bounded_timeout, default=HTTP_TIMEOUT_SECONDS,
                        help=f"Per-HTTP-transfer timeout in seconds, at most 60 "
                             f"(default: {HTTP_TIMEOUT_SECONDS:g})")
    parser.add_argument("--image", help="Explicit local PNG/JPEG path, or synthetic-red")
    parser.add_argument("--instruction", help="Explicit yes/no question about the selected image")
    parser.add_argument("--expect-image", choices=("true", "false"),
                        help="Independent expected answer; required with --image")
    parser.add_argument("--case", action="append", default=[],
                        help="Select a SQL contract by name; repeat to select multiple")
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--self-test", action="store_true", help="Run DB-free fake-provider tests")
    modes.add_argument("--list-cases", action="store_true", help="List contracts without any traffic")
    modes.add_argument("--preflight-only", action="store_true", help="Direct HTTP checks without SQL")
    options, unknown = parser.parse_known_args(arguments)
    if unknown:
        parser.error("unrecognized option(s); credentials are accepted only through --api-key-env")
    live_fields = (options.endpoint, options.model, options.binary, options.api_key_env,
                   options.image, options.instruction, options.expect_image, options.case)
    if options.self_test or options.list_cases:
        if any(value is not None and value != [] for value in live_fields):
            parser.error("offline modes cannot be combined with provider, SQL, or image arguments")
        return options
    if options.endpoint is None or options.model is None:
        parser.error("--endpoint and --model are required for real-provider requests")
    if (not options.model or len(options.model) > 512
            or any(character.isspace() or ord(character) < 32 or ord(character) == 127
                   for character in options.model)):
        parser.error("--model must be a nonempty served ID without whitespace or control characters")
    if options.api_key_env is not None and not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", options.api_key_env):
        parser.error("--api-key-env must be an environment variable name, not a credential")
    if options.image is not None:
        if not options.image or not options.instruction or options.expect_image is None:
            parser.error("--image requires --instruction and --expect-image true|false")
        if len(options.instruction) > 4096 or "\x00" in options.instruction or not options.instruction.strip():
            parser.error("--instruction must be nonempty text of at most 4096 characters without NUL")
    elif options.instruction is not None or options.expect_image is not None:
        parser.error("--instruction and --expect-image require --image")
    if any(name not in TEXT_CASES + IMAGE_CASES for name in options.case):
        parser.error("unknown --case; use --list-cases")
    if len(set(options.case)) != len(options.case):
        parser.error("duplicate --case would repeat real-provider traffic")
    if options.image is None and any(name in IMAGE_CASES for name in options.case):
        parser.error("FILE contracts require explicit --image, --instruction, and --expect-image")
    if options.preflight_only:
        if options.binary is not None or options.case:
            parser.error("--preflight-only cannot be combined with --binary or --case")
    elif options.binary is None:
        parser.error("--binary is required for SQL contracts")
    elif not options.binary.is_file() or not os.access(options.binary, os.X_OK):
        parser.error("--binary must be an existing executable file")
    return options


def credential_from_environment(name):
    if name is None:
        return None
    value = os.environ.get(name)
    if (not value or len(value) > 4096 or not value.isascii()
            or any(character.isspace() or ord(character) < 32 or ord(character) == 127
                   for character in value)):
        raise SafetyFailure("the named credential environment variable must contain a nonempty "
                            "single-line credential without whitespace (at most 4096 characters)")
    return value


class Redactor:
    def __init__(self, credential=None):
        secrets = [REGISTRATION_KEY]
        if credential is not None:
            secrets.append(credential)
        variants = set()
        for secret in secrets:
            variants.update((secret, json.dumps(secret)[1:-1],
                             json.dumps(secret, ensure_ascii=False)[1:-1],
                             quote(secret, safe=""), quote_plus(secret, safe=""),
                             base64.b64encode(secret.encode("utf-8")).decode("ascii")))
        self.variants = sorted(variants, key=len, reverse=True)

    def bounded(self, value, limit=DIAGNOSTIC_CHARS):
        text = str(value)
        for secret in self.variants:
            text = text.replace(secret, "[REDACTED]")
        text = re.sub(r'(?i)("(?:authorization|access_key|api_key|token)"\s*:\s*)'
                      r'"(?:\\.|[^"\\])*"', r'\1"[REDACTED]"', text)
        text = re.sub(r"(?i)\bBearer[ \t]+[^\s\"\\,}]+", "Bearer [REDACTED]", text)
        if len(text) > limit:
            text = text[:limit] + "...[truncated]"
        return text


@dataclass
class Observation:
    phase: str
    status: int | None
    raw_bytes: int
    raw: str
    failure: str | None = None


class Diagnostics:
    def __init__(self, redactor):
        self.redactor = redactor
        self.lock = threading.Lock()
        self.records = deque(maxlen=DIAGNOSTIC_RECORDS)
        self.first_failure = None
        self.first_failed_record = None

    def observe(self, phase, status, raw):
        record = Observation(phase, status, len(raw), self.redactor.bounded(
            raw.decode("utf-8", errors="backslashreplace")))
        with self.lock:
            self.records.append(record)
        return record

    def fail(self, record, error):
        with self.lock:
            record.failure = self.redactor.bounded(f"{error.category}: {error}", 512)
            if self.first_failure is None:
                self.first_failure = error
                self.first_failed_record = record

    def raise_if_failed(self, context=None):
        with self.lock:
            error = self.first_failure
        if error is not None:
            if context is not None:
                raise type(error)(f"{error}; {context}") from error
            raise error

    def write(self, output):
        with self.lock:
            records = list(self.records)
            if self.first_failed_record is not None:
                if self.first_failed_record in records:
                    records.remove(self.first_failed_record)
                records.insert(0, self.first_failed_record)
        print("Observed upstream responses (bounded, credential-redacted; no generated replacements):",
              file=output)
        remaining = DIAGNOSTIC_OUTPUT_CHARS
        for record in records:
            line = json.dumps(record.__dict__, ensure_ascii=True)
            if len(line) + 1 > remaining:
                print("[remaining diagnostic records omitted by the output bound]", file=output)
                break
            print(line, file=output)
            remaining -= len(line) + 1


@dataclass(frozen=True)
class HttpReply:
    status: int
    raw: bytes = field(repr=False)
    observation: Observation = field(repr=False)


class ProviderClient:
    def __init__(self, endpoint, model, credential, timeout, diagnostics):
        self.endpoint = endpoint
        self.model = model
        self.credential = credential
        self.timeout = timeout
        self.diagnostics = diagnostics

    def exchange(self, method, path, *, raw=None, phase):
        connection = self.endpoint.connection(self.timeout)
        headers = {"Host": self.endpoint.authority, "Content-Type": "application/json",
                   "Accept": "application/json", "Accept-Encoding": "identity"}
        if self.credential is not None:
            headers["Authorization"] = "Bearer " + self.credential
        response_body = bytearray()
        status = None
        response = None
        timer = None
        expired = threading.Event()
        finished = threading.Event()
        deadline = time.monotonic() + self.timeout
        try:
            connection.connect()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("HTTP transfer deadline expired during connect")
            owned_socket = connection.sock
            if owned_socket is None:
                raise LifecycleFailure("HTTP connection did not acquire its own socket")
            owned_socket.settimeout(remaining)

            def expire():
                if not finished.is_set():
                    expired.set()
                    try:
                        owned_socket.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        # The owner may have closed the socket at the same deadline.
                        pass

            timer = threading.Timer(remaining, expire)
            timer.name = "semantic-live-http-deadline"
            timer.start()
            connection.request(method, path, body=raw, headers=headers)
            response = connection.getresponse()
            status = response.status
            while len(response_body) < MAX_RESPONSE_BYTES + 1:
                chunk = response.read1(min(65536, MAX_RESPONSE_BYTES + 1 - len(response_body)))
                if not chunk:
                    if response.length not in (None, 0):
                        raise IncompleteRead(b"", response.length)
                    break
                response_body.extend(chunk)
            finished.set()
            if expired.is_set() or time.monotonic() >= deadline:
                raise TimeoutError("HTTP transfer deadline expired")
        except (OSError, HTTPException) as error:
            if isinstance(error, IncompleteRead):
                response_body.extend(error.partial[:MAX_RESPONSE_BYTES + 1 - len(response_body)])
            detail = ("TimeoutError: HTTP transfer deadline expired" if expired.is_set()
                      else f"{type(error).__name__}: {error}")
            failure = ProtocolFailure(f"{phase}: HTTP transfer failed: {detail}")
            record = self.diagnostics.observe(phase, status, bytes(response_body))
            self.diagnostics.fail(record, failure)
            raise failure from error
        finally:
            finished.set()
            if timer is not None:
                timer.cancel()
                if timer.ident is not None:
                    timer.join()
            try:
                if response is not None:
                    response.close()
            finally:
                connection.close()
        record = self.diagnostics.observe(phase, status, bytes(response_body))
        if len(response_body) > MAX_RESPONSE_BYTES:
            failure = ProtocolFailure(f"{phase}: response exceeds the 8 MiB limit")
            self.diagnostics.fail(record, failure)
            raise failure
        return HttpReply(status, bytes(response_body), record)


def output_format(kind, name):
    if kind not in ("label", "boolean"):
        raise ValueError("unknown output contract")
    return {"type": "json_schema", "json_schema": {
        "name": name, "strict": True, "schema": LABEL_SCHEMA if kind == "label" else FILTER_SCHEMA,
    }}


def request_body(model, content, kind, name):
    return {"model": model, "messages": [{"role": "user", "content": content}],
            **OPTIONS, "response_format": output_format(kind, name)}


def parse_object(value, context):
    try:
        result = strict_json(value)
    except (ValueError, UnicodeDecodeError, RecursionError) as error:
        raise ProtocolFailure(f"{context}: invalid strict JSON ({type(error).__name__})") from error
    if not isinstance(result, dict):
        raise ProtocolFailure(f"{context}: expected a JSON object")
    return result


def parse_value(content, kind):
    if not isinstance(content, str) or not content:
        raise ProtocolFailure("completion content must be nonempty JSON text")
    value = parse_object(content, "completion content")
    if kind == "boolean":
        if set(value) != {"value"} or type(value["value"]) is not bool:
            raise ProtocolFailure('Boolean output must be exactly {"value":true|false}')
        return value["value"]
    if kind != "label":
        raise ValueError("unknown output contract")
    if set(value) != {"label"} or type(value["label"]) is not str or value["label"] not in LABELS:
        raise ProtocolFailure('MAP output must be exactly {"label":"ALPHA|BETA|DROP"}')
    return value["label"]


def completion_value(raw, model, kind):
    envelope = parse_object(raw, "provider response")
    if envelope.get("model") != model:
        raise ProtocolFailure("completion model identity does not exactly match --model")
    choices = envelope.get("choices")
    if not isinstance(choices, list) or len(choices) != 1 or not isinstance(choices[0], dict):
        raise ProtocolFailure("completion must contain exactly one choice")
    choice = choices[0]
    if choice.get("finish_reason") != "stop":
        raise ProtocolFailure('completion requires finish_reason="stop"; no truncation/tool fallback')
    message = choice.get("message")
    if not isinstance(message, dict):
        raise ProtocolFailure("completion message must be an object")
    tool_calls = message.get("tool_calls")
    if (message.get("refusal") is not None
            or tool_calls is not None and (not isinstance(tool_calls, list) or len(tool_calls) != 0)
            or message.get("function_call") is not None):
        raise ProtocolFailure("completion refusal or tool/function call is not a semantic result")
    return parse_value(message.get("content"), kind)


@dataclass(frozen=True)
class OutputContract:
    kind: str
    expected: str | bool
    subject: str = "deterministic input"

    def check(self, raw, model):
        try:
            actual = completion_value(raw, model, self.kind)
        except ProtocolFailure as error:
            raise ProtocolFailure(f"{self.subject}: {error}") from error
        if type(actual) is not type(self.expected) or actual != self.expected:
            raise ModelQualityFailure(f"{self.subject}: well-formed {self.kind} answer was {actual!r}, "
                                      f"expected {self.expected!r}; schema validity is not answer correctness")


@dataclass(frozen=True)
class ImageInput:
    asset: MediaAsset = field(repr=False)
    instruction: str = field(repr=False)
    expected: bool

    def content(self):
        return [text_block(self.instruction), self.asset.block()]

    def join_rule(self):
        return ("Answer the following yes/no question about the image in LEFT INPUT:\n"
                + self.instruction + "\nRIGHT INPUT is an expected answer, true or false, "
                'not evidence about the image. Return only {"value":true} if the actual '
                'answer equals RIGHT INPUT, otherwise return only {"value":false}.')


def synthetic_red_png():
    def chunk(kind, raw):
        return struct.pack(">I", len(raw)) + kind + raw + struct.pack(
            ">I", zlib.crc32(kind + raw) & 0xffffffff)

    pixels = (b"\x00" + b"\xff\x00\x00" * 128) * 128
    return (b"\x89PNG\r\n\x1a\n"
            + chunk(b"IHDR", struct.pack(">IIBBBBB", 128, 128, 8, 2, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(pixels)) + chunk(b"IEND", b""))


def read_selected_image(value):
    if "://" in value:
        raise SafetyFailure("--image must be a local regular PNG/JPEG file, not a URI")
    path = Path(value).absolute()
    directory = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        for part in path.parts[1:-1]:
            next_directory = os.open(
                part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=directory)
            os.close(directory)
            directory = next_directory
        descriptor = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC,
                             dir_fd=directory)
        try:
            before = os.fstat(descriptor)
            if not stat.S_ISREG(before.st_mode) or not 0 < before.st_size <= MEDIA_MAX_BYTES:
                raise SafetyFailure("--image must be a nonempty regular file of at most 2 MiB")
            parts = []
            size = 0
            while size <= MEDIA_MAX_BYTES:
                part = os.read(descriptor, min(65536, MEDIA_MAX_BYTES + 1 - size))
                if not part:
                    break
                parts.append(part)
                size += len(part)
            raw = b"".join(parts)
            after = os.fstat(descriptor)
            if (len(raw) != before.st_size or before.st_size != after.st_size
                    or before.st_mtime_ns != after.st_mtime_ns
                    or before.st_ctime_ns != after.st_ctime_ns):
                raise SafetyFailure("--image changed while being copied into the private media root")
            return raw
        finally:
            os.close(descriptor)
    except OSError as error:
        raise SafetyFailure(f"cannot read selected regular image without following symlinks: "
                            f"{error.strerror}") from error
    finally:
        os.close(directory)


def prepare_image(options, root):
    if options.image is None:
        return None
    raw = synthetic_red_png() if options.image == "synthetic-red" else read_selected_image(options.image)
    if raw.startswith(b"\x89PNG\r\n\x1a\n"):
        mime_type, suffix = "image/png", "png"
    elif raw.startswith(b"\xff\xd8\xff") and raw.endswith(b"\xff\xd9"):
        mime_type, suffix = "image/jpeg", "jpg"
    else:
        raise SafetyFailure("--image has no supported PNG/JPEG container signature")
    root.mkdir()
    path = root / f"selected.{suffix}"
    path.write_bytes(raw)
    instruction = (options.instruction + '\nReturn exactly one JSON object {"value":true} or '
                   '{"value":false}, with no additional keys, explanation, or Markdown.')
    return ImageInput(MediaAsset(path, raw, mime_type), instruction, options.expect_image == "true")


def semantic_key(content):
    if isinstance(content, str) and content.startswith("{"):
        parsed = parse_object(content, "SQL AI_JOIN prompt")
        if list(parsed) != ["rule", "left", "right"]:
            raise SqlFailure("SQL AI_JOIN must preserve rule/left/right roles")
        return content_key(parsed)
    return content_key(content)


class RequestPlan:
    def __init__(self, model, image=None):
        self.model = model
        self.contracts = {}
        for label in LABELS:
            self.add(MAP_PREFIX + label, "label", label, f"MAP label={label}")
            self.add(FILTER_PREFIX + label, "boolean", label == "ALPHA", f"FILTER label={label}")
        for left in LABELS:
            for right in LABELS:
                self.add(descriptor_text({"rule": JOIN_RULE, "left": left, "right": right}),
                         "boolean", left == right, f"JOIN left={left} right={right}")
        if image is not None:
            self.add(image.content(), "boolean", image.expected, "explicit FILE question")
            for value in ("true", "false"):
                self.add(join_content(image.join_rule(), image.asset, value),
                         "boolean", image.expected == (value == "true"),
                         f"FILE JOIN right={value}")

    def add(self, content, kind, expected, subject):
        self.contracts[semantic_key(content)] = OutputContract(kind, expected, subject)

    def contract_for(self, body):
        if not isinstance(body, dict) or body.get("model") != self.model:
            raise SqlFailure("outbound SQL request changed the explicitly selected model")
        messages = body.get("messages")
        if (not isinstance(messages, list) or len(messages) != 1
                or not isinstance(messages[0], dict) or messages[0].get("role") != "user"):
            raise SqlFailure("outbound SQL request must contain only the planned user input")
        content = messages[0].get("content")
        if not isinstance(content, (str, list)):
            raise SqlFailure("outbound SQL request has no text or ordered FILE content")
        contract = self.contracts.get(semantic_key(content))
        if contract is None:
            raise SqlFailure("unplanned SQL prompt/media was blocked before reaching the provider")
        native = body.get("response_format")
        if not isinstance(native, dict) or native.get("type") != "json_schema":
            raise SqlFailure("outbound SQL request lost its native strict response_format")
        definition = native.get("json_schema")
        schema = LABEL_SCHEMA if contract.kind == "label" else FILTER_SCHEMA
        if (not isinstance(definition, dict) or definition.get("strict") is not True
                or not isinstance(definition.get("name"), str) or not definition["name"]
                or content_key(definition.get("schema")) != content_key(schema) or "Schema" in body
                or any(type(body.get(key)) is not type(value) or body.get(key) != value
                       for key, value in OPTIONS.items())):
            raise SqlFailure("outbound SQL request changed its strict schema or fixed inference options")
        return contract


def validate_reply(reply, contract, client):
    try:
        if reply.status != 200:
            raise ProtocolFailure(f"{contract.subject}: provider returned HTTP {reply.status}; "
                                  "redirects are never followed")
        contract.check(reply.raw, client.model)
    except RunFailure as error:
        failure = type(error)(f"{reply.observation.phase}: {error}")
        client.diagnostics.fail(reply.observation, failure)
        raise failure from error


def preflight(client, image=None):
    health = client.exchange("GET", "/health", phase="preflight health")
    if health.status != 200:
        error = ProtocolFailure(f"provider /health returned HTTP {health.status}")
        client.diagnostics.fail(health.observation, error)
        raise error
    models = client.exchange("GET", "/v1/models", phase="preflight model identity")
    try:
        if models.status != 200:
            raise ProtocolFailure(f"provider /v1/models returned HTTP {models.status}")
        payload = parse_object(models.raw, "model list")
        entries = payload.get("data")
        if (not isinstance(entries, list) or not entries
                or any(not isinstance(entry, dict) or not isinstance(entry.get("id"), str)
                       or not entry["id"] for entry in entries)):
            raise ProtocolFailure("model list requires nonempty data entries with string IDs")
        identities = [entry["id"] for entry in entries]
        if len(set(identities)) != len(identities) or client.model not in identities:
            raise ProtocolFailure("--model must appear exactly once in /v1/models; no automatic selection")
    except RunFailure as error:
        client.diagnostics.fail(models.observation, error)
        raise
    probes = [
        ("MAP ALPHA", MAP_PREFIX + "ALPHA", "label", "ALPHA", "semantic_live_map"),
        ("MAP BETA", MAP_PREFIX + "BETA", "label", "BETA", "semantic_live_map"),
        ("FILTER true", FILTER_PREFIX + "ALPHA", "boolean", True, "ai_filter"),
        ("FILTER false", FILTER_PREFIX + "BETA", "boolean", False, "ai_filter"),
        ("JOIN true", descriptor_text({"rule": JOIN_RULE, "left": "ALPHA", "right": "ALPHA"}),
         "boolean", True, "ai_join"),
        ("JOIN false", descriptor_text({"rule": JOIN_RULE, "left": "ALPHA", "right": "BETA"}),
         "boolean", False, "ai_join"),
    ]
    if image is not None:
        probes.append(("FILE image", image.content(), "boolean", image.expected, "ai_filter"))
    for name, content, kind, expected, schema_name in probes:
        reply = client.exchange("POST", "/v1/chat/completions", raw=descriptor_text(
            request_body(client.model, content, kind, schema_name)).encode("utf-8"),
            phase="preflight " + name)
        validate_reply(reply, OutputContract(kind, expected, name), client)


class RelayServer(ThreadingHTTPServer):
    request_queue_size = 64
    daemon_threads = False

    def __init__(self, client, plan):
        super().__init__(("127.0.0.1", 0), RelayHandler)
        self.client = client
        self.plan = plan
        self.phase = "SQL"
        self.thread = None

    @property
    def url(self):
        return f"http://127.0.0.1:{self.server_port}/v1/chat/completions"

    def start(self):
        self.thread = threading.Thread(target=self.serve_forever, name="semantic-live-observer")
        self.thread.start()
        connection = HTTPConnection("127.0.0.1", self.server_port, timeout=2)
        try:
            connection.request("GET", "/ready")
            response = connection.getresponse()
            if response.status != 204 or response.read() != b"":
                raise LifecycleFailure("private observation relay did not become responsive")
        finally:
            connection.close()

    def close(self):
        if self.thread is not None:
            self.shutdown()
        self.server_close()
        if self.thread is not None:
            self.thread.join(5)
            if self.thread.is_alive():
                raise LifecycleFailure("private observation relay left a live server thread")

    def handle_error(self, _request, _address):
        error = sys.exception()
        failure = LifecycleFailure(f"private observation relay handler failed: {type(error).__name__}")
        record = self.client.diagnostics.observe(self.phase + " relay failure", None, b"")
        self.client.diagnostics.fail(record, failure)


class RelayHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def setup(self):
        super().setup()
        self.connection.settimeout(self.server.client.timeout + 5)

    def log_message(self, *_arguments):
        pass

    def send_raw(self, status, raw):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        try:
            self.wfile.write(raw)
        except (BrokenPipeError, ConnectionResetError):
            # SQL cancellation cannot retract a request already accepted upstream.
            self.server.client.diagnostics.observe(
                self.server.phase + " downstream closed", status, b"SQL client disconnected")

    def do_GET(self):
        self.send_raw(204 if self.path == "/ready" else 404, b"")

    def do_POST(self):
        server = self.server
        phase = server.phase
        try:
            if (self.path != "/v1/chat/completions"
                    or self.headers.get("Authorization") != "Bearer " + REGISTRATION_KEY
                    or self.headers.get("Transfer-Encoding") is not None):
                raise SqlFailure("private relay rejected an unauthorized path/header")
            length = self.headers.get("Content-Length", "")
            if not length.isdecimal() or not 0 < int(length) <= MAX_REQUEST_BYTES:
                raise SqlFailure("private relay rejected a missing/oversized SQL request length")
            raw = self.rfile.read(int(length))
            if len(raw) != int(length):
                raise SqlFailure("private relay received a truncated SQL request")
            contract = server.plan.contract_for(parse_object(raw, "outbound SQL request"))
        except (RunFailure, OSError) as error:
            failure = error if isinstance(error, RunFailure) else SqlFailure(
                f"private relay request read failed: {type(error).__name__}")
            record = server.client.diagnostics.observe(phase + " blocked", None, b"")
            server.client.diagnostics.fail(record, failure)
            self.send_raw(400, b'{"error":{"message":"private live runner blocked invalid SQL traffic"}}')
            return
        try:
            reply = server.client.exchange("POST", "/v1/chat/completions", raw=raw, phase=phase)
        except ProtocolFailure:
            self.send_raw(502, b'{"error":{"message":"private live runner upstream transfer failed"}}')
            return
        try:
            validate_reply(reply, contract, server.client)
        except RunFailure:
            # Keep the actual status/body even when our independent oracle rejects it.
            pass
        self.send_raw(reply.status, reply.raw)


@dataclass(frozen=True)
class SqlCase:
    name: str
    statement: str
    parameters: tuple
    expected: tuple
    json_kind: str | None = None


def sql_cases(image=None):
    map_options = descriptor_text({**OPTIONS, "response_format": output_format("label", "semantic_live_map")})
    boolean_options = descriptor_text(OPTIONS)
    map_call = f"AI_MAP('{MODEL_NAME}', CONCAT(%s, label), %s)"
    filter_call = f"AI_FILTER('{MODEL_NAME}', CONCAT(%s, label), %s)"
    join_call = f"AI_JOIN('{MODEL_NAME}', %s, l.label, r.label, %s)"
    matches = tuple((left_id, right_id) for left_id, left in LEFT_ROWS
                    for right_id, right in RIGHT_ROWS if left == right)
    cases = [
        SqlCase("text-map", f"SELECT id, {map_call} FROM live_left ORDER BY id",
                (MAP_PREFIX, map_options), LEFT_ROWS, "label"),
        SqlCase("text-filter-projection", f"SELECT id, {filter_call} FROM live_left ORDER BY id",
                (FILTER_PREFIX, boolean_options),
                tuple((row_id, int(label == "ALPHA")) for row_id, label in LEFT_ROWS)),
        SqlCase("text-filter-where", f"SELECT id FROM live_left WHERE {filter_call} ORDER BY id",
                (FILTER_PREFIX, boolean_options), ((11,), (33,))),
        SqlCase("text-join", "SELECT l.id, r.id FROM live_left l JOIN live_right r ON "
                + join_call + " ORDER BY l.id, r.id", (JOIN_RULE, boolean_options), matches),
        SqlCase("composed-group",
                "SELECT category, COUNT(*) AS n FROM (SELECT JSON_UNQUOTE(JSON_EXTRACT("
                + map_call + ", '$.label')) AS category FROM live_left WHERE " + filter_call
                + ") classified GROUP BY category HAVING n >= 2 ORDER BY n DESC, category LIMIT 1",
                (MAP_PREFIX, map_options, FILTER_PREFIX, boolean_options), (("ALPHA", 2),)),
        SqlCase("composed-join-map",
                f"SELECT l.id, r.id, AI_MAP('{MODEL_NAME}', CONCAT(%s, l.label), %s) "
                "FROM live_left l JOIN live_right r ON " + join_call
                + f" WHERE AI_FILTER('{MODEL_NAME}', CONCAT(%s, l.label), %s) ORDER BY l.id, r.id",
                (MAP_PREFIX, map_options, JOIN_RULE, boolean_options, FILTER_PREFIX, boolean_options),
                tuple((*pair, "ALPHA") for pair in matches if pair[0] in (11, 33)), "label"),
    ]
    if image is not None:
        map_boolean_options = descriptor_text({
            **OPTIONS, "response_format": output_format("boolean", "semantic_live_image"),
        })
        image_map = f"AI_MAP('{IMAGE_MODEL_NAME}', (%s, asset), %s)"
        image_filter = f"AI_FILTER('{IMAGE_MODEL_NAME}', (%s, asset), %s)"
        cases.extend((
            SqlCase("file-map", f"SELECT id, {image_map} FROM live_images WHERE asset IS NOT NULL ORDER BY id",
                    (image.instruction, map_boolean_options), ((1, image.expected),), "boolean"),
            SqlCase("file-filter-projection",
                    f"SELECT id, CASE WHEN asset IS NOT NULL THEN {image_filter} ELSE NULL END "
                    "FROM live_images ORDER BY id", (image.instruction, boolean_options),
                    ((1, int(image.expected)), (2, None))),
            SqlCase("file-filter-where", "SELECT id FROM live_images WHERE asset IS NOT NULL AND "
                    + image_filter + " ORDER BY id", (image.instruction, boolean_options),
                    ((1,),) if image.expected else ()),
            SqlCase("file-join", "SELECT l.id, r.id FROM live_images l JOIN image_answers r ON "
                    f"AI_JOIN('{IMAGE_MODEL_NAME}', %s, l.asset, r.answer, %s) "
                    "WHERE l.asset IS NOT NULL ORDER BY l.id, r.id",
                    (image.join_rule(), boolean_options), ((1, 1 if image.expected else 2),)),
        ))
    return cases


def assert_exact_rows(actual, expected, name):
    if type(actual) is not tuple or len(actual) != len(expected):
        raise SqlFailure(f"{name}: wrong SQL row count/container; expected {expected!r}, observed {actual!r}")
    for row, wanted in zip(actual, expected):
        if (type(row) is not tuple or len(row) != len(wanted)
                or any(type(value) is not type(target) or value != target
                       for value, target in zip(row, wanted))):
            raise SqlFailure(f"{name}: wrong SQL row shape/type/value; expected {expected!r}, observed {actual!r}")


def assert_case_rows(case, rows):
    if case.json_kind is None:
        assert_exact_rows(rows, case.expected, case.name)
        return
    decoded = []
    if type(rows) is not tuple:
        raise SqlFailure(f"{case.name}: SQL must return a tuple of rows")
    for row in rows:
        if type(row) is not tuple or len(row) != len(case.expected[0]):
            raise SqlFailure(f"{case.name}: SQL MAP row width/container changed")
        try:
            value = parse_value(row[-1], case.json_kind)
        except ProtocolFailure as error:
            raise SqlFailure(f"{case.name}: SQL MAP text violated the independently observed output contract: "
                             f"{error}") from error
        decoded.append((*row[:-1], value))
    assert_exact_rows(tuple(decoded), case.expected, case.name)


class LiveSqlContracts:
    def __init__(self, cursor, relay, model, image=None):
        self.cursor = cursor
        self.relay = relay
        self.model = model
        self.image = image

    def execute(self, statement, parameters=None, *, phase):
        self.relay.phase = phase
        try:
            self.cursor.execute(statement, parameters)
            rows = self.cursor.fetchall()
        except pymysql.MySQLError as error:
            detail = f"{phase}: SQL error {error.args!r}"
            self.relay.client.diagnostics.raise_if_failed(detail)
            raise SqlFailure(detail) from error
        self.relay.client.diagnostics.raise_if_failed()
        return rows

    def setup(self):
        def execute(statement, parameters=None):
            return self.execute(statement, parameters, phase="SQL private setup")

        execute("SET ob_query_timeout = %s", (QUERY_TIMEOUT_SECONDS * 1000000,))
        execute("SET ai_pipeline_slots = 2")
        execute(f"CREATE DATABASE {DATABASE}")
        execute(f"USE {DATABASE}")
        assert_exact_rows(execute("SELECT @@GLOBAL.ai_media_root"), (("",),), "default GLOBAL media root")
        models = [(MODEL_NAME, None)]
        if self.image is not None:
            models.append((IMAGE_MODEL_NAME, ["text", "image"]))
        for name, modalities in models:
            config = {"type": "completion", "model_name": self.model}
            if modalities is not None:
                config["input_modalities"] = modalities
            execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL(%s, %s)", (name, descriptor_text(config)))
            execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT(%s, %s)",
                    (name + "_endpoint", descriptor_text({
                        "ai_model_name": name, "url": self.relay.url, "access_key": REGISTRATION_KEY,
                        "provider": "openai", "request_model_name": self.model,
                    })))
            mask = 1 if modalities is None else 3
            assert_exact_rows(execute(
                "SELECT input_modalities FROM oceanbase.__all_ai_model WHERE name=%s", (name,)),
                ((mask,),), f"{name} input modalities")
        execute("CREATE TABLE live_left (id INT PRIMARY KEY, label VARCHAR(16))")
        execute("CREATE TABLE live_right (id INT PRIMARY KEY, label VARCHAR(16))")
        for row in LEFT_ROWS:
            execute("INSERT INTO live_left VALUES (%s, %s)", row)
        for row in RIGHT_ROWS:
            execute("INSERT INTO live_right VALUES (%s, %s)", row)
        if self.image is not None:
            execute("SET GLOBAL ai_media_root = %s", (str(self.image.asset.path.parent),))
            execute("CREATE TABLE live_images (id INT PRIMARY KEY, asset FILE)")
            execute("INSERT INTO live_images VALUES (1, TO_FILE(%s)), (2, NULL)",
                    (descriptor_text(self.image.asset.descriptor(include_size=True)),))
            execute("CREATE TABLE image_answers (id INT PRIMARY KEY, answer VARCHAR(5))")
            execute("INSERT INTO image_answers VALUES (1, 'true'), (2, 'false')")
            rows = execute("SELECT FILE_URI(asset), FILE_META(asset) FROM live_images WHERE id=1")
            if (len(rows) != 1 or len(rows[0]) != 2 or rows[0][0] != self.image.asset.path.as_uri()):
                raise SqlFailure("native FILE URI did not preserve the staged image reference")
            try:
                metadata = decode_descriptor(rows[0][1])
            except AssertionError as error:
                raise SqlFailure(f"native FILE metadata is invalid: {error}") from error
            if metadata != self.image.asset.descriptor(include_size=True):
                raise SqlFailure("native FILE metadata did not preserve the staged image descriptor")


@contextmanager
def private_database(binary):
    database = PrivateDatabase(binary)
    try:
        try:
            database.start()
        except (OSError, RuntimeError) as error:
            raise LifecycleFailure(f"private database startup failed: {error}") from error
        yield database
    finally:
        previous = sys.exception()
        try:
            database.close()
        except (OSError, RuntimeError) as error:
            if isinstance(previous, RunFailure):
                previous.args = (f"{previous}; private database cleanup also failed: {error}",)
            else:
                raise LifecycleFailure(f"private database cleanup failed: {error}") from error


def run_live(options, credential, diagnostics):
    with tempfile.TemporaryDirectory(prefix="seekdb-semantic-live-") as temporary:
        image = prepare_image(options, Path(temporary) / "media")
        client = ProviderClient(options.endpoint, options.model, credential, options.timeout, diagnostics)
        preflight(client, image)
        print("PASS direct health/model identity and strict MAP/FILTER/JOIN preflight", flush=True)
        if image is not None:
            print("PASS direct opt-in image Boolean preflight", flush=True)
        if options.preflight_only:
            return
        relay = RelayServer(client, RequestPlan(options.model, image))
        try:
            relay.start()
            with private_database(options.binary.resolve()) as database:
                with database.connection.cursor() as cursor:
                    contracts = LiveSqlContracts(cursor, relay, options.model, image)
                    contracts.setup()
                    selected = options.case or TEXT_CASES + (IMAGE_CASES if image is not None else ())
                    for case in sql_cases(image):
                        if case.name in selected:
                            rows = contracts.execute(case.statement, case.parameters, phase="SQL " + case.name)
                            assert_case_rows(case, rows)
                            print("PASS " + case.name, flush=True)
        finally:
            previous = sys.exception()
            try:
                relay.close()
            except (OSError, RuntimeError, LifecycleFailure) as error:
                if isinstance(previous, RunFailure):
                    previous.args = (f"{previous}; observation relay cleanup also failed: {error}",)
                else:
                    raise LifecycleFailure(f"observation relay cleanup failed: {error}") from error
        diagnostics.raise_if_failed()
        print("PASS private database/relay cleanup; no model service was managed", flush=True)


def main(arguments=None):
    options = parse_options(arguments)
    if options.list_cases:
        print("\n".join(TEXT_CASES + IMAGE_CASES))
        return 0
    if options.self_test:
        import unittest
        import test_semantic_live_selftest
        result = unittest.TextTestRunner(verbosity=2).run(
            unittest.defaultTestLoader.loadTestsFromModule(test_semantic_live_selftest))
        return 0 if result.wasSuccessful() else 1
    credential = None
    diagnostics = Diagnostics(Redactor())
    try:
        credential = credential_from_environment(options.api_key_env)
        diagnostics = Diagnostics(Redactor(credential))
        run_live(options, credential, diagnostics)
    except RunFailure as error:
        print(diagnostics.redactor.bounded(f"FAIL [{error.category}] {error}", 4096), file=sys.stderr)
        diagnostics.write(sys.stderr)
        return error.exit_code
    except (OSError, RuntimeError) as error:
        print(diagnostics.redactor.bounded(f"FAIL [private-runner-lifecycle] {error}", 4096), file=sys.stderr)
        diagnostics.write(sys.stderr)
        return LifecycleFailure.exit_code
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
