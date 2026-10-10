"""DB-free safety, real-provider protocol, observation, and SQL-oracle tests.

All HTTP here terminates at an in-process fake provider bound to numeric
loopback. No seekdb process or model service is started.
"""

from contextlib import contextmanager, redirect_stderr, redirect_stdout
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import socket
import ssl
import struct
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock
import uuid
import zlib

import pymysql

import test_semantic_live as live


FAKE_MODEL = "loopback-fixture-model"


def envelope(content, **updates):
    result = {
        "model": FAKE_MODEL,
        "choices": [{"index": 0, "finish_reason": "stop",
                     "message": {"role": "assistant", "content": content, "refusal": None,
                                 "tool_calls": [], "function_call": None, "reasoning": None}}],
    }
    result.update(updates)
    return result


class FakeProvider(ThreadingHTTPServer):
    daemon_threads = False

    def __init__(self):
        super().__init__(("127.0.0.1", 0), FakeHandler)
        self.records = []
        self.model_ids = [FAKE_MODEL]
        self.health_status = 200
        self.override = None
        self.image_value = True
        self.length_delta = 0
        self.transfer_gate = None
        self.chunk_delay = None
        self.thread = threading.Thread(target=self.serve_forever, name="semantic-live-fake-provider")
        self.thread.start()

    @property
    def origin(self):
        return f"http://127.0.0.1:{self.server_port}"

    @property
    def endpoint(self):
        return self.origin + "/v1"

    def close(self):
        self.shutdown()
        self.server_close()
        self.thread.join(5)
        if self.thread.is_alive():
            raise AssertionError("fake provider left a live server thread")


class FakeHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_arguments):
        pass

    def send_fixture(self, status, raw, headers=None):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw) + self.server.length_delta))
        self.send_header("Connection", "close")
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        if self.server.chunk_delay is None:
            self.wfile.write(raw)
        else:
            for index in range(0, len(raw), 8):
                try:
                    self.wfile.write(raw[index:index + 8])
                except (BrokenPipeError, ConnectionResetError):
                    return
                time.sleep(self.server.chunk_delay)
        if self.server.transfer_gate is not None:
            self.server.transfer_gate.wait(5)
        self.close_connection = True

    def do_GET(self):
        self.server.records.append({"method": "GET", "path": self.path,
                                    "authorization": self.headers.get("Authorization")})
        if self.path == "/health":
            self.send_fixture(self.server.health_status, b"",
                              {"Location": "http://198.51.100.20/forbidden"} if
                              self.server.health_status == 302 else None)
        elif self.path == "/v1/models":
            self.send_fixture(200, json.dumps({
                "object": "list", "data": [{"id": name} for name in self.server.model_ids],
            }).encode("utf-8"))
        else:
            self.send_fixture(404, b'{"error":"unknown fake-provider path"}')

    def do_POST(self):
        raw = self.rfile.read(int(self.headers["Content-Length"]))
        body = json.loads(raw)
        self.server.records.append({"method": "POST", "path": self.path, "raw": raw,
                                    "body": body, "authorization": self.headers.get("Authorization")})
        content = body["messages"][-1]["content"]
        if isinstance(content, str) and content.startswith(live.MAP_PREFIX):
            answer = {"label": content[len(live.MAP_PREFIX):]}
        elif isinstance(content, str) and content.startswith(live.FILTER_PREFIX):
            answer = {"value": content[len(live.FILTER_PREFIX):] == "ALPHA"}
        elif isinstance(content, str):
            pair = json.loads(content)
            answer = {"value": pair["left"] == pair["right"]}
        elif content[0]["text"] == "AI_JOIN rule:\n":
            answer = {"value": self.server.image_value == (content[-1]["text"] == "true")}
        else:
            answer = {"value": self.server.image_value}
        response = envelope(json.dumps(answer, separators=(",", ":")))
        status, response_raw, headers = 200, json.dumps(response).encode("utf-8"), {}
        if self.server.override is not None:
            status, response_raw, headers = self.server.override(body, response)
        self.send_fixture(status, response_raw, headers)


@contextmanager
def fake_client(*, credential=None):
    server = FakeProvider()
    diagnostics = live.Diagnostics(live.Redactor(credential))
    client = live.ProviderClient(live.parse_endpoint(server.endpoint), FAKE_MODEL,
                                 credential, 2, diagnostics)
    try:
        yield server, client, diagnostics
    finally:
        server.close()


def relay_request(relay, body, *, credential=live.REGISTRATION_KEY, raw=None):
    connection = HTTPConnection("127.0.0.1", relay.server_port, timeout=5)
    try:
        payload = live.descriptor_text(body).encode("utf-8") if raw is None else raw
        connection.request("POST", "/v1/chat/completions", body=payload,
                           headers={"Authorization": "Bearer " + credential,
                                    "Content-Type": "application/json"})
        response = connection.getresponse()
        return response.status, response.read()
    finally:
        connection.close()


class CliSafetyTests(unittest.TestCase):
    def reject(self, arguments):
        output = io.StringIO()
        with redirect_stderr(output), self.assertRaises(SystemExit) as caught:
            live.parse_options(arguments)
        self.assertEqual(caught.exception.code, 2)
        return output.getvalue()

    def test_explicit_endpoint_and_model_are_required_before_any_side_effect(self):
        cases = ([], ["--binary", sys.executable],
                 ["--preflight-only", "--model", FAKE_MODEL],
                 ["--preflight-only", "--endpoint", "http://127.0.0.1:8000/v1"])
        with mock.patch.object(live, "PrivateDatabase") as database, \
                mock.patch.object(live.ProviderClient, "exchange") as exchange, \
                mock.patch.dict(os.environ, {"SEEKDB_AI_API_BASE": "http://198.51.100.20/v1",
                                             "SEEKDB_AI_MODEL": FAKE_MODEL}):
            for arguments in cases:
                with self.subTest(arguments=arguments):
                    self.reject(arguments)
            database.assert_not_called()
            exchange.assert_not_called()

    def test_remote_ambiguous_or_credential_urls_are_rejected_without_dns(self):
        urls = (
            "http://example.com/v1", "http://127.0.0.1.example.com/v1",
            "http://192.168.1.1/v1", "http://0.0.0.0:8000/v1",
            "http://[::]/v1", "http://[::ffff:127.0.0.1]/v1",
            "http://127.1/v1", "http://2130706433/v1", "http://localhost./v1",
            "http://user:private@127.0.0.1/v1", "http://127.0.0.1/v1?key=private",
            "http://127.0.0.1/v1#", "http://127.0.0.1/v1?",
            "http://127.0.0.1:0/v1", "http://127.0.0.1:65536/v1",
            "file:///tmp/model", "http://127.0.0.1/other",
            "http://127.0.0.1/v1\n", "http://[::1%lo]/v1",
        )
        with mock.patch.object(socket, "getaddrinfo") as dns:
            for url in urls:
                with self.subTest(url=url):
                    with self.assertRaises(live.argparse.ArgumentTypeError) as caught:
                        live.parse_endpoint(url)
                    self.assertNotIn("private", str(caught.exception))
            dns.assert_not_called()

    def test_only_canonical_loopback_addresses_and_supported_paths_are_accepted(self):
        for host, address in (("127.0.0.1", "127.0.0.1"), ("127.0.0.2", "127.0.0.2"),
                              ("localhost", "127.0.0.1"), ("[::1]", "::1")):
            for suffix in ("", "/", "/v1", "/v1/", "/v1/chat/completions"):
                with self.subTest(host=host, suffix=suffix):
                    endpoint = live.parse_endpoint(f"http://{host}:8080{suffix}")
                    self.assertEqual(endpoint.connect_host, address)
                    self.assertEqual(endpoint.port, 8080)
        self.assertEqual(live.parse_endpoint("https://localhost/v1").port, 443)

    def test_parent_base_endpoint_is_configuration_only_with_small_defaults(self):
        with mock.patch.object(live.ProviderClient, "exchange") as exchange, \
                mock.patch.object(live, "PrivateDatabase") as database:
            options = live.parse_options([
                "--endpoint", "http://127.0.0.1:18081",
                "--model", "seekdb-file-validation-4b", "--preflight-only",
            ])
        self.assertEqual(options.endpoint.connect_host, "127.0.0.1")
        self.assertEqual(options.endpoint.port, 18081)
        self.assertEqual(options.model, "seekdb-file-validation-4b")
        self.assertEqual(options.timeout, 15)
        self.assertEqual(live.OPTIONS, {"temperature": 0, "max_tokens": 32, "stream": False})
        self.assertEqual(live.QUERY_TIMEOUT_SECONDS, 60)
        self.assertIsNone(options.image)
        exchange.assert_not_called()
        database.assert_not_called()

    def test_https_remains_verified_and_preserves_original_tls_identity(self):
        endpoint = live.parse_endpoint("https://localhost:8443/v1")
        connection = endpoint.connection(2)
        self.assertEqual(connection.host, "127.0.0.1")
        self.assertEqual(connection.tls_name, "localhost")
        self.assertEqual(connection._context.verify_mode, ssl.CERT_REQUIRED)
        self.assertTrue(connection._context.check_hostname)
        connection.close()

    def test_offline_modes_cannot_smuggle_live_arguments(self):
        for mode in ("--self-test", "--list-cases"):
            for extra in (["--endpoint", "http://127.0.0.1/v1"],
                          ["--model", FAKE_MODEL], ["--binary", sys.executable],
                          ["--api-key-env", "EXPLICIT_TEST_KEY"], ["--image", "synthetic-red"],
                          ["--case", "text-map"]):
                with self.subTest(mode=mode, extra=extra):
                    self.reject([mode, *extra])

    def test_unknown_key_argument_is_not_echoed(self):
        secret = uuid.uuid4().hex
        output = self.reject(["--api-key", secret])
        self.assertNotIn(secret, output)
        self.assertIn("--api-key-env", output)

    def test_model_environment_name_and_http_timeout_are_validated(self):
        base = ["--preflight-only", "--endpoint", "http://127.0.0.1/v1", "--model", FAKE_MODEL]
        for extra in (["--model", ""], ["--model", "with spaces"],
                      ["--api-key-env", "not-a-name"], ["--timeout", "nan"],
                      ["--timeout", "inf"], ["--timeout", "0"], ["--timeout", "61"]):
            with self.subTest(extra=extra):
                self.reject([*base, *extra])
        options = live.parse_options([*base, "--timeout", "0.5", "--api-key-env", "LIVE_TEST_KEY"])
        self.assertEqual(options.timeout, 0.5)
        self.assertEqual(options.api_key_env, "LIVE_TEST_KEY")

    def test_image_always_requires_explicit_question_and_independent_answer(self):
        base = ["--preflight-only", "--endpoint", "http://127.0.0.1/v1", "--model", FAKE_MODEL]
        for extra in (["--image", "synthetic-red"],
                      ["--image", "synthetic-red", "--instruction", "Is it red?"],
                      ["--instruction", "Is it red?"], ["--expect-image", "true"],
                      ["--image", "synthetic-red", "--instruction", " ", "--expect-image", "true"]):
            with self.subTest(extra=extra):
                self.reject([*base, *extra])
        options = live.parse_options([*base, "--image", "synthetic-red", "--instruction", "Is it red?",
                                      "--expect-image", "true"])
        self.assertEqual(options.image, "synthetic-red")

    def test_case_selection_never_silently_skips_or_repeats_live_work(self):
        base = ["--binary", sys.executable, "--endpoint", "http://127.0.0.1/v1", "--model", FAKE_MODEL]
        for extra in (["--case", "unknown"], ["--case", "file-map"],
                      ["--case", "text-map", "--case", "text-map"],
                      ["--preflight-only"]):
            with self.subTest(extra=extra):
                self.reject([*base, *extra])

    def test_named_credentials_are_never_defaulted_or_treated_as_values(self):
        self.assertIsNone(live.credential_from_environment(None))
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(live.SafetyFailure):
                live.credential_from_environment("MISSING_TEST_KEY")
        for value in ("", "two words", "line\r\nheader", "x" * 4097, "\x7f", "\u20ac"):
            with self.subTest(length=len(value)), mock.patch.dict(os.environ, {"LIVE_TEST_KEY": value}):
                with self.assertRaises(live.SafetyFailure) as caught:
                    live.credential_from_environment("LIVE_TEST_KEY")
                self.assertNotIn(repr(value), str(caught.exception))

    def test_list_cases_does_not_create_database_or_contact_provider(self):
        output = io.StringIO()
        with redirect_stdout(output), mock.patch.object(live, "PrivateDatabase") as database, \
                mock.patch.object(live.ProviderClient, "exchange") as exchange:
            self.assertEqual(live.main(["--list-cases"]), 0)
        self.assertEqual(output.getvalue().splitlines(), list(live.TEXT_CASES + live.IMAGE_CASES))
        database.assert_not_called()
        exchange.assert_not_called()


class ProtocolOracleTests(unittest.TestCase):
    def test_exact_true_and_false_are_values_not_truthiness(self):
        self.assertIs(live.parse_value('{"value":true}', "boolean"), True)
        self.assertIs(live.parse_value('{"value":false}', "boolean"), False)
        self.assertEqual(live.parse_value('{"label":"ALPHA"}', "label"), "ALPHA")

    def test_parent_observed_spaced_boolean_envelope_is_strict_json_not_a_fallback(self):
        for content, expected in (('{ "value": true }', True), ('{ "value": false }', False)):
            with self.subTest(expected=expected):
                raw = json.dumps(envelope(content)).encode("utf-8")
                self.assertIs(live.completion_value(raw, FAKE_MODEL, "boolean"), expected)
                live.OutputContract("boolean", expected, "parent Boolean envelope fixture").check(
                    raw, FAKE_MODEL)
                with self.assertRaises(live.ModelQualityFailure):
                    live.OutputContract("boolean", not expected).check(raw, FAKE_MODEL)

    def test_missing_corrupt_nonboolean_or_extra_values_are_not_false(self):
        values = ("", None, True, "false", "0", "null", "[]", "{}", '{"value":0}',
                  '{"value":1}', '{"value":"false"}', '{"value":null}', '{"Value":false}',
                  '{"value":false,"extra":0}', '{"value":true,"value":false}',
                  '{"value":NaN}', '```json\n{"value":false}\n```', '{"value":')
        for value in values:
            with self.subTest(value=value), self.assertRaises(live.ProtocolFailure):
                live.parse_value(value, "boolean")

    def test_map_schema_is_exact_and_never_normalizes_labels(self):
        for value in ('{"label":"alpha"}', '{"label":"UNKNOWN"}', '{"label":true}',
                      '{"label":"ALPHA","extra":0}', '{"label":"ALPHA","label":"BETA"}',
                      "ALPHA", '"ALPHA"', "{}"):
            with self.subTest(value=value), self.assertRaises(live.ProtocolFailure):
                live.parse_value(value, "label")

    def test_finish_reason_identity_choice_and_refusal_are_strict(self):
        valid = envelope('{"value":true}')
        choice = valid["choices"][0]
        invalid = [
            envelope('{"value":true}', model="other-model"),
            envelope('{"value":true}', choices=[]),
            envelope('{"value":true}', choices=[choice, choice]),
            envelope('{"value":true}', choices=[{"message": choice["message"]}]),
            envelope('{"value":true}', choices=[{**choice, "finish_reason": None}]),
            envelope('{"value":true}', choices=[{**choice, "finish_reason": "length"}]),
            envelope('{"value":true}', choices=[{**choice, "finish_reason": "tool_calls"}]),
            envelope('{"value":true}', choices=[{**choice, "message": {"content": None}}]),
        ]
        for field in ("refusal", "tool_calls", "function_call"):
            invalid.append(envelope('{"value":true}', choices=[
                {**choice, "message": {**choice["message"], field: ""}},
            ]))
        for payload in invalid:
            with self.subTest(payload=payload), self.assertRaises(live.ProtocolFailure):
                live.completion_value(json.dumps(payload), FAKE_MODEL, "boolean")
        self.assertIs(live.completion_value(json.dumps(valid), FAKE_MODEL, "boolean"), True)
        null_refusal = envelope('{"value":false}', choices=[
            {**choice, "message": {"content": '{"value":false}', "refusal": None}},
        ])
        self.assertIs(live.completion_value(json.dumps(null_refusal), FAKE_MODEL, "boolean"), False)

    def test_corrupt_or_duplicate_provider_json_is_not_repaired(self):
        values = (b"{broken", b"\xff", b"null", b"[]",
                  b'{"model":"x","model":"x","choices":[]}', b'{"model":NaN}')
        for raw in values:
            with self.subTest(raw=raw), self.assertRaises(live.ProtocolFailure):
                live.completion_value(raw, FAKE_MODEL, "boolean")

    def test_vllm_empty_tool_metadata_keeps_boolean_and_finish_contracts_strict(self):
        for calls in (None, []):
            with self.subTest(calls=calls):
                response = envelope('{"value":false}')
                response["choices"][0]["message"]["tool_calls"] = calls
                self.assertIs(live.completion_value(
                    json.dumps(response), FAKE_MODEL, "boolean"), False)
                for content in ('{"value":"false"}', '{"value":0}', "false"):
                    response["choices"][0]["message"]["content"] = content
                    with self.assertRaises(live.ProtocolFailure):
                        live.completion_value(json.dumps(response), FAKE_MODEL, "boolean")
                response["choices"][0]["message"]["content"] = '{"value":false}'
                response["choices"][0]["finish_reason"] = "length"
                with self.assertRaises(live.ProtocolFailure):
                    live.completion_value(json.dumps(response), FAKE_MODEL, "boolean")

    def test_nonempty_or_wrong_type_tool_metadata_is_not_a_semantic_result(self):
        for calls in ("", False, 0, {}, [{"id": "fixture-tool", "type": "function"}]):
            with self.subTest(calls=calls):
                response = envelope('{"value":true}')
                response["choices"][0]["message"]["tool_calls"] = calls
                with self.assertRaises(live.ProtocolFailure):
                    live.completion_value(json.dumps(response), FAKE_MODEL, "boolean")

    def test_well_formed_wrong_answers_are_model_quality_not_protocol_errors(self):
        for kind, expected, content in (("boolean", True, '{"value":false}'),
                                        ("label", "ALPHA", '{"label":"BETA"}')):
            with self.subTest(kind=kind), self.assertRaises(live.ModelQualityFailure):
                live.OutputContract(kind, expected).check(json.dumps(envelope(content)), FAKE_MODEL)


class PreflightTests(unittest.TestCase):
    def test_health_model_identity_and_six_native_strict_probes(self):
        with fake_client() as (server, client, diagnostics):
            live.preflight(client)
            self.assertEqual([(record["method"], record["path"]) for record in server.records[:2]],
                             [("GET", "/health"), ("GET", "/v1/models")])
            posts = [record for record in server.records if record["method"] == "POST"]
            self.assertEqual(len(posts), 6)
            for record in posts:
                body = record["body"]
                self.assertNotIn("Schema", body)
                self.assertEqual(body["model"], FAKE_MODEL)
                definition = body["response_format"]["json_schema"]
                self.assertIs(definition["strict"], True)
                self.assertEqual(definition["schema"],
                                 live.LABEL_SCHEMA if body["messages"][0]["content"].startswith(live.MAP_PREFIX)
                                 else live.FILTER_SCHEMA)
                self.assertIsNone(record["authorization"])
            diagnostics.raise_if_failed()

    def test_base_origin_routes_the_real_cli_to_only_its_own_fake_provider(self):
        with fake_client() as (server, _client, _diagnostics):
            output = io.StringIO()
            with redirect_stdout(output), mock.patch.object(live, "PrivateDatabase") as database:
                status = live.main(["--endpoint", server.origin, "--model", FAKE_MODEL,
                                    "--preflight-only"])
            self.assertEqual(status, 0)
            database.assert_not_called()
            self.assertEqual([(record["method"], record["path"]) for record in server.records],
                             [("GET", "/health"), ("GET", "/v1/models")]
                             + [("POST", "/v1/chat/completions")] * 6)
            self.assertTrue(all(record["body"]["max_tokens"] == 32
                                for record in server.records if record["method"] == "POST"))

    def test_localhost_is_pinned_and_does_not_use_ambient_http_proxy(self):
        with fake_client() as (server, _client, diagnostics), \
                mock.patch.dict(os.environ, {"HTTP_PROXY": "http://198.51.100.20:1234",
                                             "HTTPS_PROXY": "http://198.51.100.20:1234",
                                             "NO_PROXY": ""}):
            client = live.ProviderClient(live.parse_endpoint(
                f"http://localhost:{server.server_port}/v1/chat/completions"),
                FAKE_MODEL, None, 2, diagnostics)
            live.preflight(client)
            self.assertEqual(len(server.records), 8)

    def test_health_failure_or_redirect_never_reaches_model_or_inference(self):
        for status in (302, 503):
            with self.subTest(status=status), fake_client() as (server, client, _diagnostics):
                server.health_status = status
                with self.assertRaises(live.ProtocolFailure):
                    live.preflight(client)
                self.assertEqual(len(server.records), 1)

    def test_model_identity_is_exact_and_is_not_automatically_substituted(self):
        for models in ([], ["other-model"], [FAKE_MODEL, FAKE_MODEL], [None]):
            with self.subTest(models=models), fake_client() as (server, client, _diagnostics):
                server.model_ids = models
                with self.assertRaises(live.ProtocolFailure):
                    live.preflight(client)
                self.assertTrue(all(record["method"] == "GET" for record in server.records))

    def test_native_schema_rejection_is_fail_fast_without_lax_retry(self):
        with fake_client() as (server, client, diagnostics):
            server.override = lambda _body, _response: (
                400, b'{"error":{"message":"strict json_schema is unsupported"}}', {})
            with self.assertRaises(live.ProtocolFailure):
                live.preflight(client)
            self.assertEqual(len(server.records), 3)
            self.assertIn("strict json_schema is unsupported", diagnostics.records[-1].raw)

    def test_preflight_preserves_actual_corrupt_output_and_does_not_start_database(self):
        with fake_client() as (server, _client, _diagnostics):
            server.override = lambda _body, _response: (200, b"{actual-corrupt-provider-body", {})
            output = io.StringIO()
            with redirect_stdout(output), redirect_stderr(output), \
                    mock.patch.object(live, "PrivateDatabase") as database:
                status = live.main(["--preflight-only", "--endpoint", server.endpoint,
                                    "--model", FAKE_MODEL])
            self.assertEqual(status, live.ProtocolFailure.exit_code)
            self.assertIn("provider-protocol", output.getvalue())
            self.assertIn("{actual-corrupt-provider-body", output.getvalue())
            self.assertNotIn("PASS", output.getvalue())
            database.assert_not_called()
            self.assertEqual(len(server.records), 3)

    def test_successful_preflight_only_never_starts_sql(self):
        with fake_client() as (server, _client, _diagnostics):
            output = io.StringIO()
            with redirect_stdout(output), mock.patch.object(live, "PrivateDatabase") as database:
                self.assertEqual(live.main(["--preflight-only", "--endpoint", server.endpoint,
                                            "--model", FAKE_MODEL]), 0)
            self.assertIn("PASS direct", output.getvalue())
            database.assert_not_called()

    def test_named_credential_is_used_only_for_http_and_redacted_on_failure(self):
        credential = uuid.uuid4().hex
        with fake_client() as (server, _client, _diagnostics):
            server.override = lambda _body, _response: (
                401, json.dumps({"error": "Bearer " + credential, "access_key": credential}).encode(), {})
            output = io.StringIO()
            with mock.patch.dict(os.environ, {"LIVE_FIXTURE_KEY": credential}), \
                    redirect_stderr(output), redirect_stdout(output):
                status = live.main(["--preflight-only", "--endpoint", server.endpoint,
                                    "--model", FAKE_MODEL, "--api-key-env", "LIVE_FIXTURE_KEY"])
            self.assertEqual(status, live.ProtocolFailure.exit_code)
            self.assertNotIn(credential, output.getvalue())
            self.assertIn("[REDACTED]", output.getvalue())
            self.assertTrue(all(record["authorization"] == "Bearer " + credential
                                for record in server.records))

    def test_missing_named_credential_fails_before_http(self):
        with fake_client() as (server, _client, _diagnostics), \
                mock.patch.dict(os.environ, {}, clear=True), \
                redirect_stderr(io.StringIO()):
            self.assertEqual(live.main(["--preflight-only", "--endpoint", server.endpoint,
                                       "--model", FAKE_MODEL, "--api-key-env", "MISSING_FIXTURE_KEY"]),
                             live.SafetyFailure.exit_code)
            self.assertEqual(server.records, [])

    def test_response_size_is_bounded_without_synthetic_success(self):
        with fake_client() as (server, client, diagnostics), \
                mock.patch.object(live, "MAX_RESPONSE_BYTES", 32):
            server.override = lambda _body, _response: (200, b"x" * 64, {})
            with self.assertRaises(live.ProtocolFailure):
                client.exchange("POST", "/v1/chat/completions",
                                raw=live.descriptor_text(live.request_body(
                                    FAKE_MODEL, live.MAP_PREFIX + "ALPHA", "label", "map")).encode(),
                                phase="bounded fixture")
            self.assertEqual(diagnostics.records[-1].raw_bytes, 33)
            self.assertIn("exceeds", diagnostics.records[-1].failure)

    def test_incomplete_http_transfer_preserves_the_actual_received_body(self):
        with fake_client() as (server, client, diagnostics):
            partial = b"ACTUAL_PARTIAL_PROVIDER_OUTPUT"
            server.length_delta = 20
            server.override = lambda _body, _response: (200, partial, {})
            with self.assertRaises(live.ProtocolFailure):
                client.exchange("POST", "/v1/chat/completions", raw=live.descriptor_text(
                    live.request_body(FAKE_MODEL, live.MAP_PREFIX + "ALPHA", "label", "map")).encode(),
                    phase="incomplete HTTP fixture")
            self.assertEqual(diagnostics.records[-1].raw, partial.decode())
            self.assertEqual(diagnostics.records[-1].status, 200)

    def test_timeout_preserves_already_observed_partial_body(self):
        with fake_client() as (server, client, diagnostics):
            partial = b"ACTUAL_BODY_BEFORE_TIMEOUT"
            server.length_delta = 20
            server.transfer_gate = threading.Event()
            server.override = lambda _body, _response: (200, partial, {})
            client.timeout = 0.1
            try:
                with self.assertRaises(live.ProtocolFailure):
                    client.exchange("POST", "/v1/chat/completions", raw=live.descriptor_text(
                        live.request_body(FAKE_MODEL, live.MAP_PREFIX + "ALPHA", "label", "map")).encode(),
                        phase="HTTP timeout fixture")
            finally:
                server.transfer_gate.set()
            self.assertEqual(diagnostics.records[-1].raw, partial.decode())
            self.assertIn("TimeoutError", diagnostics.records[-1].failure)

    def test_progress_does_not_reset_the_total_http_deadline(self):
        with fake_client() as (server, client, diagnostics):
            server.chunk_delay = 0.03
            server.override = lambda _body, _response: (200, b"ACTUAL_TRICKLE_" * 40, {})
            client.timeout = 0.15
            with self.assertRaises(live.ProtocolFailure):
                client.exchange("POST", "/v1/chat/completions", raw=live.descriptor_text(
                    live.request_body(FAKE_MODEL, live.MAP_PREFIX + "ALPHA", "label", "map")).encode(),
                    phase="HTTP progressing deadline fixture")
            record = diagnostics.records[-1]
            self.assertTrue(record.raw.startswith("ACTUAL"))
            self.assertLess(record.raw_bytes, 40 * len(b"ACTUAL_TRICKLE_"))
            self.assertIn("deadline expired", record.failure)
            self.assertFalse(any(thread.name == "semantic-live-http-deadline"
                                 for thread in threading.enumerate()))

    def test_failed_body_read_explicitly_closes_response_and_connection(self):
        diagnostics = live.Diagnostics(live.Redactor())
        connection = mock.Mock()
        response = mock.Mock()
        response.status = 200
        response.read1.side_effect = TimeoutError("fixture body timeout")
        connection.getresponse.return_value = response
        endpoint = mock.Mock()
        endpoint.connection.return_value = connection
        client = live.ProviderClient(endpoint, FAKE_MODEL, None, 2, diagnostics)
        with self.assertRaises(live.ProtocolFailure):
            client.exchange("GET", "/health", phase="HTTP ownership fixture")
        response.close.assert_called_once()
        connection.close.assert_called_once()
        self.assertFalse(any(thread.name == "semantic-live-http-deadline"
                             for thread in threading.enumerate()))

    def test_quality_failure_prevents_database_startup_even_in_sql_mode(self):
        with fake_client() as (server, _client, _diagnostics):
            server.override = lambda _body, _response: (
                200, json.dumps(envelope('{"label":"BETA"}')).encode(), {})
            output = io.StringIO()
            with redirect_stderr(output), redirect_stdout(output), \
                    mock.patch.object(live, "PrivateDatabase") as database:
                status = live.main(["--endpoint", server.endpoint, "--model", FAKE_MODEL,
                                    "--binary", sys.executable])
            self.assertEqual(status, live.ModelQualityFailure.exit_code)
            self.assertIn("model-quality", output.getvalue())
            self.assertIn("preflight MAP ALPHA", output.getvalue())
            self.assertIn("answer was 'BETA'", output.getvalue())
            self.assertIn("expected 'ALPHA'", output.getvalue())
            self.assertNotIn("PASS", output.getvalue())
            database.assert_not_called()


class ObservationRelayTests(unittest.TestCase):
    @contextmanager
    def relay(self, client, image=None):
        relay = live.RelayServer(client, live.RequestPlan(FAKE_MODEL, image))
        try:
            relay.start()
            yield relay
        finally:
            relay.close()

    def test_raw_request_and_success_response_are_forwarded_unchanged(self):
        credential = uuid.uuid4().hex
        with fake_client(credential=credential) as (server, client, diagnostics), self.relay(client) as relay:
            raw_reply = json.dumps(envelope('{"label":"ALPHA"}'), indent=2).encode()
            server.override = lambda _body, _response: (200, raw_reply, {})
            body = live.request_body(FAKE_MODEL, live.MAP_PREFIX + "ALPHA", "label", "map")
            raw_request = json.dumps(body, indent=3).encode()
            status, observed = relay_request(relay, body, raw=raw_request)
            self.assertEqual((status, observed), (200, raw_reply))
            self.assertEqual(server.records[-1]["raw"], raw_request)
            self.assertEqual(server.records[-1]["authorization"], "Bearer " + credential)
            self.assertNotIn(live.REGISTRATION_KEY, server.records[-1]["authorization"])
            diagnostics.raise_if_failed()

    def test_wrong_answer_is_never_replaced_by_expected_answer(self):
        with fake_client() as (server, client, diagnostics), self.relay(client) as relay:
            wrong = json.dumps(envelope('{"value":false}')).encode()
            server.override = lambda _body, _response: (200, wrong, {})
            status, observed = relay_request(relay, live.request_body(
                FAKE_MODEL, live.FILTER_PREFIX + "ALPHA", "boolean", "ai_filter"))
            self.assertEqual((status, observed), (200, wrong))
            with self.assertRaises(live.ModelQualityFailure):
                diagnostics.raise_if_failed()

    def test_join_quality_diagnostic_identifies_the_pair_phase_and_observed_answer(self):
        with fake_client() as (server, client, diagnostics), self.relay(client) as relay:
            wrong = json.dumps(envelope('{"value":true}')).encode()
            server.override = lambda _body, _response: (200, wrong, {})
            relay.phase = "SQL text-join"
            pair = live.descriptor_text({"rule": live.JOIN_RULE, "left": "ALPHA", "right": "BETA"})
            self.assertEqual(relay_request(relay, live.request_body(
                FAKE_MODEL, pair, "boolean", "ai_join")), (200, wrong))
            with self.assertRaises(live.ModelQualityFailure) as caught:
                diagnostics.raise_if_failed()
            self.assertIn("SQL text-join", str(caught.exception))
            self.assertIn("JOIN left=ALPHA right=BETA", str(caught.exception))
            self.assertIn("answer was True", str(caught.exception))
            self.assertIn("expected False", str(caught.exception))
            self.assertIn('{\\"value\\":true}', diagnostics.records[-1].raw)

    def test_corrupt_nonboolean_truncated_and_http_error_outputs_are_preserved(self):
        fixtures = [
            (200, b"{corrupt"),
            (200, json.dumps(envelope('{"value":"false"}')).encode()),
            (200, json.dumps(envelope('{"value":true}', choices=[
                {"finish_reason": "length", "message": {"content": '{"value":true}'}},
            ])).encode()),
            (400, b'{"error":"actual provider error"}'),
        ]
        for status, raw in fixtures:
            with self.subTest(status=status, raw=raw), fake_client() as (server, client, diagnostics), \
                    self.relay(client) as relay:
                server.override = lambda _body, _response: (status, raw, {})
                self.assertEqual(relay_request(relay, live.request_body(
                    FAKE_MODEL, live.FILTER_PREFIX + "ALPHA", "boolean", "ai_filter")), (status, raw))
                with self.assertRaises(live.ProtocolFailure):
                    diagnostics.raise_if_failed()

    def test_unplanned_input_and_unauthorized_traffic_are_blocked_before_upstream(self):
        for content, credential in (("unplanned private text", live.REGISTRATION_KEY),
                                     (live.MAP_PREFIX + "ALPHA", "not-the-registration-key")):
            with self.subTest(content=content), fake_client() as (server, client, diagnostics), \
                    self.relay(client) as relay:
                status, raw = relay_request(relay, live.request_body(
                    FAKE_MODEL, content, "label", "map"), credential=credential)
                self.assertEqual(status, 400)
                self.assertIn(b"blocked", raw)
                self.assertEqual(server.records, [])
                with self.assertRaises(live.SqlFailure):
                    diagnostics.raise_if_failed()

    def test_schema_downgrade_or_boolean_inference_options_are_blocked(self):
        for field, value in (("response_format", {"type": "json_object"}),
                             ("temperature", False), ("stream", 0), ("max_tokens", True),
                             ("Schema", live.FILTER_SCHEMA)):
            with self.subTest(field=field), fake_client() as (server, client, diagnostics), \
                    self.relay(client) as relay:
                body = live.request_body(FAKE_MODEL, live.FILTER_PREFIX + "ALPHA", "boolean", "filter")
                body[field] = value
                self.assertEqual(relay_request(relay, body)[0], 400)
                self.assertEqual(server.records, [])
                with self.assertRaises(live.SqlFailure):
                    diagnostics.raise_if_failed()

    def test_schema_json_types_cannot_be_downgraded_by_python_bool_int_equality(self):
        plan = live.RequestPlan(FAKE_MODEL)
        body = live.request_body(FAKE_MODEL, live.FILTER_PREFIX + "ALPHA", "boolean", "ai_filter")
        body = json.loads(json.dumps(body))
        body["response_format"]["json_schema"]["schema"]["additionalProperties"] = 0
        with self.assertRaises(live.SqlFailure):
            plan.contract_for(body)

    def test_provider_failure_keeps_its_category_when_sql_also_reports_an_error(self):
        with fake_client() as (server, client, diagnostics), self.relay(client) as relay:
            server.override = lambda _body, _response: (200, b"{corrupt", {})
            relay_request(relay, live.request_body(
                FAKE_MODEL, live.FILTER_PREFIX + "ALPHA", "boolean", "ai_filter"))
            cursor = mock.Mock()
            cursor.execute.side_effect = pymysql.OperationalError(4070, "invalid AI output")
            contracts = live.LiveSqlContracts(cursor, relay, FAKE_MODEL)
            with self.assertRaises(live.ProtocolFailure) as caught:
                contracts.execute("SELECT fixture", phase="SQL failing fixture")
            self.assertIn("4070", str(caught.exception))
            self.assertIn("{corrupt", diagnostics.records[-1].raw)

    def test_sql_error_without_provider_failure_is_not_attributed_to_model_quality(self):
        with fake_client() as (_server, client, _diagnostics), self.relay(client) as relay:
            cursor = mock.Mock()
            cursor.execute.side_effect = pymysql.ProgrammingError(1064, "SQL syntax failure")
            with self.assertRaises(live.SqlFailure) as caught:
                live.LiveSqlContracts(cursor, relay, FAKE_MODEL).execute("SELECT fixture", phase="SQL fixture")
            self.assertIn("1064", str(caught.exception))


class ImageSafetyTests(unittest.TestCase):
    def image_options(self, value="synthetic-red", expected=True):
        return live.parse_options([
            "--preflight-only", "--endpoint", "http://127.0.0.1/v1", "--model", FAKE_MODEL,
            "--image", value, "--instruction", "Is this image predominantly red?",
            "--expect-image", "true" if expected else "false",
        ])

    def test_synthetic_fixture_is_a_visible_red_png_in_a_private_root(self):
        with tempfile.TemporaryDirectory() as directory:
            image = live.prepare_image(self.image_options(), Path(directory) / "media")
            raw = image.asset.raw
            self.assertEqual(struct.unpack(">II", raw[16:24]), (128, 128))
            length = struct.unpack(">I", raw[33:37])[0]
            pixels = zlib.decompress(raw[41:41 + length])
            self.assertEqual(pixels, (b"\x00" + b"\xff\x00\x00" * 128) * 128)
            self.assertEqual(image.asset.path.read_bytes(), raw)
            self.assertEqual(image.asset.mime_type, "image/png")
            self.assertIs(image.expected, True)

    def test_user_file_is_copied_not_modified_and_path_is_not_sent_as_text(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "user-image.bin"
            raw = live.synthetic_red_png()
            source.write_bytes(raw)
            image = live.prepare_image(self.image_options(str(source)), Path(directory) / "media")
            self.assertEqual(source.read_bytes(), raw)
            self.assertNotEqual(image.asset.path, source)
            self.assertEqual(image.asset.mime_type, "image/png")
            self.assertNotIn(str(source), live.descriptor_text(image.content()))
            self.assertTrue(image.content()[1]["image_url"]["url"].startswith("data:image/png;base64,"))

    def test_uri_symlink_parent_symlink_fifo_directory_and_oversize_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            source = base / "source.png"
            source.write_bytes(live.synthetic_red_png())
            link = base / "link.png"
            link.symlink_to(source)
            linked_parent = base / "linked"
            linked_parent.symlink_to(base, target_is_directory=True)
            fifo = base / "fifo"
            os.mkfifo(fifo)
            large = base / "large.png"
            large.write_bytes(b"x" * (live.MEDIA_MAX_BYTES + 1))
            for value in (source.as_uri(), str(link), str(linked_parent / "source.png"),
                          str(fifo), str(base), str(large)):
                with self.subTest(value=value), self.assertRaises(live.SafetyFailure):
                    live.read_selected_image(value)

    def test_corrupt_container_is_rejected_before_provider_or_database(self):
        with tempfile.TemporaryDirectory() as directory, \
                mock.patch.object(live.ProviderClient, "exchange") as exchange, \
                mock.patch.object(live, "PrivateDatabase") as database:
            source = Path(directory) / "not-an-image.png"
            source.write_bytes(b"not a png")
            with self.assertRaises(live.SafetyFailure):
                live.prepare_image(self.image_options(str(source)), Path(directory) / "media")
            exchange.assert_not_called()
            database.assert_not_called()

    def test_opt_in_image_preflight_has_real_media_blocks_and_independent_false_oracle(self):
        with tempfile.TemporaryDirectory() as directory, fake_client() as (server, client, diagnostics):
            image = live.prepare_image(self.image_options(expected=False), Path(directory) / "media")
            server.image_value = False
            live.preflight(client, image)
            body = server.records[-1]["body"]
            self.assertEqual(body["messages"][0]["content"], image.content())
            self.assertNotIn("input_modalities", body)
            self.assertNotIn(str(image.asset.path), live.descriptor_text(body))
            diagnostics.raise_if_failed()
            self.assertEqual(len(server.records), 9)

    def test_image_response_false_is_a_quality_failure_when_true_was_expected(self):
        with tempfile.TemporaryDirectory() as directory, fake_client() as (server, client, _diagnostics):
            image = live.prepare_image(self.image_options(), Path(directory) / "media")
            server.image_value = False
            with self.assertRaises(live.ModelQualityFailure):
                live.preflight(client, image)

    def test_text_only_provider_image_rejection_never_registers_capabilities_or_starts_sql(self):
        with fake_client() as (server, _client, _diagnostics):
            def reject_image(body, response):
                if isinstance(body["messages"][0]["content"], list):
                    return 400, b'{"error":"this provider model is text-only"}', {}
                return 200, json.dumps(response).encode(), {}

            server.override = reject_image
            output = io.StringIO()
            with redirect_stderr(output), redirect_stdout(output), \
                    mock.patch.object(live, "PrivateDatabase") as database:
                status = live.main([
                    "--endpoint", server.endpoint, "--model", FAKE_MODEL, "--binary", sys.executable,
                    "--image", "synthetic-red", "--instruction", "Is this image predominantly red?",
                    "--expect-image", "true",
                ])
            self.assertEqual(status, live.ProtocolFailure.exit_code)
            self.assertIn("text-only", output.getvalue())
            database.assert_not_called()


class SqlOracleAndRegistrationTests(unittest.TestCase):
    def test_each_sql_case_has_exact_request_independent_result_shape(self):
        cases = live.sql_cases()
        self.assertEqual(tuple(case.name for case in cases), live.TEXT_CASES)
        for case in cases:
            rows = tuple((*row[:-1], json.dumps({"label": row[-1]})) for row in case.expected) \
                if case.json_kind else case.expected
            live.assert_case_rows(case, rows)
        join = next(case for case in cases if case.name == "text-join")
        self.assertEqual(join.expected, ((11, 101), (22, 102), (33, 101)))
        self.assertEqual(next(case for case in cases if case.name == "composed-group").expected,
                         (("ALPHA", 2),))

    def test_text_sql_budgets_bound_candidates_and_keep_native_schemas(self):
        self.assertEqual(len(live.LEFT_ROWS) * len(live.RIGHT_ROWS), 8)
        plan = live.RequestPlan(FAKE_MODEL)
        for content in plan.contracts:
            prompt = json.loads(content)
            if isinstance(prompt, dict):
                prompt = live.descriptor_text(prompt)
            self.assertLessEqual(len(prompt.encode("utf-8")), 512)
        for case in live.sql_cases():
            for parameter in case.parameters:
                if isinstance(parameter, str) and parameter.startswith("{"):
                    options = json.loads(parameter)
                    self.assertEqual(options["max_tokens"], 32)
                    self.assertIs(options["stream"], False)
                    self.assertNotIn("Schema", options)
                    if "response_format" in options:
                        self.assertIs(options["response_format"]["json_schema"]["strict"], True)

    def test_sql_bool_projection_never_accepts_text_bool_or_missing_rows(self):
        case = next(case for case in live.sql_cases() if case.name == "text-filter-projection")
        for rows in ((), ((11, True), (22, False), (33, True), (44, False)),
                     ((11, "true"), (22, "false"), (33, "true"), (44, "false")),
                     ((11, 1), (22, None), (33, 1), (44, 0)), tuple(reversed(case.expected))):
            with self.subTest(rows=rows), self.assertRaises(live.SqlFailure):
                live.assert_case_rows(case, rows)

    def test_map_oracle_rejects_bad_json_wrong_labels_and_wrong_row_binding(self):
        case = live.sql_cases()[0]
        good = tuple((row_id, json.dumps({"label": label})) for row_id, label in live.LEFT_ROWS)
        for rows in (good[:-1], tuple(reversed(good)),
                     ((11, '{"label":"BETA"}'), *good[1:]),
                     ((11, "ALPHA"), *good[1:]),
                     ((11, '{"label":"ALPHA","extra":0}'), *good[1:]),
                     ((11, None), *good[1:]),
                     ((11, '{"label":"ALPHA"}', 3), *good[1:])):
            with self.subTest(rows=rows), self.assertRaises(live.SqlFailure):
                live.assert_case_rows(case, rows)

    def test_bool_map_oracle_preserves_false_and_demands_exact_json(self):
        case = live.SqlCase("boolean MAP fixture", "SELECT fixture", (), ((1, False),), "boolean")
        live.assert_case_rows(case, ((1, '{"value":false}'),))
        for rows in (((1, '{"value":"false"}'),), ((1, '{"value":true}'),), ((1, None),), ()):
            with self.subTest(rows=rows), self.assertRaises(live.SqlFailure):
                live.assert_case_rows(case, rows)

    def test_join_prompt_canonicalization_does_not_depend_on_json_whitespace(self):
        plan = live.RequestPlan(FAKE_MODEL)
        payload = {"rule": live.JOIN_RULE, "left": "ALPHA", "right": "BETA"}
        body = live.request_body(FAKE_MODEL, json.dumps(payload, indent=3), "boolean", "ai_join")
        contract = plan.contract_for(body)
        self.assertIs(contract.expected, False)
        body["messages"][0]["content"] = json.dumps(
            {"rule": live.JOIN_RULE, "right": "BETA", "left": "ALPHA"})
        with self.assertRaises(live.SqlFailure):
            plan.contract_for(body)

    def test_registration_is_openai_text_default_and_image_only_when_selected(self):
        for with_image in (False, True):
            with self.subTest(with_image=with_image), tempfile.TemporaryDirectory() as directory, \
                    fake_client() as (_server, client, _diagnostics):
                image = None
                if with_image:
                    image = live.prepare_image(ImageSafetyTests().image_options(), Path(directory) / "media")
                relay = live.RelayServer(client, live.RequestPlan(FAKE_MODEL, image))
                self.addCleanup(relay.close)
                calls = []
                models = {}
                cursor = mock.Mock()

                def execute(statement, parameters=None):
                    calls.append((statement, parameters))
                    if "CREATE_AI_MODEL(" in statement:
                        models[parameters[0]] = json.loads(parameters[1])
                    if statement == "SELECT @@GLOBAL.ai_media_root":
                        cursor.fetchall.return_value = (("",),)
                    elif statement.startswith("SELECT input_modalities"):
                        cursor.fetchall.return_value = ((3 if parameters[0] == live.IMAGE_MODEL_NAME else 1,),)
                    elif statement.startswith("SELECT FILE_URI"):
                        cursor.fetchall.return_value = ((image.asset.path.as_uri(), live.descriptor_text(
                            image.asset.descriptor(include_size=True))),)
                    else:
                        cursor.fetchall.return_value = ()

                cursor.execute.side_effect = execute
                live.LiveSqlContracts(cursor, relay, FAKE_MODEL, image).setup()
                self.assertNotIn("input_modalities", models[live.MODEL_NAME])
                endpoints = [json.loads(parameters[1]) for statement, parameters in calls
                             if "CREATE_AI_MODEL_ENDPOINT" in statement]
                for endpoint in endpoints:
                    self.assertEqual(endpoint["provider"], "openai")
                    self.assertEqual(endpoint["access_key"], live.REGISTRATION_KEY)
                    self.assertEqual(endpoint["url"], relay.url)
                    self.assertEqual(endpoint["request_model_name"], FAKE_MODEL)
                roots = [(statement, parameters) for statement, parameters in calls
                         if statement.startswith("SET GLOBAL ai_media_root")]
                self.assertEqual(len(models), 2 if with_image else 1)
                self.assertEqual(len(roots), 1 if with_image else 0)
                if with_image:
                    self.assertEqual(models[live.IMAGE_MODEL_NAME]["input_modalities"], ["text", "image"])
                    self.assertEqual(roots[0][1], (str(image.asset.path.parent),))
                    self.assertEqual(tuple(case.name for case in live.sql_cases(image)),
                                     live.TEXT_CASES + live.IMAGE_CASES)
                    self.assertIn("l.asset, r.answer", live.sql_cases(image)[-1].statement)

    def test_private_launcher_is_reused_and_cleanup_runs_on_contract_failure(self):
        database = mock.Mock()
        with mock.patch.object(live, "PrivateDatabase", return_value=database) as factory:
            with self.assertRaises(live.SqlFailure):
                with live.private_database(Path(sys.executable)) as actual:
                    self.assertIs(actual, database)
                    raise live.SqlFailure("fixture contract failure")
            factory.assert_called_once_with(Path(sys.executable))
            database.start.assert_called_once()
            database.close.assert_called_once()

    def test_cleanup_error_is_explicit_without_hiding_primary_quality_failure(self):
        database = mock.Mock()
        database.close.side_effect = RuntimeError("fixture cleanup failed")
        with mock.patch.object(live, "PrivateDatabase", return_value=database):
            with self.assertRaises(live.ModelQualityFailure) as caught:
                with live.private_database(Path(sys.executable)):
                    raise live.ModelQualityFailure("fixture wrong answer")
        self.assertIn("fixture wrong answer", str(caught.exception))
        self.assertIn("cleanup also failed", str(caught.exception))


class DiagnosticTests(unittest.TestCase):
    def test_raw_observation_is_bounded_and_secrets_are_redacted_before_truncation(self):
        secret = uuid.uuid4().hex
        redactor = live.Redactor(secret)
        diagnostics = live.Diagnostics(redactor)
        raw = (b"x" * (live.DIAGNOSTIC_CHARS - 8) + secret.encode() + b"tail")
        record = diagnostics.observe("fixture", 401, raw)
        self.assertNotIn(secret, record.raw)
        self.assertNotIn(secret[:8], record.raw)
        self.assertLessEqual(len(record.raw), live.DIAGNOSTIC_CHARS + len("...[truncated]"))
        text = redactor.bounded(json.dumps({"access_key": secret, "authorization": "Bearer " + secret}))
        self.assertNotIn(secret, text)
        self.assertIn("[REDACTED]", text)
        self.assertNotIn(live.REGISTRATION_KEY, redactor.bounded(live.REGISTRATION_KEY))

    def test_first_failed_raw_response_survives_ring_overflow(self):
        diagnostics = live.Diagnostics(live.Redactor())
        failed = diagnostics.observe("first failure", 400, b"ACTUAL_FIRST_FAILURE")
        diagnostics.fail(failed, live.ProtocolFailure("fixture protocol rejection"))
        for index in range(live.DIAGNOSTIC_RECORDS + 3):
            diagnostics.observe(f"fixture {index}", 200, b"later provider body")
        self.assertEqual(len(diagnostics.records), live.DIAGNOSTIC_RECORDS)
        output = io.StringIO()
        diagnostics.write(output)
        self.assertIn("ACTUAL_FIRST_FAILURE", output.getvalue())
        self.assertIn("provider-protocol", output.getvalue())

    def test_total_output_bound_and_binary_or_terminal_controls_are_safe(self):
        diagnostics = live.Diagnostics(live.Redactor())
        for _ in range(live.DIAGNOSTIC_RECORDS):
            diagnostics.observe("fixture", 200, b"\x00" * live.DIAGNOSTIC_CHARS + b"\xff\x1b")
        output = io.StringIO()
        diagnostics.write(output)
        text = output.getvalue()
        self.assertNotIn("\x00", text)
        self.assertNotIn("\x1b", text)
        self.assertIn("output bound", text)
        self.assertLess(len(text), live.DIAGNOSTIC_OUTPUT_CHARS + 256)


if __name__ == "__main__":
    unittest.main(verbosity=2)
