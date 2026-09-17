"""Offline AI_COMPLETE regression: python test_runtime.py --binary /path/to/seekdb."""

import argparse
from collections import Counter
import json
import os
from pathlib import Path
import resource
import subprocess
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pymysql


class MockServer(ThreadingHTTPServer):
    request_queue_size = 64
    daemon_threads = True

    def __init__(self):
        super().__init__(("127.0.0.1", 0), MockHandler)
        self.condition = threading.Condition()
        self.active = 0
        self.peak = 0
        self.requests = []
        self.finished = []
        self.counts = Counter()

    def reset(self):
        with self.condition:
            assert self.condition.wait_for(lambda: self.active == 0, timeout=10)
            self.peak = 0
            self.requests.clear()
            self.finished.clear()
            self.counts.clear()


class MockHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_args):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        prompt = body["messages"][-1]["content"]
        server = self.server
        with server.condition:
            server.active += 1
            server.peak = max(server.peak, server.active)
            server.requests.append(body)
            server.counts[prompt] += 1
            attempt = server.counts[prompt]
            server.condition.notify_all()
        try:
            delay = 0.16
            if prompt.startswith("row-"):
                delay += (7 - int(prompt[4:]) % 8) * 0.02
            if prompt == "row-0":
                delay = 0.8
            if prompt.startswith("slow"):
                delay = 3
            threading.Event().wait(delay)
            status = 200
            payload = {"choices": [{"message": {"content": 'reply:"' + prompt + '"\n'}}],
                       "usage": {"total_tokens": 3}}
            headers = {}
            if prompt in ("retry", "retry-date", "retry-503") and attempt == 1:
                status = 503 if prompt == "retry-503" else 429
                headers["Retry-After"] = (self.date_time_string(time.time() + 1)
                                          if prompt == "retry-date" else "1")
                payload = {"error": "retry"}
            elif prompt == "rate-limit":
                status = 429
                headers["Retry-After"] = "10"
            elif prompt == "permanent":
                status = 400
            elif prompt == "invalid-provider":
                payload = {"unexpected": True}
            elif prompt == "non-object":
                payload = []
            raw = json.dumps(payload).encode()
            if prompt == "invalid-json":
                raw = b"{broken"
            elif prompt == "oversize":
                raw = b"x" * (8 * 1024 * 1024 + 1)
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            for name, value in headers.items():
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(raw)
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            with server.condition:
                server.active -= 1
                server.finished.append(prompt)
                server.condition.notify_all()


def run_checks(connection, server, socket):
    with connection.cursor() as cursor:
        cursor.execute("SET ob_query_timeout = 30000000")
        cursor.execute("CREATE DATABASE ai_runtime_test")
        cursor.execute("USE ai_runtime_test")
        cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL(%s, %s)",
                       ("runtime_model", json.dumps({"type": "completion", "model_name": "mock"})))
        endpoint = {"ai_model_name": "runtime_model", "url":
                    f"http://127.0.0.1:{server.server_port}/v1/chat/completions",
                    "access_key": "local-test-only", "provider": "openai",
                    "request_model_name": "mock"}
        cursor.execute("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT(%s, %s)",
                       ("runtime_endpoint", json.dumps(endpoint)))
        cursor.execute("CREATE TABLE inputs (id INT PRIMARY KEY, prompt LONGTEXT, model VARCHAR(64))")

        def load(prompts):
            server.reset()
            cursor.execute("TRUNCATE TABLE inputs")
            cursor.executemany("INSERT INTO inputs VALUES (%s, %s, 'runtime_model')",
                               list(enumerate(prompts)))

        def query(expression="AI_COMPLETE('runtime_model', prompt)"):
            start = time.monotonic()
            cursor.execute(f"SELECT id, {expression} FROM inputs ORDER BY id")
            return cursor.fetchall(), time.monotonic() - start

        def expect_error(expression="AI_COMPLETE('runtime_model', prompt)", *, code=None):
            try:
                query(expression)
            except pymysql.MySQLError as error:
                if code is not None:
                    assert error.args[0] == code, ("wrong SQL error", code, error.args)
                return
            raise AssertionError("expected SQL failure")

        prompts = [f"row-{index}" for index in range(24)]
        load(prompts)
        batch, batch_wall = query()
        batch_bodies = {body["messages"][-1]["content"]: body for body in server.requests}
        assert batch == tuple((index, 'reply:"' + prompt + '"\n')
                              for index, prompt in enumerate(prompts)), batch
        assert 1 < server.peak <= 50, ("not bounded/concurrent", server.peak)
        assert server.finished != prompts, "mock did not reorder responses"
        assert server.finished.index("row-8") < server.finished.index("row-0"), server.finished
        assert server.counts == Counter(prompts)
        server.reset()
        scalar, scalar_wall = query("AI_COMPLETE(model, prompt)")
        assert scalar == batch
        assert server.peak == 1, ("dynamic model should fall back", server.peak)
        assert batch_bodies == {body["messages"][-1]["content"]: body for body in server.requests}
        assert batch_wall < scalar_wall * 0.75, (batch_wall, scalar_wall)
        print(f"PASS ordered batch/scalar: batch={batch_wall:.3f}s scalar={scalar_wall:.3f}s", flush=True)

        chunk_prompts = [f"chunk-{index}" for index in range(72)]
        load(chunk_prompts)
        chunk_result, _ = query()
        assert chunk_result == tuple((index, 'reply:"' + prompt + '"\n')
                         for index, prompt in enumerate(chunk_prompts))
        assert server.counts == Counter(chunk_prompts)
        assert 1 < server.peak <= 50
        print("PASS window refill across the SQL batch", flush=True)

        for prompt in ("retry", "retry-date", "retry-503"):
            load([prompt] + prompts[:12])
            result, _ = query()
            assert len(result) == 13
            assert server.counts[prompt] == 2, server.counts
            assert all(server.counts[value] == 1 for value in prompts[:12])
        print("PASS per-request retries, Retry-After, successful rows not resent", flush=True)

        load(prompts)
        result, _ = query("CASE WHEN id % 2 = 0 THEN AI_COMPLETE('runtime_model', prompt) ELSE 'skip' END")
        assert len(result) == len(prompts)
        assert server.counts == Counter(prompts[::2]), server.counts
        load(["same"])
        cursor.execute("SELECT value, value FROM (SELECT AI_COMPLETE('runtime_model', prompt) value FROM inputs) derived")
        assert cursor.fetchone() == ('reply:"same"\n', 'reply:"same"\n')
        assert server.counts["same"] == 1, server.counts
        print("PASS CASE skip and reused expression", flush=True)

        for prompt in (None, ""):
            load([prompt])
            expect_error()
            assert not server.requests
        load(["config"])
        expect_error("AI_COMPLETE('runtime_model', prompt, NULL)")
        assert not server.requests
        print("PASS NULL and empty input errors", flush=True)

        load(["config-a", "config-b"])
        query("AI_COMPLETE('runtime_model', prompt, '{\"temperature\":0}')")
        assert all(body["temperature"] == 0 for body in server.requests)
        server.reset()
        query("AI_COMPLETE('runtime_model', prompt, CONCAT('{\"temperature\":', id, '}'))")
        assert server.peak == 1
        assert [body["temperature"] for body in server.requests] == [0, 1]
        load(["json-a", "json-b"])
        query("AI_COMPLETE('runtime_model', AI_PROMPT('hello {0}', CAST(prompt AS CHAR)))")
        assert server.counts == Counter(["hello json-a", "hello json-b"]), server.counts
        load(["lob-" + "x" * 200000])
        result, _ = query()
        assert len(result[0][1]) == 200013, len(result[0][1])
        print("PASS constant config, JSON prompt and LOB", flush=True)

        for prompt in ("permanent", "invalid-json", "non-object", "invalid-provider", "oversize"):
            load([prompt])
            expect_error()
            assert server.counts[prompt] == 1, server.counts
        load(["x" * (4 * 1024 * 1024 + 1)])
        expect_error()
        assert not server.requests
        load(["recovered"])
        query()
        print("PASS permanent/parse/provider/size errors and recovery", flush=True)

        for prompt in ("slow", "rate-limit"):
            load([prompt] * 72)
            cursor.execute("SET ob_query_timeout = 700000")
            start = time.monotonic()
            expect_error(code=4012)
            assert 0.5 <= time.monotonic() - start < 2
            assert server.peak <= 50
            submitted = len(server.requests)
            assert 0 < submitted <= (50 if prompt == "slow" else 72)
            with server.condition:
                assert server.condition.wait_for(lambda: server.active == 0, timeout=5)
                assert len(server.requests) == submitted
            cursor.execute("SET ob_query_timeout = 30000000")
        print("PASS deadline covers active requests and retry queue", flush=True)

        load(["slow-cancel"] * 72)
        errors = []
        def cancel_query():
            try:
                query()
            except pymysql.MySQLError as error:
                errors.append(error)
        worker = threading.Thread(target=cancel_query)
        worker.start()
        with server.condition:
            assert server.condition.wait_for(lambda: bool(server.requests), timeout=5)
        with pymysql.connect(unix_socket=socket, user="root", autocommit=True) as control:
            with control.cursor() as kill_cursor:
                kill_cursor.execute(f"KILL QUERY {connection.thread_id()}")
        worker.join(3)
        assert not worker.is_alive() and errors, errors
        assert errors[0].args[0] == 1317, errors[0].args
        assert len(server.requests) <= 50
        print("PASS query cancellation stops refill", flush=True)


def main(checks=run_checks, *, live=False, server_env=None, sql_timeout=40, description=__doc__):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--binary", required=True)
    if live:
        parser.add_argument("--allow-live", action="store_true", required=True,
                            help="Allow real model requests, which may incur charges")
    options = parser.parse_args()
    server = None if live else MockServer()
    thread = None
    if server is not None:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
    try:
        with tempfile.TemporaryDirectory(prefix="seekdb-ai-runtime-") as directory:
            base = Path(directory)
            environment = dict(os.environ, no_proxy="127.0.0.1,localhost", NO_PROXY="127.0.0.1,localhost")
            if server_env is not None:
                environment.update(server_env)
            with (base / "startup.log").open("w+") as log:
                process = subprocess.Popen([options.binary, "--nodaemon", "--base-dir", directory,
                    "--parameter", "mysql_port_mode=disabled", "--parameter", "enable_rpc_service=false",
                    "--parameter", "memory_budget=2G", "--parameter", "datafile_size=1G",
                    "--parameter", "datafile_maxsize=4G", "--parameter", "log_disk_size=2G"],
                    stdout=log, stderr=subprocess.STDOUT, env=environment)
                connection = None
                try:
                    socket = str(base / "run/sql.sock")
                    deadline = time.monotonic() + 180
                    while time.monotonic() < deadline and process.poll() is None:
                        try:
                            connection = pymysql.connect(unix_socket=socket, user="root", autocommit=True,
                                                         read_timeout=sql_timeout, write_timeout=sql_timeout,
                                                         charset="utf8mb4")
                            break
                        except pymysql.MySQLError:
                            threading.Event().wait(0.2)
                    if connection is None:
                        log.seek(0)
                        raise RuntimeError("test server failed to start: " + log.read()[-4000:])
                    checks(connection, server, socket)
                finally:
                    if connection is not None:
                        connection.close()
                    process.terminate()
                    try:
                        process.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
                usage = resource.getrusage(resource.RUSAGE_CHILDREN)
                print(f"Database process including bootstrap: cpu={usage.ru_utime + usage.ru_stime:.2f}s "
                      f"peak_rss={usage.ru_maxrss}KiB", flush=True)
    finally:
        if server is not None:
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == "__main__":
    main()