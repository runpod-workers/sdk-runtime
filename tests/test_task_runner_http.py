"""integration tests for the task runner http server.

boots the real ThreadingHTTPServer on a random port and exercises the
protocol over actual sockets. engine behavior (dependency install,
serialization, stdout capture) is covered in test_executor.py.
"""

import base64
import json
import threading
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

import cloudpickle
import pytest

from runpod_sdk_runtime.task import runner as task_runner
from runpod_sdk_runtime.task.runner import Handler

# http.server's per-request sockets are collected lazily; the unraisable
# checker flags them as ResourceWarnings non-deterministically
pytestmark = pytest.mark.filterwarnings(
    "ignore::pytest.PytestUnraisableExceptionWarning"
)

TOKEN = "test-token"


@pytest.fixture()
def server(monkeypatch, request):
    from runpod.apps.volume import _configure_mounts

    monkeypatch.setattr(task_runner, "TOKEN", TOKEN)
    monkeypatch.setattr(task_runner, "_job_state", {"status": "NONE", "response": None})
    monkeypatch.setenv("RUNPOD_MOUNTS", json.dumps(getattr(request, "param", [])))
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    httpd.daemon_threads = True
    ready = threading.Event()

    def started_server(*_):
        ready.set()
        return httpd

    monkeypatch.setattr(task_runner, "ThreadingHTTPServer", started_server)
    monkeypatch.setattr(task_runner, "_watchdog", lambda: None)
    thread = threading.Thread(target=task_runner.main, daemon=True)
    thread.start()
    assert ready.wait(timeout=5), "task runner did not finish startup"
    yield f"http://127.0.0.1:{httpd.server_port}"
    httpd.shutdown()
    thread.join(timeout=5)
    httpd.server_close()
    _configure_mounts([])


def _request(url, method="GET", body=None, token=TOKEN):
    req = urllib.request.Request(url, method=method)
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, data=data, timeout=10) as resp:
        return resp.status, json.loads(resp.read())


def _b64(value):
    return base64.b64encode(cloudpickle.dumps(value)).decode()


def _unb64(value):
    return cloudpickle.loads(base64.b64decode(value))


def test_ping_unauthenticated(server):
    status, body = _request(f"{server}/ping", token=None)
    assert status == 200
    assert body == {"ready": True}


def test_execute_requires_auth(server):
    with pytest.raises(urllib.error.HTTPError) as exc_info:
        _request(f"{server}/execute", method="POST", body={}, token="wrong")
    assert exc_info.value.code == 401


def test_execute_roundtrip(server):
    status, body = _request(
        f"{server}/execute",
        method="POST",
        body={
            "function_name": "mul",
            "function_code": "def mul(a, b):\n    return a * b",
            "args": [_b64(6), _b64(7)],
            "kwargs": {},
        },
    )
    assert status == 200
    assert body["success"] is True
    assert _unb64(body["result"]) == 42


def test_submit_and_result(server):
    status, body = _request(
        f"{server}/submit",
        method="POST",
        body={
            "function_name": "quick",
            "function_code": "def quick():\n    return 'done'",
            "args": [],
            "kwargs": {},
            "serialization_format": "json",
        },
    )
    assert status == 200
    assert body == {"status": "RUNNING"}

    import time

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        status, body = _request(f"{server}/result")
        if body["status"] == "DONE":
            break
        time.sleep(0.1)

    assert body["status"] == "DONE"
    assert body["response"]["success"] is True
    assert body["response"]["json_result"] == "done"


@pytest.mark.parametrize(
    "server",
    [[{"kind": "network", "reference": "data", "id": "nv-1", "path": "/data"}]],
    indirect=True,
)
def test_mounts_survive_threaded_execution(server, monkeypatch):
    monkeypatch.setenv("RUNPOD_MOUNTS", "[]")
    status, body = _request(
        f"{server}/execute",
        method="POST",
        body={
            "function_name": "mounted",
            "function_code": (
                "from runpod.apps.volume import NetworkVolume\n"
                "imported = str(NetworkVolume('data').path)\n"
                "async def mounted():\n"
                "    yield imported\n"
                "    yield str(NetworkVolume('nv-1').path)\n"
            ),
            "serialization_format": "json",
            "mounts": [],
        },
    )
    assert status == 200
    assert body["success"] is True
    assert body["json_result"] == ["/data", "/data"]


def test_unknown_path_404(server):
    with pytest.raises(urllib.error.HTTPError) as exc_info:
        _request(f"{server}/nope")
    assert exc_info.value.code == 404


class TestWatchdog:
    def test_running_job_never_killed(self):
        from runpod_sdk_runtime.task.runner import _should_self_terminate

        assert not _should_self_terminate("RUNNING", 0, 10_000, 600)

    def test_abandoned_before_submit(self):
        from runpod_sdk_runtime.task.runner import _should_self_terminate

        assert _should_self_terminate("NONE", 0, 601, 600)

    def test_uncollected_result(self):
        from runpod_sdk_runtime.task.runner import _should_self_terminate

        assert _should_self_terminate("DONE", 0, 601, 600)

    def test_live_client_keeps_pod(self):
        from runpod_sdk_runtime.task.runner import _should_self_terminate

        # polls every ~2s: last contact is always recent
        assert not _should_self_terminate("DONE", 599, 600, 600)

    def test_no_contact_recorded_yet(self):
        from runpod_sdk_runtime.task.runner import _should_self_terminate

        assert not _should_self_terminate("NONE", None, 10_000, 600)

    def test_inline_execution_never_killed(self):
        assert not task_runner._should_self_terminate("NONE", 0, 10_000, 600, inline=1)

    def test_authed_requests_touch_contact(self, server, monkeypatch):
        monkeypatch.setattr(task_runner, "_last_contact", {"ts": None})
        _request(f"{server}/result")
        assert task_runner._last_contact["ts"] is not None


class TestSelfTermination:
    def _api(self, monkeypatch, responses):
        import io

        remaining = iter(responses)
        exits = []
        attempts = []

        def request(req, timeout=None):
            attempts.append(req)
            response = next(remaining)
            if isinstance(response, Exception):
                raise response
            return io.BytesIO(json.dumps(response).encode())

        monkeypatch.setenv("RUNPOD_POD_ID", "pod-1")
        monkeypatch.setenv("RUNPOD_API_KEY", "pod-scoped-key")
        monkeypatch.setattr("urllib.request.urlopen", request)
        monkeypatch.setattr(task_runner.os, "_exit", exits.append)
        monkeypatch.setattr(task_runner.time, "sleep", lambda delay: None)
        return exits, attempts

    def test_void_acknowledgment_allows_exit(self, monkeypatch):
        exits, _ = self._api(monkeypatch, [{"data": {"podTerminate": None}}])
        assert task_runner._terminate_self() is True
        assert exits == [0]

    @pytest.mark.parametrize(
        "body",
        [
            {},
            {"data": {}},
            {"data": {"podTerminate": False}},
            {"errors": [{"message": "forbidden"}], "data": {"podTerminate": None}},
        ],
    )
    def test_unacknowledged_deletion_keeps_runtime_alive(self, monkeypatch, body):
        exits, attempts = self._api(monkeypatch, [body])
        assert task_runner._terminate_self() is False
        assert not exits
        assert len(attempts) == 1

    @pytest.mark.parametrize("missing", ["RUNPOD_POD_ID", "RUNPOD_API_KEY"])
    def test_missing_credentials_do_not_exit(self, monkeypatch, missing):
        exits, attempts = self._api(monkeypatch, [])
        monkeypatch.delenv(missing)
        assert task_runner._terminate_self() is False
        assert not attempts
        assert not exits

    @pytest.mark.parametrize(
        "failure",
        [
            urllib.error.HTTPError("https://api/graphql", 429, "slow down", {}, None),
            urllib.error.HTTPError("https://api/graphql", 503, "unavailable", {}, None),
            urllib.error.URLError("connection reset"),
            {"errors": [{"extensions": {"code": "INTERNAL_SERVER_ERROR"}}]},
        ],
    )
    def test_transient_failure_recovers(self, monkeypatch, failure):
        exits, _ = self._api(monkeypatch, [failure, {"data": {"podTerminate": None}}])
        assert task_runner._terminate_self() is True
        assert exits == [0]

    def test_retry_exhaustion_keeps_runtime_alive(self, monkeypatch):
        exits, attempts = self._api(
            monkeypatch, [OSError("network down")] * task_runner.TERMINATE_ATTEMPTS
        )
        assert task_runner._terminate_self() is False
        assert len(attempts) == task_runner.TERMINATE_ATTEMPTS
        assert not exits

    def test_auth_failure_does_not_retry_or_exit(self, monkeypatch):
        error = urllib.error.HTTPError("https://api/graphql", 401, "denied", {}, None)
        exits, attempts = self._api(monkeypatch, [error])
        assert task_runner._terminate_self() is False
        assert len(attempts) == 1
        assert not exits
