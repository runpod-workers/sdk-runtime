"""production task bootstrap, http transport and lifecycle contracts."""

import io
import json
import threading
import time
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from unittest.mock import Mock

import pytest

from runpod_sdk_runtime import bootstrap
from runpod_sdk_runtime.task import deployed, runner

TOKEN = "execution-token"


@pytest.fixture
def artifact(tmp_path, monkeypatch):
    import hashlib

    module = "task_" + hashlib.sha256(str(tmp_path).encode()).hexdigest()[:12]
    source = """import runpod
app = runpod.App("example")
initialized = False
@app.task(name="work", cpu="cpu3c")
async def work(value=1, *, mode="success"):
    assert initialized
    print("task output")
    if mode == "error":
        raise ValueError("task failed")
    if mode == "nonfinite":
        return float("nan")
    if mode == "nonjson":
        return {1, 2}
    return {"value": value}
@work.init
async def initialize():
    global initialized
    initialized = True
"""
    (tmp_path / (module + ".py")).write_text(source)
    manifest = {
        "app": "example",
        "resources": [
            {"name": "work", "kind": "task", "module": module, "qualname": "work"}
        ],
    }
    (tmp_path / bootstrap.MANIFEST_NAME).write_text(json.dumps(manifest))
    for key, value in {
        "RUNPOD_FLASH_PROTOCOL": "1",
        "RUNPOD_FLASH_TASK": "1",
        "RUNPOD_RUNTIME_KIND": "task",
        "RUNPOD_BUILD_ID": "build-1",
        "FLASH_APP_NAME": "example",
        "FLASH_ENVIRONMENT": "production",
        "FLASH_RESOURCE_NAME": "work",
        "FLASH_BUILD_ARTIFACT_PATH": str(tmp_path / "artifact.tar.gz"),
        "RUNPOD_APP_DIR": str(tmp_path),
        "RUNPOD_MOUNTS": "[]",
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("RUNPOD_TASK_EXECUTION_TIMEOUT_SECONDS", raising=False)
    monkeypatch.syspath_prepend(str(tmp_path))
    return tmp_path, manifest


def request(url, method="GET", body=None, token=TOKEN):
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, method=method, headers=headers, data=data)
    try:
        response = urllib.request.urlopen(req, timeout=5)
    except urllib.error.HTTPError as exc:
        response = exc
    with response:
        return response.status, json.load(response)


def wait_ready(url):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        status, body = request(url + "/ping", token=None)
        if status != 503:
            return status, body
        time.sleep(0.01)
    pytest.fail("task did not initialize")


@pytest.fixture
def start_server(artifact, monkeypatch):
    from runpod.apps.volume import _configure_mounts

    servers = []
    monkeypatch.setattr(runner, "TOKEN", "execution-token")
    monkeypatch.setattr(runner, "_watchdog", lambda: None)
    for attr, value in {
        "_job_state": {"status": "NONE", "response": None},
        "_execution_deadline": None,
        "_watchdog_wakeup": threading.Event(),
        "_inline_executions": {"count": 0},
        "_ready": False,
        "_bootstrap_error": None,
        "_deployed_handle": None,
        "_timeout_cap": None,
        "_production": False,
    }.items():
        monkeypatch.setattr(runner, attr, value)

    def start(error=None):
        httpd = ThreadingHTTPServer(("127.0.0.1", 0), runner.Handler)
        httpd.daemon_threads = True
        monkeypatch.setattr(runner, "ThreadingHTTPServer", lambda *_: httpd)
        thread = threading.Thread(
            target=runner.main, kwargs={"bootstrap_error": error}, daemon=True
        )
        servers.append((httpd, thread))
        thread.start()
        return f"http://127.0.0.1:{httpd.server_port}"

    yield start
    for httpd, thread in servers:
        httpd.shutdown()
        thread.join(5)
        httpd.server_close()
    _configure_mounts([])


def test_registered_task_submit_result_and_single_shot(start_server):
    url = start_server()
    assert wait_ready(url) == (200, {"ready": True})
    assert request(url + "/submit", "POST", {"args": [42]}) == (
        200,
        {"status": "RUNNING"},
    )
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        _, result = request(url + "/result")
        if result["status"] == "DONE":
            break
        time.sleep(0.01)
    assert result == {
        "status": "DONE",
        "response": {
            "success": True,
            "json_result": {"value": 42},
            "stdout": "task output\n",
        },
    }
    assert request(url + "/submit", "POST", {})[0] == 409


@pytest.mark.parametrize(
    "mode, message",
    [
        ("error", "task failed"),
        ("nonfinite", "Out of range float"),
        ("nonjson", "json-serializable"),
    ],
)
def test_registered_task_errors_preserve_stdout(start_server, mode, message):
    url = start_server()
    assert wait_ready(url)[0] == 200
    status, response = request(url + "/execute", "POST", {"kwargs": {"mode": mode}})
    assert status == 200
    assert response["success"] is False
    assert message in response["error"]
    assert response["stdout"] == "task output\n"


@pytest.mark.parametrize(
    "body",
    [
        {"function_code": "raise RuntimeError()"},
        {"function_name": "other"},
        {"dependencies": []},
        {"system_dependencies": []},
        {"mounts": []},
        {"serialization_format": "cloudpickle"},
        {"args": "pickle"},
        {"kwargs": []},
        {"args": [float("nan")]},
        {"args": None},
        {"kwargs": None},
        {"timeout": 10**400},
    ],
)
def test_production_rejects_request_overrides(start_server, body):
    url = start_server()
    assert wait_ready(url)[0] == 200
    assert request(url + "/submit", "POST", body)[0] == 400
    assert runner._job_state["status"] == "NONE"
    assert runner._execution_deadline is None


@pytest.mark.parametrize("body", [[], None, "text", 4])
def test_production_rejects_malformed_body(start_server, body):
    url = start_server()
    assert wait_ready(url)[0] == 200
    req = urllib.request.Request(
        url + "/submit",
        data=json.dumps(body).encode(),
        headers={"Authorization": "Bearer execution-token"},
    )
    with pytest.raises(urllib.error.HTTPError) as exc:
        urllib.request.urlopen(req, timeout=5)
    assert exc.value.code == 400
    exc.value.close()


def test_production_readiness_waits_for_init(start_server, monkeypatch):
    entered, release = threading.Event(), threading.Event()
    original = deployed.load_handle

    def load():
        entered.set()
        release.wait(5)
        return original()

    monkeypatch.setattr(deployed, "load_handle", load)
    url = start_server()
    try:
        assert entered.wait(5)
        assert request(url + "/ping", token=None) == (503, {"ready": False})
        assert request(url + "/submit", "POST", {})[0] == 503
    finally:
        release.set()
    assert wait_ready(url)[0] == 200


def test_init_failure_has_task_error_surface(start_server, monkeypatch):
    monkeypatch.setattr(deployed, "load_handle", Mock(side_effect=ValueError("init")))
    url = start_server()
    status, body = wait_ready(url)
    assert status == 500
    assert body["ready"] is False
    assert body["error_type"] == "BootstrapError"
    assert "init" in body["error_message"]
    assert request(url + "/submit", "POST", {}) == (status, body)
    assert request(url + "/result") == (status, body)
    assert request(url + "/submit", "POST", {}, token="wrong")[0] == 401


@pytest.mark.parametrize(
    "key,value",
    [
        ("RUNPOD_FLASH_PROTOCOL", "0"),
        ("RUNPOD_FLASH_TASK", "0"),
        ("RUNPOD_RUNTIME_KIND", "queue"),
        ("FLASH_APP_NAME", ""),
        ("FLASH_RESOURCE_NAME", ""),
        ("FLASH_ENVIRONMENT", ""),
        ("RUNPOD_BUILD_ID", ""),
        ("FLASH_BUILD_ARTIFACT_PATH", ""),
    ],
)
def test_invalid_production_identity_never_falls_back(
    artifact, monkeypatch, key, value
):
    monkeypatch.setenv(key, value)
    with pytest.raises(bootstrap.PhaseError):
        deployed.load_handle()


@pytest.mark.parametrize("change", ["app", "kind", "name", "qualname", "module"])
def test_manifest_must_identify_registered_task(artifact, change):
    path, manifest = artifact
    if change == "app":
        manifest["app"] = "other"
    else:
        manifest["resources"][0][change] = "other"
    (path / bootstrap.MANIFEST_NAME).write_text(json.dumps(manifest))
    with pytest.raises((bootstrap.PhaseError, RuntimeError, ImportError)):
        deployed.load_handle()


def test_duplicate_task_manifest_rejected(artifact):
    _, manifest = artifact
    manifest["resources"] *= 2
    with pytest.raises(bootstrap.PhaseError, match="exactly one"):
        bootstrap._task_resource(manifest)


def test_manifest_qualname_resolves_class_attribute(artifact):
    path, manifest = artifact
    entry = manifest["resources"][0]
    entry["qualname"] = "Tasks.work"
    (path / bootstrap.MANIFEST_NAME).write_text(json.dumps(manifest))
    (path / (entry["module"] + ".py")).write_text(
        'import runpod\napp = runpod.App("example")\nclass Tasks:\n'
        '    @app.task(name="work", cpu="cpu3c")\n'
        "    def work():\n        return 7\n"
    )
    assert deployed.execute(deployed.load_handle(), {})["json_result"] == 7


def test_production_mode_cannot_fall_back_after_startup(start_server, monkeypatch):
    url = start_server()
    assert wait_ready(url)[0] == 200
    for key in ("RUNPOD_FLASH_TASK", "RUNPOD_FLASH_PROTOCOL", "RUNPOD_RUNTIME_KIND"):
        monkeypatch.delenv(key, raising=False)
    assert (
        request(
            url + "/execute",
            "POST",
            {
                "function_name": "live",
                "function_code": "def live(): return 7",
                "serialization_format": "json",
            },
        )[0]
        == 400
    )
    assert request(url + "/execute", "POST", {})[1]["json_result"] == {"value": 1}


def test_configured_cap_starts_at_first_execution(start_server, monkeypatch):
    monkeypatch.setenv("RUNPOD_TASK_EXECUTION_TIMEOUT_SECONDS", "60")
    url = start_server()
    assert wait_ready(url)[0] == 200
    assert runner._execution_deadline is None
    start = time.monotonic()
    assert request(url + "/execute", "POST", {"timeout": 600})[0] == 200
    deadline = runner._execution_deadline
    assert start + 60 <= deadline <= time.monotonic() + 60
    assert request(url + "/timeout", "POST", {"timeout": 6000})[0] == 200
    assert request(url + "/timeout", "POST", {})[0] == 200
    assert runner._execution_deadline == deadline
    request(url + "/timeout", "POST", {"timeout": 1})
    assert runner._execution_deadline < deadline


@pytest.mark.parametrize("value", ["", "0", "-1", "nan", "inf", "text"])
def test_invalid_configured_cap_fails_readiness(start_server, monkeypatch, value):
    monkeypatch.setenv("RUNPOD_TASK_EXECUTION_TIMEOUT_SECONDS", value)
    url = start_server()
    status, body = wait_ready(url)
    assert status == 500
    assert "finite and positive" in body["error_message"]


def test_no_policy_cap_does_not_add_deadline(start_server):
    url = start_server()
    assert wait_ready(url)[0] == 200
    request(url + "/execute", "POST", {})
    assert runner._execution_deadline is None


def test_deployed_execution_never_resolves_source_or_pickle(artifact, monkeypatch):
    from runpod_sdk_runtime import executor

    resolve = Mock(side_effect=AssertionError("source resolution"))
    pickle = Mock(side_effect=AssertionError("pickle resolution"))
    monkeypatch.setattr(executor, "resolve_request", resolve)
    monkeypatch.setattr(executor, "_load_cloudpickle", pickle)
    result = deployed.execute(deployed.load_handle(), {"kwargs": {"value": 7}})
    assert result["json_result"] == {"value": 7}
    resolve.assert_not_called()
    pickle.assert_not_called()


def test_source_execution_ignores_production_policy_cap(start_server, monkeypatch):
    for key in ("RUNPOD_FLASH_TASK", "RUNPOD_FLASH_PROTOCOL", "RUNPOD_RUNTIME_KIND"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("RUNPOD_TASK_EXECUTION_TIMEOUT_SECONDS", "invalid")
    url = start_server()
    assert wait_ready(url)[0] == 200
    status, response = request(
        url + "/execute",
        "POST",
        {
            "function_name": "live",
            "function_code": "def live(): return 7",
            "serialization_format": "json",
        },
    )
    assert status == 200
    assert response["json_result"] == 7
    assert runner._execution_deadline is None


@pytest.mark.parametrize(
    "result,expected", [(True, True), (False, False), (None, False)]
)
def test_production_self_termination_uses_pod_auth(monkeypatch, result, expected):
    monkeypatch.setenv("RUNPOD_FLASH_TASK", "1")
    monkeypatch.setenv("RUNPOD_POD_ID", "pod-1")
    monkeypatch.setenv("RUNPOD_API_KEY", "internal-key")
    response = io.BytesIO(json.dumps({"data": {"flashTaskTerminate": result}}).encode())
    urlopen = Mock(return_value=response)
    exits = []
    monkeypatch.setattr(runner.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(runner.os, "_exit", exits.append)
    assert runner._terminate_self() is expected
    req = urlopen.call_args.args[0]
    assert req.get_header("Authorization") == "Bearer internal-key"
    assert req.get_header("X-runpod-pod-id") == "pod-1"
    payload = json.loads(req.data)
    assert "FlashTaskTerminateInput!" in payload["query"]
    assert "flashTaskTerminate(input:" in payload["query"]
    assert payload["variables"] == {"input": {"podId": "pod-1"}}
    assert exits == ([0] if expected else [])


def test_task_bootstrap_error_never_enters_queue_loop(monkeypatch):
    monkeypatch.setenv("RUNPOD_FLASH_TASK", "1")
    monkeypatch.setenv("RUNPOD_RUNTIME_KIND", "queue")
    main = Mock()
    queue = Mock()
    monkeypatch.setattr(runner, "main", main)
    monkeypatch.setattr(bootstrap, "_queue_error_loop", queue)
    bootstrap._report_error(bootstrap.PhaseError("locate", "missing"))
    queue.assert_not_called()
    assert main.call_args.kwargs["bootstrap_error"]["error_type"] == "BootstrapError"


def test_production_bootstrap_requires_exact_archive(artifact, monkeypatch):
    path, _ = artifact
    prebuilt = path / "prebuilt"
    prebuilt.mkdir()
    monkeypatch.setattr(bootstrap, "PREBUILT_APP_DIR", str(prebuilt))
    monkeypatch.setattr(bootstrap, "ARTIFACT_PATH", str(path / "build-specific.tar.gz"))
    monkeypatch.setattr(bootstrap, "APP_DIR", str(path / "app"))
    monkeypatch.setattr(bootstrap, "ARTIFACT_WAIT_SECONDS", 0)
    with pytest.raises(bootstrap.PhaseError, match="artifact not found"):
        bootstrap._locate()


def test_production_bootstrap_rejects_other_artifact_marker(artifact, monkeypatch):
    path, _ = artifact
    (path / bootstrap.UNPACK_MARKER).write_text("other-build")
    monkeypatch.setattr(bootstrap, "APP_DIR", str(path))
    with pytest.raises(bootstrap.PhaseError, match="another artifact"):
        bootstrap._locate()


@pytest.mark.parametrize("contents", ["{bad", "[]", '{"app":"wrong"}'])
def test_malformed_manifest_reports_bootstrap_error(artifact, monkeypatch, contents):
    path, _ = artifact
    (path / bootstrap.MANIFEST_NAME).write_text(contents)
    monkeypatch.setattr(bootstrap, "_locate", lambda: str(path))
    report = Mock()
    monkeypatch.setattr(bootstrap, "_report_error", report)
    bootstrap.main()
    assert report.call_args.args[0].phase == "attach"


@pytest.mark.parametrize("async_generator", [True, False])
def test_registered_generators_aggregate_json(artifact, async_generator):
    path, manifest = artifact
    module = manifest["resources"][0]["module"]
    prefix = "async " if async_generator else ""
    (path / (module + ".py")).write_text(
        'import runpod\napp = runpod.App("example")\n'
        '@app.task(name="work", cpu="cpu3c")\n'
        f'{prefix}def work():\n    print("chunks")\n    yield 1\n    yield 2\n'
    )
    response = deployed.execute(deployed.load_handle(), {})
    assert response == {"success": True, "json_result": [1, 2], "stdout": "chunks\n"}


def test_real_bootstrap_archive_http_and_deadline_cleanup(
    artifact, tmp_path, monkeypatch
):
    import os
    import socket
    import subprocess
    import sys
    import tarfile
    from http.server import BaseHTTPRequestHandler

    path, manifest = artifact
    (path / "env").mkdir()
    (path / "env" / "vendored_probe.py").write_text("value = 42\n")
    module = manifest["resources"][0]["module"]
    source = path / (module + ".py")
    source.write_text("import vendored_probe\n" + source.read_text())
    archive = path / "artifact.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        for name in (source.name, bootstrap.MANIFEST_NAME, "env"):
            tar.add(path / name, arcname=name)

    terminated = threading.Event()
    calls = []

    class GraphQL(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            calls.append(
                (
                    dict(self.headers),
                    json.load(
                        io.BytesIO(self.rfile.read(int(self.headers["Content-Length"])))
                    ),
                )
            )
            body = b'{"data":{"flashTaskTerminate":true}}'
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            terminated.set()

        def log_message(self, *_):
            pass

    graphql = ThreadingHTTPServer(("127.0.0.1", 0), GraphQL)
    graphql_thread = threading.Thread(target=graphql.serve_forever, daemon=True)
    graphql_thread.start()
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    env = dict(os.environ)
    env.update(
        {
            "RUNPOD_TASK_PORT": str(port),
            "RUNPOD_TASK_TOKEN": "execution-token",
            "RUNPOD_POD_ID": "pod-integration",
            "RUNPOD_API_KEY": "internal-integration-key",
            "RUNPOD_API_BASE_URL": f"http://127.0.0.1:{graphql.server_port}",
            "RUNPOD_TASK_EXECUTION_TIMEOUT_SECONDS": "1.5",
            "RUNPOD_APP_DIR": str(tmp_path / "unpacked"),
        }
    )
    with (tmp_path / "worker.log").open("w+") as log:
        process = subprocess.Popen(  # noqa: S603
            [sys.executable, "-m", "runpod_sdk_runtime.bootstrap"],
            env=env,
            stdout=log,
            stderr=log,
        )
        try:
            url = f"http://127.0.0.1:{port}"
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                try:
                    if request(url + "/ping")[0] == 200:
                        break
                except OSError:
                    pass
                assert process.poll() is None
                time.sleep(0.02)
            else:
                pytest.fail("bootstrap did not become ready")
            assert not terminated.is_set()
            status, response = request(url + "/execute", "POST", {"args": [42]})
            assert status == 200
            assert response["json_result"] == {"value": 42}
            assert response["stdout"] == "task output\n"
            assert terminated.wait(5)
            assert process.wait(timeout=5) == 0
            headers, payload = calls[0]
            assert headers["Authorization"] == "Bearer internal-integration-key"
            assert headers["X-Runpod-Pod-Id"] == "pod-integration"
            assert "flashTaskTerminate" in payload["query"]
            assert payload["variables"]["input"] == {"podId": "pod-integration"}
            log.seek(0)
            output = log.read()
            assert "internal-integration-key" not in output
            assert "execution-token" not in output
        finally:
            if process.poll() is None:
                process.terminate()
            process.wait(timeout=5)
            graphql.shutdown()
            graphql_thread.join(5)
            graphql.server_close()
