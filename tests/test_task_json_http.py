"""strict json handling across task http request and response boundaries."""

import http.client
import importlib.util
import json
import sys
import threading
import time
from http.server import ThreadingHTTPServer

import pytest

from runpod_sdk_runtime.task import runner


@pytest.fixture
def connection(monkeypatch):
    monkeypatch.setattr(runner, "TOKEN", "json-token")
    monkeypatch.setattr(runner, "_bootstrap_error", None)
    monkeypatch.setattr(runner, "_ready", True)
    monkeypatch.setattr(runner, "_production", False)
    monkeypatch.setattr(runner, "_inline_executions", {"count": 0})
    monkeypatch.setattr(runner, "_timeout_cap", None)
    monkeypatch.setattr(runner, "_job_state", {"status": "NONE", "response": None})
    monkeypatch.setattr(runner, "_execution_deadline", None)
    server = ThreadingHTTPServer(("127.0.0.1", 0), runner.Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    client = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=5)
    try:
        yield client
    finally:
        client.close()
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def post(connection, body, path="/execute"):
    connection.request("POST", path, body, {"Authorization": "Bearer json-token"})
    response = connection.getresponse()
    return response.status, json.loads(response.read())


@pytest.mark.parametrize("path", ["/execute", "/submit", "/timeout"])
@pytest.mark.parametrize("number", ["NaN", "Infinity", "-Infinity", "1e400"])
def test_nonfinite_request_json_rejected_before_execution(
    connection, monkeypatch, path, number
):
    calls = []
    monkeypatch.setattr(
        runner, "execute_request", lambda request: calls.append(request)
    )
    status, body = post(connection, '{"args": [' + number + "]}", path)
    assert status == 400
    assert body == {"error": "invalid request"}
    assert calls == []
    assert runner._job_state["status"] == "NONE"


@pytest.mark.parametrize("number", ["NaN", "Infinity", "1e400"])
def test_nonfinite_json_cannot_hide_in_duplicate_key(connection, number):
    status, body = post(connection, '{"value": ' + number + ', "value": 1}')
    assert status == 400
    assert body == {"error": "invalid request"}


@pytest.mark.parametrize("timeout", [10**1000, -(10**1000)])
def test_unrepresentable_timeout_is_safe_client_error(connection, timeout):
    status, body = post(connection, json.dumps({"timeout": timeout}), "/timeout")
    assert status == 400
    assert "timeout" in body["error"]


@pytest.mark.parametrize("value", [float("nan"), object(), b"bytes"])
def test_invalid_response_is_safe_json_error(connection, monkeypatch, value):
    monkeypatch.setattr(
        runner,
        "execute_request",
        lambda request: {"success": True, "json_result": value},
    )
    status, body = post(connection, "{}")
    assert status == 500
    assert body["success"] is False
    assert "json-serializable" in body["error"]


def test_default_json_roundtrip_over_http(connection):
    status, body = post(
        connection,
        json.dumps(
            {
                "function_name": "echo",
                "function_code": "def echo(value):\n    return value",
                "args": [{"nested": [None, True, 42]}],
            }
        ),
    )
    assert status == 200
    assert body["success"] is True
    assert body["json_result"] == {"nested": [None, True, 42]}


@pytest.mark.parametrize(
    ("value", "invalid"), [(None, False), ({"nested": [1, True]}, False), (None, True)]
)
def test_sdk_payload_submit_result_roundtrip(
    connection, monkeypatch, tmp_path, value, invalid
):
    from runpod.apps.errors import RemoteExecutionError
    from runpod.apps.spec import ResourceSpec
    from runpod.apps.targets import PodTarget
    from runpod.apps.tasks import unwrap_task_response

    source = tmp_path / "json_task_app.py"
    source.write_text(
        "def echo(value, invalid=False):\n"
        "    if invalid:\n"
        "        return object()\n"
        "    return value\n"
    )
    module_spec = importlib.util.spec_from_file_location("json_task_app", source)
    module = importlib.util.module_from_spec(module_spec)
    monkeypatch.setitem(sys.modules, module.__name__, module)
    module_spec.loader.exec_module(module)
    spec = ResourceSpec(kind="task", name="echo")
    target = PodTarget(spec, module.echo)
    payload = target.build_payload(module.echo, spec, (value,), {"invalid": invalid})
    assert payload["serialization_format"] == "json"
    status, body = post(connection, json.dumps(payload), "/submit")
    assert status == 200
    assert body == {"status": "RUNNING"}

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        connection.request(
            "GET", "/result", headers={"Authorization": "Bearer json-token"}
        )
        response = connection.getresponse()
        assert response.status == 200
        body = json.loads(response.read())
        if body["status"] == "DONE":
            break
        time.sleep(0.01)
    assert body["status"] == "DONE"
    result = body["response"]
    if invalid:
        assert result["success"] is False
        with pytest.raises(RemoteExecutionError, match="return value.*object"):
            unwrap_task_response(result)
    else:
        assert result["success"] is True
        assert "json_result" in result
        assert unwrap_task_response(result) == value
