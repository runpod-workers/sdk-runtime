"""json value and error contracts shared by task and queue workers."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from runpod_sdk_runtime import bootstrap, executor
from runpod_sdk_runtime.queue import worker


def request_for(code="def echo(value=None):\n    return value", **fields):
    return {"function_name": "echo", "function_code": code, **fields}


@pytest.mark.parametrize("value", [None, True, 42, 1.25, "text", [], {"a": [None]}])
def test_default_format_returns_json(value):
    response = executor.execute_request(request_for(args=[value]))
    assert response["success"] is True
    assert response["json_result"] == value
    assert "result" not in response


@pytest.mark.parametrize("fmt", ["cloudpickle", "pickle", "unknown", "", None, 1])
def test_formats_rejected_before_side_effects(monkeypatch, fmt):
    install = Mock(side_effect=AssertionError("install must not run"))
    source = Mock(side_effect=AssertionError("source must not run"))
    monkeypatch.setattr(executor, "_install", install)
    monkeypatch.setattr(executor, "_install_system", install)
    monkeypatch.setattr(executor, "_materialize_source", source)
    response = executor.execute_request(request_for(serialization_format=fmt))
    assert response["success"] is False
    assert "serialization_format must be 'json'" in response["error"]
    install.assert_not_called()
    source.assert_not_called()
    with pytest.raises(ValueError, match="serialization_format"):
        executor._serialize_result(None, fmt)
    with pytest.raises(ValueError, match="serialization_format"):
        executor.serialize_chunk(None, {"serialization_format": fmt})


@pytest.mark.parametrize(
    "fields",
    [
        {"args": None},
        {"args": ()},
        {"args": {}},
        {"kwargs": None},
        {"kwargs": []},
        {"args": [{"nested": float("nan")}]},
        {"kwargs": {"value": float("inf")}},
        {"kwargs": {"value": {object(): "value"}}},
        {"args": [b"bytes"]},
        {"args": [object()]},
        {"function_code": ["code"]},
        {"function_name": ["echo"]},
        {"dependencies": "package"},
        {"system_dependencies": [1]},
    ],
)
def test_malformed_requests_fail_before_installs_or_source(monkeypatch, fields):
    install = Mock(side_effect=AssertionError("install must not run"))
    source = Mock(side_effect=AssertionError("source must not run"))
    monkeypatch.setattr(executor, "_install", install)
    monkeypatch.setattr(executor, "_install_system", install)
    monkeypatch.setattr(executor, "_materialize_source", source)
    response = executor.execute_request(request_for(**fields))
    assert response["success"] is False
    assert "TypeError" in response["error"]
    install.assert_not_called()
    source.assert_not_called()


@pytest.mark.parametrize("payload", [None, [], "request", 1])
def test_non_object_request_is_structured_failure(payload):
    response = executor.execute_request(payload)
    assert response["success"] is False
    assert "function request must be a dictionary" in response["error"]


@pytest.mark.parametrize(
    "value", [b"bytes", {object(): "value"}, float("nan"), float("inf"), object()]
)
def test_invalid_results_and_chunks_are_rejected(value):
    with pytest.raises(TypeError, match="return value"):
        executor._serialize_result(value, "json")
    with pytest.raises(TypeError, match="return value"):
        executor.serialize_chunk(value, {})


def test_cycles_are_rejected_in_arguments_and_results():
    value = []
    value.append(value)
    response = executor.execute_request(request_for(args=[value]))
    assert response["success"] is False
    assert "circular reference" in response["error"].lower()
    with pytest.raises(TypeError, match="(?i)circular reference"):
        executor._serialize_result(value, "json")


@pytest.mark.parametrize("prefix", ["", "async "])
@pytest.mark.parametrize("statement", ["yield object()", "raise RuntimeError('boom')"])
async def test_live_stream_failures_are_structured(prefix, statement):
    request = request_for(f"{prefix}def echo():\n    yield None\n    {statement}")
    chunks = [chunk async for chunk in worker._live_handler({"input": request})]
    assert chunks[0] == {"success": True, "__stream__": True, "json_result": None}
    assert len(chunks) == 2
    assert chunks[1]["success"] is False
    assert "error" in chunks[1]
    response = executor.execute_request(request)
    assert response["success"] is False
    assert response["error"].splitlines()[-1] == chunks[1]["error"].splitlines()[-1]


@pytest.mark.parametrize("prefix", ["", "async "])
def test_task_stops_at_invalid_generator_chunk(prefix):
    response = executor.execute_request(
        request_for(
            f"{prefix}def echo():\n"
            "    yield object()\n"
            "    raise RuntimeError('must not advance')"
        )
    )
    assert response["success"] is False
    assert "Object of type object" in response["error"]
    assert "must not advance" not in response["error"]


@pytest.mark.parametrize(
    "payload",
    [
        request_for(serialization_format="cloudpickle"),
        request_for(args=None),
        request_for("raise RuntimeError('module failed')"),
        request_for("this is invalid python!"),
        None,
    ],
)
async def test_live_resolution_errors_are_structured(payload):
    chunks = [chunk async for chunk in worker._live_handler({"input": payload})]
    assert len(chunks) == 1
    assert chunks[0]["success"] is False
    assert chunks[0]["error"]


async def test_live_plain_request_resolves_source_once(monkeypatch):
    resolve = Mock(wraps=executor.resolve_request)
    monkeypatch.setattr(executor, "resolve_request", resolve)
    chunks = [
        chunk async for chunk in worker._live_handler({"input": request_for(args=[7])})
    ]
    assert chunks[0]["json_result"] == 7
    resolve.assert_called_once()


@pytest.mark.parametrize(
    "body", [None, [], "input", {object(): "value"}, {"v": b"bytes"}]
)
async def test_deployed_queue_rejects_invalid_input_before_call(body):
    fn = Mock()
    handler = worker._make_deployed_handler(SimpleNamespace(_fn=fn))
    with pytest.raises(TypeError):
        await handler({"input": body})
    fn.assert_not_called()


def test_deployed_kwargs_preserve_json_and_strip_empty_marker():
    body = {"__empty": True, "value": [None, {"a": 1}]}
    assert worker._job_kwargs({"input": body}) == {"value": body["value"]}
    assert "__empty" in body


def test_source_and_deployed_tasks_share_json_conversion():
    from runpod_sdk_runtime.task import deployed

    request = {"args": [{1: (2, 3)}], "kwargs": {}}
    deployed.validate_request(request)
    source = executor.execute_request(
        request_for("def echo(value):\n    return (value,)", **request)
    )
    registered = deployed.execute(SimpleNamespace(_fn=lambda value: (value,)), request)
    assert source["success"] is True
    assert registered["success"] is True
    assert source["json_result"] == registered["json_result"] == [{"1": [2, 3]}]


@pytest.mark.parametrize("kind", ["queue", "api"])
def test_bootstrap_installs_sdk_without_serializers(monkeypatch, kind):
    monkeypatch.setenv("RUNPOD_RUNTIME_KIND", kind)
    monkeypatch.setenv("RUNPOD_PACKAGE_SPEC", "runpod==1.99.0")
    probes = iter([(False, "missing sdk"), (True, "")])
    monkeypatch.setattr(bootstrap, "_sdk_importable", lambda paths: next(probes))
    install = Mock()
    monkeypatch.setattr(bootstrap, "_pip_install", install)
    bootstrap._ensure_sdk_installed()
    expected = ["runpod==1.99.0"]
    if kind == "api":
        expected.extend(["fastapi[standard]", "uvicorn>=0.30"])
    install.assert_called_once_with(expected, "runtime")


def test_bootstrap_probe_requires_json_protocol(monkeypatch, tmp_path):
    monkeypatch.setenv("RUNPOD_RUNTIME_KIND", "queue")
    package = tmp_path / "runpod"
    package.mkdir()
    for filename in ("__init__.py", "apps.py", "serverless.py"):
        (package / filename).write_text("")
    ok, error = bootstrap._sdk_importable([str(tmp_path)])
    assert ok is False
    assert "runpod._serialization" in error
