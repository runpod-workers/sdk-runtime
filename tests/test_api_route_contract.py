import json
import sys
import textwrap

import pytest
from fastapi.testclient import TestClient
from runpod.apps.app import _clear_registry

from runpod_sdk_runtime.api import server


@pytest.fixture(params=["live", "deployed"])
def api_client_factory(request, tmp_path, monkeypatch):
    _clear_registry()
    monkeypatch.delenv("RUNPOD_MOUNTS", raising=False)
    module = "_api_route_contract"
    monkeypatch.syspath_prepend(str(tmp_path))

    def build(source):
        if request.param == "live":
            client = TestClient(server._build_live_app())
            response = client.post(
                "/_runpod/sync", json={"source": source, "resource": "contract"}
            )
            assert response.status_code == 200, response.text
            return client

        (tmp_path / f"{module}.py").write_text(source)
        (tmp_path / "runpod_manifest.json").write_text(
            json.dumps(
                {
                    "version": 1,
                    "app": "contract-test",
                    "resources": [
                        {"kind": "api", "name": "contract", "module": module}
                    ],
                }
            )
        )
        monkeypatch.setattr(server, "APP_DIR", str(tmp_path))
        monkeypatch.setenv("FLASH_RESOURCE_NAME", "contract")
        return TestClient(server.build_app())

    yield build
    sys.modules.pop(module, None)
    _clear_registry()


@pytest.fixture(params=["", "async "], ids=["sync", "async"])
def handler_prefix(request):
    return request.param


@pytest.fixture
def native_client(api_client_factory, handler_prefix):
    source = textwrap.dedent(
        """
        from typing import Any, Dict
        from runpod import App, get, post, put, patch, delete

        app = App("contract-test")

        @app.api(name="contract", cpu="cpu3c-1-2")
        class Native:
            calls = 0

            @get("/calls")
            def calls_count(self):
                return {"calls": self.calls}
        """
    )
    for method, annotation in (
        ("post", ""),
        ("put", ": Any"),
        ("patch", ": dict"),
        ("delete", ": Dict"),
    ):
        source += (
            f'\n    @{method}("/{method}/body")\n'
            f"    {handler_prefix}def {method}_body(self, body{annotation}):\n"
            "        self.calls += 1\n"
            f'        return {{"method": "{method}", "body": body}}\n'
        )
    for method in ("get", "post", "put", "patch", "delete"):
        source += (
            f'\n    @{method}("/{method}/noarg")\n'
            f"    {handler_prefix}def {method}_noarg(self):\n"
            "        self.calls += 1\n"
            f'        return {{"method": "{method}"}}\n'
        )
    with api_client_factory(source) as client:
        yield client


@pytest.mark.parametrize("method", ["post", "put", "patch", "delete"])
@pytest.mark.parametrize(
    "body",
    [{}, {"count": "3", "extra": [None, True, {"nested": [1, "2", 3.5]}]}],
    ids=["empty", "nested"],
)
def test_native_body_receives_complete_object(native_client, method, body):
    response = native_client.request(method, f"/{method}/body?ignored=value", json=body)
    assert response.status_code == 200
    assert response.json() == {"method": method, "body": body}
    assert native_client.get("/calls").json() == {"calls": 1}


@pytest.fixture(
    params=[
        None,
        "application/json",
        "application/json; charset=utf-8",
        "application/vnd.api+json",
        "application/x-www-form-urlencoded",
        "text/plain",
        "application/octet-stream",
    ]
)
def body_headers(request):
    return {"Content-Type": request.param} if request.param else {}


@pytest.mark.parametrize("method", ["post", "put", "patch", "delete"])
def test_native_json_object_ignores_content_type(native_client, body_headers, method):
    response = native_client.request(
        method,
        f"/{method}/body",
        content='{"prompt":"hi","nested":[null,true,1]}',
        headers=body_headers,
    )
    assert response.status_code == 200
    assert response.json() == {
        "method": method,
        "body": {"prompt": "hi", "nested": [None, True, 1]},
    }
    assert native_client.get("/calls").json() == {"calls": 1}


@pytest.mark.parametrize(
    "content",
    [b"", b"null", b"[]", b'"text"', b"42", b"true", b'{"x":', b"prompt=hi", b"\xff"],
)
def test_native_invalid_body_rejected_for_every_content_type(
    native_client, body_headers, content
):
    response = native_client.post("/post/body", content=content, headers=body_headers)
    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["body"]
    assert native_client.get("/calls").json() == {"calls": 0}


def test_native_openapi_describes_required_object_body(native_client):
    paths = native_client.get("/openapi.json").json()["paths"]
    for method in ("post", "put", "patch", "delete"):
        operation = paths[f"/{method}/body"][method]
        assert operation["requestBody"]["required"] is True
        schema = operation["requestBody"]["content"]["application/json"]["schema"]
        assert schema["type"] == "object"
        assert "422" in operation["responses"]
    for method in ("get", "post", "put", "patch", "delete"):
        assert "requestBody" not in paths[f"/{method}/noarg"][method]


@pytest.mark.parametrize("method", ["post", "put", "patch", "delete"])
@pytest.mark.parametrize(
    "content",
    [None, "null", "[]", '[["key", "value"]]', '"text"', "42", "true", '{"x":'],
    ids=["missing", "null", "array", "pairs", "string", "number", "bool", "invalid"],
)
def test_native_body_rejects_invalid_input(native_client, method, content):
    response = native_client.request(
        method,
        f"/{method}/body?body=query",
        content=content,
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 422
    assert response.json()["detail"]
    assert native_client.get("/calls").json() == {"calls": 0}


@pytest.mark.parametrize("method", ["get", "post", "put", "patch", "delete"])
@pytest.mark.parametrize("content", [None, '{"extra": 1}', "null", "[]", "invalid"])
def test_native_noarg_ignores_body(native_client, method, content):
    response = native_client.request(
        method,
        f"/{method}/noarg?body=query",
        content=content,
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 200
    assert response.json() == {"method": method}
    assert native_client.get("/calls").json() == {"calls": 1}


@pytest.fixture
def factory_client(api_client_factory, handler_prefix):
    source = textwrap.dedent(
        f"""
        from fastapi import Body, FastAPI
        from runpod import App

        app = App("contract-test")

        @app.api(name="contract", cpu="cpu3c-1-2")
        def factory():
            api = FastAPI()

            @api.post("/items/{{item_id}}")
            {handler_prefix}def item(
                item_id: int, limit: int, count: int = Body(...), label: str = Body(...)
            ):
                return {{
                    "item_id": item_id,
                    "limit": limit,
                    "count": count,
                    "label": label,
                }}

            return api
        """
    )
    with api_client_factory(source) as client:
        yield client


@pytest.mark.parametrize("postponed", [False, True], ids=["lazy", "postponed"])
def test_native_return_annotation_is_not_evaluated(
    api_client_factory, handler_prefix, postponed
):
    if not postponed and sys.version_info < (3, 14):
        pytest.skip("lazy annotations require Python 3.14")
    source = textwrap.dedent(
        f"""
        from runpod import App, get, post

        app = App("contract-test")

        @app.api(name="contract", cpu="cpu3c-1-2")
        class Native:
            @get("/value")
            {handler_prefix}def value(self) -> UnknownResponse:
                return {{"value": 1}}

            @post("/echo")
            {handler_prefix}def echo(self, body: dict) -> UnknownResponse:
                return body
        """
    )
    if postponed:
        source = "from __future__ import annotations\n" + source
    with api_client_factory(source) as client:
        assert client.get("/value").json() == {"value": 1}
        response = client.post("/echo", json={"value": "raw"})
        assert response.status_code == 200
        assert response.json() == {"value": "raw"}


def test_factory_typed_path_query_and_body(factory_client):
    response = factory_client.post(
        "/items/7?limit=3", json={"count": "2", "label": "hello"}
    )
    assert response.status_code == 200
    assert response.json() == {
        "item_id": 7,
        "limit": 3,
        "count": 2,
        "label": "hello",
    }


def test_factory_keeps_content_type_validation(factory_client):
    response = factory_client.post(
        "/items/7?limit=3",
        content='{"count":2,"label":"hello"}',
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    assert response.status_code == 422


@pytest.mark.parametrize(
    "path, body, location",
    [
        ("/items/nope?limit=3", {"count": 2, "label": "ok"}, ["path", "item_id"]),
        ("/items/7?limit=nope", {"count": 2, "label": "ok"}, ["query", "limit"]),
        ("/items/7", {"count": 2, "label": "ok"}, ["query", "limit"]),
        ("/items/7?limit=3", {"label": "ok"}, ["body", "count"]),
        ("/items/7?limit=3", {"count": "nope", "label": "ok"}, ["body", "count"]),
        ("/items/7?limit=3", {"count": 2, "label": []}, ["body", "label"]),
    ],
    ids=["path", "query", "missing-query", "missing-field", "body-int", "body-str"],
)
def test_factory_validation_returns_422(factory_client, path, body, location):
    response = factory_client.post(path, json=body)
    assert response.status_code == 422
    assert location in [error["loc"] for error in response.json()["detail"]]
