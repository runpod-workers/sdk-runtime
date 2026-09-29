"""unit tests for the generic queue worker runtime."""

import inspect
import json
from unittest.mock import MagicMock, patch

import pytest

from runpod_sdk_runtime.queue import worker


@pytest.fixture(autouse=True)
def clean_mounts(monkeypatch):
    from runpod.apps.volume import _configure_mounts

    monkeypatch.delenv("RUNPOD_MOUNTS", raising=False)
    _configure_mounts([])
    yield
    _configure_mounts([])


class TestModeDetection:
    def test_resource_name_flash_env(self, monkeypatch):
        monkeypatch.setenv("FLASH_RESOURCE_NAME", "chat")
        assert worker._resource_name() == "chat"

    def test_resource_name_runpod_env(self, monkeypatch):
        monkeypatch.delenv("FLASH_RESOURCE_NAME", raising=False)
        monkeypatch.setenv("RUNPOD_RESOURCE_NAME", "embed")
        assert worker._resource_name() == "embed"

    def test_resource_name_empty(self, monkeypatch):
        monkeypatch.delenv("FLASH_RESOURCE_NAME", raising=False)
        monkeypatch.delenv("RUNPOD_RESOURCE_NAME", raising=False)
        assert worker._resource_name() == ""

    def test_is_deployed_requires_name_and_manifest(self, monkeypatch, tmp_path):
        monkeypatch.delenv("FLASH_RESOURCE_NAME", raising=False)
        monkeypatch.delenv("RUNPOD_RESOURCE_NAME", raising=False)
        monkeypatch.setattr(worker, "APP_DIR", str(tmp_path))
        assert not worker._is_deployed()

        monkeypatch.setenv("FLASH_RESOURCE_NAME", "chat")
        assert not worker._is_deployed()

        (tmp_path / worker.MANIFEST_NAME).write_text("{}")
        assert worker._is_deployed()


class TestDeployedHandle:
    def _write_manifest(self, tmp_path, resources):
        (tmp_path / worker.MANIFEST_NAME).write_text(
            json.dumps({"resources": resources})
        )

    def test_load_deployed_handle(self, monkeypatch, tmp_path):
        module_src = (
            "import runpod\n"
            "app = runpod.App('t')\n"
            "@app.queue(name='chat', gpu='4090')\n"
            "def chat(prompt: str):\n"
            "    return {'echo': prompt}\n"
        )
        (tmp_path / "user_mod_a.py").write_text(module_src)
        self._write_manifest(tmp_path, [{"name": "chat", "module": "user_mod_a"}])
        monkeypatch.setattr(worker, "APP_DIR", str(tmp_path))
        monkeypatch.setenv("FLASH_RESOURCE_NAME", "chat")

        handle = worker._load_deployed_handle()
        assert handle.spec.name == "chat"

    def test_load_deployed_handle_missing_resource(self, monkeypatch, tmp_path):
        self._write_manifest(tmp_path, [{"name": "other", "module": "x"}])
        monkeypatch.setattr(worker, "APP_DIR", str(tmp_path))
        monkeypatch.setenv("FLASH_RESOURCE_NAME", "chat")

        with pytest.raises(RuntimeError, match="not in manifest"):
            worker._load_deployed_handle()

    def test_load_deployed_handle_missing_handle(self, monkeypatch, tmp_path):
        (tmp_path / "user_mod_b.py").write_text("x = 1\n")
        self._write_manifest(tmp_path, [{"name": "chat", "module": "user_mod_b"}])
        monkeypatch.setattr(worker, "APP_DIR", str(tmp_path))
        monkeypatch.setenv("FLASH_RESOURCE_NAME", "chat")

        with pytest.raises(RuntimeError, match="no @app.queue"):
            worker._load_deployed_handle()


class TestDeployedHandler:
    def _handle(self, fn):
        handle = MagicMock()
        handle._fn = fn
        return handle

    async def test_sync_function(self):
        handler = worker._make_deployed_handler(
            self._handle(lambda a, b: {"sum": a + b})
        )
        result = await handler({"input": {"a": 1, "b": 2}})
        assert result == {"sum": 3}

    async def test_async_function(self):
        async def fn(x):
            return x * 2

        handler = worker._make_deployed_handler(self._handle(fn))
        assert await handler({"input": {"x": 5}}) == 10

    async def test_empty_marker_stripped(self):
        handler = worker._make_deployed_handler(self._handle(lambda: "ok"))
        assert await handler({"input": {"__empty": True}}) == "ok"

    async def test_no_input(self):
        handler = worker._make_deployed_handler(self._handle(lambda: "ok"))
        assert await handler({}) == "ok"

    async def test_sync_generator(self):
        def fn(n):
            for i in range(n):
                yield i

        handler = worker._make_deployed_handler(self._handle(fn))
        assert inspect.isgeneratorfunction(handler)
        assert list(handler({"input": {"n": 3}})) == [0, 1, 2]

    async def test_async_generator(self):
        async def fn(n):
            for i in range(n):
                yield i

        handler = worker._make_deployed_handler(self._handle(fn))
        assert inspect.isasyncgenfunction(handler)
        chunks = [c async for c in handler({"input": {"n": 2}})]
        assert chunks == [0, 1]


class TestInitHook:
    def test_no_init(self):
        handle = MagicMock(spec=[])
        worker._run_init(handle)  # no _init_fn attr: no-op

    def test_sync_init(self):
        calls = []
        handle = MagicMock()
        handle._init_fn = lambda: calls.append(1)
        worker._run_init(handle)
        assert calls == [1]

    def test_async_init(self):
        calls = []

        async def init():
            calls.append(1)

        handle = MagicMock()
        handle._init_fn = init
        worker._run_init(handle)
        assert calls == [1]


class TestConcurrency:
    def test_default(self, monkeypatch):
        monkeypatch.delenv("RUNPOD_MAX_CONCURRENCY", raising=False)
        assert worker._max_concurrency() == 1

    def test_env_value(self, monkeypatch):
        monkeypatch.setenv("RUNPOD_MAX_CONCURRENCY", "8")
        assert worker._max_concurrency() == 8

    def test_invalid_value(self, monkeypatch):
        monkeypatch.setenv("RUNPOD_MAX_CONCURRENCY", "banana")
        assert worker._max_concurrency() == 1

    def test_floor_of_one(self, monkeypatch):
        monkeypatch.setenv("RUNPOD_MAX_CONCURRENCY", "0")
        assert worker._max_concurrency() == 1

    def test_worker_config_single(self, monkeypatch):
        monkeypatch.delenv("RUNPOD_MAX_CONCURRENCY", raising=False)
        config = worker._worker_config(lambda job: job)
        assert "concurrency_modifier" not in config

    def test_worker_config_concurrent(self, monkeypatch):
        monkeypatch.setenv("RUNPOD_MAX_CONCURRENCY", "4")
        config = worker._worker_config(lambda job: job)
        assert config["concurrency_modifier"](1) == 4

    def test_worker_config_plain_handler(self, monkeypatch):
        monkeypatch.delenv("RUNPOD_MAX_CONCURRENCY", raising=False)
        config = worker._worker_config(lambda job: job)
        assert "return_aggregate_stream" not in config

    def test_worker_config_generator_handler(self, monkeypatch):
        monkeypatch.delenv("RUNPOD_MAX_CONCURRENCY", raising=False)

        def gen_handler(job):
            yield job

        config = worker._worker_config(gen_handler)
        assert config["return_aggregate_stream"] is True


class TestMain:
    async def test_live_mode(self, monkeypatch):
        monkeypatch.delenv("FLASH_RESOURCE_NAME", raising=False)
        monkeypatch.delenv("RUNPOD_RESOURCE_NAME", raising=False)
        monkeypatch.setenv(
            "RUNPOD_MOUNTS",
            json.dumps(
                [
                    {
                        "kind": "network",
                        "reference": "data",
                        "id": "nv-1",
                        "path": "/data",
                    }
                ]
            ),
        )
        with patch("runpod.serverless.start") as start:
            worker.main()
        handler = start.call_args[0][0]["handler"]
        # later environment changes and job metadata cannot replace startup bindings.
        monkeypatch.setenv("RUNPOD_MOUNTS", "[]")
        request = {
            "function_name": "mounted",
            "function_code": (
                "from runpod.apps.volume import NetworkVolume\n"
                "imported = str(NetworkVolume('data').path)\n"
                "async def mounted():\n"
                "    return [imported, str(NetworkVolume('nv-1').path)]\n"
            ),
            "serialization_format": "json",
            "mounts": [
                {"kind": "network", "reference": "data", "id": "nv-1", "path": "/wrong"}
            ],
        }
        chunks = [chunk async for chunk in handler({"input": request})]
        assert chunks[0]["success"] is True
        assert chunks[0]["json_result"] == ["/data", "/data"]

    async def test_deployed_mode(self, monkeypatch, tmp_path):
        monkeypatch.setenv(
            "RUNPOD_MOUNTS",
            json.dumps(
                [
                    {
                        "kind": "global",
                        "reference": "gv-1",
                        "id": "gv-1",
                        "path": "/shared",
                    }
                ]
            ),
        )
        module_src = (
            "import runpod\n"
            "from runpod.apps.volume import GlobalVolume\n"
            "imported = str(GlobalVolume('gv-1').path)\n"
            "app = runpod.App('t')\n"
            "@app.queue(name='greet', gpu='4090')\n"
            "async def greet(name: str):\n"
            "    yield {'name': name, 'imported': imported}\n"
            "    yield {'mounted': str(GlobalVolume('gv-1').path)}\n"
        )
        (tmp_path / "user_mod_c.py").write_text(module_src)
        (tmp_path / worker.MANIFEST_NAME).write_text(
            json.dumps({"resources": [{"name": "greet", "module": "user_mod_c"}]})
        )
        monkeypatch.setattr(worker, "APP_DIR", str(tmp_path))
        monkeypatch.setenv("FLASH_RESOURCE_NAME", "greet")

        with patch("runpod.serverless.start") as start:
            worker.main()
        handler = start.call_args[0][0]["handler"]
        chunks = [chunk async for chunk in handler({"input": {"name": "reader"}})]
        assert chunks == [
            {"name": "reader", "imported": "/shared"},
            {"mounted": "/shared"},
        ]

    @pytest.mark.parametrize("raw", ["", "null"])
    def test_invalid_mounts_stop_startup(self, monkeypatch, raw):
        from runpod.apps.volume import VolumeError

        monkeypatch.setenv("RUNPOD_MOUNTS", raw)
        with patch("runpod.serverless.start") as start:
            with pytest.raises((RuntimeError, VolumeError)):
                worker.main()
        start.assert_not_called()

    async def test_live_handler_plain_function(self):
        def fn(x):
            return {"ok": x}

        with (
            patch(
                "runpod_sdk_runtime.executor.resolve_request",
                return_value=((fn, [1], {}), None),
            ),
            patch(
                "runpod_sdk_runtime.executor.execute_request",
                return_value={"success": True, "json_result": {"ok": 1}},
            ) as execute,
        ):
            chunks = [c async for c in worker._live_handler({"input": {"foo": 1}})]
        assert chunks == [{"success": True, "json_result": {"ok": 1}}]
        execute.assert_called_once_with({"foo": 1})

    async def test_live_handler_resolve_error(self):
        with patch(
            "runpod_sdk_runtime.executor.resolve_request",
            return_value=(None, {"success": False, "error": "boom"}),
        ):
            chunks = [c async for c in worker._live_handler({"input": {}})]
        assert chunks == [{"success": False, "error": "boom"}]

    async def test_live_handler_streams_generator(self):
        def fn(n):
            for i in range(n):
                yield i

        with patch(
            "runpod_sdk_runtime.executor.resolve_request",
            return_value=((fn, [], {"n": 2}), None),
        ):
            chunks = [c async for c in worker._live_handler({"input": {}})]
        assert all(c["__stream__"] for c in chunks)
        assert all(c["success"] for c in chunks)
        assert len(chunks) == 2

    async def test_live_handler_streams_async_generator(self):
        async def fn(n):
            for i in range(n):
                yield i

        with patch(
            "runpod_sdk_runtime.executor.resolve_request",
            return_value=((fn, [], {"n": 3}), None),
        ):
            chunks = [c async for c in worker._live_handler({"input": {}})]
        assert len(chunks) == 3
        assert all(c["__stream__"] for c in chunks)
