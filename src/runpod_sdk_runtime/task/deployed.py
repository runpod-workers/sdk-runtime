"""registered task loading and json-only execution."""

import importlib
import json
import os
import sys

from runpod_sdk_runtime import bootstrap
from runpod_sdk_runtime.executor import _run_awaitable, execute_request


def load_handle():
    bootstrap._validate_task_environment()
    app_dir = os.environ.get("RUNPOD_APP_DIR", "/app")
    entry = bootstrap._task_resource(bootstrap._manifest(app_dir))
    if app_dir not in sys.path:
        sys.path.insert(0, app_dir)
    module = importlib.import_module(entry["module"])

    from runpod.apps.handles import FunctionHandle
    from runpod.apps.spec import ResourceKind

    target = module
    for name in entry["qualname"].split("."):
        target = getattr(target, name, None)
    handles = {
        value
        for value in [target, *vars(module).values()]
        if isinstance(value, FunctionHandle)
        and value.spec.name == entry["name"]
        and value.spec.kind == ResourceKind.TASK
        and value._app.name == os.environ["FLASH_APP_NAME"]
        and value._fn.__qualname__ == entry["qualname"]
    }
    if len(handles) != 1:
        raise RuntimeError("module must expose exactly one matching @app.task handle")
    handle = handles.pop()
    if handle._init_fn is not None:
        import inspect

        result = handle._init_fn()
        if inspect.isawaitable(result):
            _run_awaitable(result)
    return handle


def validate_request(request, *, timeout_only=False):
    allowed = (
        {"timeout"}
        if timeout_only
        else {"args", "kwargs", "serialization_format", "timeout"}
    )
    if request.keys() - allowed:
        raise ValueError("deployed tasks accept only json args, kwargs and timeout")
    if request.get("serialization_format", "json") != "json":
        raise ValueError("deployed tasks require json serialization")
    if not isinstance(request.get("args", []), list):
        raise ValueError("args must be a json array")
    if not isinstance(request.get("kwargs", {}), dict):
        raise ValueError("kwargs must be a json object")
    json.dumps(request, allow_nan=False)


def execute(handle, request):
    return execute_request(
        {"serialization_format": "json"},
        prepared=(handle._fn, request.get("args", []), request.get("kwargs", {})),
    )
