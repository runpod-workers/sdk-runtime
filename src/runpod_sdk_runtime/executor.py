"""shared FunctionRequest execution engine.

runs one function from shipped source: installs dependencies, execs the
module, resolves the function, and validates json arguments and results.
every runtime that executes live requests (task pods, queue workers in
dev mode, api servers) drives this same engine.

the runpod sdk supplies the shared protocol and json validation rules.
"""

import inspect
import io
import os
import shutil
import subprocess
import sys
import threading
import traceback
from contextlib import redirect_stdout


def _install(packages, label):
    if not packages:
        return None
    result = subprocess.run(
        [sys.executable, "-m", "pip", "install", "-q", *packages],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return f"failed to install {label}: {result.stderr[-2000:]}"
    return None


_apt_updated = threading.Event()


def _install_system(packages):
    """install apt packages; requires a debian-family image with root."""
    if not packages:
        return None
    if shutil.which("apt-get") is None:
        return (
            f"system dependencies {packages} requested but apt-get is not "
            f"available in this image; use a debian-based image or bake "
            f"them in"
        )
    env = dict(os.environ, DEBIAN_FRONTEND="noninteractive")
    if not _apt_updated.is_set():
        update = subprocess.run(
            ["apt-get", "update", "-qq"], capture_output=True, text=True, env=env
        )
        if update.returncode != 0:
            return f"apt-get update failed: {update.stderr[-2000:]}"
        _apt_updated.set()
    result = subprocess.run(
        ["apt-get", "install", "-y", "-qq", "--no-install-recommends", *packages],
        capture_output=True,
        text=True,
        env=env,
    )
    if result.returncode != 0:
        return (
            f"failed to install system dependencies {packages}: {result.stderr[-2000:]}"
        )
    return None


def _serialize_result(result, fmt):
    from runpod._serialization import normalize_json
    from runpod.apps.protocol import FORMAT_JSON

    if fmt != FORMAT_JSON:
        raise ValueError("serialization_format must be 'json'")
    return {"json_result": normalize_json(result, "return value")}


def resolve_request(request):
    """prepare one FunctionRequest for execution.

    installs dependencies, execs the shipped source, and resolves the
    target function. returns ((fn, args, kwargs), None) on success or
    (None, response_dict) on failure.
    """
    from runpod.apps.protocol import FunctionRequest

    if isinstance(request, dict) and not request.get("function_name"):
        return None, {
            "success": False,
            "error": "function_name and function_code are required",
        }
    parsed = FunctionRequest.from_input(request)
    function_name = parsed.function_name
    function_code = parsed.function_code
    if not function_name or not function_code:
        return None, {
            "success": False,
            "error": "function_name and function_code are required",
        }

    if not isinstance(function_name, str) or not isinstance(function_code, str):
        raise TypeError("function_name and function_code must be strings")
    for label in ("dependencies", "system_dependencies"):
        packages = getattr(parsed, label)
        if packages is not None and (
            not isinstance(packages, list)
            or not all(isinstance(package, str) for package in packages)
        ):
            raise TypeError(f"{label} must be a list of strings")

    error = _install_system(parsed.system_dependencies)
    if error:
        return None, {"success": False, "error": error}
    error = _install(parsed.dependencies, "dependencies")
    if error:
        return None, {"success": False, "error": error}

    # exec mirrors deployed-mode module import: the code is the
    # user's full module, so __name__ is set like an import would
    # (main guards stay inert) and decorated handles resolve to
    # their wrapped functions. the source is written to a real
    # file first so inspect.getsource works inside the function
    # (nested .remote() calls re-extract sibling source)
    source_path = _materialize_source(function_code)
    namespace = {"__name__": "__runpod_live__", "__file__": source_path}
    code_obj = compile(function_code, source_path, "exec")
    exec(code_obj, namespace)  # noqa: S102 - that is the job
    if function_name not in namespace:
        return None, {
            "success": False,
            "error": f"function '{function_name}' not found in provided code",
        }
    fn = namespace[function_name]
    fn = getattr(fn, "_fn", fn)

    return (fn, parsed.args, parsed.kwargs), None


def serialize_chunk(chunk, request):
    """wrap one generator chunk in the response envelope.

    the __stream__ marker lets clients tell a stream of chunks apart
    from a single aggregated function response.
    """
    from runpod.apps.protocol import FORMAT_JSON

    response = {"success": True, "__stream__": True}
    response.update(
        _serialize_result(chunk, request.get("serialization_format", FORMAT_JSON))
    )
    return response


def execute_request(request, *, prepared=None, run_awaitable=None):
    """run one FunctionRequest to completion, returning a response dict."""
    stdout_io = io.StringIO()
    await_result = run_awaitable or _run_awaitable
    try:
        from runpod._serialization import normalize_json, validate_json
        from runpod.apps.protocol import FORMAT_JSON

        if prepared is None:
            prepared, error_response = resolve_request(request)
            if error_response is not None:
                return error_response
        fn, args, kwargs = prepared
        fmt = request.get("serialization_format", FORMAT_JSON)
        if fmt != FORMAT_JSON:
            raise ValueError("serialization_format must be 'json'")
        args, kwargs = normalize_json([args, kwargs], "arguments")

        # stdout is teed: captured for the job response and written
        # through to the real stdout so container logs (and live log
        # streaming in dev sessions) carry the function's prints.
        # sys.stderr carries the runner's own request logs from other
        # threads, which must not leak into job output
        with redirect_stdout(_Tee(stdout_io, sys.__stdout__)):
            result = fn(*args, **kwargs)
            if hasattr(result, "__await__"):
                result = await_result(result)
            # generators aggregate so live .remote() matches the
            # deployed worker's return_aggregate_stream output
            elif inspect.isgenerator(result):
                chunks = []
                for chunk in result:
                    validate_json(chunk, "return value")
                    chunks.append(chunk)
                result = chunks
            elif inspect.isasyncgen(result):
                result = await_result(_drain_async_gen(result))

        response = {"success": True, "stdout": stdout_io.getvalue()}
        response.update(_serialize_result(result, fmt))
        return response
    except Exception:  # noqa: BLE001 - all failures go over the wire
        return {
            "success": False,
            "error": traceback.format_exc(),
            "stdout": stdout_io.getvalue(),
        }


def _materialize_source(function_code):
    """persist request source to a stable path for inspect/linecache."""
    import hashlib
    import tempfile

    digest = hashlib.sha256(function_code.encode()).hexdigest()[:16]
    path = os.path.join(tempfile.gettempdir(), f"runpod_live_{digest}.py")
    if not os.path.exists(path):
        with open(path, "w") as f:
            f.write(function_code)
    return path


class _Tee(io.TextIOBase):
    """write-through to multiple streams, flushing eagerly so log
    followers see lines as they happen."""

    def __init__(self, *streams):
        self._streams = [s for s in streams if s is not None]

    def write(self, s):
        for stream in self._streams:
            try:
                stream.write(s)
                stream.flush()
            except (ValueError, OSError):
                # one sink failing (closed pipe) must not lose the
                # write on the other sinks
                pass
        return len(s)

    def flush(self):
        for stream in self._streams:
            try:
                stream.flush()
            except (ValueError, OSError):
                # closed sinks are skipped, same as write
                pass


def _run_awaitable(awaitable):
    """drive an awaitable to completion from sync code.

    task pods run this on plain threads, but queue workers in live mode
    call execute_request from inside the serverless event loop, where
    asyncio.run would raise; a private loop on a helper thread covers
    both.
    """
    import asyncio

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(_await(awaitable))

    import concurrent.futures

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, _await(awaitable)).result()


async def _drain_async_gen(agen):
    from runpod._serialization import validate_json

    chunks = []
    async for chunk in agen:
        validate_json(chunk, "return value")
        chunks.append(chunk)
    return chunks


async def _await(awaitable):
    return await awaitable
