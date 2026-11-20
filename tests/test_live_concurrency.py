import asyncio
import contextvars
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from types import ModuleType

import pytest
from runpod.serverless.modules.rp_job import run_job_generator
from runpod.serverless.modules.rp_scale import JobScaler

from runpod_sdk_runtime import executor
from runpod_sdk_runtime.queue import worker


@pytest.fixture(autouse=True)
def live_executor(monkeypatch):
    monkeypatch.setattr(worker, "_live_executor", None, raising=False)
    yield
    if worker._live_executor is not None:
        worker._live_executor.shutdown(wait=True, cancel_futures=True)


@pytest.mark.parametrize("async_function", [False, True])
async def test_live_jobs_overlap_with_isolated_stdout(
    monkeypatch, capfd, async_function
):
    state = ModuleType("live_concurrency_state")
    state.started = threading.Barrier(4, timeout=5)
    state.release = [threading.Event() for _ in range(4)]
    monkeypatch.setitem(sys.modules, state.__name__, state)
    monkeypatch.setenv("RUNPOD_MAX_CONCURRENCY", "4")
    config = worker._worker_config(worker._live_handler)
    assert config["concurrency_modifier"](1) == 4
    assert config["return_aggregate_stream"] is True
    source = (
        "import asyncio\n"
        "from live_concurrency_state import started, release\n"
        f"{'async ' if async_function else ''}def work(i):\n"
        "    print(f'start {i}')\n"
        "    started.wait()\n"
        + (
            "    while not release[i].is_set():\n        await asyncio.sleep(0.001)\n"
            if async_function
            else "    assert release[i].wait(5)\n"
        )
        + "    print(f'end {i}')\n"
        "    if i == 1:\n"
        "        raise ValueError('job failed')\n"
        "    return i\n"
    )

    async def run(i):
        return [
            chunk
            async for chunk in run_job_generator(
                config["handler"],
                {
                    "id": str(i),
                    "input": {
                        "function_name": "work",
                        "function_code": source,
                        "args": [i],
                    },
                },
            )
        ]

    executor.execute_request({}, prepared=(lambda: None, [], {}))
    stdout = sys.stdout
    tasks = [asyncio.create_task(run(i)) for i in range(4)]
    try:
        for i in (0, 2, 1, 3):
            state.release[i].set()
            chunks = await asyncio.wait_for(asyncio.shield(tasks[i]), 10)
            response = chunks[0]["output"]
            assert response["stdout"] == f"start {i}\nend {i}\n"
            assert response["success"] is (i != 1)
            if i == 1:
                assert "ValueError: job failed" in response["error"]
            else:
                assert response["json_result"] == i
    finally:
        for release in state.release:
            release.set()
        await asyncio.gather(*tasks, return_exceptions=True)

    assert sys.stdout is stdout
    logs = capfd.readouterr().out
    for i in range(4):
        assert f"start {i}" in logs
        assert f"end {i}" in logs


@pytest.mark.parametrize("limit", [1, 2, 4])
async def test_serverless_scheduler_honors_live_concurrency(monkeypatch, limit):
    monkeypatch.delenv("RUNPOD_MAX_CONCURRENCY", raising=False)
    if limit > 1:
        monkeypatch.setenv("RUNPOD_MAX_CONCURRENCY", str(limit))
    scaler = JobScaler(worker._worker_config(worker._live_handler))
    await scaler.set_scale()
    assert scaler.current_concurrency == limit
    started = threading.Barrier(limit, timeout=5)
    lock = threading.Lock()
    active = peak = 0

    async def fn():
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        try:
            started.wait()
            await asyncio.sleep(0.01)
            return "ok"
        finally:
            with lock:
                active -= 1

    monkeypatch.setattr(
        executor, "resolve_request", lambda request: ((fn, [], {}), None)
    )
    results = []

    async def handle_job(session, job):
        async for chunk in run_job_generator(scaler.config["handler"], job):
            results.append(chunk["output"])

    monkeypatch.setattr(scaler, "handle_job", handle_job)
    monkeypatch.setattr(scaler, "is_alive", lambda: False)
    scaler.jobs_queue = asyncio.Queue()
    for i in range(8):
        scaler.jobs_queue.put_nowait({"id": str(i), "input": {}})
    await asyncio.wait_for(scaler.run_jobs(None), 10)
    assert len(results) == 8
    assert all(result["success"] for result in results)
    assert all(result["json_result"] == "ok" for result in results)
    assert peak == limit
    assert active == 0


async def test_sync_executor_captures_async_output_with_running_loop():
    executor.execute_request({}, prepared=(lambda: None, [], {}))
    stdout = sys.stdout
    response = executor.execute_request(
        {
            "function_name": "speak",
            "function_code": (
                "import asyncio\n"
                "async def speak():\n"
                "    await asyncio.sleep(0)\n"
                "    print('async output')\n"
                "    return 42\n"
            ),
        }
    )
    assert response == {
        "success": True,
        "stdout": "async output\n",
        "json_result": 42,
    }
    assert sys.stdout is stdout


@pytest.mark.parametrize("limit", [1, 4])
async def test_cancelled_jobs_hold_capacity_until_execution_finishes(
    monkeypatch, limit
):
    monkeypatch.setenv("RUNPOD_MAX_CONCURRENCY", str(limit))
    loop = asyncio.get_running_loop()
    started = [asyncio.Event() for _ in range(limit * 2)]
    replacements_submitted = asyncio.Event()
    release = [threading.Event(), threading.Event()]
    lock = threading.Lock()
    active = peak = 0

    def fn(i):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        loop.call_soon_threadsafe(started[i].set)
        try:
            assert release[i // limit].wait(10)
            return i
        finally:
            with lock:
                active -= 1

    def resolve(request):
        i = request["i"]
        if i == limit * 2 - 1:
            replacements_submitted.set()
        return (fn, [i], {}), None

    monkeypatch.setattr(executor, "resolve_request", resolve)
    scaler = JobScaler(worker._worker_config(worker._live_handler))
    await scaler.set_scale()
    results = []

    async def handle_job(session, job):
        async for chunk in run_job_generator(scaler.config["handler"], job):
            results.append(chunk["output"])

    monkeypatch.setattr(scaler, "handle_job", handle_job)
    monkeypatch.setattr(scaler, "is_alive", lambda: False)
    scaler.jobs_queue = asyncio.Queue()
    for i in range(limit * 2):
        scaler.jobs_queue.put_nowait({"id": str(i), "input": {"i": i}})
    dispatcher = asyncio.create_task(scaler.run_jobs(None))
    try:
        await asyncio.wait_for(
            asyncio.gather(*(event.wait() for event in started[:limit])), 5
        )
        for i in range(limit):
            assert await scaler.stop_job(str(i))
        await asyncio.wait_for(replacements_submitted.wait(), 5)
        await asyncio.sleep(0.05)
        assert not any(event.is_set() for event in started[limit:])
        with lock:
            assert active == limit
        release[0].set()
        await asyncio.wait_for(
            asyncio.gather(*(event.wait() for event in started[limit:])), 5
        )
        release[1].set()
        await asyncio.wait_for(asyncio.shield(dispatcher), 5)
        assert sorted(result["json_result"] for result in results) == list(
            range(limit, limit * 2)
        )
        assert peak == limit
        assert active == 0
    finally:
        for event in release:
            event.set()
        await asyncio.wait_for(dispatcher, 5)


@pytest.mark.parametrize("limit", [8, 64])
async def test_live_capacity_is_independent_of_default_executor(monkeypatch, limit):
    monkeypatch.setenv("RUNPOD_MAX_CONCURRENCY", str(limit))
    loop = asyncio.get_running_loop()
    loop.set_default_executor(ThreadPoolExecutor(max_workers=1))
    default_started = asyncio.Event()
    release_default = threading.Event()
    started = threading.Barrier(limit, timeout=5)
    request_context = contextvars.ContextVar("live_test_request")

    def block_default():
        loop.call_soon_threadsafe(default_started.set)
        assert release_default.wait(10)

    def fn():
        started.wait()
        return request_context.get()

    monkeypatch.setattr(
        executor, "resolve_request", lambda request: ((fn, [], {}), None)
    )

    async def run(i):
        token = request_context.set(i)
        try:
            return [chunk async for chunk in worker._live_handler({"input": {}})]
        finally:
            request_context.reset(token)

    blocker = asyncio.create_task(asyncio.to_thread(block_default))
    tasks = []
    try:
        await asyncio.wait_for(default_started.wait(), 5)
        tasks = [asyncio.create_task(run(i)) for i in range(limit)]
        results = await asyncio.wait_for(asyncio.gather(*tasks), 7)
        assert [chunks[0]["json_result"] for chunks in results] == list(range(limit))
    finally:
        release_default.set()
        await blocker
        await asyncio.gather(*tasks, return_exceptions=True)
