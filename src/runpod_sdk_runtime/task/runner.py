"""single-shot task runner: the process a task pod boots into.

a minimal http server speaking the FunctionRequest/FunctionResponse
protocol, driving the shared execution engine in
runpod_sdk_runtime.executor.

endpoints:
    GET  /ping     readiness probe (unauthenticated)
    POST /execute  run a function, block, return the response
    POST /submit   start a function in the background, return immediately
    GET  /result   status/result of the submitted job

auth: every endpoint except /ping requires
    Authorization: Bearer $RUNPOD_TASK_TOKEN

one pod runs one function; the client terminates the pod after
collecting the result.

the runtime package provides the shared executor alongside this server.
"""

import json
import math
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from runpod_sdk_runtime.executor import execute_request
from runpod_sdk_runtime.mounts import configure_mounts

PORT = int(os.environ.get("RUNPOD_TASK_PORT", "8080"))
TOKEN = os.environ.get("RUNPOD_TASK_TOKEN", "")

# active work is exempt from idle cleanup.
IDLE_TIMEOUT = float(os.environ.get("RUNPOD_TASK_IDLE_TIMEOUT", "600"))
WATCHDOG_INTERVAL = 15.0
TERMINATE_ATTEMPTS = 3
TERMINATE_TIMEOUT = 5.0

# single background job slot for /submit + /result
_job_lock = threading.Lock()
_job_state = {"status": "NONE", "response": None}
_last_contact = {"ts": None}  # set at server start
# inline requests are protected from idle cleanup while running.
_inline_executions = {"count": 0}
_execution_deadline = None
_watchdog_wakeup = threading.Event()


def _touch_contact():
    import time

    _last_contact["ts"] = time.monotonic()


def _set_timeout(timeout):
    global _execution_deadline
    if timeout is not None:
        deadline = time.monotonic() + timeout
        if _execution_deadline is None or deadline < _execution_deadline:
            _execution_deadline = deadline
            _watchdog_wakeup.set()


def _should_self_terminate(status, last_contact, now, idle_timeout, inline=0):
    if status == "RUNNING" or inline > 0:
        return False
    if last_contact is None:
        return False
    return (now - last_contact) > idle_timeout


def _terminate_self():
    """exit only after the control plane acknowledges pod deletion."""
    pod_id = os.environ.get("RUNPOD_POD_ID")
    api_key = os.environ.get("RUNPOD_API_KEY")
    if not pod_id or not api_key:
        sys.stderr.write(
            "[task-runner] cannot delete pod: missing RUNPOD_POD_ID or RUNPOD_API_KEY\n"
        )
        return False
    api_base = os.environ.get("RUNPOD_API_BASE_URL", "https://api.runpod.io")
    payload = json.dumps(
        {
            "query": (
                "mutation podTerminate($input: PodTerminateInput!) "
                "{ podTerminate(input: $input) }"
            ),
            "variables": {"input": {"podId": pod_id}},
        }
    ).encode()
    request = urllib.request.Request(
        f"{api_base.rstrip('/')}/graphql",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
    )
    for attempt in range(TERMINATE_ATTEMPTS):
        retryable = False
        try:
            with urllib.request.urlopen(request, timeout=TERMINATE_TIMEOUT) as response:  # noqa: S310
                body = json.load(response)
            errors = body.get("errors") if isinstance(body, dict) else None
            data = body.get("data") if isinstance(body, dict) else None
            if (
                not errors
                and isinstance(data, dict)
                and "podTerminate" in data
                and data["podTerminate"] is None
            ):
                sys.stderr.write(f"[task-runner] pod {pod_id} deletion acknowledged\n")
                os._exit(0)
                return True
            if isinstance(errors, list) and errors:
                retryable = all(
                    isinstance(error, dict)
                    and isinstance(error.get("extensions"), dict)
                    and error["extensions"].get("code")
                    in {
                        "INTERNAL_SERVER_ERROR",
                        "SERVICE_UNAVAILABLE",
                        "TOO_MANY_REQUESTS",
                        "TIMEOUT",
                    }
                    for error in errors
                )
            failure = "graphql errors" if errors else "missing deletion acknowledgment"
        except urllib.error.HTTPError as exc:
            retryable = exc.code == 429 or 500 <= exc.code < 600
            failure = f"http {exc.code}"
            exc.close()
        except (urllib.error.URLError, OSError) as exc:
            retryable = True
            failure = str(exc)
        except (ValueError, TypeError) as exc:
            failure = f"invalid deletion response: {exc}"
        if not retryable or attempt == TERMINATE_ATTEMPTS - 1:
            sys.stderr.write(
                f"[task-runner] pod {pod_id} deletion failed: {failure}; "
                "keeping runtime alive for watchdog retry\n"
            )
            return False
        time.sleep(0.5 * (2**attempt))
    return False


def _watchdog():
    import time

    while True:
        with _job_lock:
            now = time.monotonic()
            expired = _execution_deadline is not None and now >= _execution_deadline
            if expired or _should_self_terminate(
                _job_state["status"],
                _last_contact["ts"],
                now,
                IDLE_TIMEOUT,
                inline=_inline_executions["count"],
            ):
                _terminate_self()
            delay = WATCHDOG_INTERVAL
            if _execution_deadline is not None and _execution_deadline > now:
                delay = min(delay, _execution_deadline - now)
            _watchdog_wakeup.clear()
        _watchdog_wakeup.wait(delay)


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _authed(self):
        header = self.headers.get("Authorization", "")
        authed = TOKEN and header == f"Bearer {TOKEN}"
        if authed:
            # any authenticated contact proves the client is alive
            _touch_contact()
        return authed

    def _read_request(self):
        length = int(self.headers.get("Content-Length", "0"))
        return json.loads(self.rfile.read(length) or b"{}")

    def do_GET(self):  # noqa: N802 - BaseHTTPRequestHandler api
        if self.path == "/ping":
            self._send(200, {"ready": True})
            return
        if self.path == "/result":
            if not self._authed():
                self._send(401, {"error": "unauthorized"})
                return
            with _job_lock:
                self._send(
                    200,
                    {
                        "status": _job_state["status"],
                        "response": _job_state["response"],
                    },
                )
            return
        self._send(404, {"error": "not found"})

    def do_POST(self):  # noqa: N802 - BaseHTTPRequestHandler api
        if not self._authed():
            self._send(401, {"error": "unauthorized"})
            return
        if self.path not in ("/execute", "/submit", "/timeout"):
            self._send(404, {"error": "not found"})
            return
        try:
            request = self._read_request()
        except (ValueError, TypeError):
            self._send(400, {"error": "invalid request"})
            return
        if not isinstance(request, dict):
            self._send(400, {"error": "invalid request"})
            return
        timeout = request.get("timeout")
        if timeout is not None and (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout)
            or timeout < 0
        ):
            self._send(400, {"error": "timeout must be a finite non-negative number"})
            return
        if self.path == "/timeout":
            with _job_lock:
                _set_timeout(timeout)
            self._send(200, {"timeout": timeout})
            return
        if self.path == "/execute":
            with _job_lock:
                _set_timeout(timeout)
                _inline_executions["count"] += 1
            try:
                self._send(200, execute_request(request))
            finally:
                with _job_lock:
                    _inline_executions["count"] -= 1
            return
        if self.path == "/submit":
            with _job_lock:
                if _job_state["status"] == "RUNNING":
                    self._send(409, {"error": "a job is already running"})
                    return
                _set_timeout(timeout)
                _job_state["status"] = "RUNNING"
                _job_state["response"] = None

            def run():
                response = execute_request(request)
                with _job_lock:
                    _job_state["status"] = "DONE"
                    _job_state["response"] = response

            threading.Thread(target=run, daemon=True).start()
            response = {"status": "RUNNING"}
            if timeout is not None:
                response["timeout"] = timeout
            self._send(200, response)
            return

    def log_message(self, format, *args):  # noqa: A002 - stdlib signature
        # request logging is noise in container logs (dev sessions
        # stream them as the function's output)
        pass


def main() -> None:
    if not TOKEN:
        sys.stderr.write("[task-runner] RUNPOD_TASK_TOKEN not set, exiting\n")
        sys.exit(1)
    configure_mounts()
    _touch_contact()
    threading.Thread(target=_watchdog, daemon=True).start()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    server.serve_forever()


if __name__ == "__main__":
    main()
