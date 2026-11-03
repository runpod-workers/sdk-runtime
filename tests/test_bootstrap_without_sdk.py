"""task bootstrap failures remain observable without an importable sdk."""

import os
import subprocess
import sys
from pathlib import Path


def test_task_bootstrap_error_http_without_sdk():
    script = """
import importlib.abc
import json
import sys
import threading
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

class NoSdk(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "runpod" or fullname.startswith("runpod."):
            raise ModuleNotFoundError("sdk unavailable")

sys.meta_path.insert(0, NoSdk())
from runpod_sdk_runtime.task import runner

runner.TOKEN = "test-token"
runner._bootstrap_error = {
    "error_type": "BootstrapError", "error_message": "sdk unavailable"
}
server = ThreadingHTTPServer(("127.0.0.1", 0), runner.Handler)
thread = threading.Thread(target=server.serve_forever, daemon=True)
thread.start()
try:
    for path in ("/ping", "/execute"):
        request = urllib.request.Request(
            f"http://127.0.0.1:{server.server_port}{path}",
            data=b"{}" if path == "/execute" else None,
            headers={"Authorization": "Bearer test-token"},
        )
        try:
            urllib.request.urlopen(request, timeout=5)
        except urllib.error.HTTPError as response:
            assert response.code == 500
            body = json.load(response)
            response.close()
            assert body["error_type"] == "BootstrapError"
            assert body["ready"] is False
            if path == "/execute":
                assert body["error_message"] == "sdk unavailable"
            else:
                assert "error_message" not in body
        else:
            raise AssertionError("bootstrap error was not reported")
finally:
    server.shutdown()
    thread.join(5)
    server.server_close()
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        env={**os.environ, "PYTHONPATH": str(Path(__file__).parents[1] / "src")},
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == 0, result.stderr
