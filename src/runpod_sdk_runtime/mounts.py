"""install provisioning-owned filesystem bindings at worker startup."""

import json
import os


def configure_mounts() -> None:
    """initialize the sdk after bootstrap, before loading any user code."""
    from runpod.apps.volume import _configure_mounts

    try:
        bindings = json.loads(os.environ.get("RUNPOD_MOUNTS", "[]"))
    except json.JSONDecodeError as exc:
        raise RuntimeError("RUNPOD_MOUNTS must contain a valid JSON array") from exc
    _configure_mounts(bindings)
