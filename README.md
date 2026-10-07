# Runpod SDK Runtime

Worker runtime package and container images for apps built with the Runpod Python SDK.
The runtime has its own release cadence so worker fixes and image updates can ship
independently from SDK releases.

## Runtime images

Each runtime kind has CPU and GPU variants for every supported Python version.
GPU variants use the shared `runpod/gpu-base` image, which provides the torch
packages excluded from GPU deployment artifacts.

| Image | Purpose |
| --- | --- |
| `runpod/queue:py3.12-latest` | serverless queue workers |
| `runpod/queue-gpu:py3.12-latest` | GPU serverless queue workers |
| `runpod/api:py3.12-latest` | load-balanced API workers |
| `runpod/api-gpu:py3.12-latest` | GPU load-balanced API workers |
| `runpod/task:py3.12-latest` | ephemeral task pods |
| `runpod/task-gpu:py3.12-latest` | ephemeral GPU task pods |

Python 3.10 through 3.14 are published. Release tags use
`py<python>-<runtime-version>`, and `latest` tracks the newest release. Manual image
publishes support channels such as `dev`.

## Package

`runpod-sdk-runtime` contains the bootstrap, execution engine, and worker entrypoints.
The container images install it during their build. The Runpod SDK also installs it at
startup for custom images that do not have the runtime baked in.

The package uses these entrypoints:

```text
python -m runpod_sdk_runtime.bootstrap
python -m runpod_sdk_runtime.task.runner
```

`RUNPOD_RUNTIME_KIND` selects `queue` or `api` when running the shared bootstrap.

## Storage bindings

Apps provisioning supplies `RUNPOD_MOUNTS`, a JSON array of bindings with `kind`
(`network` or `global`), declared `reference`, resolved volume `id`, and absolute
`path`. Queue, API, and task workers install these before importing user code.
The compatible Apps SDK's `volume.path` uses this worker-level map; request
payloads cannot replace it. Missing configuration means no mounts; malformed
configuration fails startup.

## Task cleanup

Active background jobs and inline executions can run indefinitely, including
after the client disconnects. The watchdog checks for idle cleanup every 15
seconds. `RUNPOD_TASK_IDLE_TIMEOUT` (600 seconds by default) applies only to
pods waiting for submission or holding uncollected results.

Self-deletion requires `RUNPOD_API_KEY` and `RUNPOD_POD_ID`. The runtime exits
only after GraphQL acknowledges `podTerminate` without errors. Transient
failures receive three bounded attempts; any failed deletion leaves the runtime
alive for the next watchdog pass. Container exit alone does not stop pod billing.

## Development

Install the Apps SDK checkout first, then install this project:

```bash
uv sync --all-groups
uv pip install -e ../runpod-python
uv run --no-sync pytest
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
```

Build a CPU queue image against a specific SDK package source:

```bash
docker build \
  -f docker/queue.Dockerfile \
  --build-arg BASE_IMAGE=python:3.12-slim \
  --build-arg RUNPOD_PACKAGE_SPEC=runpod \
  -t runpod/queue:local .
```

## Releases

Conventional commits drive release-please. A release publishes the Python package and
versioned Docker images, then advances the corresponding `latest` tags.
