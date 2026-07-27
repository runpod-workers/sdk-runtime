# Runpod SDK Runtime

Worker runtime package and container images for apps built with the Runpod Python SDK.

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
`py<python>-<runtime-version>`, and `latest` tracks the newest release.

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

## Cold starts

Deployment artifacts contain the application source, the matching Runpod SDK, and the
resolved Python environment. The bootstrap follows five phases:

1. locate the host-provided app tree or extract the artifact
2. attach the artifact environment and source to `PYTHONPATH`
3. verify packages excluded from the artifact are available in the image
4. install resource-level system dependencies
5. start the queue or API worker

Queue bootstrap failures are returned as structured job errors. API bootstrap failures
are served as HTTP 500 responses so failures remain visible without a crash loop.

Live workers receive source with each request. Queue and API workers use the shared
execution engine for dependency installation, argument deserialization, execution,
streaming, and result serialization.

## GPU package contract

GPU images provide `torch`, `torchvision`, `torchaudio`, and `triton` for deployment
artifacts that exclude those packages by size. The GPU base uses CUDA 12.8 wheels to
support the driver versions available across the worker fleet. The SDK exclusion set
and the GPU base package set form one compatibility contract.

## Custom images

The SDK generates a POSIX `dockerArgs` launcher for custom images. The launcher finds a
Python interpreter, installs `runpod-sdk-runtime` when needed, and starts the selected
entrypoint. These environment variables support prerelease and pinned builds:

- `RUNPOD_RUNTIME_PACKAGE_SPEC` selects the runtime package
- `RUNPOD_PACKAGE_SPEC` selects the Runpod SDK package

Any PEP 508 package spec accepted by pip can be used, including a version pin or HTTPS
source archive.

## Compatibility

`runpod.apps.protocol` defines the function request and response contract shared by the
SDK and runtime. Runtime CI exercises the supported Python matrix against the Apps SDK
branch before runtime releases are published.

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
