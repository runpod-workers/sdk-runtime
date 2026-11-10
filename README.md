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

`RUNPOD_RUNTIME_KIND` selects `queue`, `api`, or deployed `task` when running the
shared bootstrap. Source-mode tasks start through `runpod_sdk_runtime.task.runner`.

## Native APIs

Native API routes use literal paths and ordinary instance methods. GET handlers
take only `self`. POST, PUT, PATCH, and DELETE handlers take `self` or
`self, body`, where `body` is one required positional parameter annotated with
`Any`, bare `dict`, bare `typing.Dict`, or no annotation. Defaults, variadic
parameters, keyword-only bodies, schemas, typed generics, and path or query
binding are not supported. The SDK validates declarations at `@app.api`.

Body handlers receive the complete JSON object without field validation or
coercion. Missing bodies, JSON null, non-object values, and invalid JSON return
HTTP 422. Handlers taking only `self` do not consume the request body. Both sync
and async methods work in live and deployed APIs.

ASGI factories support their framework's typed path, query, and body parameters
and validation. See the [SDK native API documentation](https://github.com/runpod/runpod-python/blob/main/docs/apps/native-api.md)
for the native route contract and examples.

## Deployed tasks

The control plane starts production task pods through the shared bootstrap with
`RUNPOD_RUNTIME_KIND=task`.
`RUNPOD_BUILD_ID`, `FLASH_APP_NAME`, `FLASH_ENVIRONMENT`, and `FLASH_RESOURCE_NAME`
identify the selected build and resource. The host delivers the environment's
active artifact at `/root/.runpod/artifact.tar.gz` on its read-only mount. The
bootstrap waits for that archive, extracts it and attaches its vendored
environment. The runner validates the manifest app, resource name, and
task kind, then imports the registered function and runs its init hook.

`GET /ping` reports readiness only after initialization succeeds. Pending
initialization returns HTTP 503 with `{"ready": false}`. Bootstrap and
initialization failures return HTTP 500 with
`{"ready": false, "error_type": "BootstrapError"}`. Error details are available
through authenticated `GET /result`. Clients should treat the bootstrap failure
as terminal and retrieve its details using the task token. Task pods do not
consume serverless queue jobs. `RUNPOD_TASK_TOKEN` authenticates `/submit`, `/result`,
`/execute`, and `/timeout` on `RUNPOD_TASK_PORT` (8080 by default).

Production execution accepts JSON `args` and `kwargs`, with an optional transport
`timeout`. Results use `json_result`, with captured `stdout` and the shared error
envelope. Requests cannot supply source, dependencies, mounts, or pickle
serialization. Source-mode tasks use the shared live executor.

## Remote values

Tasks and queues accept finite JSON-compatible arguments, results, and stream
chunks. JSON conversion turns tuples into lists and supported dictionary keys
into strings. Python objects, bytes, non-finite numbers, and cyclic containers
fail validation. Use JSON representations for custom types and encode binary
data as strings.

Live requests use `serialization_format: "json"`, which is also the default.
Successful responses contain `json_result`, including for a null return value.
Workers validate arguments and the format before installing request dependencies
or executing source. Task and live queue failures use a `success: false` response
with an `error` message; a failing stream emits that response after any valid chunks.
Deployed queue failures use the serverless job error response.

The runtime requires Runpod SDK 2.2.0 or later, with shared JSON protocol validators in
`runpod.apps.protocol` and `runpod._serialization`, including in vendored
deployment environments and custom images. SDK and runtime releases must be
deployed together for this protocol contract.

## Storage bindings

Apps provisioning supplies `RUNPOD_MOUNTS`, a JSON array of bindings with `kind`
(`network` or `global`), declared `reference`, resolved volume `id`, and absolute
`path`. Queue, API, and task workers install these before importing user code.
The compatible Apps SDK's `volume.path` uses this worker-level map; request
payloads cannot replace it. Missing configuration means no mounts; malformed
configuration fails startup.

## Task cleanup

Active background jobs and inline executions can run indefinitely when no
execution timeout is configured, including after the client disconnects. The
watchdog checks for idle cleanup every 15
seconds. `RUNPOD_TASK_IDLE_TIMEOUT` (600 seconds by default) applies only to
pods waiting for submission or holding uncollected results.

`RUNPOD_TASK_INIT_TIMEOUT` bounds deployed task initialization, including storage
setup, module imports, and init hooks. It defaults to 600 seconds and must be
finite and positive. Initialization timeout produces a terminal `BootstrapError`
and triggers pod deletion, with retries until deletion is acknowledged. Client
contact cannot extend this deadline, and completion after timeout cannot make
the pod ready. Idle monitoring begins after initialization succeeds or fails;
the initialization deadline is independent of the execution timeout.

`RUNPOD_TASK_EXECUTION_TIMEOUT_SECONDS` is an optional, finite positive production
policy cap selected by the control plane. Its deadline starts at the first accepted
execution. Request timeouts can shorten that deadline but cannot extend it. The
watchdog enforces execution deadlines even while work is active.

Self-deletion requires `RUNPOD_API_KEY` and `RUNPOD_POD_ID`. Production task pods
call `flashTaskTerminate` with the internal pod key in `Authorization` and the pod ID
in `X-RunPod-Pod-Id`; source-mode pods call `podTerminate`. The runtime exits
only after GraphQL acknowledges deletion without errors. Transient
failures receive three bounded attempts; any failed deletion leaves the runtime
alive for the next watchdog pass. Container exit alone does not stop pod billing.

## Development

Install this project with its published SDK dependency:

```bash
uv sync --locked --all-groups
uv run --no-sync pytest
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
```

Build a CPU queue image against a specific SDK package source:

```bash
docker build \
  -f docker/queue.Dockerfile \
  --build-arg BASE_IMAGE=python:3.12-slim \
  --build-arg 'RUNPOD_PACKAGE_SPEC=runpod>=2.2.0' \
  -t runpod/queue:local .
```

## Releases

Conventional commits drive release-please. A release publishes the Python package and
versioned Docker images, then advances the corresponding `latest` tags.
