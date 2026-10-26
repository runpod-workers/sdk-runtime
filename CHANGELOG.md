# Changelog

## 0.1.0 (2026-10-09)


### Features

* add independently released sdk runtimes ([6d37c5f](https://github.com/runpod-workers/sdk-runtime/commit/6d37c5f3b9ed5200406dc1a54036b6aea018c056))
* initialize worker volume bindings before user code ([0f2d7f6](https://github.com/runpod-workers/sdk-runtime/commit/0f2d7f646d0f7bff0326070ecf249f5b5d477b6b))


### Bug Fixes

* acknowledge CON-1751 task pod termination before exit ([9bed9ef](https://github.com/runpod-workers/sdk-runtime/commit/9bed9ef03a4077a9b1682fd83ff3aaf9a6e48fbe))
* bind worker volumes and acknowledge task pod termination ([0cbb5bf](https://github.com/runpod-workers/sdk-runtime/commit/0cbb5bf3a5ca029969ec00c186e6108c1e643c0e))
* enforce absolute CON-1751 task deadlines ([dcc5bee](https://github.com/runpod-workers/sdk-runtime/commit/dcc5bee9c91c8c69a5ebe605a026080592beb9e5))
* enforce task deadlines in the runtime watchdog ([ccfe0b3](https://github.com/runpod-workers/sdk-runtime/commit/ccfe0b39af32483cbf0f3c9bda13077af6c0c221))
* preserve unlimited active task execution ([6c7abdf](https://github.com/runpod-workers/sdk-runtime/commit/6c7abdf4ec8aab642755c63a95d64c2bad0b89c3))

## Changelog

## Unreleased

### Features

* Initialize resolved volume bindings before user code in queue, API, and task workers.

### Bug Fixes

* Keep active tasks running indefinitely; retry failed idle cleanup and exit only after acknowledged pod deletion.
