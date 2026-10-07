# Changelog

## Unreleased

### Features

* Initialize resolved volume bindings before user code in queue, API, and task workers.

### Bug Fixes

* Enforce the SDK's absolute task deadline during active work; exit only after acknowledged pod deletion and retry failed cleanup.
