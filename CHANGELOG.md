# Changelog

## Unreleased

### Features

* Initialize resolved volume bindings before user code in queue, API, and task workers.

### Bug Fixes

* Keep active tasks running indefinitely; retry failed idle cleanup and exit only after acknowledged pod deletion.
