# Changelog

## Unreleased

### Features

* Initialize resolved volume bindings before user code in queue, API, and task workers.

### Bug Fixes

* Require acknowledged task pod deletion before exiting; retry transient termination failures and keep idle runtimes available for watchdog recovery without interrupting active detached work.
