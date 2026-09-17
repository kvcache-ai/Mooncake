# CI layout

GitHub Actions requires workflow YAML files directly under `.github/workflows/`;
workflow subdirectories are not supported. Use these filename groups:

- `ci.yml`: main pull-request build and test entry point (kept stable for API callers).
- `ci-*`: platform builds, integration tests, nightly tests, and CI triggers.
- `release*`: wheel releases, pre-releases, and container image publishing.
- `automation-*`: repository maintenance, assistants, CI cancellation, and docs deployment.
- `_*`: reusable wheel build and publish workflows.

CI implementation scripts are grouped here by purpose:

- `common/`: shared service startup, readiness, cleanup, and diagnostics.
- `smoke/`: RPC, Store API, SSD offload, and Rust smoke suites.
- `integration/`: Go integration suite.
- `release/`: TestPyPI wheel validation.
- `tests/`: unit tests for CI scripts and helpers.

When moving a script or workflow, update callers, path filters, relative imports,
and documentation together. Preserve workflow display names and job identifiers
so this organization does not change check names.

Run lightweight tests from the repository root:

```sh
python -m unittest discover -s scripts/ci/tests -v
python -m pytest -q mooncake-wheel/tests/test_testpypi_wheel_gate.py
```
