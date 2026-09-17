# CI layout

Keep workflow entry points at their established paths directly under
`.github/workflows/`: GitHub Actions does not discover workflow subdirectories.

Workflows declare triggers, permissions, runners, job dependencies, build options,
and the sequence of named steps. Shared implementation belongs in composite
actions under `.github/actions/` or scripts here, not copied inline shell blocks.

## Shared steps

The PR and nightly source-build jobs use these composite actions:

- `setup-etcd`: install etcd 3.6.1 and start/check the local service.
- `setup-sccache`: install the compiler cache and export Actions cache credentials.
- `setup-cuda-runtime`: expose the driver library selected in `build/CMakeCache.txt`.
- `setup-metadata-server`: start the Python HTTP metadata service and export its
  PID as `NIGHTLY_METADATA_SERVER_PID` for the later nightly API suite to stop.
- `run-ctest`: run the `build/` suite with shared runtime environment settings;
  optionally write JUnit results and reserve RPC port 50052.
- `ctest-diagnostics`: preserve failure logs and JUnit reports.

These actions require checkout first and run from the repository root on Linux.
Service setup actions start job-scoped background processes; they are not intended
for repeated invocation within a job or unmanaged persistent hosts. CUDA runtime
setup requires CMake configuration first. CTest requires a completed build and
metadata service. Keep failure-only diagnostics at the workflow level so its
condition and artifact identity remain explicit.

For example, after building and starting services:

```yaml
- name: Run unit tests
  id: ctest
  uses: ./.github/actions/run-ctest
  with:
    junit-report: build/test-results/ctest.xml
```

Platform-specific build flags, permissions, secrets, matrices, and job IDs remain
in the workflows. Do not hide these behind a universal shell dispatcher.

## Script directories

- `common/`: service lifecycle helpers for smoke and integration suites.
- `smoke/`: RPC, Store API, SSD offload, and Rust smoke suites.
- `integration/`: Go integration suite.
- `release/`: TestPyPI wheel validation.
- `tests/`: unit tests for CI scripts and shared actions.

Run lightweight tests from the repository root:

```sh
python -m unittest discover -s scripts/ci/tests -v
python -m pytest -q mooncake-wheel/tests/test_testpypi_wheel_gate.py
```
