---
name: mooncake-ci-local
description: Run Mooncake pre-PR validation with existing repository commands. Use this skill whenever the user wants to validate a branch before opening or submitting a PR, run local CI, run CI tests, check changes before a PR, or reproduce a GitHub Actions failure. Trigger on phrases like "提交 PR 前验证", "run ci test", "run local CI", "check my branch", "test before PR", "pre-submit validation", and "reproduce CI locally".
---

# Mooncake Pre-PR Local Validation

GitHub Actions is the authoritative full validation environment. There is no
single local command that reproduces every job, so select checks based on the
changed files and the available platform. Always report checks that could not
run locally.

## Default Workflow

1. Inspect the branch diff against `origin/main`:

```console
git diff --stat origin/main...HEAD
git diff --name-only origin/main...HEAD
```

2. Run pre-commit on the committed branch diff:

```console
pre-commit run --from-ref origin/main --to-ref HEAD
```

For uncommitted changes, pass the changed paths explicitly instead:

```console
pre-commit run --files <changed-file> [<changed-file> ...]
```

3. If C or C++ files changed, check only the lines changed from the base branch:

```console
./scripts/code_format.sh --changed-lines --check --base origin/main
```

4. Run the relevant build or test lane below. Do not claim full local coverage
when hardware, services, or a Linux environment are unavailable.

## C++ Tests

If the appropriate build directory is already configured, build and run its
tests directly:

```console
cmake --build build
ctest --test-dir build -j --output-on-failure
```

Use `ctest -R <pattern>` for a targeted rerun. Match the CMake flags in
`.github/workflows/ci.yml` when reproducing a specific CI job.

## TENT Tests

The CPU-only TENT lane used by CI can be reproduced on Linux:

```console
cmake -S . -B build-tent -G Ninja \
  -DUSE_TENT=ON -DUSE_HTTP=ON \
  -DBUILD_UNIT_TESTS=ON -DBUILD_EXAMPLES=ON
cmake --build build-tent
ctest --test-dir build-tent/mooncake-transfer-engine/tent/tests \
  -j --output-on-failure
```

## Python Wheel Tests

After building and installing the wheel in a Linux test environment and
starting the required metadata service, run:

```console
source test_env/bin/activate
MC_STORE_MEMCPY=false TEST_SSD_OFFLOAD_IN_EVICT=true ./scripts/run_tests.sh
```

The wheel and integration jobs install additional dependencies and services.
Use the corresponding steps in `.github/workflows/ci.yml` when reproducing a
failure instead of assuming they are already present.

## Platform Limits

- Native macOS is suitable for lightweight pre-commit and documentation checks.
- Run C++ builds, wheel tests, and service-based integration tests in a Linux
  container or VM when working from macOS.
- Ascend, MUSA, EFA, CUDA, and other hardware-specific jobs require their
  matching CI runner or hardware.

## Report Format

Report:

- changed areas detected from the diff;
- commands that passed;
- commands that failed and the first actionable error;
- checks blocked by missing dependencies;
- checks not run because the local platform does not support them.

See `.claude/skills/mooncake-ci-local/examples/minimal.md` for a minimal
pre-PR example.
