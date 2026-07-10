---
name: mooncake-ci-local
description: Run Mooncake pre-PR local validation through scripts/run_ci_test.sh. Use this skill whenever the user wants to validate a branch before opening or submitting a PR, run local CI, run ci test, check changes before PR, reproduce GitHub Actions locally, or force a full pre-submit verification. Trigger on phrases like "提交 PR 前验证", "run ci test", "run local CI", "check my branch", "test before PR", "pre-submit validation", and "reproduce CI locally".
---

# Mooncake Pre-PR Local Validation

Use `bash scripts/run_ci_test.sh` as the default entry point. This is the single local lane for PR-before-submit validation, and it already coordinates the reproducible parts of GitHub Actions.

## Default Entry Point

When the user asks for any of the following, run the repo script first instead of reconstructing the workflow by hand:

- 提交 PR 前本地验证
- run ci test
- run local CI
- check my branch before PR
- reproduce CI locally

Default command:

```bash
bash scripts/run_ci_test.sh
```

What this script already covers:

- GitHub-like `paths-filter` against `origin/main`
- `typos`
- `scripts/code_format.sh --check`
- default CMake configure/build/install in `build-ci-local`
- `ctest`
- wheel build in `build-wheel-local`
- wheel installation validation
- `scripts/run_tests.sh`
- selected Python API and integration tests
- per-stage summary and logs under `local_test/run-ci-logs/<timestamp>/`

## Standard Agent Workflow

1. Run `bash scripts/run_ci_test.sh` from the repo root unless the user explicitly asks for a narrower subset.
2. Read the stage summary instead of dumping raw terminal output.
3. Report these items back to the user:
   - passed stages
   - failed stages
   - blocked stages
   - unsupported stages
   - whether `paths-filter` skipped downstream stages
   - the log directory under `local_test/run-ci-logs/...`
4. If there is a failure, inspect the corresponding stage log and summarize the root cause.

## Common Options

Force a full lane even if `paths-filter` would skip downstream stages:

```bash
bash scripts/run_ci_test.sh --skip-path-filter
```

Use another base ref:

```bash
bash scripts/run_ci_test.sh --base origin/main
```

Auto-install missing dependencies:

```bash
bash scripts/run_ci_test.sh --install-deps
```

Keep services running for follow-up debugging:

```bash
bash scripts/run_ci_test.sh --keep-services
```

## Minimal Example

User prompt:

- 提交 PR 前，帮我跑一遍本地 CI 验证当前分支。

Expected action:

```bash
bash scripts/run_ci_test.sh
```

If the user wants to ignore changed-path optimization and force the full lane:

```bash
bash scripts/run_ci_test.sh --skip-path-filter
```

See also `.claude/skills/mooncake-ci-local/examples/minimal.md`.

## How To Interpret Results

- `passed`: the stage succeeded locally.
- `failed`: the stage reproduced a real local failure and needs investigation.
- `blocked`: local environment or dependency issue prevented execution.
- `unsupported`: intentionally not run in the local lane because it needs external platforms, special hardware, or a non-default build.

If `paths-filter` skips downstream stages, explain that the current branch changed only non-source paths relative to the selected base.

## Current Local Coverage

Included by default:

- spell check
- code format check
- default ASan CMake lane in `build-ci-local`
- `ctest`
- wheel build and installation test
- `scripts/run_tests.sh`
- selected Python API tests

Unsupported by design in the default local lane:

- Ascend jobs
- T-one integration jobs
- MUSA jobs
- Docker image build jobs
- CUDA 13 wheel jobs
- PG-backend tests absent from the default wheel build
- Python drain-http API stage in the local ASan lane

## Targeted Reruns For Debugging

Use targeted reruns only after the full script identifies a failing area, or when the user explicitly asks for a smaller scope.

Rerun a specific C++ test pattern:

```bash
cd build-ci-local
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata DEFAULT_KV_LEASE_TTL=500 ctest -R <pattern> --output-on-failure
```

Rerun the Python wheel integration lane:

```bash
source test_env/bin/activate
MC_STORE_MEMCPY=false TEST_SSD_OFFLOAD_IN_EVICT=true ./scripts/run_tests.sh
```

Rerun the safetensor unittest:

```bash
source test_env/bin/activate
python -m unittest mooncake-wheel.tests.test_safetensor_functions
```

## Notes For The Agent

- Prefer the repo script over rebuilding the CI workflow step by step.
- Preserve the separation between `build-ci-local` and `build-wheel-local`.
- Summarize failing stages from their logs instead of pasting raw output.
- If the user only asks whether the branch is safe before opening a PR, the default answer path is `bash scripts/run_ci_test.sh`.
