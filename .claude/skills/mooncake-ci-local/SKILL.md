---
name: mooncake-ci-local
description: Validate a Mooncake branch before a pull request by running the local checks from .github/workflows/ci.yml. Use when the user wants pre-PR validation, local CI, a ci test, a branch check before a PR, or a local reproduction of GitHub Actions. Trigger on "提交 PR 前验证", "run ci test", "run local CI", "check my branch", "test before PR", "pre-submit validation", and "reproduce CI locally".
---

# Mooncake pre-PR local validation

Run from the repository root. That directory contains `scripts/code_format.sh` and `.github/workflows/ci.yml`.

Do not run `scripts/run_ci_test.sh`. That file is not in this repository.

Do not use build directories named `build-ci-local` or `build-wheel-local`. Those names are not in this repository.

Read CMake flags from the job you are reproducing. Do not reconstruct them from memory.

Use base ref `origin/main` unless the user names another ref. In the commands below, replace `origin/main` with that ref.

## Default checks

Run these checks on every pre-PR request. They are the unconditional `ci-gate` jobs that do not need a native build. `engram-cpu-contracts` also needs no native build, and `ci-gate` does not require it.

1. Fetch the base and list changed files.

```bash
git fetch origin main
git diff --name-only --diff-filter=ACMR origin/main...HEAD
```

2. Run pre-commit on the changed files, as `CONTRIBUTING.md` requires. If the diff is empty, skip pre-commit and say the branch matches the base.

```bash
pre-commit run --files $(git diff --name-only --diff-filter=ACMR origin/main...HEAD)
```

If `pre-commit` is missing, mark this check blocked. The install steps are in `CONTRIBUTING.md`. Do not install packages unless the user asked.

Pre-commit may rewrite files. For a validation-only run, report those files and leave them unstaged. Stage them only when the user asked to fix the branch.

The C/C++ pre-commit hook runs `scripts/code_format.sh --staged`. The `clang-format` job does not. Run the job command as well.

```bash
./scripts/code_format.sh --check --changed-lines --base origin/main
```

The job needs clang-format 20. If that binary is missing, mark `clang-format` blocked and give the install command from `CONTRIBUTING.md`.

3. If the diff includes Python files outside `extern/` and `FAST25-release/`, run the `python-lint` commands. The workflow installs `ruff==0.6.9`.

```bash
ruff check <changed-python-files>
python scripts/ci/check_changed_python_format.py origin/main <changed-python-files>
```

If `ruff` is missing, mark `python-lint` blocked.

4. The `spell-check` job uses `crate-ci/typos@v1.30.2`. Codespell from pre-commit does not replace it.

```bash
typos
```

If `typos` is missing, mark `spell-check` blocked.

5. Run `reshard-type-check`. The workflow installs `pyright==1.1.411` before this command.

```bash
bash scripts/check_reshard_types.sh
```

If `pyright` is missing, mark `reshard-type-check` blocked.

6. Run `ci-script-tests`. The workflow installs `packaging`, `pytest==8.3.5`, and `pyyaml` first. If those imports fail, mark the job blocked. If the unittest output says `jq: command not found`, mark the job blocked. Those nightly gate tests call `jq`, and the workflow does not install it.

```bash
python -m pytest -q mooncake-wheel/tests/test_testpypi_wheel_gate.py
python -m unittest discover -s scripts/ci/tests -v
```

7. Run `tone-script-tests`.

```bash
bash scripts/tone_tests/tests/test_common_cleanup.sh
python3 -m unittest discover -s scripts/tone_tests/tests -p 'test_*.py'
```

Do not pip install, apt install, or run `dependencies.sh` unless the user asked to install dependencies. A missing tool is a blocked job. Put the workflow install command in the report.

## Path filters

Read the `filters` block of job `check-paths` in `.github/workflows/ci.yml`. Compare it with the changed-file list. The filter names are `src`, `tent`, `store_rs`, and `docs`.

On a normal pre-PR request, run a filtered job only when its filter matches. When the user asks for a full local run, also run the `src`, `tent`, `store_rs`, and `docs` jobs whose tools are present.

## Docs job

When `docs` matches, run `docs-check` from `docs/`.

```bash
make html SPHINXOPTS=-W
```

If Sphinx is missing, mark `docs-check` blocked. `docs/AGENTS.md` names `docs/requirements-docs.txt`.

## Native jobs

Run native jobs after the default checks. If a job configures with `-DUSE_CUDA=ON` and `nvcc` is missing, mark that job blocked. Do not install CUDA unless the user asked.

For each job, run its `run` steps from `.github/workflows/ci.yml` in order. Keep that job's `env` and CMake arguments unchanged. Skip steps that only check out the repo, set up Python, free disk space, or configure sccache.

Before a script under `.github/actions/`, export `GITHUB_WORKSPACE` to the repository root, `GITHUB_ENV` to a new temp file, and `GITHUB_ACTION_PATH` to that action's directory. After the script, export every `KEY=VALUE` line it appended to `GITHUB_ENV`.

`.github/actions/setup-etcd/run.sh` always downloads etcd. If `etcd` and `etcdctl` are already on `PATH`, skip the download. Start etcd with the listen URLs and health check from that script.

Filter `src` selects these jobs.

- `unit-tests`. The build directory is `build`. Start metadata with `.github/actions/setup-metadata-server/run.sh`. Apply the CUDA driver with `.github/actions/setup-cuda-runtime/run.sh`. Run CTest with `.github/actions/run-ctest/run.sh` and these variables.

```bash
export RESERVE_RPC_PORT=true
export JUNIT_REPORT=build/test-results/ctest.xml
export CTEST_LABEL_EXCLUDE='^nvlink_gpu$'
bash .github/actions/run-ctest/run.sh
```

- `build-wheel`, then `test-wheel-ubuntu`. Reproduce `.github/workflows/_build-wheel.yaml` for variant `cuda`, Python 3.10 and 3.12. After `bash scripts/test_installation.sh`, later steps activate `test_env`. Run `scripts/smoke_conductor_wheel.py`, `scripts/run_standalone_store_e2e.sh`, `scripts/ci/run_store_api_smoke.sh`, `scripts/ci/run_rpc_smoke.sh`, and `scripts/ci/run_ssd_offload_smoke.sh` with the environment from those steps.
- `build-flags`. The job configures `mooncake-transfer-engine/build` and then deletes that directory. Do not reuse it for another job.
- `efa-unit-tests`. Follow that job, including its libfabric header pin. The `ctest -R` step needs no EFA hardware. It still needs CUDA.

Filter `tent` selects `tent-ci`. Its build directory is `build-tent`. The non-CUDA legs run `scripts/ci/run_transfer_engine_rust_smoke.sh`.

Filter `store_rs` selects `store-rs-ci`. Copy that job's `env`, and use the repository root where the workflow uses `github.workspace`. Then run `./scripts/ci/run_store_rs_smoke.sh`. The script exits when those variables are unset. This job sets `-DUSE_CUDA=OFF`.

## Not run on this machine

Report these as not run. GitHub still runs the first four when `src` matches.

- `build-wheel-cu13`
- `build-wheel-cu13-arm64`
- `build-wheel-rocm`
- `build-wheel-xpu`
- Workflows other than `.github/workflows/ci.yml`

`scripts/run_tests.sh` is called from `.github/workflows/nightly.yml`. Do not use it for pull-request checks.

## Report

Use one row per job id from `.github/workflows/ci.yml`.

- `passed`. The command exited 0.
- `failed`. The command exited non-zero. Include the first error and the command that printed it.
- `blocked`. A tool or service was missing, so the command did not run.
- `not run`. The path filter skipped the job, or the job needs another architecture or GPU vendor.

Say which of `src`, `tent`, `store_rs`, and `docs` matched.

Stop a process this run started. Do not stop an etcd or metadata server that was already running.

## Example

See [examples/minimal.md](examples/minimal.md).
