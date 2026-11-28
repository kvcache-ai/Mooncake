# Minimal example

Validate the current branch before opening a pull request.

The user asks:

- 提交 PR 前，帮我跑一遍本地 CI 验证当前分支。

Run from the repository root. Replace `origin/main` when the user names another base.

```bash
git fetch origin main
git diff --name-only --diff-filter=ACMR origin/main...HEAD
pre-commit run --files $(git diff --name-only --diff-filter=ACMR origin/main...HEAD)
./scripts/code_format.sh --check --changed-lines --base origin/main
```

Then run the remaining default checks in `SKILL.md`. Read `.github/workflows/ci.yml` job `check-paths` before any native build.

Report passed, failed, blocked, and not run rows. Name the job id from `.github/workflows/ci.yml`. There is no `local_test` results directory.
