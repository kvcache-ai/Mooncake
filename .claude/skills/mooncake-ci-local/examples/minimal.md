# Minimal Example

Goal: validate the current branch before opening or submitting a PR.

User prompt:

- 提交 PR 前，帮我跑一遍本地 CI 验证当前分支。

Expected action:

```bash
bash scripts/run_ci_test.sh
```

Force a full rerun even if `paths-filter` would skip downstream stages:

```bash
bash scripts/run_ci_test.sh --skip-path-filter
```

Result location:

- `local_test/run-ci-logs/<timestamp>/`

Typical report format:

- passed stages
- failed or blocked stages
- unsupported stages
- first actionable failure and its log path
