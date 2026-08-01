# Minimal Example

Goal: validate the current branch before opening or submitting a PR.

User prompt:

- 提交 PR 前，帮我跑一遍本地 CI 验证当前分支。

Expected action:

```console
git diff --name-only origin/main...HEAD
pre-commit run --from-ref origin/main --to-ref HEAD
```

If the branch contains C or C++ changes, also run:

```console
./scripts/code_format.sh --changed-lines --check --base origin/main
```

Typical report format:

- changed areas
- checks that passed
- checks that failed or were blocked
- checks not run because the local platform does not support them
- the first actionable failure, if any
