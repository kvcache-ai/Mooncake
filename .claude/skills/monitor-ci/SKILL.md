---
name: monitor-ci
description: Monitor CI status for a PR and take action on failures. Use when asked to "check CI", "monitor CI", "watch CI", "is CI passing", or after opening a PR with /dev-feature. Also use when CI fails and you need to diagnose and fix the issue.
---

# CI Monitoring

Monitor and respond to CI status for Mooncake PRs.

## Usage

```
/monitor-ci <PR-number>
```

## Check CI Status

```bash
# View all checks
gh pr checks <N> --repo kvcache-ai/Mooncake

# View specific workflow run
gh run list --repo kvcache-ai/Mooncake \
  --branch <branch-name> --limit 5

# View failed job logs
gh run view <run-id> --repo kvcache-ai/Mooncake --log-failed
```

## Diagnose Failures

### Build Failures
```bash
# Get the failed step output
gh run view <run-id> --repo kvcache-ai/Mooncake --log-failed 2>&1 | head -100
```

Common build failures:
- **CMake error:** Check CMakeLists.txt changes, missing dependencies
- **Compilation error:** Check C++ syntax, missing includes, ABI issues
- **Linking error:** Check library dependencies, symbol visibility

### Test Failures
```bash
# Get test output
gh run view <run-id> --repo kvcache-ai/Mooncake --log-failed 2>&1 | grep -A 20 "FAILED"
```

### Format/Lint Failures
```bash
# Fix locally
./scripts/code_format.sh
pre-commit run --all-files
git add -u && git commit -m "[Misc] Fix formatting" && git push
```

## Auto-Fix Flow

When CI fails:
1. Download and analyze the failure log
2. Identify the root cause
3. If it's a formatting issue: auto-fix and push
4. If it's a test failure: investigate, fix, add regression test, push
5. If it's a build failure: investigate, fix, push
6. Comment on the PR with what was fixed
7. Re-check CI status

## Slash Commands

From a PR comment (requires write access):
- `/rerun-ci` -- Retrigger full CI
- `/rerun-failed` -- Rerun only failed jobs

## CI Health Dashboard

```bash
# Check overall CI health
gh run list --repo kvcache-ai/Mooncake --limit 10 --json status,conclusion,name \
  --jq '.[] | "\(.name): \(.conclusion // .status)"'

# Download latest failure report (if CI monitor is set up)
gh run download --repo kvcache-ai/Mooncake \
  --name ci-failure-analysis-latest --dir /tmp/ci-report 2>/dev/null
```
