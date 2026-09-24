---
name: dev-feature
description: AI-driven feature development workflow. Takes a GitHub issue number, reads the issue, creates a branch, designs and implements the feature with TDD, opens a PR, and updates the issue. Use when asked to "develop feature #N", "implement issue #N", "work on #N", or "dev-feature #N". Also triggers on "pick up issue", "start feature", or "implement this issue".
---

# AI-Driven Feature Development

Orchestrate the full feature development lifecycle from a GitHub issue.

## Usage

```
/dev-feature <issue-number>
```

## Workflow

### Phase 1: Issue Analysis

```bash
# Read the issue
gh issue view <N> --repo kvcache-ai/Mooncake --json title,body,labels,assignees,comments

# Check if already assigned
# If assigned to someone else, STOP and report
```

1. Parse the issue title, body, and labels
2. Identify which module(s) are affected (Transfer Engine, Store, EP, PG, Wheel, etc.)
3. Estimate scope: small (<100 LOC), medium (100-500 LOC), large (>500 LOC)
4. If large (>500 LOC): check if an RFC issue exists. If not, suggest creating one first.

### Phase 2: Branch & Setup

```bash
# Assign yourself
gh issue edit <N> --repo kvcache-ai/Mooncake --add-label "ai-in-progress"

# Create branch from main
git checkout -b feat/issue-<N>-<short-description> main

# Comment on the issue
gh issue comment <N> --repo kvcache-ai/Mooncake --body "🤖 AI agent starting work on this issue.

**Plan:**
<brief plan from Phase 1>

**Estimated scope:** <small/medium/large>
**Module(s):** <affected modules>
**Branch:** \`feat/issue-<N>-<short-description>\`"
```

### Phase 3: Design (for medium/large features)

For features >100 LOC:
1. Read the relevant design docs (see `.claude/rules/module-guidance.md`)
2. Draft a brief design in the issue comment
3. If the issue has an RFC label, verify the design matches the RFC

For small features (<100 LOC):
- Skip design, proceed directly to implementation

### Phase 4: Implementation (TDD)

Follow test-driven development:
1. Write failing tests first
2. Implement the minimal code to pass
3. Refactor if needed
4. Commit frequently with descriptive messages

Use the module-specific skills:
- `.claude/skills/write-mooncake-test/SKILL.md` for test patterns
- `.claude/skills/mooncake-env-conventions/SKILL.md` for env vars
- `.claude/skills/debug-mooncake-crash/SKILL.md` if debugging is needed

### Phase 5: Verification

```bash
# Format code
./scripts/code_format.sh

# Run pre-commit
pre-commit run --all-files

# Run local CI
bash scripts/run_ci_test.sh
```

### Phase 6: PR Creation

```bash
# Push branch
git push -u fork feat/issue-<N>-<short-description>

# Create PR with proper template
gh pr create --repo kvcache-ai/Mooncake \
  --head stmatengss:feat/issue-<N>-<short-description> \
  --base main \
  --title "[<Module>] <description>" \
  --body "$(cat <<'PREOF'
## Description

<description of changes>

Closes #<N>

## Module

- [x] <affected module>

## Type of Change

- [x] <type>

## How Has This Been Tested?

**Test commands:**
\`\`\`bash
<actual test commands run>
\`\`\`

**Test results:**
- [x] Unit tests pass
- [x] Pre-commit hooks pass
- [x] Local CI validation pass

## Checklist

- [x] I have performed a self-review of my own code
- [x] I have formatted my code using `./scripts/code_format.sh`
- [x] I have run `pre-commit run --all-files` and all hooks pass
- [x] I have added tests to prove my changes are effective

## AI Assistance Disclosure

- [x] AI tools were used (Claude Code — full implementation)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
PREOF
)"
```

### Phase 7: Issue Update

```bash
# Update the issue with PR link
gh issue comment <N> --repo kvcache-ai/Mooncake --body "🤖 PR created: <PR-URL>

**Changes:**
<summary of what was implemented>

**Tests added:**
<list of test files>

**CI status:** Pending — will update when CI completes."

# Update label
gh issue edit <N> --repo kvcache-ai/Mooncake --remove-label "ai-in-progress" --add-label "ai-pr-open"
```

### Phase 8: CI Monitoring

After PR is created:
1. Monitor CI status: `gh pr checks <PR-N> --repo kvcache-ai/Mooncake`
2. If CI fails: read the failure, fix the issue, push, comment on PR
3. If CI passes: comment on issue that PR is ready for review

```bash
# Check CI status
gh pr checks <PR-N> --repo kvcache-ai/Mooncake --watch

# If failures, investigate
gh run view <run-id> --repo kvcache-ai/Mooncake --log-failed
```

## Error Handling

- **Issue already assigned:** Report and stop. Don't take work from others.
- **RFC required but missing:** Comment on issue suggesting an RFC, stop.
- **Build fails:** Use debug-mooncake-crash skill to diagnose.
- **Tests fail:** Fix and re-push. Max 3 retry cycles before escalating to human.
- **Scope creep detected:** If implementation exceeds estimate by >2x, stop and comment.

## Labels Used

| Label | Meaning |
|-------|---------|
| `ai-ready` | Issue is ready for AI to pick up |
| `ai-in-progress` | AI agent is actively working on it |
| `ai-pr-open` | AI has opened a PR for this issue |
| `ai-needs-help` | AI is stuck and needs human intervention |
