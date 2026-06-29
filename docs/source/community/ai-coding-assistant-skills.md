# AI Coding Assistant Skills

Mooncake ships a set of **built-in skills** under
[`.claude/skills`](https://github.com/kvcache-ai/Mooncake/tree/main/.claude/skills)
— reusable, task-focused playbooks that an AI coding assistant (such as Claude
Code) invokes automatically when your request matches, or that you can run as a
slash command:

| Skill | Description |
|-------|-------------|
| `/mooncake-troubleshoot` | Diagnose Mooncake deployment and runtime issues (services, RDMA, env vars, logs). |
| `/mooncake-ci-local` | Run pre-PR local validation via `scripts/run_ci_test.sh`. |
| `/mooncake-api` | Work with the Mooncake Store, Transfer Engine, and EP/Backend Python APIs. |

Install them without cloning the repository via the
[Claude Code plugin marketplace](https://code.claude.com/docs/en/plugin-marketplaces):

```text
/plugin marketplace add kvcache-ai/Mooncake --sparse .claude-plugin
/plugin install mooncake-troubleshoot@mooncake
/plugin install mooncake-ci-local@mooncake
/plugin install mooncake-api@mooncake
```

The `--sparse .claude-plugin` flag fetches only the marketplace catalog, and
each plugin is published as a `git-subdir` source, so installing one fetches
only that single skill directory — never the whole repo. If you are already
working inside a Mooncake checkout, the skills under `.claude/skills/` load
automatically with no setup.
