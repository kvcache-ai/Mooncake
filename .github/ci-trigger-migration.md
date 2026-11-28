# CI after the pull request event policy change

GitHub is [enforcing the default event policy for affected public repositories
on December 2, 2026][policy]. Do not allow `pull_request_target` repository-wide
to preserve hardware CI.

| Old entry point | Replacement |
| --- | --- |
| `run-ci` label | Open the existing **Build & Test (Linux)** run at the desired SHA and select **Re-run jobs**. New PR commits still start Linux CI automatically. |
| `run-e2e-ci` label | In the upstream repository, open **E2E CI**, select **Run workflow**, and leave the branch set to `main`. |
| `run-hygon-ci` label | In the upstream repository, open **Hygon HCU Build & Test**, select **Run workflow**, and leave the branch set to `main`. |

A hardware run tests the exact upstream `main` commit selected at dispatch.
It is **not** a pre-merge check of a PR head. A workflow run on a different ref
or a fork is not a valid substitute. Confirm the tested SHA on the Actions run
before using the result. If a merge ruleset currently requires pre-merge hardware
checks, agree on a safe migration with the runner owners before merging this
change; do not treat a skipped or post-merge run as equivalent. Restoring
pre-merge hardware coverage requires a separate isolated runner design.

**Auto Label PRs**, **PR Size**, and **PR Tidy** still use
`pull_request_target` for metadata writes. Their automatic behavior is not
restored by this change. Keep the default block unless the maintainers separately
approve a policy limited to reviewed metadata-only workflows.

[policy]: https://docs.github.com/en/actions/reference/security/securely-using-pull_request_target#default-policy-for-pull_request_target
