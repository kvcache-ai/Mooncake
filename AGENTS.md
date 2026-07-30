# AGENTS.md

## `docs/` Directory Changes

- Before modifying files under `docs/`, read `docs/AGENTS.md`.

## Pull Request Guidelines

- Follow `CONTRIBUTING.md` for PR title prefixes, RFC expectations, and
  contribution workflow.
- Before opening a PR for nontrivial work, check whether an existing issue or
  open PR already covers the same change. If the work overlaps, explain the
  difference instead of duplicating it.
- Do not open low-value busywork PRs for isolated typo, style, or mechanical
  changes unless they are part of a substantive requested change.
- Use `.github/pull_request_template.md` when preparing a PR, and fill in the
  relevant sections for description, module, type of change, testing,
  checklist, and AI assistance disclosure.
- For AI-assisted changes, make sure the human submitter has reviewed every
  changed line and can defend the change end-to-end.
- Run pre-commit locally on the files touched by the change before handoff when
  the toolchain is available. If broader hooks or `pre-commit run --all-files`
  rewrite unrelated files, do not include those unrelated edits in the PR.
- Keep PRs lean: review `git diff` before staging, and include only changes
  required for the requested task.
