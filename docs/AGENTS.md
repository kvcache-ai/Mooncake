# AGENTS.md - Mooncake Documentation

This file gives coding agents the repo-local rules for modifying files under
`docs/`. Keep `README.md` as the human-facing quickstart. Use this file for
agent workflow, verification, and maintenance guidance.

## Scope

- Applies to changes under `docs/`, especially `docs/source/`.
- Prefer small, reviewable documentation changes.
- Do not rewrite unrelated pages, generated files, or formatting-only content.
- Preserve existing documentation structure unless the user asks for a broader
  reorganization.

## Build and Preview

Run documentation commands from the `docs` directory:

```
cd docs
```

Install dependencies with `uv` when needed. If there is an existing venv,
prefer using the existing one. Otherwise, create one before installing
dependencies:

```
uv venv
uv pip install -r requirements-docs.txt
```

Clean stale build output when validating navigation, generated API pages, or
theme behavior:

```
make clean
```

Build HTML before handing off user-visible documentation changes:

```
make html
```

Set `locale` correctly before building when the change depends on localized
content or translated output.

Serve the generated site for review:

```
python -m http.server -d build/html/
```

The default URL is `http://localhost:8000`. If port 8000 is busy, choose another
available port.

## Editing Guidance

- Source pages live under `docs/source/`.
- Keep links relative and Sphinx-compatible unless an external URL is required.
- For navigation changes, inspect `docs/source/index.md` and the relevant
  toctree before editing individual pages.
- Use Sphinx-native structure for documentation behavior. Do not use client-side
  JavaScript or post-render DOM patches for navigation or theme behavior.
- If the requested behavior is not supported by Sphinx or the active theme,
  prefer adding a Sphinx extension/plugin instead of patching rendered HTML.
- When a page should be linked from content but excluded from the main sidebar,
  use a content link plus appropriate Sphinx metadata such as `orphan: true`
  instead of hiding rendered sidebar nodes.
- Keep homepage toctree depth conservative. Do not increase `index.md` maxdepth
  unless the user explicitly asks for deeper landing-page nesting.

## Validation Checklist

- Run `make html` for docs changes that affect rendered pages, navigation,
  cross-references, or Sphinx configuration.
- Check the generated HTML for the changed pages.
- For sidebar or toctree changes, verify both the article body and left sidebar
  render the intended entries.
- If a local preview server is useful for review, start one from `docs/` with
  `python -m http.server -d build/html/` or an alternate port.

## Pull Request Hygiene

- Keep docs-only changes narrowly scoped.
- Review `git diff` before staging so generated files or hook-only formatting
  changes do not leak into the PR.
- If opening a PR, use the repository pull request template.
- Use the repository PR title prefix rules from the root `AGENTS.md`.
