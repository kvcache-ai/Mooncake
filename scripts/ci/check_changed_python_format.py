#!/usr/bin/env python3
"""Check Ruff formatting only for lines changed by the current revision."""

from __future__ import annotations

import re
import subprocess
import sys
from collections.abc import Iterable

HUNK_RE = re.compile(
    r"^@@ -(?P<old_start>\d+)(?:,(?P<old_count>\d+))? "
    r"\+(?P<new_start>\d+)(?:,(?P<new_count>\d+))? @@"
)


def _changed_line_ranges(base_ref: str, path: str) -> list[tuple[int, int]]:
    result = subprocess.run(
        ["git", "diff", "--unified=0", f"{base_ref}...HEAD", "--", path],
        check=True,
        capture_output=True,
        text=True,
    )
    ranges: list[tuple[int, int]] = []
    for line in result.stdout.splitlines():
        match = HUNK_RE.match(line)
        if match is None:
            continue
        start = int(match.group("new_start"))
        count = int(match.group("new_count") or 1)
        end = start + max(count, 1) - 1
        ranges.append((start, end))
    return ranges


def _ruff_format_changed_lines(diff: str) -> set[int]:
    changed_lines: set[int] = set()
    old_line = 0
    for line in diff.splitlines():
        match = HUNK_RE.match(line)
        if match is not None:
            old_line = int(match.group("old_start"))
            continue
        if not old_line or line.startswith("\\"):
            continue
        if line.startswith("-"):
            changed_lines.add(old_line)
            old_line += 1
        elif line.startswith("+"):
            changed_lines.add(old_line)
        elif line.startswith(" "):
            old_line += 1
    return changed_lines


def _overlaps(
    changed_ranges: Iterable[tuple[int, int]],
    format_lines: Iterable[int],
) -> bool:
    changed_ranges = list(changed_ranges)
    format_lines = list(format_lines)
    return any(
        changed_start <= format_line <= changed_end
        for changed_start, changed_end in changed_ranges
        for format_line in format_lines
    )


def main() -> int:
    if len(sys.argv) < 3:
        print(
            f"usage: {sys.argv[0]} BASE_REF PYTHON_FILE [PYTHON_FILE ...]",
            file=sys.stderr,
        )
        return 2

    base_ref, *paths = sys.argv[1:]
    failures: list[str] = []
    for path in paths:
        changed_ranges = _changed_line_ranges(base_ref, path)
        if not changed_ranges:
            continue

        result = subprocess.run(
            ["ruff", "format", "--diff", path],
            capture_output=True,
            text=True,
        )
        if result.returncode not in (0, 1):
            print(result.stderr, end="", file=sys.stderr)
            failures.append(path)
            continue

        if not result.stdout:
            continue
        if _overlaps(changed_ranges, _ruff_format_changed_lines(result.stdout)):
            failures.append(path)
            print(
                f"Ruff formatting changes overlap lines modified in {path}:",
                file=sys.stderr,
            )
            print(result.stdout, end="", file=sys.stderr)
        else:
            print(
                f"Ignoring pre-existing Ruff formatting differences in {path}; "
                "none overlap the changed lines."
            )

    if failures:
        print(
            "Ruff format check failed for changed Python lines in: "
            + ", ".join(failures),
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
