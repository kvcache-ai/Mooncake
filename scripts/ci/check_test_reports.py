"""Check JUnit/GoogleTest reports, optionally checking exact case identities."""

import argparse
from pathlib import Path
import xml.etree.ElementTree as ET


def validate_report(
    path: str | Path,
    expected: set[str] | None = None,
    require_gtest_status: bool = False,
    allow_skipped: bool = False,
) -> None:
    report = Path(path)
    cases = ET.parse(report).findall(".//testcase")
    if not cases:
        raise RuntimeError(f"{report}: no tests executed")
    if expected is not None:
        identities = [f"{case.get('classname')}.{case.get('name')}" for case in cases]
        if len(identities) != len(expected) or set(identities) != expected:
            raise RuntimeError(f"{report}: test identity mismatch: {identities}")
    for case in cases:
        status = case.get("status")
        result = case.get("result")
        skipped = case.find("skipped") is not None or (status, result) in {
            ("run", "skipped"),
            ("notrun", "suppressed"),
        }
        if any(case.find(tag) is not None for tag in ("failure", "error")):
            raise RuntimeError(f"{report}: test did not pass: {case.attrib}")
        if skipped:
            if not allow_skipped or require_gtest_status:
                raise RuntimeError(f"{report}: test was skipped: {case.attrib}")
            continue
        if require_gtest_status:
            completed = (status, result) == ("run", "completed")
        else:
            completed = status in (None, "run") and result in (None, "completed")
        if not completed:
            raise RuntimeError(f"{report}: test did not pass: {case.attrib}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--require-gtest-status",
        action="store_true",
        help="Require GoogleTest's run/completed testcase attributes",
    )
    parser.add_argument(
        "--allow-skipped",
        action="store_true",
        help="Permit skipped testcases in CTest reports",
    )
    parser.add_argument("reports", nargs="+", type=Path)
    args = parser.parse_args()
    for report in args.reports:
        validate_report(
            report,
            require_gtest_status=args.require_gtest_status,
            allow_skipped=args.allow_skipped,
        )


if __name__ == "__main__":
    main()
