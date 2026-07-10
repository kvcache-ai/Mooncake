from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.benchmarks.rfc_eval_workloads import export_dev_small_manifest  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        default=str(REPO_ROOT / "artifacts" / "rfc_dev_small_workloads.json"),
    )
    parser.add_argument("--mixed-size", type=int, default=20)
    args = parser.parse_args()

    out = export_dev_small_manifest(args.output, mixed_size=args.mixed_size)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
