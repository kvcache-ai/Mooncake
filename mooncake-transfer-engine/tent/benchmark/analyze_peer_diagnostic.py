#!/usr/bin/env python3
"""Calibrate TTL from measured V1 aggregate decisions, never completion gaps."""

import argparse
import json
import math
from pathlib import Path
import re


def analyze(run, log):
    summary = json.loads((run / "summary.json").read_text())
    start = summary["measurement_origin_ns"]
    end = start + summary["measurement_ns"]
    requests = [
        json.loads(s) for s in (run / "requests.jsonl").read_text().splitlines()
    ]
    by_key = {
        (r["detail"]["peer"], r["detail"]["request_key"]): r
        for r in requests
        if "peer" in r["detail"]
    }
    if len(by_key) != len(requests):
        raise ValueError(
            "calibration requires submitted requests with unique peer/source keys"
        )
    decisions = {}
    for line in log.read_text().splitlines():
        if "peer_allocation " not in line:
            continue
        f = dict(re.findall(r"(\w+)=([^\s]+)", line))
        now = int(f["now_ns"])
        if not start <= now <= end or int(f["num_slices"]) <= 1:
            continue
        key = (int(f["peer"]), int(f["request_key"]))
        if key not in by_key:
            continue
        decisions.setdefault((key, now), []).append(f)
    ages, cold, incomplete = [], 0, 0
    for (key, now), rows in decisions.items():
        if len(rows) != int(rows[0]["candidates"]) or len(
            {r["dev"] for r in rows}
        ) != len(rows):
            incomplete += 1
            continue
        by_key[key].setdefault("allocation", []).extend(rows)
        if any(int(r["samples"]) == 0 for r in rows):
            cold += 1
            continue
        age = max(now - int(r["last_sample_ns"]) for r in rows)
        if any(int(r["last_sample_ns"]) > now for r in rows):
            raise ValueError("candidate sample is newer than its decision")
        ages.append(age)
        by_key[key]["maximum_candidate_age_ns"] = age
    complete = (
        bool(ages)
        and not incomplete
        and len(decisions) == len(requests)
        and summary.get("variant") == "v1"
        and summary.get("evidence") == "rdma_diagnostic"
        and summary.get("data_verification_complete") is True
        and summary["all"]["success"] == len(requests)
    )
    p99 = sorted(ages)[math.ceil(len(ages) * 0.99) - 1] if ages else None
    # Ceiling to whole milliseconds, with 1 ms minimum for zero-age samples.
    ttl = max(1, (p99 + 999999) // 1000000) * 1000000 if complete else None
    return {
        "evidence": "diagnostic_only",
        "aggregate_decisions": len(decisions),
        "cold_decisions": cold,
        "noncold_decisions": len(ages),
        "incomplete_decisions": incomplete,
        "maximum_candidate_age_p99_ns": p99,
        "ttl_rule": "nearest-rank P99 of noncold request max age; ceil to ms; min 1 ms",
        "calibration_complete": complete,
        "proposed_frozen_ttl_ns": ttl,
        "requests": requests,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run", type=Path)
    parser.add_argument("log", type=Path)
    args = parser.parse_args()
    print(json.dumps(analyze(args.run, args.log), indent=2))
