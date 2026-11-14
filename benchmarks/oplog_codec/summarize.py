"""Summarize raw repeated codec samples from stdin; emit JSON to stdout."""

import csv
import json
from statistics import median
import sys


def main():
    groups = {}
    for row in csv.DictReader(sys.stdin):
        groups.setdefault((row["workload"], row["format"]), []).append(row)
    if not groups:
        raise ValueError("no benchmark samples")
    results = []
    for (workload, codec), samples in sorted(groups.items()):
        baseline = groups[(workload, "json")]
        control = groups[(workload, "json-control")]
        if len({row["repeat"] for row in samples}) != len(samples):
            raise ValueError("duplicate repetition")
        for expected in ("json", "json-control", "cbor", "msgpack"):
            if {row["repeat"] for row in samples} != {
                row["repeat"] for row in groups[(workload, expected)]
            }:
                raise ValueError("missing format/repetition")

        def metric(rows, name):
            return median(float(row[name]) for row in rows)

        for name in ("wire_bytes", "entries", "payload_bytes"):
            if len({row[name] for row in samples}) != 1:
                raise ValueError("workload changed between repetitions")
        results.append(
            {
                "workload": workload,
                "format": codec,
                "profile": "generated-production-schema"
                if workload.startswith("typed_")
                else "synthetic-opaque-payload",
                "repeats": len(samples),
                "entries": int(samples[0]["entries"]),
                "payload_bytes": int(samples[0]["payload_bytes"]),
                "wire_bytes": int(samples[0]["wire_bytes"]),
                "size_reduction_percent": 100
                * (1 - metric(samples, "wire_bytes") / metric(baseline, "wire_bytes")),
                "encode_cpu_us_median": metric(samples, "encode_cpu_us"),
                "decode_cpu_us_median": metric(samples, "decode_cpu_us"),
                "encode_cpu_speedup": metric(baseline, "encode_cpu_us")
                / metric(samples, "encode_cpu_us"),
                "decode_cpu_speedup": metric(baseline, "decode_cpu_us")
                / metric(samples, "decode_cpu_us"),
                "encode_cpu_speedup_vs_json_control": metric(control, "encode_cpu_us")
                / metric(samples, "encode_cpu_us"),
                "decode_cpu_speedup_vs_json_control": metric(control, "decode_cpu_us")
                / metric(samples, "decode_cpu_us"),
                "encode_cpu_us_min": min(
                    float(row["encode_cpu_us"]) for row in samples
                ),
                "encode_cpu_us_max": max(
                    float(row["encode_cpu_us"]) for row in samples
                ),
                "decode_cpu_us_min": min(
                    float(row["decode_cpu_us"]) for row in samples
                ),
                "decode_cpu_us_max": max(
                    float(row["decode_cpu_us"]) for row in samples
                ),
                "encode_batches_s_median": metric(samples, "encode_batches_s"),
                "decode_batches_s_median": metric(samples, "decode_batches_s"),
            }
        )
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
