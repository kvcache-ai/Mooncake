#!/usr/bin/env python3
"""Extract an allocation-size trace from mooncake_master logs.

``mooncake_master --v=1`` logs one ``action=put_start_begin`` line per PutStart
with ``key=`` and ``value_length=``. This script turns such logs into the
one-size-per-line format consumed by
``allocation_strategy_bench --rl_trace_file`` and prints a per-octave size
histogram so the captured distribution can be sanity-checked against the
``master_value_size_bytes`` metric.

With ``--events``, it also writes an ordered event log (``put``, ``remove``,
``evict``) keyed by object, using the ``action=remove_object`` and
``action=evict_object`` lines, for lifetime-aware replay.

Usage:
    extract_alloc_trace.py master.INFO [more logs...] -o rl_sizes.txt
    extract_alloc_trace.py master.INFO -o rl_sizes.txt --events rl_events.txt
"""

import argparse
import re
import sys
from collections import Counter

PUT_RE = re.compile(
    r"key=(?P<key>.*?), value_length=(?P<size>\d+), .*action=put_start_begin"
)
REMOVE_RE = re.compile(r"key=(?P<key>.*?), size=(?P<size>\d+), .*action=remove_object")
EVICT_RE = re.compile(r"key=(?P<key>.*?), size=(?P<size>\d+), .*action=evict_object")


def octave_label(size):
    units = ["B", "K", "M", "G", "T"]
    lo = 1
    while lo * 2 <= size:
        lo *= 2
    hi = lo * 2

    def fmt(value):
        unit = 0
        while value >= 1024 and unit < len(units) - 1:
            value //= 1024
            unit += 1
        return f"{value}{units[unit]}"

    return f"{fmt(lo)}-{fmt(hi)}", lo


def parse_logs(paths):
    """Return (sizes, events). events is a list of (kind, key, size)."""
    sizes = []
    events = []
    for path in paths:
        with open(path, "r", errors="replace") as handle:
            for line in handle:
                match = PUT_RE.search(line)
                if match:
                    size = int(match.group("size"))
                    sizes.append(size)
                    events.append(("put", match.group("key"), size))
                    continue
                match = EVICT_RE.search(line)
                if match:
                    events.append(
                        ("evict", match.group("key"), int(match.group("size")))
                    )
                    continue
                match = REMOVE_RE.search(line)
                if match:
                    events.append(
                        ("remove", match.group("key"), int(match.group("size")))
                    )
    return sizes, events


def print_histogram(sizes, out=None):
    out = out if out is not None else sys.stderr
    if not sizes:
        print("no put_start_begin lines found", file=out)
        return
    buckets = Counter()
    order = {}
    for size in sizes:
        label, lo = octave_label(size)
        buckets[label] += 1
        order[label] = lo
    total = len(sizes)
    total_bytes = sum(sizes)
    print(
        f"{total} allocations, {total_bytes / 2**30:.2f} GiB total, "
        f"min={min(sizes)} max={max(sizes)} bytes",
        file=out,
    )
    print(f"{'octave':>12} {'count':>10} {'share':>8} {'bytes_share':>12}", file=out)
    bytes_by_label = Counter()
    for size in sizes:
        bytes_by_label[octave_label(size)[0]] += size
    for label in sorted(buckets, key=lambda name: order[name]):
        count = buckets[label]
        print(
            f"{label:>12} {count:>10} {100.0 * count / total:>7.2f}% "
            f"{100.0 * bytes_by_label[label] / total_bytes:>11.2f}%",
            file=out,
        )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("logs", nargs="+", help="mooncake_master log files")
    parser.add_argument("-o", "--output", required=True, help="size trace output")
    parser.add_argument(
        "--events", help="optional ordered put/remove/evict event log output"
    )
    args = parser.parse_args(argv)

    sizes, events = parse_logs(args.logs)
    with open(args.output, "w") as handle:
        handle.write("# allocation sizes in bytes, one per PutStart\n")
        handle.writelines(f"{size}\n" for size in sizes)
    if args.events:
        with open(args.events, "w") as handle:
            handle.write("# kind key size\n")
            handle.writelines(f"{kind} {key} {size}\n" for kind, key, size in events)
    print_histogram(sizes)
    return 0 if sizes else 1


if __name__ == "__main__":
    sys.exit(main())
