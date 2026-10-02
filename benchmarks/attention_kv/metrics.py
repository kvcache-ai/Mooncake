# Parsing and differencing of Prometheus text metrics, shared by collection and reporting.

import urllib.error
import urllib.request


def http_get(url, timeout=10.0):
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="replace")


def parse_prometheus(text):
    """Parse /metrics into {full name including labels: value}."""
    values = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if " " not in line:
            continue
        name, _, raw = line.rpartition(" ")
        try:
            values[name.strip()] = float(raw)
        except ValueError:
            continue
    return values


def split_metric(name):
    """Split sglang:x{a="b"} into its base name and its label part."""
    if "{" not in name:
        return name, ""
    base, _, labels = name.partition("{")
    return base, labels.rstrip("}")


def sum_metric(metrics, base_name, label_filters=()):
    """Sum one metric across every rank, filtered by label. Tensor parallelism
    gives every rank its own series, so a run total needs the sum."""
    total = 0.0
    found = False
    for name, value in metrics.items():
        base, labels = split_metric(name)
        if base != base_name:
            continue
        if any(fragment not in labels for fragment in label_filters):
            continue
        total += value
        found = True
    return total if found else None


COUNT_SUFFIXES = ("_total", "_count", "_sum", "_created")


def diff_counters(before, after):
    """Difference counters, that is series ending in _total/_count/_sum.

    Returns (delta, series that only exist in after). Histograms and newly
    labelled series are often created on the first hit, so they are absent from
    before. A counter starts at zero, so the delta equals the current value;
    the names come back with it so the caller can account for those increments.
    """
    delta = {}
    appeared = []
    for name, value in after.items():
        base, _ = split_metric(name)
        if not base.endswith(COUNT_SUFFIXES):
            continue
        if name not in before:
            appeared.append(name)
            if value != 0.0:
                delta[name] = value
            continue
        difference = value - before[name]
        if difference != 0.0:
            delta[name] = difference
    return delta, sorted(appeared)


def scrape(base_url, timeout=20.0):
    """Fetch /metrics. An unreachable server yields an empty dict, which the
    caller reports as unavailable."""
    try:
        return parse_prometheus(http_get(f"{base_url}/metrics", timeout=timeout))
    except (urllib.error.URLError, OSError, TimeoutError):
        return {}
