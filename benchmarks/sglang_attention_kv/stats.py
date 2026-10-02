# Percentiles and summary statistics. No torch dependency, so dry runs and
# reporting also work on a machine without a GPU.


def percentile(samples, q):
    """Linearly interpolated percentile over a sequence of floats."""
    ordered = sorted(float(value) for value in samples)
    if not ordered:
        raise ValueError("cannot take a percentile of an empty sample")
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * q
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def summarize(samples):
    ordered = sorted(float(value) for value in samples)
    if not ordered:
        raise ValueError("cannot summarize an empty sample")
    return {
        "count": len(ordered),
        "mean": sum(ordered) / len(ordered),
        "p50": percentile(ordered, 0.50),
        "p95": percentile(ordered, 0.95),
        "p99": percentile(ordered, 0.99),
        "min": ordered[0],
        "max": ordered[-1],
    }
