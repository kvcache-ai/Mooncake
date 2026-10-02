import csv
import json

# Columns of kernel_summary.csv, in the order they are written. The step total is
# indices + kv_write + attention_plan + attention; the gather is a read-only probe
# and is reported beside the step, never inside it.
STEP_PHASES = ("indices", "kv_write", "attention_plan", "attention")
REPORTED_PHASES = STEP_PHASES + ("kv_gather", "total_step")

CSV_COLUMNS = [
    "case_id",
    "mode",
    "branch",
    "reads_before_write",
    "batch_size",
    "prefix_lens",
    "new_lens",
    "context_lens",
    "page_size",
    "layout",
    "new_tokens",
    "context_tokens",
    "pages",
    "padding_tokens",
    "kv_bytes_written",
    "kv_bytes_read_valid",
    "kv_bytes_read_pages",
    "gather_rows",
    "gather_bytes",
    "attention_pairs",
    "attention_flops",
    "kv_cache_bytes_per_rank",
    "num_pages_allocated",
    "warmup",
    "timed",
    "device_name",
    "indices_passed",
    "history_passed",
    "attention_passed",
]
for _phase in REPORTED_PHASES:
    # min as well as the percentiles: on a shared machine a co-tenant can slow a
    # window for part of a run, and min is the uncontended floor of that phase.
    CSV_COLUMNS.extend(
        [
            f"{_phase}_min_ms",
            f"{_phase}_p50_ms",
            f"{_phase}_p95_ms",
            f"{_phase}_p99_ms",
        ]
    )
CSV_COLUMNS.extend(
    [
        "attention_plan_us_per_layer",
        "attention_run_us_per_layer",
        "kv_write_us_per_new_token",
        "indices_us_per_new_token",
        "attention_us_per_context_token",
        "kv_write_effective_gbps",
        "kv_gather_effective_gbps",
        "attention_effective_gbps",
        "attention_valid_token_gbps",
        "attention_tflops",
        "attention_arithmetic_intensity",
        "attention_share_of_step",
        "kv_write_share_of_step",
        "indices_share_of_step",
        "attention_plan_share_of_step",
        "kv_gather_share_of_attention_run",
        "total_tokens_per_s",
    ]
)


def load_records(path):
    records = []
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records


def kernel_summary(records):
    """One row per measured step."""
    rows = []
    for record in records:
        if record.get("kind") != "kernel":
            continue
        case = record["case"]
        configuration = record["configuration"]
        correctness = record["correctness"]
        row = {
            "case_id": case["label"],
            "mode": case["mode"],
            "branch": configuration["branch"],
            "reads_before_write": configuration["reads_before_write"],
            "batch_size": case["batch_size"],
            "prefix_lens": case["prefix_lens"],
            "new_lens": case["new_lens"],
            "context_lens": case["context_lens"],
            "page_size": case["page_size"],
            "layout": case["layout"],
            "new_tokens": case["new_tokens"],
            "context_tokens": case["context_tokens"],
            "pages": case["pages"],
            "padding_tokens": case["padding_tokens"],
            "kv_bytes_written": case["kv_bytes_written"],
            "kv_bytes_read_valid": case["kv_bytes_read_valid"],
            "kv_bytes_read_pages": case["kv_bytes_read_pages"],
            "gather_rows": record["gather_rows"],
            "gather_bytes": record["gather_bytes"],
            "attention_pairs": case["attention_pairs"],
            "attention_flops": case["attention_flops"],
            "kv_cache_bytes_per_rank": record["kv_cache_bytes_per_rank"],
            "num_pages_allocated": record["num_pages_allocated"],
            "warmup": record["warmup"],
            "timed": record["timed"],
            "device_name": record["device_name"],
        }
        for name in ("indices", "history", "attention"):
            check = correctness[name]
            row[f"{name}_passed"] = check["passed"] if isinstance(check, dict) else None
        for phase in REPORTED_PHASES:
            stats = record["phases"][phase]
            row[f"{phase}_min_ms"] = stats["min"]
            row[f"{phase}_p50_ms"] = stats["p50"]
            row[f"{phase}_p95_ms"] = stats["p95"]
            row[f"{phase}_p99_ms"] = stats["p99"]
        row.update(record["derived"])
        rows.append(row)
    return rows


def write_csv(path, rows, columns=None):
    """Write one table as CSV, with dictionaries and lists as JSON cells."""
    if not rows:
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("")
        return
    fieldnames = columns or list(rows[0].keys())
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            flat = {
                key: (json.dumps(value) if isinstance(value, (dict, list)) else value)
                for key, value in row.items()
            }
            writer.writerow(flat)
