import csv
import json

from .stats import PHASES

# The windows this table writes one column group per: the phases the benchmark
# measures plus the step as one span and the sum of its parts.
REPORTED_PHASES = PHASES

CSV_COLUMNS = [
    "case_id",
    "mode",
    "branch",
    "reads_before_write",
    "attention_backend",
    "wrapper_page_size",
    "decode_use_tensor_cores",
    "kv_write_stream",
    "slot_allocator",
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
    "kv_bytes_paged_read",
    "kv_bytes_ragged_read",
    "kv_bytes_attention_read",
    "kv_bytes_page_capacity",
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
    # min as well as the percentiles: min is the smallest sample the window took
    # in that row, and on a shared machine it is the one a slower sample during
    # the run does not move.
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
        "attention_plan_us_per_step",
        "attention_component_us_per_layer",
        "kv_write_component_us_per_layer",
        "indices_us_per_new_token",
        "attention_us_per_context_token",
        "kv_gather_unique_payload_gbps",
        "attention_unique_payload_gbps",
        "attention_tflops",
        "attention_flops_per_unique_payload_byte",
        "indices_ratio_of_phase_sum",
        "attention_plan_ratio_of_phase_sum",
        "layer_loop_ratio_of_phase_sum",
        "attention_component_ratio_of_phase_sum",
        "kv_write_component_ratio_of_phase_sum",
        "components_ratio_of_phase_sum",
        "new_tokens_per_s",
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
            "attention_backend": configuration["attention_backend"],
            "wrapper_page_size": configuration["wrapper_page_size"],
            "decode_use_tensor_cores": configuration["decode_use_tensor_cores"],
            "kv_write_stream": configuration["kv_write_stream"],
            "slot_allocator": configuration["slot_allocator"],
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
            "kv_bytes_paged_read": record["read_bytes"]["paged"],
            "kv_bytes_ragged_read": record["read_bytes"]["ragged"],
            "kv_bytes_attention_read": record["read_bytes"]["attention"],
            "kv_bytes_page_capacity": case["kv_page_capacity_bytes"],
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
        # The declared schema is the contract: a figure that derive() produces but
        # the schema does not name would be dropped by the writer, and a column the
        # schema promises has to be filled here.
        unexpected = sorted(set(row) - set(CSV_COLUMNS))
        missing = sorted(set(CSV_COLUMNS) - set(row))
        if unexpected or missing:
            raise ValueError(
                f"row {case['label']} does not match the declared schema: "
                f"undeclared {unexpected}, missing {missing}"
            )
        rows.append(row)
    return rows


def write_csv(path, rows, columns=None):
    """Write one table as CSV, with dictionaries and lists as JSON cells.

    The kernel table passes CSV_COLUMNS, so the file's schema is the one declared
    here rather than whatever the first row happens to hold; a row missing a column
    fails on the spot instead of writing a short line.
    """
    if not rows:
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("")
        return
    fieldnames = list(columns) if columns is not None else list(rows[0].keys())
    missing = sorted(set(fieldnames) - set(rows[0].keys()))
    if missing:
        raise ValueError(f"rows are missing the declared columns {missing}")
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            flat = {
                key: (json.dumps(value) if isinstance(value, (dict, list)) else value)
                for key, value in row.items()
            }
            writer.writerow(flat)
