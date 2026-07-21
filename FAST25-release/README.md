# Mooncake FAST'25 Trace Release

This directory contains the open-source request traces associated with Mooncake.

The `traces/` directory is the updated trace release used by the FAST'25 paper. The older `arxiv-trace/mooncake_trace.jsonl` file is the historical single-file trace release from the arXiv technical report.

## Directory Layout

| Path | Description |
| --- | --- |
| `traces/conversation_trace.jsonl` | FAST'25 conversation workload trace. |
| `traces/toolagent_trace.jsonl` | FAST'25 tool and agent workload trace. |
| `traces/synthetic_trace.jsonl` | FAST'25 synthetic workload trace built from public datasets. |
| `arxiv-trace/mooncake_trace.jsonl` | Historical trace released with the arXiv technical report. |
| `Mooncake-FAST25.pdf` | FAST'25 paper with the trace appendix. |

For new experiments that reproduce or build on the FAST'25 paper, prefer the three files under `traces/`. Use `arxiv-trace/mooncake_trace.jsonl` when referring to the original arXiv technical report trace or comparing against the earlier single-file trace release.

## Workloads

The FAST'25 trace release contains three workloads:

| Workload | File | Requests | Avg input length | Avg output length | Arrival pattern |
| --- | --- | ---: | ---: | ---: | --- |
| Conversation | `traces/conversation_trace.jsonl` | 12,031 | 12,035 | 343 | Timestamp |
| Tool and Agent | `traces/toolagent_trace.jsonl` | 23,608 | 8,596 | 182 | Timestamp |
| Synthetic | `traces/synthetic_trace.jsonl` | 3,993 | 15,325 | 149 | Poisson |

The conversation and tool and agent traces are sampled from one hour of online request data. The synthetic trace is constructed from public datasets and uses Poisson-generated arrivals while preserving the order within multi-turn conversations.

## Trace Format

Each trace is a JSONL file. Each line is one request:

```json
{
    "timestamp": 27482,
    "input_length": 6955,
    "output_length": 52,
    "hash_ids": [46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 2353, 2354]
}
{
    "timestamp": 30535,
    "input_length": 6472,
    "output_length": 26,
    "hash_ids": [46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 2366]
}
```

Fields:

- `timestamp`: Relative request arrival time in milliseconds. In the real traces, requests should be replayed according to these timestamps. In the synthetic trace, timestamps are generated from a Poisson process.
- `input_length`: Number of input tokens. Raw text and tokens are not included for privacy protection.
- `output_length`: Number of output tokens.
- `hash_ids`: Remapped prefix block hashes. The block size is 512 tokens. Identical hash IDs indicate reusable prefix KV cache blocks. For example, the two sample requests share the first 12 hash IDs, so they can share prefix caching for the first `12 * 512 = 6144` tokens.

The trace files contain only anonymized timing, length, and remapped hash information. They are intended for reproducible simulation and evaluation of KV cache reuse behavior without exposing user content.

For more details, see the FAST'25 paper appendix in [`Mooncake-FAST25.pdf`](Mooncake-FAST25.pdf).
