"""RFC §8.5 baseline matrix helpers."""

from __future__ import annotations

from typing import Dict, List


BASELINE_DEFINITIONS = [
    ("B0", "Single-node colocated", "单机同置 vLLM / SGLang", "赛题要求的单机端到端基线"),
    ("B1", "Native PD", "原生 PD 分离", "验证 EPD 相对传统 PD 的增益"),
    ("B2", "PD + Prefix Cache", "PD + 文本 prefix cache", "隔离文本前缀缓存收益"),
    ("B3", "PD + Feature Cache", "PD + 图像 FeatureBundle cache", "隔离视觉特征缓存收益"),
    ("B4", "Naive EPD", "Encoder / Prefill / Decode 三阶段分离", "验证 TransferPolicy 与页级状态管理收益"),
    ("B5", "EPD + Deep-copy Fork", "支持 fork，但深拷贝完整 KV", "验证页级引用计数 + CoW 的收益"),
    ("B6", "EPD + CoW no cross-step", "支持 O(1) fork，无跨步骤复用", "隔离跨步骤复用收益"),
    ("B7", "EPD + Exact Prefix", "只允许 exact prefix reuse", "验证安全复用上界"),
    ("B8", "EPD + Approx Reuse", "加入近似 / 非连续复用", "验证激进复用策略收益与风险"),
    ("B9", "Full System", "EPD + cache + CoW + scheduler + offload + A2A", "最终方案"),
]


def build_baseline_matrix(phase6_metrics: Dict, soak_report: Dict) -> List[Dict]:
    phase6_summary = phase6_metrics.get("summary", {})
    soak_summary = soak_report.get("summary", {})
    ablations = phase6_metrics.get("ablations", {})

    measured = {
        "B0": {
            "available": "baseline_avg_ms" in phase6_summary,
            "latency_ms": phase6_summary.get("baseline_avg_ms"),
            "decode_tps": phase6_summary.get("baseline_avg_tps"),
        },
        "B4": {
            "available": "epd_avg_ttft_ms" in phase6_summary,
            "latency_ms": phase6_summary.get("epd_avg_ttft_ms"),
            "decode_tps": phase6_summary.get("epd_avg_decode_tps"),
        },
        "B9": {
            "available": bool(soak_summary),
            "latency_ms": soak_summary.get("epd_total_avg_ms"),
            "decode_tps": soak_summary.get("epd_decode_tps_avg"),
        },
    }
    for baseline_id, payload in ablations.items():
        if isinstance(payload, dict):
            measured[baseline_id] = {
                "available": True,
                "latency_ms": payload.get("latency_ms") or payload.get("ttft_ms") or payload.get("total_ms"),
                "decode_tps": payload.get("decode_tps") or payload.get("tokens_per_sec"),
            }

    rows: List[Dict] = []
    for baseline_id, name, config, purpose in BASELINE_DEFINITIONS:
        data = measured.get(baseline_id, {})
        rows.append(
            {
                "id": baseline_id,
                "name": name,
                "config": config,
                "purpose": purpose,
                "available": bool(data.get("available", False)),
                "latency_ms": data.get("latency_ms"),
                "decode_tps": data.get("decode_tps"),
            }
        )
    return rows
