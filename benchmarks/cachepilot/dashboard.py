"""CachePilot Streamlit dashboard for competition demos and screenshots."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent
CSV_DIR = PROJECT_ROOT / "results" / "csv"
FIG_DIR = PROJECT_ROOT / "results" / "figures"

PAGE_TITLE = "CachePilot：基于 Mooncake 的 KVCache 复用与调度评测系统"


def load_csv(name: str) -> Optional[pd.DataFrame]:
    path = CSV_DIR / name
    if not path.exists():
        st.warning(f"CSV 不存在：`{path.relative_to(PROJECT_ROOT)}`")
        return None
    try:
        return pd.read_csv(path)
    except Exception as exc:  # noqa: BLE001
        st.warning(f"读取失败 `{path.name}`：{exc}")
        return None


def show_image(name: str, caption: str | None = None) -> None:
    path = FIG_DIR / name
    if not path.exists():
        st.warning(f"图片不存在：`{path.relative_to(PROJECT_ROOT)}`")
        return
    st.image(str(path), caption=caption or name, use_container_width=True)


def fmt_num(value, digits: int = 2, suffix: str = "") -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "N/A"
    try:
        return f"{float(value):.{digits}f}{suffix}"
    except (TypeError, ValueError):
        return str(value)


def overview_page() -> None:
    st.header("Overview")
    st.markdown(
        """
**CachePilot** 是基于 Mooncake 的 KVCache 复用与调度评测系统。  
以官方 `store_kv_bench.py` 为真实底座，完成 Mooncake Store 的 put/get/`read_perf` **实机实测**，
并扩展 Prefix Reuse 可控 workload 评估与 Retrieval Scheduler 离线策略评估。
        """.strip()
    )

    st.subheader("实验环境")
    cols = st.columns(3)
    env_items = [
        ("平台", "AutoDL 容器实例"),
        ("GPU", "RTX 4090 24GB"),
        ("OS", "Ubuntu 22.04"),
        ("Python", "3.10"),
        ("CUDA", "11.8"),
        ("Store", "Mooncake Store"),
        ("Protocol", "TCP"),
        ("Package", "mooncake-transfer-engine-non-cuda"),
        ("Binding", "MooncakeDistributedStore"),
    ]
    for i, (k, v) in enumerate(env_items):
        cols[i % 3].markdown(f"**{k}**  \n{v}")

    st.subheader("核心指标")
    verify = load_csv("store_verify_real.csv")
    store = load_csv("store_benchmark.csv")

    v_rc = v_mib = v_p50 = v_p99 = None
    if verify is not None and not verify.empty:
        row = verify.iloc[0]
        v_rc = row.get("return_code")
        v_mib = row.get("mib_s")
        v_p50 = row.get("lat_p50")
        v_p99 = row.get("lat_p99")

    max_mib = min_p99 = None
    if store is not None and not store.empty:
        ok = store
        if "return_code" in store.columns:
            ok = store[store["return_code"] == 0]
            if ok.empty:
                ok = store
        if "mib_s" in ok.columns and ok["mib_s"].notna().any():
            max_mib = float(ok["mib_s"].max())
        if "lat_p99" in ok.columns and ok["lat_p99"].notna().any():
            min_p99 = float(ok["lat_p99"].min())

    c1, c2, c3 = st.columns(3)
    c1.metric("verify_write return_code", fmt_num(v_rc, 0))
    c2.metric("verify_write MiB/s", fmt_num(v_mib, 2))
    c3.metric("verify_write p50 (ms)", fmt_num(v_p50, 3))

    c4, c5, c6 = st.columns(3)
    c4.metric("verify_write p99 (ms)", fmt_num(v_p99, 3))
    c5.metric("read_perf 最高 MiB/s", fmt_num(max_mib, 2))
    c6.metric("read_perf 最低 p99 (ms)", fmt_num(min_p99, 3))

    st.info(
        "**数据性质：** Store Benchmark 是真实 Mooncake Store 实机实测；"
        "Prefix Reuse 和 Retrieval Scheduler 是基于可控 workload 的策略评估。"
    )


def store_page() -> None:
    st.header("Store Benchmark")
    st.caption("真实 Mooncake Store 实机实测（AutoDL RTX 4090 · TCP · MooncakeDistributedStore）")

    df = load_csv("store_benchmark.csv")
    if df is not None:
        st.subheader("store_benchmark.csv")
        st.dataframe(df, use_container_width=True, hide_index=True)

        # Interactive charts from CSV (fallback if PNG missing still useful)
        plot_df = df.copy()
        if "return_code" in plot_df.columns:
            ok = plot_df[plot_df["return_code"] == 0]
            if not ok.empty:
                plot_df = ok

        left, right = st.columns(2)
        with left:
            st.subheader("value_size vs MiB/s")
            show_image(
                "store_bandwidth_by_value_size.png",
                "真实 Store：带宽随 value_size 变化",
            )
            if (
                plot_df is not None
                and {"value_size", "mib_s", "batch_size"}.issubset(plot_df.columns)
            ):
                chart = (
                    plot_df.dropna(subset=["mib_s"])
                    .pivot_table(
                        index="value_size",
                        columns="batch_size",
                        values="mib_s",
                        aggfunc="mean",
                    )
                    .sort_index()
                )
                st.line_chart(chart)

        with right:
            st.subheader("batch_size vs p99 latency")
            show_image(
                "store_p99_by_batch_size.png",
                "真实 Store：p99 延迟随 batch_size 变化",
            )
            if (
                plot_df is not None
                and {"batch_size", "lat_p99", "value_size"}.issubset(plot_df.columns)
            ):
                chart = (
                    plot_df.dropna(subset=["lat_p99"])
                    .pivot_table(
                        index="batch_size",
                        columns="value_size",
                        values="lat_p99",
                        aggfunc="mean",
                    )
                    .sort_index()
                )
                st.line_chart(chart)

    st.subheader("重点结果")
    st.markdown(
        """
- **1MB** value_size、**batch=4** 时最高带宽约 **1399.08 MiB/s**（真实 Mooncake Store 实测）
- **4KB** 小对象 p99 可达到**亚毫秒级**（例如 batch=1 时 p99≈0.380 ms）
- **4MB** 大对象 **batch=16** 时 p99 明显升高（约 93.267 ms），需权衡吞吐与尾延迟
- `verify_write` / `read_perf` 全程 `return_code=0`，`misses=0`，`verify_failures=0`
        """.strip()
    )


def prefix_page() -> None:
    st.header("Prefix Reuse Evaluation")
    st.markdown(
        "这是面向长上下文 prefix 复用的**可控 workload 评估**，"
        "用于分析 cache hit ratio 对 TTFT 的影响。"
        "可与真实 Store get 延迟参数对齐，**不是**对 Store API 的替代实现。"
    )

    df = load_csv("prefix_reuse.csv")
    if df is not None:
        st.subheader("prefix_reuse.csv")
        st.dataframe(df, use_container_width=True, hide_index=True)

    c1, c2 = st.columns(2)
    with c1:
        show_image(
            "prefix_length_vs_ttft_reduction.png",
            "Prefix 长度 vs TTFT 降低比例",
        )
    with c2:
        show_image(
            "cache_hit_ratio_vs_ttft.png",
            "Cache hit ratio vs TTFT",
        )


def scheduler_page() -> None:
    st.header("Retrieval Scheduler Evaluation")
    st.markdown(
        "面向多节点 KVCache 检索场景的**离线策略评估**，"
        "对比 **Random / Nearest / Reuse-aware / CachePilot**。"
        "基于可控 workload，不作为真实分布式集群端到端性能声明。"
    )

    df = load_csv("retrieval_scheduler.csv")
    if df is not None:
        st.subheader("retrieval_scheduler.csv")
        st.dataframe(df, use_container_width=True, hide_index=True)

    c1, c2, c3 = st.columns(3)
    with c1:
        show_image("scheduler_latency.png", "检索延迟对比")
    with c2:
        show_image("scheduler_remote_traffic.png", "远程流量对比")
    with c3:
        show_image("scheduler_hotspot.png", "热点比对比")


def implementation_page() -> None:
    st.header("Implementation")

    st.subheader("项目目录结构")
    st.code(
        """
CachePilot/
├── README.md
├── DESIGN.md
├── EVALUATION.md
├── dashboard.py
├── requirements.txt
├── benchmark/
│   ├── mooncake_store_runner.py
│   ├── parse_store_output.py
│   ├── prefix_reuse_benchmark.py
│   ├── retrieval_scheduler_sim.py
│   └── plot_results.py
├── scripts/
│   ├── run_all.sh
│   └── run_dashboard.sh
└── results/
    ├── csv/
    ├── logs/
    └── figures/
        """.strip(),
        language="text",
    )

    st.subheader("运行命令")
    st.code(
        """
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
bash scripts/run_all.sh
bash scripts/run_dashboard.sh
        """.strip(),
        language="bash",
    )
    st.markdown(
        "Dashboard 启动后访问：`http://localhost:8501` "
        "（或服务器外部映射地址，如 AutoDL 自定义服务端口）。"
    )

    st.subheader("输出文件说明")
    st.markdown(
        """
| 目录 | 说明 |
|------|------|
| `results/csv` | Store 实测 CSV、Prefix / Scheduler 评估 CSV |
| `results/logs` | Store 原始日志、评估日志、决策明细 |
| `results/figures` | 带宽 / 延迟 / TTFT / 调度对比图 |
        """.strip()
    )

    st.subheader("真实调用链路")
    st.code(
        """
mooncake_master
  → HTTP metadata server
  → MooncakeDistributedStore
  → store_kv_bench.py
  → mooncake_store_runner.py
  → parse_store_output.py
  → results/csv + results/figures
  → dashboard.py
        """.strip(),
        language="text",
    )


def main() -> None:
    st.set_page_config(
        page_title=PAGE_TITLE,
        page_icon="📦",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.title(PAGE_TITLE)
    st.caption(
        "Non-invasive evaluation suite · Real Mooncake Store benchmark · "
        "Prefix / Scheduler offline strategy evaluation"
    )

    page = st.sidebar.radio(
        "页面导航",
        [
            "Overview",
            "Store Benchmark",
            "Prefix Reuse Evaluation",
            "Retrieval Scheduler Evaluation",
            "Implementation",
        ],
        index=0,
    )
    st.sidebar.markdown("---")
    st.sidebar.markdown(
        f"**数据目录**  \n`{CSV_DIR.relative_to(PROJECT_ROOT)}`  \n"
        f"`{FIG_DIR.relative_to(PROJECT_ROOT)}`"
    )
    st.sidebar.success("Store = 真实实机实测")
    st.sidebar.info("Prefix / Scheduler = 可控 workload 策略评估")

    if page == "Overview":
        overview_page()
    elif page == "Store Benchmark":
        store_page()
    elif page == "Prefix Reuse Evaluation":
        prefix_page()
    elif page == "Retrieval Scheduler Evaluation":
        scheduler_page()
    else:
        implementation_page()


if __name__ == "__main__":
    main()
