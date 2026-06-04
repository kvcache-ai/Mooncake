# Logging

Mooncake uses **[Google glog](https://github.com/google/glog)** as the single C++ logging system. `LOG` and `VLOG` are both glog APIs (not separate libraries).

## LOG vs VLOG

| API | When it prints | Use for |
|-----|----------------|---------|
| `LOG(INFO)` | Severity ≥ `FLAGS_minloglevel` | Startup, configuration, rare operational events |
| `LOG(WARNING)` / `LOG(ERROR)` | Same | Problems and failures |
| `VLOG(n)` | Only when `FLAGS_v >= n` | Per-request traces, eviction, hot paths |

Enable verbose output with:

```bash
export MC_LOG_LEVEL=TRACE   # sets min level INFO and FLAGS_v≥1
# or
export MC_VLOG_LEVEL=1
```

## One knob: `MC_LOG_LEVEL`

| Variable | Purpose |
|----------|---------|
| **`MC_LOG_LEVEL`** | **Primary control** for C++ glog, Python `logging`, and ylt easylog (RPC) |
| `MC_VLOG_LEVEL` | Optional glog verbose depth (`FLAGS_v`) |
| `MC_LOG_DIR` | glog log file directory |
| `MC_YLT_LOG_LEVEL` | *Deprecated override* for RPC easylog only; if unset, derived from `MC_LOG_LEVEL` |

Supported `MC_LOG_LEVEL` values: `TRACE`, `INFO`, `WARNING`, `ERROR` (case-insensitive).

## Initialization

All binaries and Python extensions call `InitMooncakeLogging()` (see `mooncake-common/include/mooncake_log.h`). It is safe from multiple extension modules (`engine.so`, `store.so`) because glog is initialized at most once per process:

- `mooncake_master`, `mooncake_client`
- pybind modules `store` and `engine`
- transfer-engine `loadGlobalConfig()` (applies env to glog)

Python packages call `configure_logging_from_env()` from `mooncake.logging_config` on import (see `mooncake-wheel/mooncake/__init__.py`).

## Guidelines

1. Use **glog only** in C++ (`LOG` / `VLOG`). Do not add `printf`, `std::cerr`, or new logging libraries.
2. Default verbosity: **`LOG(INFO)`** for operator-visible events; **`VLOG(1)`** for repetitive paths.
3. Do not log secrets or full payload buffers at INFO.
4. Prefer `MC_LOG_LEVEL` over `MC_YLT_LOG_LEVEL`.
