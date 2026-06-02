# Logging

Mooncake uses a single primary logging stack for C++ application code: [Google glog](https://github.com/google/glog).

## LOG vs VLOG (both are glog)

| API | When it prints | Typical use |
|-----|----------------|-------------|
| `LOG(INFO)` | When severity ≥ `FLAGS_minloglevel` (set via `MC_LOG_LEVEL`) | Startup, rare state changes, successful one-off operations |
| `LOG(WARNING)` / `LOG(ERROR)` | Same threshold | Recoverable issues, failures |
| `VLOG(n)` | Only when `FLAGS_v >= n` | Per-request traces, hot-path diagnostics, eviction details |

`LOG` and `VLOG` are not different libraries. `VLOG` is glog's verbose channel on the same backend. The recent store log-noise change moved high-volume `LOG(INFO)` lines to `VLOG(1)` so default deployments stay quiet while `MC_LOG_LEVEL=TRACE` or `MC_VLOG_LEVEL=1` still enables them.

## Environment variables

| Variable | Affects | Values |
|----------|---------|--------|
| `MC_LOG_LEVEL` | glog `FLAGS_minloglevel`; `TRACE` also sets `FLAGS_v≥1` | `TRACE`, `INFO`, `WARNING`, `ERROR` |
| `MC_VLOG_LEVEL` | glog `FLAGS_v` (verbose depth) | Non-negative integer |
| `MC_LOG_DIR` | glog file output directory | Path |
| `MC_YLT_LOG_LEVEL` | yalantinglibs **easylog** (coro_rpc stack) | `trace`, `debug`, `info`, `warn`, `error`, `critical` |

If `MC_YLT_LOG_LEVEL` is unset, it is derived from `MC_LOG_LEVEL` so RPC and application logs stay aligned.

## Initialization

Call once per process entry point (after `gflags::ParseCommandLineFlags` if used):

```cpp
#include "mooncake_log.h"

mooncake::InitMooncakeLogging(argv[0]);
```

Used by `mooncake_master`, `mooncake_client`, and transfer-engine config loading (`ApplyGlogEnvironment`).

## Python

Python bindings and `mooncake-integration` use the standard library `logging` module. Set levels via normal Python APIs or framework env vars (e.g. `VLLM_LOGGING_LEVEL`); C++ glog is controlled separately via `MC_LOG_LEVEL`.

## Guidelines for contributors

1. Prefer **glog** (`LOG` / `VLOG`) in C++; do not add new `printf` / `std::cerr` in production paths.
2. Use **`LOG(INFO)`** for messages operators should see at default verbosity.
3. Use **`VLOG(1)`** (or higher) for per-operation or high-frequency messages.
4. Use **`LOG(ERROR)`** / **`LOG(WARNING)`** for failures; do not downgrade errors to `VLOG`.
5. RPC framework noise: tune with `MC_YLT_LOG_LEVEL`, not glog.
