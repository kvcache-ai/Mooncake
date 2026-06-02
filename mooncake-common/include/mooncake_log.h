// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <glog/logging.h>

namespace mooncake {

/**
 * Mooncake C++ services use Google glog as the primary application logger.
 *
 * LOG(severity)  — Standard severity-based logging (INFO, WARNING, ERROR,
 *                  FATAL). A message is emitted when its severity is >=
 *                  FLAGS_minloglevel (controlled by MC_LOG_LEVEL).
 *
 * VLOG(level)    — Verbose / debug logging on the same glog backend. A message
 *                  is emitted only when FLAGS_v >= level (enable with
 *                  MC_LOG_LEVEL=TRACE or MC_VLOG_LEVEL=N). Use VLOG for
 *                  high-volume or per-request traces; use LOG for lifecycle,
 *                  errors, and rare operational events.
 *
 * yalantinglibs coro_rpc uses easylog (MC_YLT_LOG_LEVEL). InitMooncakeLogging
 * configures both from the same environment when possible.
 *
 * Environment variables:
 *   MC_LOG_LEVEL   — TRACE | INFO | WARNING | ERROR (glog min level; TRACE
 *                    also sets FLAGS_v>=1 for VLOG(1))
 *   MC_VLOG_LEVEL  — Optional integer FLAGS_v (verbose depth for VLOG(n))
 *   MC_LOG_DIR     — glog log directory (falls back to stderr if invalid)
 *   MC_YLT_LOG_LEVEL — easylog level for RPC stack (trace/debug/info/warn/...)
 *                    If unset, derived from MC_LOG_LEVEL.
 */

// Call once per process after parsing gflags (if any). Idempotent.
void InitMooncakeLogging(const char* argv0);

// Apply MC_LOG_LEVEL / MC_VLOG_LEVEL / MC_LOG_DIR without calling
// InitGoogleLogging. Returns true when MC_LOG_LEVEL=TRACE (transfer-engine
// trace mode).
bool ApplyGlogEnvironment(bool* enable_transfer_trace = nullptr);

// Configure ylt easylog only (safe to call without glog init).
void InitYltLogLevelFromEnv();

// Apply a log directory (e.g. from gflags --log_dir) after InitMooncakeLogging.
void SetGlogLogDir(const char* log_dir_path);

}  // namespace mooncake
