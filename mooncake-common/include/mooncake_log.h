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
 * Mooncake standard logging: Google glog for all C++ application code.
 *
 *   LOG(INFO|WARNING|ERROR|FATAL)  — severity channel (MC_LOG_LEVEL)
 *   VLOG(n)                        — verbose channel (MC_LOG_LEVEL=TRACE or MC_VLOG_LEVEL)
 *
 * yalantinglibs coro_rpc emits through easylog internally; InitMooncakeLogging()
 * maps MC_LOG_LEVEL to easylog so RPC and application logs share one env knob.
 *
 * Environment (use MC_LOG_LEVEL as the single control):
 *   MC_LOG_LEVEL    — TRACE | INFO | WARNING | ERROR
 *   MC_VLOG_LEVEL   — optional FLAGS_v for VLOG depth
 *   MC_LOG_DIR      — glog file directory
 *   MC_YLT_LOG_LEVEL — optional override for RPC easylog only (deprecated)
 */

void InitMooncakeLogging(const char* argv0);

bool ApplyGlogEnvironment(bool* enable_transfer_trace = nullptr);

void InitYltLogLevelFromEnv();

void SetGlogLogDir(const char* log_dir_path);

}  // namespace mooncake
