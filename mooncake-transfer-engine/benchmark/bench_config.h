// Copyright 2026 KVCache.AI
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

#ifndef TEBENCH_BENCH_CONFIG_H
#define TEBENCH_BENCH_CONFIG_H

#include <cstdint>
#include <random>
#include <string>
#include <vector>

#include "utils.h"
#include "workload_config.h"

namespace mooncake {
namespace tent {

// How each worker issues operations, derived from --op_type,
// --check_consistency and --target_access.
struct OpMode {
    OpCode opcode = READ;
    bool mixed_opcode = false;
    bool write_seed = false;
    bool read_verify = false;
    bool random_access = false;
};

// Flag values parsed and checked once before the run starts.
struct BenchConfig {
    OpMode op_mode;
    std::vector<QosClassConfig> qos_classes;
    std::vector<WorkloadClassConfig> workload_classes;
};

// Parses the operation mode from XferBenchConfig. Returns false and sets
// *error if the flags are invalid.
bool parseOpMode(OpMode* mode, std::string* error);

// Validates all XferBenchConfig flags and fills *config. Initiator-only flags
// are checked only when --target_seg_name is set. Returns false and sets
// *error on the first invalid flag.
bool validateConfig(BenchConfig* config, std::string* error);

// A random slot in [0, slots) other than prev, so consecutive operations never
// rewrite the same blocks. prev == slots means no previous slot. slots >= 2.
uint64_t pickTargetSlot(std::mt19937_64& rng, uint64_t slots, uint64_t prev);

}  // namespace tent
}  // namespace mooncake

#endif  // TEBENCH_BENCH_CONFIG_H
