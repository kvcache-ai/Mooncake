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

// common.h must precede tent/common/types.h (via bench_config.h), which
// #defines LOCAL_SEGMENT_ID.
#include "common.h"
#include "bench_config.h"

#include <cmath>
#include <sstream>

namespace mooncake {
namespace tent {

namespace {

bool fail(std::string* error, std::string message) {
    *error = std::move(message);
    return false;
}

template <typename T>
bool validateSweepBounds(const char* name, T start, T max, std::string* error) {
    if (start <= 0) {
        return fail(error,
                    std::string("--start_") + name + " must be positive");
    }
    if (start > max) {
        std::ostringstream os;
        os << "--start_" << name << " (" << start << ") must not exceed --max_"
           << name << " (" << max << ")";
        return fail(error, os.str());
    }
    return true;
}

bool validateHugepage(std::string* error) {
    if (XferBenchConfig::backend != "classic" ||
        (XferBenchConfig::xport_type != "shm" &&
         XferBenchConfig::xport_type != "rdma")) {
        return fail(error,
                    "--use_hugepage requires --backend=classic and "
                    "--xport_type=shm or rdma");
    }
    const char* kBadHugepageSize =
        "--hugepage_size must be 2MB, 512MB, 1GB, or a byte "
        "count (2097152, 536870912, 1073741824)";
    if (XferBenchConfig::hugepage_size == static_cast<size_t>(-1)) {
        return fail(error, kBadHugepageSize);
    }
    const size_t hp = XferBenchConfig::hugepage_size == 0
                          ? mooncake::SharedMemoryOptions::kHugepage2MB
                          : XferBenchConfig::hugepage_size;
    if (!mooncake::SharedMemoryOptions::isSupportedHugepageSize(hp)) {
        return fail(error, kBadHugepageSize);
    }
    if (XferBenchConfig::total_buffer_size % hp != 0) {
        return fail(error,
                    "--total_buffer_size must be a multiple of "
                    "hugepage_size");
    }
    return true;
}

bool parseClasses(BenchConfig* config, std::string* error) {
    auto& workload_classes = config->workload_classes;
    auto& qos_classes = config->qos_classes;
    if (!XferBenchConfig::workload_classes_json.empty() &&
        (!XferBenchConfig::qos_classes.empty() ||
         !XferBenchConfig::qos_classes_json.empty())) {
        return fail(error,
                    "Use --workload_classes_json or QoS class flags, not both");
    }
    if (!XferBenchConfig::qos_classes.empty() &&
        !XferBenchConfig::qos_classes_json.empty()) {
        return fail(error,
                    "Use only one of --qos_classes or --qos_classes_json");
    }
    std::string detail;
    if (!XferBenchConfig::workload_classes_json.empty()) {
        if (!parseWorkloadClassesJson(XferBenchConfig::workload_classes_json,
                                      &workload_classes, &detail)) {
            return fail(error, "Invalid --workload_classes_json: " + detail);
        }
        qos_classes = qosClassesFromWorkload(workload_classes);
    } else if (!XferBenchConfig::qos_classes_json.empty()) {
        if (!parseQosClassesJson(XferBenchConfig::qos_classes_json,
                                 &qos_classes, &detail)) {
            return fail(error, "Invalid --qos_classes_json: " + detail);
        }
    } else if (!XferBenchConfig::qos_classes.empty()) {
        if (!parseQosClasses(XferBenchConfig::qos_classes, &qos_classes,
                             &detail)) {
            return fail(error, "Invalid --qos_classes: " + detail);
        }
    }
    if (!qos_classes.empty()) {
        if (XferBenchConfig::start_num_threads !=
            XferBenchConfig::max_num_threads) {
            return fail(
                error,
                "QoS metrics require start_num_threads == max_num_threads");
        }
        if (!validateQosClasses(qos_classes, XferBenchConfig::start_num_threads,
                                &detail)) {
            return fail(error, "Invalid QoS classes: " + detail);
        }
        if (!workload_classes.empty() &&
            !validateWorkloadClasses(
                workload_classes, XferBenchConfig::start_num_threads,
                XferBenchConfig::total_buffer_size, &detail)) {
            return fail(error, "Invalid workload classes: " + detail);
        }
    }
    return true;
}

bool validateDeadlineAndIntent(const BenchConfig& config, std::string* error) {
    const auto& workload_classes = config.workload_classes;
    if (XferBenchConfig::qos_link_capacity_gbps < 0.0 ||
        !std::isfinite(XferBenchConfig::qos_link_capacity_gbps)) {
        return fail(error,
                    "qos_link_capacity_gbps must be finite and non-negative");
    }
    if (XferBenchConfig::deadline_tight_threads < 0 ||
        XferBenchConfig::deadline_tight_threads >
            XferBenchConfig::max_num_threads) {
        return fail(error,
                    "deadline_tight_threads must be in [0, max_num_threads]");
    }
    if (XferBenchConfig::deadline_us > 0 &&
        XferBenchConfig::backend != "tent") {
        return fail(error,
                    "deadline tagging is supported only by the tent backend");
    }
    if (!workload_classes.empty() &&
        XferBenchConfig::tent_intent_type != "unspec") {
        return fail(error,
                    "--tent_intent_type cannot be combined with per-class "
                    "intent_type");
    }
    if (!workload_classes.empty() &&
        (XferBenchConfig::deadline_us != 0 ||
         XferBenchConfig::deadline_tight_threads != 0)) {
        return fail(error,
                    "--deadline_us and --deadline_tight_threads cannot be "
                    "combined with per-class workload deadlines");
    }
    if (!workload_classes.empty() && XferBenchConfig::backend != "tent") {
        for (const auto& workload : workload_classes) {
            if (workload.deadline_us != 0 ||
                workload.intent_type != IntentType::INTENT_UNSPEC) {
                return fail(error,
                            "per-class deadline_us and intent_type require "
                            "the tent backend");
            }
        }
    }
    return true;
}

bool validateSweep(std::string* error) {
    if (!validateSweepBounds("num_threads", XferBenchConfig::start_num_threads,
                             XferBenchConfig::max_num_threads, error) ||
        !validateSweepBounds("block_size", XferBenchConfig::start_block_size,
                             XferBenchConfig::max_block_size, error) ||
        !validateSweepBounds("batch_size", XferBenchConfig::start_batch_size,
                             XferBenchConfig::max_batch_size, error)) {
        return false;
    }
    if (XferBenchConfig::duration <= 0) {
        return fail(error, "--duration must be positive");
    }
    return true;
}

}  // namespace

bool parseOpMode(OpMode* mode, std::string* error) {
    *mode = OpMode{};
    const std::string& op_type = XferBenchConfig::op_type;
    if (XferBenchConfig::check_consistency &&
        (op_type == "write_seed" || op_type == "read_verify")) {
        return fail(error,
                    "--check_consistency cannot be combined with "
                    "write_seed/read_verify");
    }
    if (XferBenchConfig::check_consistency || op_type == "mix") {
        mode->mixed_opcode = true;
    } else if (op_type == "read") {
        mode->opcode = READ;
    } else if (op_type == "write") {
        mode->opcode = WRITE;
    } else if (op_type == "write_seed") {
        mode->opcode = WRITE;
        mode->write_seed = true;
    } else if (op_type == "read_verify") {
        mode->opcode = READ;
        mode->read_verify = true;
    } else {
        return fail(error,
                    "Invalid args: workload only support "
                    "read|write|mix|write_seed|read_verify");
    }
    const std::string& target_access = XferBenchConfig::target_access;
    mode->random_access = target_access == "random";
    if (!mode->random_access && target_access != "fixed") {
        return fail(error,
                    "Invalid args: --target_access only supports "
                    "fixed|random");
    }
    if (!mode->random_access && XferBenchConfig::target_window_size != 0) {
        return fail(error,
                    "--target_window_size requires --target_access=random");
    }
    // The window is also the per-thread stride, and file targets are opened
    // with O_DIRECT, which needs aligned offsets.
    if (XferBenchConfig::target_window_size % 4096 != 0) {
        return fail(error, "--target_window_size must be a multiple of 4096");
    }
    // Seeded data is checked at the offset it was written to, which a moving
    // offset in a separate write_seed run would not revisit.
    if (mode->random_access && (mode->write_seed || mode->read_verify)) {
        return fail(error,
                    "--target_access=" + target_access +
                        " cannot be combined with write_seed/read_verify");
    }
    return true;
}

bool validateConfig(BenchConfig* config, std::string* error) {
    *config = BenchConfig{};
    if (XferBenchConfig::use_hugepage && !validateHugepage(error)) return false;
    if (!parseClasses(config, error)) return false;
    if (!validateDeadlineAndIntent(*config, error)) return false;
    // The target ignores the operation and sweep flags.
    if (XferBenchConfig::target_seg_name.empty()) return true;
    if (!parseOpMode(&config->op_mode, error)) return false;
    return validateSweep(error);
}

uint64_t pickTargetSlot(std::mt19937_64& rng, uint64_t slots, uint64_t prev) {
    if (prev == slots) return rng() % slots;
    const uint64_t slot = rng() % (slots - 1);
    return slot >= prev ? slot + 1 : slot;
}

}  // namespace tent
}  // namespace mooncake
