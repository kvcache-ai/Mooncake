// Copyright 2025 KVCache.AI
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

#include "tent/runtime/transport_selector.h"
#include "tent/runtime/transport.h"
#include "tent/runtime/platform.h"
#include "tent/thirdparty/nlohmann/json.h"

#include <glog/logging.h>

namespace mooncake {
namespace tent {

// Transport type name mapping
static const std::unordered_map<std::string, TransportType> kTransportNameMap =
    {
        {"rdma", RDMA},           {"tcp", TCP},     {"shm", SHM},
        {"nvlink", NVLINK},       {"gds", GDS},     {"io_uring", IOURING},
        {"ascend", AscendDirect}, {"mnnvl", MNNVL},
};

static const std::unordered_map<TransportType, std::string>
    kTransportTypeNames = {
        {RDMA, "rdma"},           {TCP, "tcp"},     {SHM, "shm"},
        {NVLINK, "nvlink"},       {GDS, "gds"},     {IOURING, "io_uring"},
        {AscendDirect, "ascend"}, {MNNVL, "mnnvl"}, {UNSPEC, "unspec"},
};

// Memory type name mapping for pattern matching
static const std::string kMemoryTypeCpu = "cpu";
static const std::string kMemoryTypeCuda = "cuda";
static const std::string kMemoryTypeNpu = "npu";
static const std::string kMemoryTypeWildcard = "*";

std::string TransportSelector::transportTypeName(TransportType type) {
    auto it = kTransportTypeNames.find(type);
    if (it != kTransportTypeNames.end()) {
        return it->second;
    }
    return "unknown";
}

TransportType TransportSelector::parseTransportType(const std::string& str) {
    auto it = kTransportNameMap.find(str);
    if (it != kTransportNameMap.end()) {
        return it->second;
    }
    LOG(WARNING) << "Unknown transport type: " << str;
    return UNSPEC;
}

std::vector<SelectionPolicy> TransportSelector::getDefaultPolicies() {
    // Default policies that match the original hardcoded behavior
    return {
        {
            "file_storage",
            SegmentType::File,
            std::nullopt,   // same_machine doesn't matter for file
            std::nullopt,   // local_memory_pattern
            std::nullopt,   // remote_memory_pattern
            std::nullopt,   // min_size
            std::nullopt,   // max_size
            std::nullopt,   // priority
            {},             // rdma_device_ids (empty = all devices)
            {GDS, IOURING}  // File segment priority (original: GDS → IOURING)
        },
        {
            "memory_default",
            SegmentType::Memory,
            std::nullopt,  // any machine
            std::nullopt,  // any local memory
            std::nullopt,  // any remote memory
            std::nullopt,  // any size
            std::nullopt,  // min_priority
            {},            // rdma_device_ids (empty = all devices)
            {}  // Empty priority = use buffer_transports order (original
                // behavior)
        },
    };
}

void TransportSelector::loadPolicies() {
    policies_.clear();

    // Try to load from configuration - read from "policy" subtree
    auto policies_array = config_->getArray<json>("policy");

    if (policies_array.empty()) {
        LOG(INFO)
            << "No 'policy' configured, using default transport selection";
        policies_ = getDefaultPolicies();
        return;
    }

    LOG(INFO) << "Loading transport selection policies from config";

    for (const auto& policy_json : policies_array) {
        if (!policy_json.is_object()) continue;

        SelectionPolicy policy;
        policy.name = policy_json.value("name", "unnamed");

        // Parse segment_type
        std::string segment_type_str = policy_json.value("segment_type", "");
        if (segment_type_str == "file") {
            policy.segment_type = SegmentType::File;
        } else if (segment_type_str == "memory") {
            policy.segment_type = SegmentType::Memory;
        } else {
            LOG(WARNING) << "Invalid segment_type in policy " << policy.name;
            continue;
        }

        // Parse same_machine (optional)
        if (policy_json.contains("same_machine")) {
            policy.same_machine = policy_json["same_machine"].get<bool>();
        } else {
            policy.same_machine = std::nullopt;
        }

        // Parse memory patterns (optional)
        if (policy_json.contains("local_memory")) {
            policy.local_memory_pattern =
                policy_json["local_memory"].get<std::string>();
        }
        if (policy_json.contains("remote_memory")) {
            policy.remote_memory_pattern =
                policy_json["remote_memory"].get<std::string>();
        }

        // Parse size filters (optional)
        if (policy_json.contains("min_size")) {
            policy.min_size = policy_json["min_size"].get<uint64_t>();
        }
        if (policy_json.contains("max_size")) {
            policy.max_size = policy_json["max_size"].get<uint64_t>();
        }

        // Parse priority filter (optional)
        if (policy_json.contains("priority")) {
            policy.priority = policy_json["priority"].get<int>();
        } else {
            policy.priority = std::nullopt;
        }

        // Parse devices (optional)
        if (policy_json.contains("devices")) {
            for (const auto& device_name : policy_json["devices"]) {
                if (device_name.is_string()) {
                    policy.devices.push_back(device_name.get<std::string>());
                }
            }
        }

        // Parse transports list
        policy.transports.clear();
        if (policy_json.contains("transports")) {
            for (const auto& transport_str : policy_json["transports"]) {
                if (!transport_str.is_string()) continue;
                TransportType type =
                    parseTransportType(transport_str.get<std::string>());
                if (type != UNSPEC) {
                    policy.transports.push_back(type);
                }
            }
        }

        policies_.push_back(std::move(policy));
        LOG(INFO) << "Loaded transport policy: " << policy.name
                  << " (segment_type=" << segment_type_str
                  << ", transports_count=" << policy.transports.size() << ")";
    }
}

TransportSelector::TransportSelector(std::shared_ptr<Config> config)
    : config_(config) {
    loadPolicies();
}

bool TransportSelector::matchesMemoryPattern(const std::string& pattern,
                                             MemoryType type) const {
    if (pattern == kMemoryTypeWildcard) {
        return true;
    }

    // Convert MemoryType to string for comparison
    std::string type_str;
    switch (type) {
        case MTYPE_CPU:
            type_str = kMemoryTypeCpu;
            break;
        case MTYPE_CUDA:
            type_str = kMemoryTypeCuda;
            break;
        case MTYPE_ASCEND:
            type_str = kMemoryTypeNpu;
            break;  // "npu" for ascend
        case MTYPE_HIP:
            type_str = "hip";
            break;
        case MTYPE_MUSA:
            type_str = "musa";
            break;
        case MTYPE_MACA:
            type_str = "maca";
            break;
        default:
            type_str = "unknown";
            break;
    }

    return pattern == type_str;
}

bool TransportSelector::matchesPolicy(const SelectionPolicy& policy,
                                      const SelectionContext& context) const {
    // Check segment type
    if (policy.segment_type != context.segment_type) {
        return false;
    }

    // Check same_machine constraint
    if (policy.same_machine.has_value()) {
        if (policy.same_machine.value() != context.same_machine) {
            return false;
        }
    }

    // Check local memory pattern
    if (policy.local_memory_pattern.has_value()) {
        if (!matchesMemoryPattern(policy.local_memory_pattern.value(),
                                  context.local_memory_type)) {
            return false;
        }
    }

    // Check remote memory pattern
    if (policy.remote_memory_pattern.has_value()) {
        if (!matchesMemoryPattern(policy.remote_memory_pattern.value(),
                                  context.remote_memory_type)) {
            return false;
        }
    }

    // Check size constraints
    if (policy.min_size.has_value()) {
        if (context.transfer_size < policy.min_size.value()) {
            return false;
        }
    }
    if (policy.max_size.has_value()) {
        if (context.transfer_size > policy.max_size.value()) {
            return false;
        }
    }

    // Check priority constraint: exact match required
    if (policy.priority.has_value()) {
        if (context.priority_level != policy.priority.value()) {
            return false;
        }
    }

    return true;
}

bool TransportSelector::isTransportAvailable(
    TransportType type, const SelectionContext& context,
    const std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>&
        available_transports) const {
    // Check if transport type is valid
    if (type < 0 || type >= kSupportedTransportTypes) {
        return false;
    }

    // Check if transport exists
    const auto& transport = available_transports[type];
    if (!transport) {
        return false;
    }

    // Special constraints
    if ((type == NVLINK || type == SHM) && !context.same_machine) {
        return false;  // NVLINK and SHM only work on same machine
    }

    // Check transport capabilities for the memory type combination
    MemoryType remote_mtype = context.remote_memory_type;
    if (context.segment_type == SegmentType::File) {
        // For file segments, always use CPU as remote (file is on host)
        remote_mtype = MTYPE_CPU;
    }

    const auto& caps = transport->capabilities();

    // Helper to check if memory type is GPU/NPU
    auto is_gpu = [](MemoryType t) {
        return t == MTYPE_CUDA || t == MTYPE_ASCEND || t == MTYPE_HIP ||
               t == MTYPE_MUSA || t == MTYPE_MACA;
    };

    // CPU to CPU
    if (context.local_memory_type == MTYPE_CPU && remote_mtype == MTYPE_CPU)
        return caps.dram_to_dram || caps.dram_to_file;

    // GPU/NPU to GPU/NPU (same type)
    if (is_gpu(context.local_memory_type) && is_gpu(remote_mtype))
        return caps.gpu_to_gpu;

    // CPU to GPU/NPU
    if (context.local_memory_type == MTYPE_CPU && is_gpu(remote_mtype))
        return caps.dram_to_gpu;

    // GPU/NPU to CPU
    if (is_gpu(context.local_memory_type) && remote_mtype == MTYPE_CPU)
        return caps.gpu_to_dram || caps.gpu_to_file;

    return false;
}

SelectionResult TransportSelector::select(
    const SelectionContext& context,
    const std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>&
        available_transports,
    int priority_offset) {
    SelectionResult result;

    // Find the first matching policy (JSON order wins)
    const SelectionPolicy* matching_policy = nullptr;
    for (const auto& policy : policies_) {
        if (matchesPolicy(policy, context)) {
            matching_policy = &policy;
            break;  // First match wins
        }
    }

    if (!matching_policy) {
        LOG(WARNING) << "No matching transport policy for segment_type="
                     << (context.segment_type == SegmentType::File ? "file"
                                                                   : "memory")
                     << ", size=" << context.transfer_size
                     << ", priority_level=" << context.priority_level;
        return result;  // UNSPEC, all devices
    }

    // Convert device names to mask
    result.device_mask = ~0ULL;  // Default: all devices
    if (!matching_policy->devices.empty() && topology_) {
        result.device_mask = 0;
        for (const auto& name : matching_policy->devices) {
            int dev_id = topology_->getNicId(name);
            if (dev_id >= 0 && dev_id < 64) {
                result.device_mask |= (1ULL << dev_id);
            } else {
                LOG(WARNING) << "RDMA device not found or ID >= 64: " << name;
            }
        }
        if (result.device_mask == 0) {
            result.device_mask = ~0ULL;  // Fallback to all if none found
        }
    }

    // If policy has transports list, use it
    if (!matching_policy->transports.empty()) {
        int priority_index = priority_offset;
        for (size_t i = 0; i < matching_policy->transports.size(); ++i) {
            TransportType type = matching_policy->transports[i];
            if (isTransportAvailable(type, context, available_transports)) {
                if (priority_index-- <= 0) {
                    result.transport = type;
                    VLOG(1) << "Selected transport " << transportTypeName(type)
                            << " for policy " << matching_policy->name
                            << ", device_mask=0x" << std::hex
                            << result.device_mask << std::dec;
                    return result;
                }
            }
        }
        return result;  // UNSPEC
    }

    // Otherwise, use buffer_transports order (original behavior)
    if (context.buffer_transports) {
        for (auto type : *context.buffer_transports) {
            if (isTransportAvailable(type, context, available_transports)) {
                if (priority_offset-- <= 0) {
                    result.transport = type;
                    VLOG(1) << "Selected transport " << transportTypeName(type)
                            << " from buffer_transports"
                            << ", device_mask=0x" << std::hex
                            << result.device_mask << std::dec;
                    return result;
                }
            }
        }
    }

    return result;
}

}  // namespace tent
}  // namespace mooncake
