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

// NOTE: Guard renamed from TOPOLOGY_H to avoid collision with the classic
// transfer-engine's mooncake-transfer-engine/include/topology.h, which also
// uses TOPOLOGY_H.
#ifndef TENT_TOPOLOGY_H
#define TENT_TOPOLOGY_H

#include <glog/logging.h>
#include <netdb.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "tent/common/config.h"
#include "tent/common/status.h"
#include "tent/runtime/amd_location.h"
namespace mooncake {
namespace tent {
class Platform;
class Topology {
   public:
    inline static constexpr size_t DevicePriorityRanks = 3;

    // Keep the existing RDMA/TCP numeric values stable for serialized
    // topologies. UB is a distinct link type and must never be selected as an
    // RDMA verbs device.
    enum NicType {
        NIC_RDMA = 0,
        NIC_TCP = 1,
        NIC_UNKNOWN = 2,
        NIC_UB = 3,
    };
    enum MemType { MEM_HOST, MEM_CUDA, MEM_ROCM, MEM_ASCEND, MEM_UNKNOWN };

    using NicID = int;
    struct NicEntry {
        std::string name;
        std::string pci_bus_id;
        NicType type{NIC_UNKNOWN};
        int numa_node{-1};

        // Hardware-specific discovery metadata is opaque to the common
        // topology layer. Transports own namespaced keys and interpretation.
        std::unordered_map<std::string, std::string> device_attrs{};
    };

    using MemID = int;
    struct MemEntry {
        std::string name;
        std::string pci_bus_id;
        MemType type;
        int numa_node;
        std::vector<NicID> device_list[DevicePriorityRanks];
    };

   public:
    Topology();

    ~Topology();

    bool empty() const;

    void clear();

    // Preserve the original one-argument symbol for source and binary
    // compatibility with callers that do not opt into UB discovery.
    Status discover(const std::vector<Platform*>& platforms);

    Status discover(const std::vector<Platform*>& platforms, bool discover_ub);

    Status parse(const std::string& json_content);

    // Parse classic TE NIC priority matrix:
    // {"cpu:0": [["mlx5_0"], ["mlx5_1"]], ...}
    // preferred → device_list[rank0], avail → device_list[rank1].
    Status parsePriorityMatrix(const std::string& json_content);

    // Auto-detect native {"nics","mems"} JSON vs classic priority matrix.
    Status parseCustomTopology(const std::string& json_content);

    // Load topology from Config (inline matrix / file path) or discover.
    Status loadFromConfig(const Config& conf,
                          const std::vector<Platform*>& platforms);

    std::string toString() const;

    void print() const;

    size_t getNicCount(NicType type = NIC_UNKNOWN) const;

    size_t getMemCount(MemType type = MEM_UNKNOWN) const;

    const NicEntry* getNicEntry(NicID id) const;

    const MemEntry* getMemEntry(MemID id) const;

    const NicEntry* getNicEntry(const std::string& name) const;

    const MemEntry* getMemEntry(const std::string& name) const;

    // True only when both NUMA ids are known and differ. Unknown (-1) is not
    // treated as remote; rank is ignored because probes disagree on placement.
    bool isCrossNuma(const MemEntry& mem, NicID nic_id) const;

    NicID getNicId(const std::string& name) const;

    MemID getMemId(const std::string& name) const;

    std::string getNicName(NicID id) const;

    NicType getNicType(NicID id) const;

    const std::string findNearMem(const std::string& name,
                                  MemType type = MEM_HOST) const;

   public:
    std::vector<NicEntry> nic_list_;
    std::vector<MemEntry> mem_list_;
};

class LocationParser {
   public:
    LocationParser(const std::string& location) {
        size_t colonPos = location.find(':');
        if (location == kWildcardLocation || colonPos == std::string::npos) {
            type_ = kWildcardLocation;
            index_ = -1;
            return;
        }
        std::string type = location.substr(0, colonPos);
        std::string indexStr = location.substr(colonPos + 1);
        try {
            type_ = type;
            index_ = std::stoi(indexStr);
        } catch (const std::exception& e) {
            index_ = -1;
        }
    }

    std::string type() const { return type_; }

    int index() const { return index_; }

   private:
    std::string type_;
    int index_;
};

// Canonical AMD GPU location prefix and alias handling live in
// amd_location.h, shared with the rocm device plugin.

inline std::string makeAmdGpuLocation(int device) {
    return std::string(kAmdGpuLocationType) + ":" + std::to_string(device);
}

// Canonicalize the legacy AMD alias: "rocm:N" becomes "hip:N". All other
// location strings (including malformed ones) are returned unchanged.
// Applied where names enter or leave topology storage so entries stored
// under a legacy matrix key remain reachable via canonical names.
inline std::string canonicalizeLocation(const std::string& location) {
    LocationParser parser(location);
    if (isAmdGpuLocationType(parser.type()) && parser.index() >= 0) {
        return makeAmdGpuLocation(parser.index());
    }
    return location;
}

inline Topology::MemType memTypeFromLocation(const std::string& location) {
    LocationParser parser(location);
    const auto type = parser.type();
    if (type == "cpu") return Topology::MEM_HOST;
    if (type == "cuda" || type == "gpu") return Topology::MEM_CUDA;
    if (isAmdGpuLocationType(type)) return Topology::MEM_ROCM;
    if (type == "ascend") return Topology::MEM_ASCEND;
    return Topology::MEM_UNKNOWN;
}

struct RangeLocation {
    uint64_t start;
    size_t len;
    std::string location;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TOPOLOGY_H
