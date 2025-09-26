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

#ifndef TOPOLOGY_H
#define TOPOLOGY_H

#include <glog/logging.h>
#include <netdb.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "v1/common/config.h"
#include "v1/common/status.h"
namespace mooncake {
namespace v1 {
class Platform;
class Topology {
   public:
    const static size_t DevicePriorityRanks = 3;

    enum NicType { NIC_RDMA, NIC_TCP, NIC_UNKNOWN };
    enum MemType { MEM_HOST, MEM_CUDA, MEM_ROCM, MEM_ASCEND, MEM_UNKNOWN };

    using NicID = int;
    struct NicEntry {
        std::string name;
        std::string pci_bus_id;
        NicType type;
        int numa_node;
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

    Status discover(const std::vector<Platform *> &platforms);

    Status parse(const std::string &json_content);

    std::string toString() const;

    void print() const;

    size_t getNicCount(NicType type = NIC_UNKNOWN) const;

    size_t getMemCount(MemType type = MEM_UNKNOWN) const;

    const NicEntry *getNicEntry(NicID id) const;

    const MemEntry *getMemEntry(MemID id) const;

    const NicEntry *getNicEntry(const std::string &name) const;

    const MemEntry *getMemEntry(const std::string &name) const;

    NicID getNicId(const std::string &name) const;

    MemID getMemId(const std::string &name) const;

    std::string getNicName(NicID id) const;

    NicType getNicType(NicID id) const;

   public:
    std::vector<NicEntry> nic_list_;
    std::vector<MemEntry> mem_list_;
};

}  // namespace v1
}  // namespace mooncake

#endif  // TOPOLOGY_H