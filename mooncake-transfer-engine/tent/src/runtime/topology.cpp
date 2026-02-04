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

#include "tent/runtime/topology.h"

#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>
#include <unordered_set>

#include "tent/common/status.h"
#include "tent/runtime/platform.h"
#include "tent/common/utils/random.h"
#include "tent/thirdparty/nlohmann/json.h"

namespace mooncake {
namespace tent {
Topology::Topology() {}

Topology::~Topology() {}

bool Topology::empty() const { return nic_list_.empty() && mem_list_.empty(); }

void Topology::clear() {
    nic_list_.clear();
    mem_list_.clear();
}

std::string Topology::toString() const {
    nlohmann::json j;
    j["nics"] = nlohmann::json::array();
    for (const auto& nic : nic_list_) {
        nlohmann::json nj;
        nj["name"] = nic.name;
        nj["pci_bus_id"] = nic.pci_bus_id;
        nj["type"] = nic.type;
        nj["numa_node"] = nic.numa_node;
        j["nics"].push_back(nj);
    }

    j["mems"] = nlohmann::json::array();
    for (const auto& mem : mem_list_) {
        nlohmann::json mj;
        mj["name"] = mem.name;
        mj["pci_bus_id"] = mem.pci_bus_id;
        mj["type"] = mem.type;
        mj["numa_node"] = mem.numa_node;
        mj["device_list"] = nlohmann::json::object();
        for (size_t rank = 0; rank < DevicePriorityRanks; ++rank) {
            mj["device_list"]["rank" + std::to_string(rank)] =
                mem.device_list[rank];
        }
        j["mems"].push_back(mj);
    }

    return j.dump(4);
}

void Topology::print() const {
    LOG(INFO) << "NIC: ";
    int id = 0;
    for (auto& entry : nic_list_) {
        LOG(INFO) << "[" << id << "] " << entry.name << " (type " << entry.type
                  << ") " << entry.pci_bus_id << " on NUMA " << entry.numa_node;
        id++;
    }

    LOG(INFO) << "MEMORY: ";
    id = 0;
    for (auto& entry : mem_list_) {
        LOG(INFO) << "[" << id << "] " << entry.name << " (type " << entry.type
                  << ") " << entry.pci_bus_id << " on NUMA " << entry.numa_node;
        for (size_t rank = 0; rank < DevicePriorityRanks; rank++) {
            std::stringstream ss;
            if (entry.device_list[rank].empty()) continue;
            ss << "    Tier " << rank << ": ";
            for (auto& id : entry.device_list[rank]) {
                ss << nic_list_[id].name << " ";
            }
            LOG(INFO) << ss.str();
        }
        id++;
    }
}

Status Topology::discover(const std::vector<Platform*>& platforms) {
    clear();
    for (auto& entry : platforms) {
        CHECK_STATUS(entry->probe(nic_list_, mem_list_));
    }
    return Status::OK();
}

Status Topology::parse(const std::string& json_content) {
    try {
        clear();
        nlohmann::json j = nlohmann::json::parse(json_content);
        if (j.contains("nics")) {
            for (auto& item : j["nics"]) {
                NicEntry nic;
                nic.name = item.value("name", "");
                nic.pci_bus_id = item.value("pci_bus_id", "");
                nic.type =
                    static_cast<NicType>(item.value("type", NIC_UNKNOWN));
                nic.numa_node = item.value("numa_node", -1);
                nic_list_.push_back(nic);
            }
        }

        if (j.contains("mems")) {
            for (auto& item : j["mems"]) {
                MemEntry mem;
                mem.name = item.value("name", "");
                mem.pci_bus_id = item.value("pci_bus_id", "");
                mem.type =
                    static_cast<MemType>(item.value("type", MEM_UNKNOWN));
                mem.numa_node = item.value("numa_node", -1);
                if (item.contains("device_list")) {
                    for (size_t rank = 0; rank < DevicePriorityRanks; ++rank) {
                        std::string key = "rank" + std::to_string(rank);
                        if (item["device_list"].contains(key)) {
                            mem.device_list[rank] =
                                item["device_list"][key]
                                    .get<std::vector<NicID>>();
                        }
                    }
                }
                mem_list_.push_back(mem);
            }
        }
    } catch (std::exception& e) {
        return Status::MalformedJson(std::string(e.what()) + LOC_MARK);
    }
    return Status::OK();
}

size_t Topology::getNicCount(NicType type) const {
    if (type == NIC_UNKNOWN) return nic_list_.size();
    size_t count = 0;
    for (auto& entry : nic_list_) {
        if (entry.type == type) count++;
    }
    return count;
}

size_t Topology::getMemCount(MemType type) const {
    if (type == MEM_UNKNOWN) return mem_list_.size();
    size_t count = 0;
    for (auto& entry : mem_list_) {
        if (entry.type == type) count++;
    }
    return count;
}

const Topology::NicEntry* Topology::getNicEntry(NicID id) const {
    if (id < 0 || id >= (int)nic_list_.size()) return nullptr;
    return &nic_list_[id];
}

const Topology::MemEntry* Topology::getMemEntry(MemID id) const {
    if (id < 0 || id >= (int)mem_list_.size()) return nullptr;
    return &mem_list_[id];
}

const Topology::NicEntry* Topology::getNicEntry(const std::string& name) const {
    for (size_t i = 0; i < nic_list_.size(); ++i) {
        if (nic_list_[i].name == name) return &nic_list_[i];
    }
    return nullptr;
}

const Topology::MemEntry* Topology::getMemEntry(const std::string& name) const {
    for (size_t i = 0; i < mem_list_.size(); ++i) {
        if (mem_list_[i].name == name) return &mem_list_[i];
    }
    return nullptr;
}

Topology::NicID Topology::getNicId(const std::string& name) const {
    for (size_t i = 0; i < nic_list_.size(); ++i) {
        if (nic_list_[i].name == name) return (NicID)i;
    }
    return -1;
}

Topology::MemID Topology::getMemId(const std::string& name) const {
    for (size_t i = 0; i < mem_list_.size(); ++i) {
        if (mem_list_[i].name == name) return (MemID)i;
    }
    return -1;
}

std::string Topology::getNicName(NicID id) const {
    auto entry = getNicEntry(id);
    return entry ? entry->name : "";
}

Topology::NicType Topology::getNicType(NicID id) const {
    auto entry = getNicEntry(id);
    return entry ? entry->type : NIC_UNKNOWN;
}

const std::string Topology::findNearMem(const std::string& name,
                                        MemType type) const {
    const auto* src = getMemEntry(name);
    if (!src) return "";
    int numa = src->numa_node;
    for (const auto& mem : mem_list_) {
        if (mem.type == type && mem.numa_node == numa) {
            return mem.name;
        }
    }
    for (const auto& mem : mem_list_) {
        if (mem.type == type) {
            return mem.name;
        }
    }
    return "";
}

}  // namespace tent
}  // namespace mooncake
