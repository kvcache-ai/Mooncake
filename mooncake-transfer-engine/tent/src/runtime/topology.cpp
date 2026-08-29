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

#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "tent/common/status.h"
#include "tent/runtime/platform.h"
#include "tent/common/utils/random.h"
#include "tent/thirdparty/nlohmann/json.h"
#ifdef USE_UB
#include "tent/transport/ub/topology_attrs.h"
#endif

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
        if (!nic.device_attrs.empty()) {
            nj["device_attrs"] = nic.device_attrs;
        }
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
    return discover(platforms, false);
}

Status Topology::discover(const std::vector<Platform*>& platforms,
                          bool discover_ub) {
    clear();
    for (auto& entry : platforms) {
        CHECK_STATUS(entry->probe(nic_list_, mem_list_));
    }
#ifdef USE_UB
    // UB discovery is intentionally adapter-backed instead of inferring UB
    // devices from verbs/sysfs names. One topology NIC is emitted per EID and
    // carries both the globally serialized identity and the native URMA name.
    auto adapter = discover_ub ? ub::createDefaultUrmaAdapter() : nullptr;
    if (adapter && adapter->available()) {
        auto status = adapter->initialize();
        if (status.ok()) {
            std::vector<ub::DeviceInfo> devices;
            status = adapter->discoverDevices(devices);
            if (status.ok()) {
                std::unordered_map<std::string, int> native_device_indices;
                for (const auto& device : devices) {
                    auto [native_it, inserted] = native_device_indices.emplace(
                        device.native_device_name,
                        static_cast<int>(native_device_indices.size()));
                    (void)inserted;

                    int numa_node = -1;
                    std::string pci_bus_id;
                    if (!device.native_device_path.empty()) {
                        std::error_code error;
                        const auto device_path = std::filesystem::canonical(
                            std::filesystem::path(device.native_device_path) /
                                "device",
                            error);
                        if (!error) {
                            pci_bus_id = device_path.filename().string();
                            std::ifstream(device_path / "numa_node") >>
                                numa_node;
                        }
                    }

                    const NicID nic_id = static_cast<NicID>(nic_list_.size());
                    NicEntry nic{.name = device.topology_name,
                                 .pci_bus_id = std::move(pci_bus_id),
                                 .type = NIC_UB,
                                 .numa_node = numa_node};
                    ub::encodeTopologyDeviceAttributes(
                        device, native_it->second, nic.device_attrs);
                    nic_list_.push_back(std::move(nic));

                    for (auto& memory : mem_list_) {
                        const size_t rank =
                            numa_node >= 0 && memory.numa_node == numa_node
                                ? 0
                                : DevicePriorityRanks - 1;
                        memory.device_list[rank].push_back(nic_id);
                    }
                }
            } else {
                LOG(WARNING) << "Unable to discover optional UB devices: "
                             << status.ToString();
            }
            auto shutdown_status = adapter->shutdown();
            if (!shutdown_status.ok()) {
                LOG(WARNING) << "Unable to release UB discovery runtime: "
                             << shutdown_status.ToString();
            }
        } else {
            LOG(WARNING) << "Unable to initialize optional UB discovery: "
                         << status.ToString();
        }
    }
#endif
    (void)discover_ub;
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
                if (item.contains("device_attrs")) {
                    nic.device_attrs =
                        item.at("device_attrs")
                            .get<
                                std::unordered_map<std::string, std::string>>();
                }
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

namespace {

Topology::NicID ensureNic(
    Topology& topo, const std::string& name,
    std::unordered_map<std::string, Topology::NicID>* ids) {
    auto it = ids->find(name);
    if (it != ids->end()) return it->second;
    Topology::NicID id = static_cast<Topology::NicID>(topo.nic_list_.size());
    Topology::NicEntry nic;
    nic.name = name;
    nic.type = Topology::NIC_RDMA;
    nic.numa_node = -1;
    topo.nic_list_.push_back(std::move(nic));
    (*ids)[name] = id;
    return id;
}

}  // namespace

Status Topology::parsePriorityMatrix(const std::string& json_content) {
    try {
        clear();
        if (json_content.empty()) {
            return Status::MalformedJson("empty priority matrix" LOC_MARK);
        }
        nlohmann::json root = nlohmann::json::parse(json_content);
        if (!root.is_object()) {
            return Status::MalformedJson(
                "priority matrix must be a JSON object" LOC_MARK);
        }

        std::unordered_map<std::string, NicID> nic_ids;
        std::vector<NicID> all_preferred;
        std::vector<NicID> all_avail;
        std::unordered_set<NicID> seen_preferred;
        std::unordered_set<NicID> seen_avail;
        // Canonical mem names, to reject configs that specify the same
        // device twice via the "rocm:N" alias and the canonical "hip:N".
        std::unordered_set<std::string> seen_mem_names;

        for (auto it = root.begin(); it != root.end(); ++it) {
            const auto& value = it.value();
            if (!value.is_array() || value.size() != 2 ||
                !value[0].is_array() || !value[1].is_array()) {
                return Status::MalformedJson(
                    "each priority matrix entry must be "
                    "[[preferred...],[avail...]]" LOC_MARK);
            }

            MemEntry mem;
            // Store canonical names: a legacy "rocm:N" key becomes "hip:N"
            // so runtime lookups with canonical names match.
            mem.name = canonicalizeLocation(it.key());
            if (!seen_mem_names.insert(mem.name).second) {
                return Status::MalformedJson(
                    "duplicate location key \"" + mem.name +
                    "\" (the rocm:N alias and hip:N key refer to the same "
                    "device)" LOC_MARK);
            }
            mem.pci_bus_id = "";
            mem.type = memTypeFromLocation(mem.name);
            mem.numa_node = -1;

            for (const auto& hca : value[0]) {
                if (!hca.is_string()) {
                    return Status::MalformedJson(
                        "HCA names must be strings" LOC_MARK);
                }
                NicID id = ensureNic(*this, hca.get<std::string>(), &nic_ids);
                mem.device_list[0].push_back(id);
                if (seen_preferred.insert(id).second) {
                    all_preferred.push_back(id);
                }
            }
            for (const auto& hca : value[1]) {
                if (!hca.is_string()) {
                    return Status::MalformedJson(
                        "HCA names must be strings" LOC_MARK);
                }
                NicID id = ensureNic(*this, hca.get<std::string>(), &nic_ids);
                mem.device_list[1].push_back(id);
                if (seen_avail.insert(id).second) {
                    all_avail.push_back(id);
                }
            }
            mem_list_.push_back(std::move(mem));
        }

        // Wildcard entry used when memory location is unknown.
        MemEntry wildcard;
        wildcard.name = kWildcardLocation;
        wildcard.pci_bus_id = "";
        wildcard.type = MEM_UNKNOWN;
        wildcard.numa_node = -1;
        wildcard.device_list[0] = all_preferred;
        wildcard.device_list[1] = all_avail;
        mem_list_.push_back(std::move(wildcard));
    } catch (std::exception& e) {
        clear();
        return Status::MalformedJson(std::string(e.what()) + LOC_MARK);
    }
    return Status::OK();
}

Status Topology::parseCustomTopology(const std::string& json_content) {
    nlohmann::json j;
    try {
        j = nlohmann::json::parse(json_content);
    } catch (std::exception& e) {
        return Status::MalformedJson(std::string(e.what()) + LOC_MARK);
    }
    if (j.is_object() && (j.contains("nics") || j.contains("mems"))) {
        return parse(json_content);
    }
    return parsePriorityMatrix(json_content);
}

Status Topology::loadFromConfig(const Config& conf,
                                const std::vector<Platform*>& platforms) {
    if (conf.contains("topology/priority_matrix")) {
        std::string matrix_json;
        if (conf.dumpSubtree("topology/priority_matrix", &matrix_json)) {
            auto status = parsePriorityMatrix(matrix_json);
            if (status.ok()) {
                LOG(INFO) << "Using custom NIC priority matrix from config";
                return Status::OK();
            }
            LOG(WARNING) << "Failed to parse topology/priority_matrix: "
                         << status.ToString()
                         << ", falling back to auto-discover";
            return discover(platforms);
        }
    }

    auto path = conf.get("topology/custom_json_path", std::string());
    if (!path.empty()) {
        LOG(INFO) << "Using custom topology from: " << path;
        std::ifstream file(path);
        if (!file.is_open()) {
            LOG(WARNING) << "Failed to load custom topology from " << path
                         << ", falling back to auto-detect.";
            return discover(platforms);
        }
        std::stringstream buffer;
        buffer << file.rdbuf();
        std::string content = buffer.str();
        if (content.empty()) {
            LOG(WARNING) << "Failed to load custom topology from " << path
                         << ", falling back to auto-detect.";
            return discover(platforms);
        }
        auto status = parseCustomTopology(content);
        if (status.ok()) return Status::OK();
        LOG(WARNING) << "Failed to parse custom topology from " << path << ": "
                     << status.ToString() << ", falling back to auto-detect.";
        return discover(platforms);
    }

    return discover(platforms);
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
    const auto canonical = canonicalizeLocation(name);
    for (size_t i = 0; i < mem_list_.size(); ++i) {
        if (mem_list_[i].name == canonical) return &mem_list_[i];
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
    const auto canonical = canonicalizeLocation(name);
    for (size_t i = 0; i < mem_list_.size(); ++i) {
        if (mem_list_[i].name == canonical) return (MemID)i;
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
