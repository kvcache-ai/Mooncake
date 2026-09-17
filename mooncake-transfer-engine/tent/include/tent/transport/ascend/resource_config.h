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

#ifndef TENT_ASCEND_RESOURCE_CONFIG_H_
#define TENT_ASCEND_RESOURCE_CONFIG_H_

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "tent/common/config.h"
#include "tent/runtime/segment.h"

namespace mooncake {
namespace tent {

constexpr int32_t kAscendDefaultTransferTimeoutMs = 10000;
constexpr int32_t kAscendDefaultBasePort = 20000;
constexpr int32_t kAscendTimeoutDisconnectMs = 1000;
constexpr const char* kDefaultLocalCommRes = R"({"version":"1.3"})";
constexpr const char* kHixlNameAttr = "hixl_name";
constexpr const char* kHixlNamesAttr = "hixl_names";

struct AscendDirectOptions {
    int32_t transfer_timeout_ms = kAscendDefaultTransferTimeoutMs;
    int32_t base_port = kAscendDefaultBasePort;
    bool agent_mode = false;
    bool store_te_init = false;
    bool use_fabric_mem = false;
    bool roce_mode = false;
    std::string rdma_tc;
    std::string rdma_sl;
    std::string local_comm_res = kDefaultLocalCommRes;
    std::string global_resource_config;
};

bool ParseEnvEnabled(const char* name);

AscendDirectOptions LoadAscendDirectOptions(
    const std::shared_ptr<Config>& conf);

std::map<std::string, std::string> BuildHixlInitOptions(
    const AscendDirectOptions& options);

// RoCE/HCCS dataplane listen port. Distinct from the HIXL engine TCP name.
uint16_t HixlRoceListenPort(uint16_t hixl_port);

// Copy HIXL init options and set comm_resource_config.listen_port.
std::map<std::string, std::string> WithHixlListenPort(
    const std::map<std::string, std::string>& init, uint16_t listen_port);

std::string HostIpFromSegmentName(const std::string& segment_name);
std::string MakeHixlEngineName(const std::string& host_ip, uint16_t port);
uint16_t FindHixlListenPort(int32_t base_port, int32_t device_id);

std::vector<std::string> ParseHixlNames(const MemorySegmentDesc& detail);
std::string ResolveRemoteHixlName(const MemorySegmentDesc& detail,
                                  uint64_t dest_addr);

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_ASCEND_RESOURCE_CONFIG_H_
