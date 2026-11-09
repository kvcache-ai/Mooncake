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

#include <gtest/gtest.h>
#include <infiniband/verbs.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "tent/runtime/transfer_engine_impl.h"

// Replace only the hardware boundary. The engines, platform singleton, CPU
// probe, NIC filtering, and topology construction are production code. RDMA
// transports are disabled, so no device is opened for data transfers.
extern "C" {
ibv_device** __wrap_ibv_get_device_list(int* count) {
    static auto devices = [] {
        std::array<ibv_device, 2> result{};
        std::strcpy(result[0].name, "test_mlx5_0");
        std::strcpy(result[1].name, "test_mlx5_1");
        for (auto& device : result) std::strcpy(device.dev_name, "test_uverbs");
        return result;
    }();
    static ibv_device* list[] = {&devices[0], &devices[1], nullptr};
    *count = 2;
    return list;
}

void __wrap_ibv_free_device_list(ibv_device**) {}

const char* __wrap_ibv_get_device_name(ibv_device* device) {
    return device->name;
}

ibv_context* __wrap_ibv_open_device(ibv_device*) {
    static ibv_context context{};
    return &context;
}

int __wrap_ibv_close_device(ibv_context*) { return 0; }

int __wrap_ibv_query_device(ibv_context*, ibv_device_attr* attributes) {
    *attributes = {};
    attributes->phys_port_cnt = 1;
    return 0;
}

int __wrap_ibv_query_port(ibv_context*, uint8_t, ibv_port_attr* attributes) {
    *attributes = {};
    attributes->gid_tbl_len = 1;
    attributes->state = IBV_PORT_ACTIVE;
    return 0;
}

char* __real_realpath(const char*, char*);
int __real_stat(const char*, struct stat*);
int __real_access(const char*, int);

int __wrap_stat(const char* path, struct stat* attributes) {
    if (std::strcmp(path, "/dev/infiniband/test_uverbs") == 0) {
        *attributes = {};
        attributes->st_mode = S_IFCHR | S_IRUSR | S_IWUSR;
        return 0;
    }
    return __real_stat(path, attributes);
}

int __wrap_access(const char* path, int mode) {
    if (std::strcmp(path, "/dev/infiniband/test_uverbs") == 0) return 0;
    return __real_access(path, mode);
}

char* __wrap_realpath(const char* path, char* resolved) {
    constexpr char kPrefix[] = "/sys/class/infiniband/test_mlx5_";
    if (std::strncmp(path, kPrefix, sizeof(kPrefix) - 1) == 0) {
        return std::strcpy(resolved, "/sys/devices/pci0000:00/0000:00:01.0");
    }
    return __real_realpath(path, resolved);
}

// glibc's fortified realpath uses this symbol in Release builds.
char* __wrap___realpath_chk(const char* path, char* resolved, size_t) {
    return __wrap_realpath(path, resolved);
}
}

namespace mooncake::tent {
namespace {

std::shared_ptr<Config> engineConfig() {
    auto config = std::make_shared<Config>();
    config->set("rpc_server_hostname", "127.0.0.1");
    config->set("metadata_type", "p2p");
    config->set("transports/tcp/enable", true);
    config->set("transports/rdma/enable", false);
    config->set("transports/io_uring/enable", false);
    config->set("transports/hp_tcp/enable", false);
    config->set("transports/shm/enable", false);
    return config;
}

void expectNic(const TransferEngineImpl& engine, const std::string& name) {
    ASSERT_TRUE(engine.available());
    auto topology = engine.getLocalTopology();
    ASSERT_NE(topology, nullptr);
    ASSERT_EQ(topology->getNicCount(), 1u);
    EXPECT_EQ(topology->getNicName(0), name);
    const auto* wildcard = topology->getMemEntry(kWildcardLocation);
    ASSERT_NE(wildcard, nullptr);
    EXPECT_EQ(wildcard->device_list[2], std::vector<Topology::NicID>{0});
}

TEST(PlatformConfigTest, EachEngineUsesItsOwnNicFilters) {
    auto first_config = engineConfig();
    first_config->set("topology/rdma_whitelist",
                      std::vector<std::string>{"test_mlx5_0"});
    TransferEngineImpl first(first_config);
    expectNic(first, "test_mlx5_0");

    auto second_config = engineConfig();
    second_config->set("topology/rdma_whitelist",
                       std::vector<std::string>{"test_mlx5_1"});
    // Preserve the existing rule that a nonempty whitelist takes precedence.
    second_config->set("topology/rdma_blacklist",
                       std::vector<std::string>{"test_mlx5_1"});
    TransferEngineImpl second(second_config);
    expectNic(second, "test_mlx5_1");

    auto third_config = engineConfig();
    third_config->set("topology/rdma_blacklist",
                      std::vector<std::string>{"test_mlx5_0"});
    TransferEngineImpl third(third_config);
    expectNic(third, "test_mlx5_1");

    TransferEngineImpl unfiltered(engineConfig());
    ASSERT_TRUE(unfiltered.available());
    EXPECT_EQ(unfiltered.getLocalTopology()->getNicCount(), 2u);
    expectNic(first, "test_mlx5_0");
}

}  // namespace
}  // namespace mooncake::tent
