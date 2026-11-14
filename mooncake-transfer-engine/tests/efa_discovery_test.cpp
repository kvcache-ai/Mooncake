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

// EFA NICs listed through libfabric must be the ones libibverbs reports, with
// the same PCI address and NUMA node, and topology discovery must keep finding
// them. Skips on hosts without libfabric or EFA devices.

#include <gtest/gtest.h>
#include <infiniband/verbs.h>
#include <limits.h>
#include <libgen.h>
#include <stdlib.h>

#include <algorithm>
#include <fstream>
#include <map>
#include <string>
#include <vector>

#include "topology.h"
#include "transport/efa_transport/efa_discovery.h"

namespace mooncake {
namespace {

struct VerbsNic {
    std::string pci_bus_id;
    int numa_node = -1;
};

// EFA devices as libibverbs and sysfs see them, the way the topology code
// resolved them before it used libfabric.
std::map<std::string, VerbsNic> verbsEfaNics() {
    std::map<std::string, VerbsNic> nics;
    int num = 0;
    struct ibv_device** list = ibv_get_device_list(&num);
    for (int i = 0; list && i < num; ++i) {
        std::string name = ibv_get_device_name(list[i]);
        char path[PATH_MAX + 64];
        char resolved[PATH_MAX];
        snprintf(path, sizeof(path), "/sys/class/infiniband/%s/device/driver",
                 name.c_str());
        if (!realpath(path, resolved) ||
            std::string(basename(resolved)) != "efa") {
            continue;
        }
        snprintf(path, sizeof(path), "/sys/class/infiniband/%s/../..",
                 name.c_str());
        if (!realpath(path, resolved)) continue;
        VerbsNic nic;
        nic.pci_bus_id = basename(resolved);
        std::ifstream(std::string(resolved) + "/numa_node") >> nic.numa_node;
        nics[name] = nic;
    }
    if (list) ibv_free_device_list(list);
    return nics;
}

TEST(EfaDiscoveryTest, LibfabricMatchesLibibverbs) {
    std::vector<EfaNic> nics;
    std::string error;
    if (!listEfaNics(nics, &error)) {
        GTEST_SKIP() << "libfabric unavailable: " << error;
    }
    auto verbs = verbsEfaNics();
    if (nics.empty() && verbs.empty()) GTEST_SKIP() << "no EFA devices";

    ASSERT_EQ(nics.size(), verbs.size());
    for (const auto& nic : nics) {
        auto it = verbs.find(nic.name);
        ASSERT_NE(it, verbs.end()) << nic.name << " is not a verbs EFA device";
        EXPECT_EQ(nic.pci_bus_id, it->second.pci_bus_id) << nic.name;
        EXPECT_EQ(nic.numa_node, it->second.numa_node) << nic.name;
    }
}

TEST(EfaDiscoveryTest, TopologyListsEveryEfaNic) {
    std::vector<EfaNic> nics;
    std::string error;
    if (!listEfaNics(nics, &error) || nics.empty()) {
        GTEST_SKIP() << "no EFA devices through libfabric";
    }
    Topology topology;
    ASSERT_EQ(topology.discover({}, /*exclude_efa=*/false), 0);
    const auto& hcas = topology.getHcaList();
    for (const auto& nic : nics) {
        EXPECT_NE(std::find(hcas.begin(), hcas.end(), nic.name), hcas.end())
            << nic.name;
    }

    // RDMA discovery still drops EFA NICs.
    Topology rdma;
    ASSERT_EQ(rdma.discover({}, /*exclude_efa=*/true), 0);
    for (const auto& nic : nics) {
        const auto& rdma_hcas = rdma.getHcaList();
        EXPECT_EQ(std::find(rdma_hcas.begin(), rdma_hcas.end(), nic.name),
                  rdma_hcas.end())
            << nic.name;
    }
}

TEST(EfaDiscoveryTest, FilterSelectsOneNic) {
    std::vector<EfaNic> nics;
    std::string error;
    if (!listEfaNics(nics, &error) || nics.empty()) {
        GTEST_SKIP() << "no EFA devices through libfabric";
    }
    Topology topology;
    ASSERT_EQ(topology.discover({nics.front().name}, /*exclude_efa=*/false), 0);
    EXPECT_EQ(topology.getHcaList(),
              std::vector<std::string>{nics.front().name});
}

TEST(EfaDiscoveryTest, ReportsLoadedLibibverbs) {
    // This binary links libibverbs, so the path must be found.
    EXPECT_NE(loadedLibibverbsPath().find("libibverbs.so"), std::string::npos);
}

}  // namespace
}  // namespace mooncake
