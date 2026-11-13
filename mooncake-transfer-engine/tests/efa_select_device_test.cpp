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

// EfaTransport::selectDevice() against peer segment descriptors that use the
// disjoint per-NIC partition, where each chunk carries a key on one NIC only.
// No EFA hardware is needed: the descriptor is built by hand.

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <set>
#include <string>
#include <vector>

#include "transport/efa_transport/efa_transport.h"

namespace mooncake {
namespace {

constexpr uint64_t kBufferAddr = 0x7f0000000000ULL;
constexpr uint64_t kBufferLength = 1ULL << 30;
constexpr int kNumNics = 4;
constexpr int kIterations = 1000;

// Four NICs. "cpu:0" prefers rdmap0/rdmap1 and lists rdmap2/rdmap3 as
// available, so the HCA indices resolve to 0..3 in that order.
Transport::SegmentDesc makePeerDesc(int keyed_nic) {
    Transport::SegmentDesc desc;
    desc.name = "peer";
    desc.protocol = "efa";
    for (int i = 0; i < kNumNics; ++i) {
        TransferMetadata::DeviceDesc device;
        device.name = "rdmap" + std::to_string(i);
        desc.devices.push_back(device);
    }
    EXPECT_EQ(desc.topology.parse("{\"cpu:0\": [[\"rdmap0\", \"rdmap1\"], "
                                  "[\"rdmap2\", \"rdmap3\"]]}"),
              0);

    Transport::BufferDesc buffer;
    buffer.name = "cpu:0";
    buffer.addr = kBufferAddr;
    buffer.length = kBufferLength;
    buffer.lkey.assign(kNumNics, 0);
    buffer.rkey.assign(kNumNics, 0);
    if (keyed_nic >= 0) {
        buffer.lkey[keyed_nic] = 0x1000 + keyed_nic;
        buffer.rkey[keyed_nic] = 0x2000 + keyed_nic;
    }
    desc.buffers.push_back(buffer);
    return desc;
}

// A chunk registered on one NIC must be selectable whichever NIC that is,
// including the last available one, which the topology walk used to reach
// only through its random first draw.
TEST(EfaSelectDeviceTest, FindsTheOnlyKeyedNic) {
    for (int keyed_nic = 0; keyed_nic < kNumNics; ++keyed_nic) {
        Transport::SegmentDesc desc = makePeerDesc(keyed_nic);
        for (int i = 0; i < kIterations; ++i) {
            int buffer_id = -1;
            int device_id = -1;
            ASSERT_EQ(EfaTransport::selectDevice(&desc, kBufferAddr + 4096,
                                                 4096, buffer_id, device_id),
                      0)
                << "keyed_nic=" << keyed_nic << " iteration=" << i;
            EXPECT_EQ(buffer_id, 0);
            EXPECT_EQ(device_id, keyed_nic);
        }
    }
}

TEST(EfaSelectDeviceTest, FindsTheOnlyKeyedNicForAnyRetryCount) {
    Transport::SegmentDesc desc = makePeerDesc(kNumNics - 1);
    for (int retry = 0; retry < 3 * kNumNics; ++retry) {
        int buffer_id = -1;
        int device_id = -1;
        ASSERT_EQ(EfaTransport::selectDevice(&desc, kBufferAddr, 4096,
                                             buffer_id, device_id, retry),
                  0)
            << "retry=" << retry;
        EXPECT_EQ(device_id, kNumNics - 1);
    }
}

// A hint that names an HCA pins every attempt of the topology walk to that HCA,
// so a chunk keyed on another NIC is reached only through the fallback scan.
TEST(EfaSelectDeviceTest, FindsTheOnlyKeyedNicWhenTheHintNamesAnotherNic) {
    for (int keyed_nic = 1; keyed_nic < kNumNics; ++keyed_nic) {
        Transport::SegmentDesc desc = makePeerDesc(keyed_nic);
        int buffer_id = -1;
        int device_id = -1;
        ASSERT_EQ(EfaTransport::selectDevice(&desc, kBufferAddr, 4096, "rdmap0",
                                             buffer_id, device_id),
                  0)
            << "keyed_nic=" << keyed_nic;
        EXPECT_EQ(device_id, keyed_nic);
    }
}

TEST(EfaSelectDeviceTest, FailsWhenNoNicHoldsAKey) {
    Transport::SegmentDesc desc = makePeerDesc(-1);
    int buffer_id = -1;
    int device_id = -1;
    EXPECT_EQ(EfaTransport::selectDevice(&desc, kBufferAddr, 4096, buffer_id,
                                         device_id),
              ERR_ADDRESS_NOT_REGISTERED);
}

TEST(EfaSelectDeviceTest, FailsOutsideRegisteredBuffers) {
    Transport::SegmentDesc desc = makePeerDesc(0);
    int buffer_id = -1;
    int device_id = -1;
    EXPECT_EQ(EfaTransport::selectDevice(&desc, kBufferAddr + kBufferLength,
                                         4096, buffer_id, device_id),
              ERR_ADDRESS_NOT_REGISTERED);
}

// A host whose topology lists HCAs besides the EFA devices the transport
// opens, as on p6-b300: two non-EFA InfiniBand HCAs (here ibp0, ibp1) sort
// ahead of the sixteen EFA devices, so the topology numbers rdmap0 as 2.
// "cuda:0" prefers rdmap0/rdmap1 and "cuda:1" prefers rdmap2/rdmap3.
constexpr const char* kMixedTopology =
    "{\"cpu:0\": [[\"ibp0\", \"ibp1\", \"rdmap0\", \"rdmap1\"], "
    "[\"rdmap2\", \"rdmap3\"]], "
    "\"cuda:0\": [[\"rdmap0\", \"rdmap1\"], [\"rdmap2\", \"rdmap3\"]], "
    "\"cuda:1\": [[\"rdmap2\", \"rdmap3\"], [\"rdmap0\", \"rdmap1\"]]}";

std::set<std::string> namesSelected(Topology& topology,
                                    const std::string& location) {
    std::set<std::string> names;
    const auto& hcas = topology.getHcaList();
    for (int i = 0; i < kIterations; ++i) {
        int id = topology.selectDevice(location);
        EXPECT_GE(id, 0);
        if (id < 0 || static_cast<size_t>(id) >= hcas.size()) continue;
        names.insert(hcas[id]);
    }
    return names;
}

// Disabling the non-EFA HCAs keeps their indices, which is what shifted every
// EFA selection: rdmap0 keeps topology index 2, while context_list_ and the
// buffer keys hold it at 0.
TEST(EfaTopologyIndexTest, DisableDeviceKeepsNonEfaIndices) {
    Topology topology;
    ASSERT_EQ(topology.parse(kMixedTopology), 0);
    topology.disableDevice("ibp0");
    topology.disableDevice("ibp1");
    const auto& hcas = topology.getHcaList();
    auto it = std::find(hcas.begin(), hcas.end(), "rdmap0");
    ASSERT_NE(it, hcas.end());
    EXPECT_EQ(it - hcas.begin(), 2);
}

// Indexed over the opened EFA devices, a topology index is a position in the
// opened set, and each GPU still selects only its own NICs.
TEST(EfaTopologyIndexTest, IndexesOverOpenedEfaDevices) {
    Topology mixed;
    ASSERT_EQ(mixed.parse(kMixedTopology), 0);
    const std::vector<std::string> opened = {"rdmap0", "rdmap1", "rdmap2",
                                             "rdmap3"};
    Topology indexed;
    ASSERT_EQ(EfaTransport::indexTopologyOverDevices(mixed, opened, indexed),
              0);
    auto hcas = indexed.getHcaList();
    std::sort(hcas.begin(), hcas.end());
    EXPECT_EQ(hcas, opened);
    EXPECT_EQ(namesSelected(indexed, "cuda:0"),
              (std::set<std::string>{"rdmap0", "rdmap1"}));
    EXPECT_EQ(namesSelected(indexed, "cuda:1"),
              (std::set<std::string>{"rdmap2", "rdmap3"}));
}

// An EFA device that failed to open leaves the numbering too, not just the
// selection.
TEST(EfaTopologyIndexTest, DropsEfaDevicesThatFailedToOpen) {
    Topology mixed;
    ASSERT_EQ(mixed.parse(kMixedTopology), 0);
    Topology indexed;
    ASSERT_EQ(EfaTransport::indexTopologyOverDevices(
                  mixed, {"rdmap0", "rdmap1", "rdmap3"}, indexed),
              0);
    EXPECT_EQ(indexed.getHcaList().size(), 3u);
    EXPECT_EQ(namesSelected(indexed, "cuda:1"),
              (std::set<std::string>{"rdmap3"}));
}

// End to end through selectDevice(): with keys registered only on each GPU's
// topology-local NICs (MC_EFA_NIC_SELECTION=local) and stored per opened
// device, a "cuda:0" buffer is posted on rdmap0/rdmap1 without falling back.
TEST(EfaTopologyIndexTest, SelectsTheGpusOwnNicsWithLocalKeys) {
    Topology mixed;
    ASSERT_EQ(mixed.parse(kMixedTopology), 0);
    Transport::SegmentDesc desc;
    desc.name = "peer";
    desc.protocol = "efa";
    ASSERT_EQ(
        EfaTransport::indexTopologyOverDevices(
            mixed, {"rdmap0", "rdmap1", "rdmap2", "rdmap3"}, desc.topology),
        0);
    const auto& hcas = desc.topology.getHcaList();
    Transport::BufferDesc buffer;
    buffer.name = "cuda:0";
    buffer.addr = kBufferAddr;
    buffer.length = kBufferLength;
    for (const auto& name : hcas) {
        TransferMetadata::DeviceDesc device;
        device.name = name;
        desc.devices.push_back(device);
        const bool local = name == "rdmap0" || name == "rdmap1";
        buffer.lkey.push_back(local ? 0x1000 : 0);
        buffer.rkey.push_back(local ? 0x2000 : 0);
    }
    desc.buffers.push_back(buffer);
    for (int i = 0; i < kIterations; ++i) {
        int buffer_id = -1;
        int device_id = -1;
        ASSERT_EQ(EfaTransport::selectDevice(&desc, kBufferAddr, 4096,
                                             buffer_id, device_id),
                  0);
        ASSERT_GE(device_id, 0);
        ASSERT_LT(static_cast<size_t>(device_id), hcas.size());
        const auto& name = hcas[device_id];
        EXPECT_TRUE(name == "rdmap0" || name == "rdmap1") << name;
    }
}

}  // namespace
}  // namespace mooncake
