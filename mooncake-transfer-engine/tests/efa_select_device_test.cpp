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

#include <cstdint>
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

}  // namespace
}  // namespace mooncake
