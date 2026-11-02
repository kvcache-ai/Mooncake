// Copyright 2026 Mooncake Authors
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

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <hip/hip_runtime.h>
#include <json/json.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstring>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "transfer_engine.h"
#include "transfer_metadata.h"
#include "transport/hip_transport/hip_share_handle.h"
#include "transport/hip_transport/ualink_topology.h"
#include "transport/transport.h"

using namespace mooncake;

namespace {

hip_share::Chunk makeChunk(uint64_t offset, uint64_t size, uint8_t fill) {
    hip_share::Chunk chunk;
    chunk.offset = offset;
    chunk.size = size;
    chunk.handle.assign(64, fill);
    return chunk;
}

}  // namespace

TEST(HipShareHandleTest, TypedRoundTrip) {
    hip_share::Descriptor desc;
    desc.kind = hip_share::Kind::kFabric;
    desc.owner_pid = 1234;
    desc.owner_device = 3;
    desc.ppod_id = "a674683c-423e-4398-91fe-4f132ece26fa";
    desc.vpod_id = 16;
    desc.chunks.push_back(makeChunk(0, 2ull << 30, 0x11));
    desc.chunks.push_back(makeChunk(2ull << 30, 2ull << 30, 0x22));
    desc.chunks.push_back(makeChunk(4ull << 30, 1ull << 21, 0x33));

    auto bytes = hip_share::encode(desc);
    EXPECT_NE(bytes.size(), hip_share::kLegacyIpcBytes);
    EXPECT_NE(bytes.size(), hip_share::kLegacyPosixFdBytes);

    hip_share::Descriptor out;
    ASSERT_TRUE(hip_share::decode(bytes, out));
    EXPECT_FALSE(out.legacy);
    EXPECT_EQ(out.kind, hip_share::Kind::kFabric);
    EXPECT_EQ(out.owner_pid, 1234);
    EXPECT_EQ(out.owner_device, 3);
    EXPECT_EQ(out.ppod_id, desc.ppod_id);
    EXPECT_EQ(out.vpod_id, 16u);
    ASSERT_EQ(out.chunks.size(), 3u);
    for (size_t i = 0; i < 3; ++i) {
        EXPECT_EQ(out.chunks[i].offset, desc.chunks[i].offset);
        EXPECT_EQ(out.chunks[i].size, desc.chunks[i].size);
        EXPECT_EQ(out.chunks[i].handle, desc.chunks[i].handle);
    }

    hip_share::Descriptor from_hex;
    ASSERT_TRUE(hip_share::decodeFromShmName(hip_share::encodeToShmName(desc),
                                             from_hex));
    EXPECT_EQ(from_hex.chunks.size(), 3u);
}

TEST(HipShareHandleTest, LegacyLayouts) {
    std::vector<uint8_t> ipc(64, 0xab);
    hip_share::Descriptor out;
    ASSERT_TRUE(hip_share::decode(ipc, out));
    EXPECT_TRUE(out.legacy);
    EXPECT_EQ(out.kind, hip_share::Kind::kIpc);
    ASSERT_EQ(out.chunks.size(), 1u);
    EXPECT_EQ(out.chunks[0].handle, ipc);

    int32_t fd_pid[2] = {17, 4242};
    std::vector<uint8_t> posix(sizeof(fd_pid));
    memcpy(posix.data(), fd_pid, sizeof(fd_pid));
    ASSERT_TRUE(hip_share::decode(posix, out));
    EXPECT_TRUE(out.legacy);
    EXPECT_EQ(out.kind, hip_share::Kind::kPosixFd);
    EXPECT_EQ(out.owner_pid, 4242);
}

TEST(HipShareHandleTest, RejectsMalformed) {
    hip_share::Descriptor desc;
    desc.kind = hip_share::Kind::kFabric;
    desc.chunks.push_back(makeChunk(0, 4096, 1));
    desc.chunks.push_back(makeChunk(4096, 4096, 2));
    auto good = hip_share::encode(desc);
    hip_share::Descriptor out;

    auto bad_magic = good;
    bad_magic[0] = 'X';
    EXPECT_FALSE(hip_share::decode(bad_magic, out));

    auto truncated = good;
    truncated.pop_back();
    EXPECT_FALSE(hip_share::decode(truncated, out));

    auto trailing = good;
    trailing.push_back(0);
    EXPECT_FALSE(hip_share::decode(trailing, out));

    desc.chunks[1].offset = 8192;  // gap between chunks
    EXPECT_FALSE(hip_share::decode(hip_share::encode(desc), out));

    EXPECT_FALSE(hip_share::decodeFromShmName("XYZ", out));
    EXPECT_FALSE(hip_share::decode(nullptr, 0, out));
}

class UalinkTopologyTest : public ::testing::Test {
   protected:
    void SetUp() override {
        char tmpl[] = "/tmp/mc_ualink_XXXXXX";
        ASSERT_NE(mkdtemp(tmpl), nullptr);
        root_ = tmpl;
        unsetenv("MC_UALOE_VPOD_ID");
        unsetenv("MC_UALOE_DISABLE");
    }

    void TearDown() override {
        unsetenv("MC_UALOE_VPOD_ID");
        unsetenv("MC_UALOE_DISABLE");
        std::string cmd = "rm -rf '" + root_ + "'";
        ASSERT_EQ(system(cmd.c_str()), 0);
    }

    void writeDevice(const std::string& bdf, const std::string& state,
                     const std::string& vpod, const std::string& ppod) {
        std::string dir = root_ + "/" + bdf;
        ASSERT_EQ(mkdir(dir.c_str(), 0755), 0);
        dir += "/ualink";
        ASSERT_EQ(mkdir(dir.c_str(), 0755), 0);
        std::ofstream(dir + "/accel_state") << state << "\n";
        std::ofstream(dir + "/vpod_id") << vpod << "\n";
        std::ofstream(dir + "/ppod_id") << ppod << "\n";
        std::ofstream(dir + "/vpod_size") << "24\n";
        std::ofstream(dir + "/link_type") << "UALoE\n";
    }

    std::string root_;
};

TEST_F(UalinkTopologyTest, ReadsActiveDevice) {
    writeDevice("0001:01:00.0", "active", "16", "ppod-a");
    auto info = ualink::readVpodFromSysfs("0001:01:00.0", root_);
    ASSERT_TRUE(info.valid);
    EXPECT_EQ(info.vpod_id, 16u);
    EXPECT_EQ(info.ppod_id, "ppod-a");
    EXPECT_EQ(info.vpod_size, 24u);
    EXPECT_EQ(info.link_type, "UALoE");
    EXPECT_EQ(info.source, "sysfs");

    // hipDeviceGetPCIBusId may report uppercase hex.
    writeDevice("0002:0a:00.0", "active", "7", "ppod-a");
    EXPECT_TRUE(ualink::readVpodFromSysfs("0002:0A:00.0", root_).valid);
}

TEST_F(UalinkTopologyTest, RejectsInactiveOrMissing) {
    writeDevice("0001:01:00.0", "inactive", "16", "ppod-a");
    EXPECT_FALSE(ualink::readVpodFromSysfs("0001:01:00.0", root_).valid);
    EXPECT_FALSE(ualink::readVpodFromSysfs("0009:01:00.0", root_).valid);
    writeDevice("0003:01:00.0", "active", "garbage", "ppod-a");
    EXPECT_FALSE(ualink::readVpodFromSysfs("0003:01:00.0", root_).valid);
}

TEST_F(UalinkTopologyTest, EnvironmentOverrides) {
    writeDevice("0001:01:00.0", "active", "16", "ppod-a");
    setenv("MC_UALOE_VPOD_ID", "5", 1);
    auto info = ualink::resolveVpod("0001:01:00.0", root_);
    ASSERT_TRUE(info.valid);
    EXPECT_EQ(info.vpod_id, 5u);
    EXPECT_TRUE(info.ppod_id.empty());
    EXPECT_EQ(info.source, "env");

    setenv("MC_UALOE_DISABLE", "1", 1);
    EXPECT_TRUE(ualink::fabricDisabledByEnv());
    EXPECT_FALSE(ualink::resolveVpod("0001:01:00.0", root_).valid);
}

TEST(UalinkSameVpodTest, Rules) {
    ualink::VpodInfo local;
    local.valid = true;
    local.vpod_id = 16;
    local.ppod_id = "ppod-a";
    EXPECT_TRUE(ualink::sameVpod(local, "ppod-a", 16));
    EXPECT_FALSE(ualink::sameVpod(local, "ppod-b", 16));
    EXPECT_FALSE(ualink::sameVpod(local, "ppod-a", 17));
    EXPECT_TRUE(ualink::sameVpod(local, "", 16));
    EXPECT_FALSE(ualink::sameVpod(local, "ppod-a", ualink::kNoVpod));
    local.valid = false;
    EXPECT_FALSE(ualink::sameVpod(local, "ppod-a", 16));
}

namespace {

struct FabricBuffer {
    void* ptr = nullptr;
    size_t size = 0;
    std::vector<hipMemGenericAllocationHandle_t> handles;
};

// One VA range backed by `chunks` separate fabric-exportable allocations.
bool allocFabric(int device, size_t chunk_size, int chunks, FabricBuffer& buf) {
    if (hipSetDevice(device) != hipSuccess) return false;
    hipMemAllocationProp prop = {};
    prop.type = hipMemAllocationTypePinned;
    prop.location.type = hipMemLocationTypeDevice;
    prop.location.id = device;
    prop.requestedHandleTypes = hipMemHandleTypeFabric;
    size_t gran = 0;
    if (hipMemGetAllocationGranularity(
            &gran, &prop, hipMemAllocationGranularityMinimum) != hipSuccess)
        return false;
    chunk_size = (chunk_size + gran - 1) / gran * gran;
    buf.size = chunk_size * chunks;
    if (hipMemAddressReserve(&buf.ptr, buf.size, gran, nullptr, 0) !=
        hipSuccess)
        return false;
    for (int i = 0; i < chunks; ++i) {
        hipMemGenericAllocationHandle_t handle;
        if (hipMemCreate(&handle, chunk_size, &prop, 0) != hipSuccess)
            return false;
        if (hipMemMap((char*)buf.ptr + i * chunk_size, chunk_size, 0, handle,
                      0) != hipSuccess)
            return false;
        buf.handles.push_back(handle);
    }
    hipMemAccessDesc access = {};
    access.location = prop.location;
    access.flags = hipMemAccessFlagsProtReadWrite;
    return hipMemSetAccess(buf.ptr, buf.size, &access, 1) == hipSuccess;
}

void freeFabric(FabricBuffer& buf) {
    if (!buf.ptr) return;
    size_t chunk = buf.size / buf.handles.size();
    for (size_t i = 0; i < buf.handles.size(); ++i) {
        (void)hipMemUnmap((char*)buf.ptr + i * chunk, chunk);
        (void)hipMemRelease(buf.handles[i]);
    }
    (void)hipMemAddressFree(buf.ptr, buf.size);
    buf = FabricBuffer();
}

bool fabricAvailable() {
    int count = 0;
    if (hipGetDeviceCount(&count) != hipSuccess || count < 2) return false;
    int supported = 0;
    if (hipDeviceGetAttribute(&supported,
                              hipDeviceAttributeHandleTypeFabricSupported,
                              0) != hipSuccess)
        return false;
    return supported != 0;
}

int transfer(TransferEngine& engine, SegmentHandle target,
             TransferRequest::OpCode opcode,
             const std::vector<std::tuple<void*, uint64_t, size_t>>& items) {
    auto batch = engine.allocateBatchID(items.size());
    std::vector<TransferRequest> requests;
    for (auto& [local, remote, length] : items) {
        TransferRequest req;
        req.opcode = opcode;
        req.source = local;
        req.target_id = target;
        req.target_offset = remote;
        req.length = length;
        requests.push_back(req);
    }
    Status s = engine.submitTransfer(batch, requests);
    if (!s.ok()) {
        (void)engine.freeBatchID(batch);
        return -1;
    }
    for (size_t i = 0; i < items.size(); ++i) {
        TransferStatus status;
        do {
            if (!engine.getTransferStatus(batch, i, status).ok()) return -1;
        } while (status.s == TransferStatusEnum::WAITING ||
                 status.s == TransferStatusEnum::PENDING);
        if (status.s != TransferStatusEnum::COMPLETED) {
            (void)engine.freeBatchID(batch);
            return -1;
        }
    }
    (void)engine.freeBatchID(batch);
    return 0;
}

std::vector<uint8_t> pattern(size_t size, uint8_t seed) {
    std::vector<uint8_t> data(size);
    for (size_t i = 0; i < size; ++i)
        data[i] = static_cast<uint8_t>(seed * 131 + i * 7 + (i >> 11));
    return data;
}

}  // namespace

// Two engines in one process stand in for prefill and decode: the "remote"
// side exports its segment, the "local" side imports it (EXTERNAL metadata),
// binds, and moves data through imported fabric handles.
class HipFabricExternalTest : public ::testing::TestWithParam<const char*> {};

TEST_P(HipFabricExternalTest, ImportBindTransferRemove) {
    if (!fabricAvailable()) GTEST_SKIP() << "No HIP fabric handle support";

    constexpr size_t kChunk = 64ull << 20;
    constexpr int kChunks = 3;
    FabricBuffer remote_buf, local_buf;
    ASSERT_TRUE(allocFabric(1, kChunk, kChunks, remote_buf));
    ASSERT_TRUE(allocFabric(0, kChunk * kChunks, 1, local_buf));
    const size_t size = remote_buf.size;
    auto remote_data = pattern(size, 3);
    // Each buffer is only accessible from its own device.
    ASSERT_EQ(hipSetDevice(1), hipSuccess);
    ASSERT_EQ(hipMemcpy(remote_buf.ptr, remote_data.data(), size,
                        hipMemcpyHostToDevice),
              hipSuccess);
    ASSERT_EQ(hipSetDevice(0), hipSuccess);

    TransferEngine remote(true), local(true);
    ASSERT_EQ(remote.init(EXTERNAL_METADATA, "127.0.0.1"), 0);
    ASSERT_EQ(local.init(EXTERNAL_METADATA, GetParam()), 0);
    ASSERT_EQ(remote.registerLocalMemory(remote_buf.ptr, size, "hip:1"), 0);
    ASSERT_EQ(local.registerLocalMemory(local_buf.ptr, size, "hip:0"), 0);

    std::string blob;
    ASSERT_EQ(remote.exportLocalSegment(blob), 0);
    Json::Value blob_json;
    ASSERT_TRUE(Json::Reader().parse(blob, blob_json));
    const std::string remote_name = blob_json["name"].asString();
    SegmentHandle sid = local.importSegment("", blob);
    ASSERT_GE(static_cast<int64_t>(sid), 0);
    ASSERT_EQ(local.bindSegment(sid, 0), 0);
    // Importing the same blob again keeps the segment id.
    EXPECT_EQ(local.importSegment("", blob), sid);

    // READ across both inner chunk boundaries, in one batch.
    const uint64_t base = (uint64_t)remote_buf.ptr;
    std::vector<std::tuple<void*, uint64_t, size_t>> reads;
    for (int i = 1; i < kChunks; ++i) {
        size_t off = i * kChunk - (1 << 20) - 5;
        reads.emplace_back((char*)local_buf.ptr + off, base + off,
                           (2 << 20) + 11);
    }
    for (int i = 0; i < 8; ++i) {
        size_t off = i * (5 << 20) + 13;
        reads.emplace_back((char*)local_buf.ptr + off, base + off, 4096 + i);
    }
    ASSERT_EQ(hipMemset(local_buf.ptr, 0, size), hipSuccess);
    ASSERT_EQ(transfer(local, sid, TransferRequest::READ, reads), 0);
    std::vector<uint8_t> got(size);
    ASSERT_EQ(hipMemcpy(got.data(), local_buf.ptr, size, hipMemcpyDeviceToHost),
              hipSuccess);
    for (auto& [lptr, raddr, length] : reads) {
        size_t off = raddr - base;
        ASSERT_EQ(memcmp(got.data() + off, remote_data.data() + off, length), 0)
            << "READ mismatch at offset " << off;
    }

    // Small single READ takes the hipMemcpyAsync path.
    ASSERT_EQ(transfer(local, sid, TransferRequest::READ,
                       {{local_buf.ptr, base + 100, 256}}),
              0);

    // WRITE the whole buffer back with a new pattern.
    auto local_data = pattern(size, 9);
    ASSERT_EQ(hipMemcpy(local_buf.ptr, local_data.data(), size,
                        hipMemcpyHostToDevice),
              hipSuccess);
    ASSERT_EQ(transfer(local, sid, TransferRequest::WRITE,
                       {{local_buf.ptr, base, size}}),
              0);
    ASSERT_EQ(hipSetDevice(1), hipSuccess);
    ASSERT_EQ(
        hipMemcpy(got.data(), remote_buf.ptr, size, hipMemcpyDeviceToHost),
        hipSuccess);
    ASSERT_EQ(hipSetDevice(0), hipSuccess);
    EXPECT_EQ(memcmp(got.data(), local_data.data(), size), 0);

    // Out-of-range target is rejected.
    EXPECT_NE(transfer(local, sid, TransferRequest::READ,
                       {{local_buf.ptr, base + size - 16, 64}}),
              0);

    ASSERT_EQ(local.removeSegment(remote_name), 0);
    EXPECT_NE(local.removeSegment(remote_name), 0);
    EXPECT_EQ(static_cast<int64_t>(local.openSegment(remote_name)), -1);

    ASSERT_EQ(local.unregisterLocalMemory(local_buf.ptr), 0);
    ASSERT_EQ(remote.unregisterLocalMemory(remote_buf.ptr), 0);
    freeFabric(remote_buf);
    freeFabric(local_buf);
}

// 127.0.0.2 makes the peer look like another host, so routing must accept
// the buffer through its fabric handle and vPOD rather than host locality.
INSTANTIATE_TEST_SUITE_P(Locality, HipFabricExternalTest,
                         ::testing::Values("127.0.0.1", "127.0.0.2"));

TEST(HipFabricExternalTest2, UnknownSegmentIsNotFetched) {
    if (!fabricAvailable()) GTEST_SKIP() << "No HIP fabric handle support";
    TransferEngine engine(true);
    ASSERT_EQ(engine.init(EXTERNAL_METADATA, "127.0.0.1"), 0);
    EXPECT_EQ(static_cast<int64_t>(engine.openSegment("127.0.0.1:1")), -1);
    EXPECT_LT(static_cast<int64_t>(engine.importSegment("x", "not json")), 0);
}
