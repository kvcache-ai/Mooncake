// Copyright 2025 KVCache.AI
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

#include <glog/logging.h>

#include <algorithm>
#include <mutex>
#include <string>
#include <vector>

#include "tent/common/transport_slots.h"
#include "tent/runtime/segment.h"
#include "transport_wire_codec.h"

namespace mooncake {
namespace tent {
namespace {

// json(desc).dump() of makeGoldenDesc() produced by main 559f83cb7, before
// plugin transports existed. Built-in-only descriptors must keep it.
const char kGoldenJson[] =
    R"({"detail":{"buffers":[{"addr":4096,"internal":true,"length":4096,)"
    R"("location":"cpu:0","mnnvl_handle":"h","regions":[{"location":"cpu:0",)"
    R"("size":4096}],"rkey":[1,2],"shm_path":"/shm","transport_attrs":[[1,)"
    R"("rdma-attr"]],"transports":[1,7]},{"addr":8192,"internal":false,)"
    R"("length":64,"location":"cpu:1","mnnvl_handle":"","regions":[],)"
    R"("rkey":[],"shm_path":"","transport_attrs":[],"transports":[7]}],)"
    R"("device_attrs":{"mlx5_0":"x"},"devices":[{"gid":"fe80::1","lid":7,)"
    R"("name":"mlx5_0","transport_attrs":[[1,"dev-attr"]]}],"topology":)"
    R"("{\n    \"mems\": [],\n    \"nics\": []\n}","transport_attrs":[[1,)"
    R"("seg-attr"]]},"machine_id":"machine-a","name":"127.0.0.1:12345",)"
    R"("rdma_server_name":"10.0.0.1","rpc_server_addr":"127.0.0.1:12345",)"
    R"("type":"memory"})";

SegmentDesc makeGoldenDesc() {
    SegmentDesc desc;
    desc.name = "127.0.0.1:12345";
    desc.type = SegmentType::Memory;
    desc.machine_id = "machine-a";
    desc.rpc_server_addr = "127.0.0.1:12345";
    desc.rdma_server_name = "10.0.0.1";
    MemorySegmentDesc m;
    BufferDesc b;
    b.addr = 0x1000;
    b.length = 4096;
    b.location = "cpu:0";
    b.transports = {RDMA, TCP};
    b.regions = {Region{4096, "cpu:0"}};
    b.transport_attrs = {{RDMA, "rdma-attr"}};
    b.rkey = {1, 2};
    b.shm_path = "/shm";
    b.mnnvl_handle = "h";
    b.internal = true;
    BufferDesc b2;
    b2.addr = 0x2000;
    b2.length = 64;
    b2.location = "cpu:1";
    b2.transports = {TCP};
    m.buffers = {b, b2};
    m.device_attrs = {{"mlx5_0", "x"}};
    DeviceDesc d;
    d.name = "mlx5_0";
    d.lid = 7;
    d.gid = "fe80::1";
    d.transport_attrs = {{RDMA, "dev-attr"}};
    m.devices = {d};
    m.transport_attrs = {{static_cast<int>(RDMA), "seg-attr"}};
    desc.detail = m;
    return desc;
}

// The BufferDesc serializer as it was before plugin transports, i.e. what a
// peer built from an older tree uses to read our descriptors.
struct LegacyBufferDesc {
    uint64_t addr;
    uint64_t length;
    std::string location;
    std::vector<TransportType> transports;
    std::vector<Region> regions;
    std::unordered_map<TransportType, std::string> transport_attrs;
    std::vector<uint32_t> rkey;
    std::string shm_path;
    std::string mnnvl_handle;
    bool internal{false};
    NLOHMANN_DEFINE_TYPE_INTRUSIVE(LegacyBufferDesc, addr, length, location,
                                   transports, regions, transport_attrs, rkey,
                                   shm_path, mnnvl_handle, internal);
};

const BufferDesc& firstBuffer(const SegmentDesc& desc) {
    return desc.getMemory().buffers.front();
}

class TransportWireCodecTest : public ::testing::Test {
   protected:
    void SetUp() override {
        resetPluginTransportSlotsForTest();
        ASSERT_EQ(assignPluginTransportSlot("loopback", 0, loopback_),
                  SlotAssignResult::kAssigned);
        ASSERT_EQ(assignPluginTransportSlot("vendorx", 0, vendorx_),
                  SlotAssignResult::kAssigned);
    }
    void TearDown() override { resetPluginTransportSlotsForTest(); }

    SegmentDesc roundTrip(const SegmentDesc& desc) {
        auto wire = segmentDescToWireJson(desc).dump();
        auto parsed = json::parse(wire).get<SegmentDesc>();
        decodeSegmentDescFromWire(parsed);
        return parsed;
    }

    TransportType loopback_ = UNSPEC;
    TransportType vendorx_ = UNSPEC;
};

TEST_F(TransportWireCodecTest, BuiltinOnlyDescriptorMatchesGolden) {
    const auto desc = makeGoldenDesc();
    EXPECT_EQ(json(desc).dump(), kGoldenJson);
    EXPECT_EQ(segmentDescToWireJson(desc).dump(), kGoldenJson);
}

TEST_F(TransportWireCodecTest, GoldenParsesAndDecodesUnchanged) {
    auto parsed = json::parse(kGoldenJson).get<SegmentDesc>();
    decodeSegmentDescFromWire(parsed);
    EXPECT_EQ(json(parsed).dump(), kGoldenJson);
}

TEST_F(TransportWireCodecTest, PluginSlotsTravelByName) {
    auto desc = makeGoldenDesc();
    auto& memory = std::get<MemorySegmentDesc>(desc.detail);
    memory.buffers[0].transports.push_back(loopback_);
    memory.buffers[0].transport_attrs[loopback_] = "lb-attr";
    memory.buffers[1].transports.push_back(vendorx_);
    memory.transport_attrs[static_cast<int>(vendorx_)] = "vx-seg";

    const json wire = segmentDescToWireJson(desc);
    const auto& wire_buffer = wire["detail"]["buffers"][0];
    EXPECT_EQ(wire_buffer["transports"], json::parse("[1,7]"));
    EXPECT_EQ(wire_buffer["plugin_transports"], json::parse(R"(["loopback"])"));
    EXPECT_EQ(wire_buffer["plugin_transport_attrs"],
              json::parse(R"({"loopback":"lb-attr"})"));
    EXPECT_EQ(wire["detail"]["buffers"][1]["plugin_transports"],
              json::parse(R"(["vendorx"])"));
    EXPECT_EQ(wire["detail"]["plugin_transport_attrs"],
              json::parse(R"({"vendorx":"vx-seg"})"));
    // No plugin slot integer leaks anywhere in the wire form.
    auto builtinOnly = [](const json& types) {
        for (const auto& type : types)
            if (type.get<int>() >= kNumBuiltinTransportTypes) return false;
        return true;
    };
    auto attrKeysBuiltinOnly = [](const json& attrs) {
        for (const auto& pair : attrs)
            if (pair[0].get<int>() >= kNumBuiltinTransportTypes) return false;
        return true;
    };
    for (const auto& buffer : wire["detail"]["buffers"]) {
        EXPECT_TRUE(builtinOnly(buffer["transports"])) << buffer.dump();
        EXPECT_TRUE(attrKeysBuiltinOnly(buffer["transport_attrs"]));
    }
    EXPECT_TRUE(attrKeysBuiltinOnly(wire["detail"]["transport_attrs"]));
    for (const auto& device : wire["detail"]["devices"])
        EXPECT_TRUE(attrKeysBuiltinOnly(device["transport_attrs"]));
    // The caller's descriptor is untouched.
    EXPECT_EQ(memory.buffers[0].transports.back(), loopback_);

    const auto back = roundTrip(desc);
    EXPECT_EQ(json(back).dump(), json(desc).dump());
    EXPECT_TRUE(firstBuffer(back).plugin_transports.empty());
}

TEST_F(TransportWireCodecTest, NamesMapToThisProcessSlots) {
    // The peer assigned "vendorx" to its first slot; here it is the second.
    std::string wire = kGoldenJson;
    auto j = json::parse(wire);
    j["detail"]["buffers"][0]["plugin_transports"] = {"vendorx"};
    j["detail"]["buffers"][0]["plugin_transport_attrs"] = {{"vendorx", "a"}};
    auto desc = j.get<SegmentDesc>();
    decodeSegmentDescFromWire(desc);
    const auto& buffer = firstBuffer(desc);
    EXPECT_EQ(buffer.transports,
              (std::vector<TransportType>{RDMA, TCP, vendorx_}));
    EXPECT_EQ(buffer.transport_attrs.at(vendorx_), "a");
}

TEST_F(TransportWireCodecTest, UnknownPluginNamesAreDropped) {
    auto j = json::parse(kGoldenJson);
    j["detail"]["buffers"][0]["plugin_transports"] = {"not_here", "loopback"};
    j["detail"]["buffers"][0]["plugin_transport_attrs"] = {{"not_here", "x"}};
    j["detail"]["plugin_transport_attrs"] = {{"not_here", "y"}};
    auto desc = j.get<SegmentDesc>();
    decodeSegmentDescFromWire(desc);
    const auto& buffer = firstBuffer(desc);
    EXPECT_EQ(buffer.transports,
              (std::vector<TransportType>{RDMA, TCP, loopback_}));
    EXPECT_EQ(buffer.transport_attrs.size(), 1u);
    EXPECT_EQ(desc.getMemory().transport_attrs.size(), 1u);
}

TEST_F(TransportWireCodecTest, OutOfRangeIntegersAreDropped) {
    auto j = json::parse(kGoldenJson);
    auto& buffer = j["detail"]["buffers"][0];
    // 99: a newer peer's built-in; loopback_: a peer that leaked a slot;
    // -1: garbage.
    buffer["transports"] = {1, 99, static_cast<int>(loopback_), 7, -1};
    buffer["transport_attrs"] =
        json::array({json::array({1, "rdma-attr"}), json::array({99, "new"}),
                     json::array({static_cast<int>(loopback_), "leak"})});
    j["detail"]["transport_attrs"] =
        json::array({json::array({1, "seg-attr"}), json::array({42, "z"})});
    j["detail"]["devices"][0]["transport_attrs"] =
        json::array({json::array({1, "dev-attr"}), json::array({30, "d"})});
    auto desc = j.get<SegmentDesc>();
    decodeSegmentDescFromWire(desc);
    EXPECT_EQ(json(desc).dump(), kGoldenJson);
}

TEST_F(TransportWireCodecTest, OldPeerReadsOurWireForm) {
    auto desc = makeGoldenDesc();
    auto& memory = std::get<MemorySegmentDesc>(desc.detail);
    memory.buffers[0].transports.push_back(loopback_);
    const json wire = segmentDescToWireJson(desc);
    auto legacy = wire["detail"]["buffers"][0].get<LegacyBufferDesc>();
    EXPECT_EQ(legacy.transports, (std::vector<TransportType>{RDMA, TCP}));
    EXPECT_EQ(json(legacy).dump(),
              json::parse(kGoldenJson)["detail"]["buffers"][0].dump());
}

TEST_F(TransportWireCodecTest, ExistingKeysStayMandatory) {
    auto j = json::parse(kGoldenJson);
    j["detail"]["buffers"][0].erase("transport_attrs");
    EXPECT_THROW(j.get<SegmentDesc>(), json::exception);
}

TEST_F(TransportWireCodecTest, DevicePluginAttributesAreNotPublished) {
    auto desc = makeGoldenDesc();
    auto& memory = std::get<MemorySegmentDesc>(desc.detail);
    memory.devices[0].transport_attrs[loopback_] = "dev-lb";
    EXPECT_EQ(segmentDescToWireJson(desc).dump(), kGoldenJson);
}

TEST_F(TransportWireCodecTest, FileSegmentsPassThrough) {
    SegmentDesc desc;
    desc.name = "file";
    desc.type = SegmentType::File;
    desc.machine_id = "m";
    FileSegmentDesc file;
    file.buffers.push_back(FileBufferDesc{"/tmp/x", 10, 0});
    desc.detail = file;
    EXPECT_EQ(segmentDescToWireJson(desc).dump(), json(desc).dump());
    auto parsed = json::parse(json(desc).dump()).get<SegmentDesc>();
    decodeSegmentDescFromWire(parsed);
    EXPECT_EQ(json(parsed).dump(), json(desc).dump());
}

class LineSink : public google::LogSink {
   public:
    LineSink() { google::AddLogSink(this); }
    ~LineSink() override { google::RemoveLogSink(this); }
    void send(google::LogSeverity, const char*, const char*, int,
              const struct ::tm*, const char* message, size_t len) override {
        std::lock_guard<std::mutex> lock(mutex_);
        lines_.emplace_back(message, len);
    }
    int count(const std::string& needle) {
        std::lock_guard<std::mutex> lock(mutex_);
        return std::count_if(lines_.begin(), lines_.end(), [&](auto& line) {
            return line.find(needle) != std::string::npos;
        });
    }

   private:
    std::mutex mutex_;
    std::vector<std::string> lines_;
};

// Each unknown plugin name from a peer is logged once, not just the first
// one the process ever sees.
TEST_F(TransportWireCodecTest, UnknownNamesAreEachLoggedOnce) {
    // Names unique to this run: the log-once set lives for the process.
    static int run = 0;
    const std::string a = "unseen_a" + std::to_string(run);
    const std::string b = "unseen_b" + std::to_string(run++);
    LineSink sink;
    for (const auto& name : {a, b, a}) {
        auto j = json::parse(kGoldenJson);
        j["detail"]["buffers"][0]["plugin_transports"] = {name};
        auto desc = j.get<SegmentDesc>();
        decodeSegmentDescFromWire(desc);
    }
    EXPECT_EQ(sink.count("transport plugin '" + a + "'"), 1);
    EXPECT_EQ(sink.count("transport plugin '" + b + "'"), 1);
}

TEST_F(TransportWireCodecTest, DuplicateNamesDoNotDuplicateSlots) {
    auto j = json::parse(kGoldenJson);
    j["detail"]["buffers"][0]["plugin_transports"] = {"loopback", "loopback"};
    auto desc = j.get<SegmentDesc>();
    decodeSegmentDescFromWire(desc);
    const auto& transports = firstBuffer(desc).transports;
    EXPECT_EQ(std::count(transports.begin(), transports.end(), loopback_), 1);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
