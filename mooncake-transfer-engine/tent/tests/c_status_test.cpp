// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <memory>

#include "tent/runtime/transfer_engine_impl.h"
#include "tent/transfer_engine.h"

extern "C" int status_c_open(tent_engine_t, tent_segment_id_t*, const char*);
extern "C" int status_c_info(tent_engine_t, tent_segment_id_t,
                             tent_segment_info_t*);
extern "C" int status_c_submit(tent_engine_t, tent_batch_id_t, tent_request_t*,
                               size_t, int);

namespace mooncake::tent {
namespace {
class CStatus : public testing::Test {
   protected:
    std::array<char, 8192> bytes{};
    std::unique_ptr<TransferEngineImpl> engine;
    void SetUp() override {
        auto config = std::make_shared<Config>();
        config->set("metadata_type", "p2p");
        config->set("rpc_server_hostname", "127.0.0.1");
        config->set("rpc_server_port", "0");
        config->set("enable_progress_worker", false);
        config->set("enable_runtime_queue", false);
        config->set("transports/tcp/enable", true);
        for (const char* name : {"rdma", "shm", "nvlink", "mnnvl", "hp_tcp",
                                 "fabric", "io_uring", "gds"})
            config->set(std::string("transports/") + name + "/enable", false);
        engine = std::make_unique<TransferEngineImpl>(config);
        ASSERT_TRUE(engine->available());
        ASSERT_EQ(
            tent_register_memory(engine.get(), bytes.data(), bytes.size()), 0);
    }
    void TearDown() override {
        if (engine) {
            EXPECT_EQ(tent_unregister_memory(engine.get(), bytes.data(),
                                             bytes.size()),
                      0);
        }
    }
};

TEST_F(CStatus, ConnectionFailurePreservesNativeCode) {
    const int socket = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(socket, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ASSERT_EQ(
        ::bind(socket, reinterpret_cast<sockaddr*>(&address), sizeof(address)),
        0);
    socklen_t length = sizeof(address);
    ASSERT_EQ(
        ::getsockname(socket, reinterpret_cast<sockaddr*>(&address), &length),
        0);
    const auto name = "127.0.0.1:" + std::to_string(ntohs(address.sin_port));
    // The bound port has no listener and cannot be reused by another process.
    SegmentID native_segment = 0;
    ASSERT_TRUE(engine->openSegment(native_segment, name).ok());
    SegmentInfo native_info;
    const auto native = engine->getSegmentInfo(native_segment, native_info);
    EXPECT_EQ(native.code(), Status::Code::kRpcConnectionError);
    tent_segment_id_t segment = 0;
    ASSERT_EQ(status_c_open(engine.get(), &segment, name.c_str()), 0);
    tent_segment_info_t info{};
    EXPECT_EQ(status_c_info(engine.get(), segment, &info), -134);
    ::close(socket);
}

TEST_F(CStatus, CapacityRejectionSurvivesBothSubmitWrappers) {
    tent_request_t r{};
    r.opcode = OPCODE_WRITE;
    r.source = bytes.data();
    r.target_offset = reinterpret_cast<uint64_t>(bytes.data() + 4096);
    r.length = 16;
    r.transport_hint = TRANSPORT_TCP;
    std::array<tent_request_t, 2> requests{r, r};
    for (int notify : {0, 1}) {
        const auto id = tent_allocate_batch(engine.get(), 1);
        ASSERT_NE(id, 0u);
        EXPECT_EQ(status_c_submit(engine.get(), id, requests.data(),
                                  requests.size(), notify),
                  -2);
        EXPECT_EQ(tent_free_batch(engine.get(), id), 0);
    }
}

TEST_F(CStatus, SuccessAndInvalidArgumentKeepTheirValues) {
    tent_segment_info_t info{};
    EXPECT_EQ(tent_get_segment_info(engine.get(), LOCAL_SEGMENT_ID, &info), 0);
    EXPECT_EQ(info.num_buffers, 1);
    tent_free_segment_info(&info);
    EXPECT_EQ(tent_get_segment_info(engine.get(), LOCAL_SEGMENT_ID, nullptr),
              -1);
    EXPECT_EQ(tent_task_status(engine.get(), 0, 0, nullptr), -1);
}
}  // namespace
}  // namespace mooncake::tent
