// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

#include "tent/runtime/control_plane.h"

namespace mooncake::tent {
namespace {

class RunningControlService {
   public:
    explicit RunningControlService(bool enable_bulk_data_rpc)
        : service_(std::make_shared<ControlService>("p2p", "", nullptr,
                                                    enable_bulk_data_rpc)) {
        uint16_t port = 0;
        status_ = service_->start(port, false, 1);
        if (status_.ok()) {
            address_ = "127.0.0.1:" + std::to_string(port);
        }
    }

    const Status& status() const { return status_; }
    const std::string& address() const { return address_; }

   private:
    std::shared_ptr<ControlService> service_;
    Status status_{Status::OK()};
    std::string address_;
};

TEST(HighPerformanceTcpRpcGateTest, SendDataIsRejectedBeforePayloadCopy) {
    RunningControlService server(false);
    ASSERT_TRUE(server.status().ok()) << server.status().ToString();

    std::vector<uint8_t> source(4096, 0x7b);
    const Status status = ControlClient::sendData(server.address(), 0x12345678,
                                                  source.data(), source.size());
    EXPECT_FALSE(status.ok());
}

TEST(HighPerformanceTcpRpcGateTest, RecvDataIsRejectedAndDestinationUnchanged) {
    RunningControlService server(false);
    ASSERT_TRUE(server.status().ok()) << server.status().ToString();

    std::vector<uint8_t> destination(4096, 0xa5);
    const auto before = destination;
    const Status status = ControlClient::recvData(
        server.address(), 0x12345678, destination.data(), destination.size());
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(destination, before);
}

TEST(HighPerformanceTcpRpcGateTest, EqualLengthErrorCannotBecomeRecvSuccess) {
    RunningControlService server(false);
    ASSERT_TRUE(server.status().ok()) << server.status().ToString();

    constexpr size_t kDisabledMessageLength =
        sizeof("RecvData disabled: high-performance TCP data plane required") -
        1;
    std::vector<uint8_t> destination(kDisabledMessageLength, 0x3c);
    const auto before = destination;
    const Status status = ControlClient::recvData(
        server.address(), 0x12345678, destination.data(), destination.size());
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(destination, before);
}

}  // namespace
}  // namespace mooncake::tent
