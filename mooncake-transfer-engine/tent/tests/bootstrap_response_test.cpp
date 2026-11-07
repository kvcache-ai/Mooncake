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

#include <string_view>

#include "tent/runtime/control_plane.h"

namespace mooncake {
namespace tent {
namespace {

struct LegacyBootstrapDesc {
    std::string local_nic_path;
    std::string peer_nic_path;
    std::vector<uint32_t> qp_num;
    uint32_t local_lid = 0;
    std::string local_gid;
    std::string reply_msg;
    uint32_t notify_qp_num = 0;
};

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(LegacyBootstrapDesc, local_nic_path,
                                   peer_nic_path, qp_num, local_lid, local_gid,
                                   reply_msg, notify_qp_num)

TEST(BootstrapResponseTest, ValidGidIsSuccess) {
    BootstrapDesc desc;
    desc.local_gid = "fe80::1";
    desc.qp_num = {1, 2};
    BootstrapDesc parsed;
    auto status =
        ControlClient::decodeBootstrapResponse(json(desc).dump(), parsed);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(parsed.local_gid, "fe80::1");
    EXPECT_EQ(parsed.qp_num, desc.qp_num);
}

TEST(BootstrapResponseTest, ProtocolVersionIsBackwardCompatible) {
    BootstrapDesc current;
    current.protocol_version = 2;
    auto wire = json(current);
    EXPECT_EQ(wire.at("protocol_version"), 2);

    wire.erase("protocol_version");
    auto legacy = wire.get<BootstrapDesc>();
    EXPECT_EQ(legacy.protocol_version, 1u);

    legacy.protocol_version = 2;
    wire.get_to(legacy);
    EXPECT_EQ(legacy.protocol_version, 1u);
}

TEST(BootstrapResponseTest, LegacyParserIgnoresProtocolVersion) {
    BootstrapDesc current;
    current.protocol_version = 2;
    current.local_gid = "fe80::1";
    auto legacy = json(current).get<LegacyBootstrapDesc>();
    EXPECT_EQ(legacy.local_gid, current.local_gid);
    EXPECT_EQ(legacy.notify_qp_num, current.notify_qp_num);
}

TEST(BootstrapResponseTest, ReplyMsgIsHandshakeFailure) {
    BootstrapDesc desc;
    desc.local_gid = "fe80::1";
    desc.reply_msg = "Endpoint retired for reconnection from peer";
    BootstrapDesc parsed;
    auto status =
        ControlClient::decodeBootstrapResponse(json(desc).dump(), parsed);
    EXPECT_TRUE(status.IsRpcServiceError());
    EXPECT_NE(status.message().find("retired for reconnection"),
              std::string_view::npos);
}

TEST(BootstrapResponseTest, ThrottledReplyIsRetryable) {
    BootstrapDesc desc;
    desc.reply_msg = "NOT_READY: bootstrap throttled, retry_after_ms=17";
    BootstrapDesc parsed;
    auto status =
        ControlClient::decodeBootstrapResponse(json(desc).dump(), parsed);
    EXPECT_TRUE(status.IsTooManyRequests()) << status.ToString();
    EXPECT_EQ(parsed.reply_msg, desc.reply_msg);
}

TEST(BootstrapResponseTest, EmptyGidIsHandshakeFailure) {
    BootstrapDesc desc;
    BootstrapDesc parsed;
    auto status =
        ControlClient::decodeBootstrapResponse(json(desc).dump(), parsed);
    EXPECT_TRUE(status.IsInvalidArgument());
    EXPECT_NE(status.message().find("Missing peer GID"),
              std::string_view::npos);
}

TEST(BootstrapResponseTest, InvalidJsonIsMalformed) {
    BootstrapDesc parsed;
    auto status = ControlClient::decodeBootstrapResponse("not-json", parsed);
    EXPECT_TRUE(status.IsMalformedJson());
}

TEST(BootstrapResponseTest, MissingLegacyFieldIsStillMalformed) {
    auto wire = json(BootstrapDesc{});
    wire.erase("local_gid");
    BootstrapDesc parsed;
    auto status = ControlClient::decodeBootstrapResponse(wire.dump(), parsed);
    EXPECT_TRUE(status.IsMalformedJson());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
