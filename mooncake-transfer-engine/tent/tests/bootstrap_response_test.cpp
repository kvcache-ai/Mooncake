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

}  // namespace
}  // namespace tent
}  // namespace mooncake
