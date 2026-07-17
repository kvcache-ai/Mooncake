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

#include "tent/metrics/tent_metrics.h"

#if TENT_METRICS_ENABLED

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace mooncake {
namespace tent {
namespace {

// RAII helper that binds and listens on an OS-assigned port, thereby keeping
// that port busy for the duration of the test.
class PortOccupier {
   public:
    PortOccupier() {
        fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        EXPECT_GE(fd_, 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        addr.sin_port = 0;  // let the kernel pick a free port
        EXPECT_EQ(::bind(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)),
                  0);
        EXPECT_EQ(::listen(fd_, 1), 0);

        sockaddr_in bound{};
        socklen_t len = sizeof(bound);
        EXPECT_EQ(::getsockname(fd_, reinterpret_cast<sockaddr*>(&bound), &len),
                  0);
        port_ = ntohs(bound.sin_port);
    }

    ~PortOccupier() {
        if (fd_ >= 0) ::close(fd_);
    }

    uint16_t port() const { return port_; }

   private:
    int fd_ = -1;
    uint16_t port_ = 0;
};

// When the configured metrics port is already in use (e.g. another rank was
// mistakenly given the same port), initialize() must NOT falsely report a
// listening endpoint. It degrades to log-only metrics: init still succeeds,
// but the HTTP server is not bound, so httpPort() reports 0.
TEST(TentMetricsHttpServer, DegradesToLogOnlyWhenConfiguredPortBusy) {
    PortOccupier occupier;
    ASSERT_GT(occupier.port(), 0);

    MetricsConfig config;
    config.enabled = true;
    config.http_host = "127.0.0.1";
    config.http_port = occupier.port();
    config.report_interval_seconds = 0;  // no periodic reporting thread

    auto& metrics = TentMetrics::instance();
    Status status = metrics.initialize(config);

    // Metrics subsystem still comes up (counters + log summary work without a
    // port), but the scrape endpoint is not listening on the busy port.
    EXPECT_TRUE(status.ok()) << status.ToString();
    EXPECT_TRUE(metrics.isInitialized());
    EXPECT_EQ(metrics.httpPort(), 0);

    metrics.shutdown();
}

}  // namespace
}  // namespace tent
}  // namespace mooncake

#else  // !TENT_METRICS_ENABLED

// With metrics compiled out, initialize() is a no-op stub. Assert only that it
// reports success so the test target still builds and runs in default builds.
TEST(TentMetricsHttpServer, DisabledAtCompileTime) {
    mooncake::tent::MetricsConfig config;
    EXPECT_TRUE(
        mooncake::tent::TentMetrics::instance().initialize(config).ok());
}

#endif  // TENT_METRICS_ENABLED
