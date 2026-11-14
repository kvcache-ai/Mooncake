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

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "tent/common/config.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/transport_selector.h"
#include "tent/transport/tcp/tcp_transport.h"

namespace mooncake {
namespace tent {

class TcpTransportTestPeer {
   public:
    static const TcpParams& params(const TcpTransport& transport) {
        return transport.params_;
    }

    static Status resolve(TcpTransport& transport, uint64_t address,
                          uint64_t length, uint64_t target_id,
                          std::optional<TcpDataPlaneIdentity>& identity) {
        std::string server;
        return transport.findRemoteSegment(address, length, target_id, server,
                                           identity);
    }

    static Status transfer(TcpTransport& transport, const Request& request) {
        TcpTask task;
        task.request = request;
        return transport.doTransferWithRetry(&task);
    }
};

class TcpBufferRegistryTestPeer {
   public:
    static void setAcquireHook(TcpBufferRegistry& registry,
                               std::function<void(uint64_t)> hook) {
        registry.setAcquireHookForTest(std::move(hook));
    }

    static bool hasRegistration(TcpBufferRegistry& registry,
                                uint64_t registration) {
        std::shared_lock<std::shared_mutex> lock(registry.mutex_);
        return registry.entries_.contains(registration);
    }

    static bool closing(TcpBufferRegistry& registry) {
        std::shared_lock<std::shared_mutex> lock(registry.mutex_);
        return registry.closing_;
    }
};

namespace {

std::shared_ptr<ControlService> makeP2PMetadata() {
    return std::make_shared<ControlService>("p2p", "", nullptr);
}

std::shared_ptr<ControlService> startUniqueTcpService(uint16_t* port) {
    static std::mutex used_ports_mutex;
    static std::unordered_set<uint16_t> used_ports;
    for (size_t attempt = 0; attempt < 64; ++attempt) {
        auto service = makeP2PMetadata();
        uint16_t candidate = 0;
        if (!service->start(candidate).ok()) continue;
        std::lock_guard<std::mutex> lock(used_ports_mutex);
        if (used_ports.insert(candidate).second) {
            *port = candidate;
            return service;
        }
    }
    return nullptr;
}

// Models an endpoint that was previously advertised by a v2 process but now
// serves the legacy data RPCs at the same address. GetSegmentDesc can switch
// to the new legacy snapshot while the client still holds the old v2 one.
class LegacyTcpPeer {
   public:
    LegacyTcpPeer(char* target, size_t length)
        : target_(target), length_(length), process_(makeTcpProcessIdentity()) {
        server_.registerFunction(GetSegmentDesc, [this](const std::string_view&,
                                                        std::string& response) {
            nlohmann::json encoded = segment();
            response = encoded.dump();
        });
        server_.registerFunction(SendData, [this](
                                               const std::string_view& request,
                                               std::string& response) {
            if (request.size() < sizeof(XferDataDesc)) {
                response = "legacy WRITE request is too short";
                return;
            }
            XferDataDesc desc;
            memcpy(&desc, request.data(), sizeof(desc));
            const uint64_t address = le64toh(desc.peer_mem_addr);
            const size_t length = le64toh(desc.length);
            if (address != reinterpret_cast<uint64_t>(target_) ||
                length != length_ || request.size() != sizeof(desc) + length) {
                response = "legacy WRITE request is invalid";
                return;
            }
            memcpy(target_, request.data() + sizeof(desc), length);
            ++v1_writes_;
        });
        server_.registerFunction(
            RecvData,
            [this](const std::string_view& request, std::string& response) {
                if (request.size() != sizeof(XferDataDesc)) {
                    response = "legacy READ request is invalid";
                    return;
                }
                XferDataDesc desc;
                memcpy(&desc, request.data(), sizeof(desc));
                const uint64_t address = le64toh(desc.peer_mem_addr);
                const size_t length = le64toh(desc.length);
                if (address != reinterpret_cast<uint64_t>(target_) ||
                    length != length_) {
                    response = "legacy READ target is invalid";
                    return;
                }
                response.assign(target_, length);
                ++v1_reads_;
            });
        uint16_t port = 0;
        status_ = server_.start(port);
        if (status_.ok()) {
            address_ = "127.0.0.1:" + std::to_string(port);
        }
    }

    bool available() const { return status_.ok(); }
    const std::string& address() const { return address_; }
    void advertiseV2(bool enabled) { advertise_v2_.store(enabled); }
    int v1Writes() const { return v1_writes_.load(); }
    int v1Reads() const { return v1_reads_.load(); }

   private:
    SegmentDesc segment() const {
        SegmentDesc result;
        result.name = address_;
        result.type = SegmentType::Memory;
        result.machine_id = "legacy-tcp-test-peer";
        result.rpc_server_addr = address_;
        result.detail = MemorySegmentDesc{};

        BufferDesc buffer;
        buffer.addr = reinterpret_cast<uint64_t>(target_);
        buffer.length = length_;
        buffer.permission = kGlobalReadWrite;
        auto& memory = std::get<MemorySegmentDesc>(result.detail);
        if (advertise_v2_.load()) {
            memory.transport_attrs[static_cast<int>(TransportType::TCP)] =
                encodeTcpProcessAttr(process_);
            buffer.transport_attrs[TransportType::TCP] =
                encodeTcpRegistrationAttr(1);
        }
        memory.buffers.push_back(std::move(buffer));
        return result;
    }

    CoroRpcAgent server_;
    char* target_;
    size_t length_;
    TcpProcessIdentity process_;
    std::atomic<bool> advertise_v2_{true};
    std::atomic<int> v1_writes_{0};
    std::atomic<int> v1_reads_{0};
    Status status_;
    std::string address_;
};

TEST(TcpTransportConfigTest, InstallWithoutConfigKeepsStructDefaults) {
    TcpTransport transport;
    auto metadata = makeP2PMetadata();
    std::string name = "tcp-config-default";
    ASSERT_TRUE(transport.install(name, metadata, nullptr).ok());

    const TcpParams defaults;
    const auto& params = TcpTransportTestPeer::params(transport);
    EXPECT_EQ(params.max_retry_count, defaults.max_retry_count);
    EXPECT_EQ(params.retry_base_delay_ms, defaults.retry_base_delay_ms);
    EXPECT_EQ(params.retry_max_delay_ms, defaults.retry_max_delay_ms);
    EXPECT_EQ(params.max_concurrent_tasks, defaults.max_concurrent_tasks);
    EXPECT_FALSE(params.allow_unsafe_legacy_data_rpc);

    EXPECT_TRUE(transport.uninstall().ok());
}

TEST(TcpTransportConfigTest, InstallReadsConfigKeys) {
    auto conf = std::make_shared<Config>();
    conf->set("transports/tcp/max_retry_count", 5);
    conf->set("transports/tcp/retry_base_delay_ms", 200ULL);
    conf->set("transports/tcp/retry_max_delay_ms", 4000ULL);
    conf->set("transports/tcp/max_concurrent_tasks", 32);
    conf->set("transports/tcp/allow_unsafe_legacy_data_rpc", true);

    TcpTransport transport;
    auto metadata = makeP2PMetadata();
    std::string name = "tcp-config-override";
    ASSERT_TRUE(transport.install(name, metadata, nullptr, conf).ok());

    const auto& params = TcpTransportTestPeer::params(transport);
    EXPECT_EQ(params.max_retry_count, 5u);
    EXPECT_EQ(params.retry_base_delay_ms, 200ULL);
    EXPECT_EQ(params.retry_max_delay_ms, 4000ULL);
    EXPECT_EQ(params.max_concurrent_tasks, 32u);
    EXPECT_TRUE(params.allow_unsafe_legacy_data_rpc);

    EXPECT_TRUE(transport.uninstall().ok());
}

TEST(TcpTransportConfigTest, RejectsZeroWorkersAndAllowsRetry) {
    auto conf = std::make_shared<Config>();
    conf->set("transports/tcp/max_concurrent_tasks", 0);

    TcpTransport transport;
    auto metadata = makeP2PMetadata();
    std::string name = "tcp-config-zero-workers";
    auto status = transport.install(name, metadata, nullptr, conf);
    ASSERT_TRUE(status.IsInvalidArgument()) << status.ToString();
    EXPECT_NE(status.ToString().find("transports/tcp/max_concurrent_tasks"),
              std::string::npos);

    conf->set("transports/tcp/max_concurrent_tasks", 1);
    ASSERT_TRUE(transport.install(name, metadata, nullptr, conf).ok());
    EXPECT_EQ(TcpTransportTestPeer::params(transport).max_concurrent_tasks, 1u);
    EXPECT_TRUE(transport.uninstall().ok());
}

class TcpIdentityRpcTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // The thread-local RPC client pool outlives each fixture. Avoid an
        // ephemeral port being reused by a later server in this process,
        // while still letting the OS avoid ports owned by parallel CI jobs.
        service_ = startUniqueTcpService(&port_);
        ASSERT_NE(service_, nullptr);
        address_ = "127.0.0.1:" + std::to_string(port_);
    }

    std::shared_ptr<ControlService> service_;
    uint16_t port_ = 0;
    std::string address_;
};

TEST_F(TcpIdentityRpcTest,
       IdentifiedWriteRejectsWrongProcessAndReusedRegistration) {
    std::array<char, 8> target{};
    std::array<char, 8> source{'n', 'e', 'w', '-', 'd', 'a', 't', 'a'};
    uint64_t first_registration = 0;
    ASSERT_TRUE(service_->tcpBufferRegistry()
                    .add(reinterpret_cast<uint64_t>(target.data()),
                         target.size(), kGlobalReadWrite, &first_registration)
                    .ok());

    TcpDataPlaneIdentity first{service_->tcpProcessIdentity(),
                               first_registration};
    auto wrong_process = first;
    wrong_process.process.low ^= 1;
    auto wrong = ControlClient::sendDataV2(
        address_, reinterpret_cast<uint64_t>(target.data()), source.data(),
        source.size(), wrong_process);
    EXPECT_TRUE(wrong.IsNeedsRefreshCache()) << wrong.ToString();
    EXPECT_EQ(target, (std::array<char, 8>{}));
    std::array<char, 8> wrong_read;
    wrong_read.fill('x');
    auto wrong_recv = ControlClient::recvDataV2(
        address_, reinterpret_cast<uint64_t>(target.data()), wrong_read.data(),
        wrong_read.size(), wrong_process);
    EXPECT_TRUE(wrong_recv.IsNeedsRefreshCache()) << wrong_recv.ToString();
    EXPECT_EQ(wrong_read,
              (std::array<char, 8>{'x', 'x', 'x', 'x', 'x', 'x', 'x', 'x'}));

    ASSERT_TRUE(service_->tcpBufferRegistry().remove(first_registration).ok());
    uint64_t second_registration = 0;
    ASSERT_TRUE(service_->tcpBufferRegistry()
                    .add(reinterpret_cast<uint64_t>(target.data()),
                         target.size(), kGlobalReadWrite, &second_registration)
                    .ok());
    ASSERT_NE(first_registration, second_registration);

    auto stale = ControlClient::sendDataV2(
        address_, reinterpret_cast<uint64_t>(target.data()), source.data(),
        source.size(), first);
    EXPECT_TRUE(stale.IsNeedsRefreshCache()) << stale.ToString();
    EXPECT_EQ(target, (std::array<char, 8>{}));
    std::array<char, 8> stale_read;
    stale_read.fill('y');
    auto stale_recv = ControlClient::recvDataV2(
        address_, reinterpret_cast<uint64_t>(target.data()), stale_read.data(),
        stale_read.size(), first);
    EXPECT_TRUE(stale_recv.IsNeedsRefreshCache()) << stale_recv.ToString();
    EXPECT_EQ(stale_read,
              (std::array<char, 8>{'y', 'y', 'y', 'y', 'y', 'y', 'y', 'y'}));

    TcpDataPlaneIdentity current{service_->tcpProcessIdentity(),
                                 second_registration};
    auto valid = ControlClient::sendDataV2(
        address_, reinterpret_cast<uint64_t>(target.data()), source.data(),
        source.size(), current);
    EXPECT_TRUE(valid.ok()) << valid.ToString();
    EXPECT_EQ(target, source);
    EXPECT_TRUE(service_->tcpBufferRegistry().remove(second_registration).ok());
}

TEST_F(TcpIdentityRpcTest, NewServerKeepsLegacyAndIdentifiedDataRpcCompatible) {
    std::array<char, 8> target{};
    uint64_t registration = 0;
    ASSERT_TRUE(service_->tcpBufferRegistry()
                    .add(reinterpret_cast<uint64_t>(target.data()),
                         target.size(), kGlobalReadWrite, &registration)
                    .ok());
    ASSERT_TRUE(service_->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        segment.type = SegmentType::Memory;
                        BufferDesc buffer;
                        buffer.addr = reinterpret_cast<uint64_t>(target.data());
                        buffer.length = target.size();
                        buffer.transport_attrs[TransportType::TCP] =
                            encodeTcpRegistrationAttr(registration);
                        std::get<MemorySegmentDesc>(segment.detail)
                            .buffers.push_back(buffer);
                        return Status::OK();
                    })
                    .ok());

    const TcpDataPlaneIdentity identity{service_->tcpProcessIdentity(),
                                        registration};

    std::array<char, 8> legacy_source{'l', 'e', 'g', 'a', 'c', 'y', '!', '!'};
    auto disabled = ControlClient::sendData(
        address_, reinterpret_cast<uint64_t>(target.data()),
        legacy_source.data(), legacy_source.size());
    EXPECT_TRUE(disabled.IsNeedsRefreshCache()) << disabled.ToString();
    EXPECT_EQ(target, (std::array<char, 8>{}));

    service_->setUnsafeLegacyTcpDataRpcEnabled(true);
    ASSERT_TRUE(ControlClient::sendData(
                    address_, reinterpret_cast<uint64_t>(target.data()),
                    legacy_source.data(), legacy_source.size())
                    .ok());
    EXPECT_EQ(target, legacy_source);

    ASSERT_TRUE(service_->segmentManager()
                    .updateLocal([](SegmentDesc& segment) {
                        std::get<MemorySegmentDesc>(segment.detail)
                            .buffers.back()
                            .transport_attrs.erase(TransportType::TCP);
                        return Status::OK();
                    })
                    .ok());
    std::array<char, 8> unleased_source{'u', 'n', 'l', 'e', 'a', 's', 'e', 'd'};
    auto unleased = ControlClient::sendData(
        address_, reinterpret_cast<uint64_t>(target.data()),
        unleased_source.data(), unleased_source.size());
    EXPECT_TRUE(unleased.IsNeedsRefreshCache()) << unleased.ToString();
    EXPECT_EQ(target, legacy_source);
    ASSERT_TRUE(service_->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        std::get<MemorySegmentDesc>(segment.detail)
                            .buffers.back()
                            .transport_attrs[TransportType::TCP] =
                            encodeTcpRegistrationAttr(registration);
                        return Status::OK();
                    })
                    .ok());

    std::array<char, 8> identified_source{'v', '2', '-', 'd',
                                          'a', 't', 'a', '!'};
    ASSERT_TRUE(ControlClient::sendDataV2(
                    address_, reinterpret_cast<uint64_t>(target.data()),
                    identified_source.data(), identified_source.size(),
                    identity)
                    .ok());
    EXPECT_EQ(target, identified_source);

    std::array<char, 8> received{};
    ASSERT_TRUE(ControlClient::recvDataV2(
                    address_, reinterpret_cast<uint64_t>(target.data()),
                    received.data(), received.size(), identity)
                    .ok());
    EXPECT_EQ(received, identified_source);
    service_->setUnsafeLegacyTcpDataRpcEnabled(false);
    EXPECT_TRUE(service_->tcpBufferRegistry().remove(registration).ok());
    EXPECT_TRUE(
        service_->segmentManager()
            .updateLocal([](SegmentDesc& segment) {
                std::get<MemorySegmentDesc>(segment.detail).buffers.clear();
                return Status::OK();
            })
            .ok());
}

TEST_F(TcpIdentityRpcTest, LegacyReadPayloadMayUseRefreshPrefix) {
    std::array<char, 64> target{};
    const std::string payload = "NeedsRefreshCache: legitimate payload bytes";
    ASSERT_LT(payload.size(), target.size());
    std::copy(payload.begin(), payload.end(), target.begin());

    uint64_t registration = 0;
    ASSERT_TRUE(service_->tcpBufferRegistry()
                    .add(reinterpret_cast<uint64_t>(target.data()),
                         target.size(), kGlobalReadOnly, &registration)
                    .ok());
    BufferDesc buffer;
    buffer.addr = reinterpret_cast<uint64_t>(target.data());
    buffer.length = target.size();
    buffer.permission = kGlobalReadOnly;
    buffer.transport_attrs[TransportType::TCP] =
        encodeTcpRegistrationAttr(registration);
    ASSERT_TRUE(service_->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        segment.type = SegmentType::Memory;
                        std::get<MemorySegmentDesc>(segment.detail).buffers = {
                            buffer};
                        return Status::OK();
                    })
                    .ok());

    service_->setUnsafeLegacyTcpDataRpcEnabled(true);
    std::vector<char> received(payload.size());
    auto status = ControlClient::recvData(
        address_, reinterpret_cast<uint64_t>(target.data()), received.data(),
        received.size());
    EXPECT_TRUE(status.ok()) << status.ToString();
    EXPECT_EQ(std::string(received.begin(), received.end()), payload);

    service_->setUnsafeLegacyTcpDataRpcEnabled(false);
    EXPECT_TRUE(service_->tcpBufferRegistry().remove(registration).ok());
    EXPECT_TRUE(
        service_->segmentManager()
            .updateLocal([](SegmentDesc& segment) {
                std::get<MemorySegmentDesc>(segment.detail).buffers.clear();
                return Status::OK();
            })
            .ok());
}

TEST_F(TcpIdentityRpcTest, IdentifiedRpcEnforcesRegistrationPermission) {
    std::array<char, 8> target{'r', 'e', 'a', 'd', 'o', 'n', 'l', 'y'};
    uint64_t registration = 0;
    ASSERT_TRUE(service_->tcpBufferRegistry()
                    .add(reinterpret_cast<uint64_t>(target.data()),
                         target.size(), kGlobalReadOnly, &registration)
                    .ok());
    const TcpDataPlaneIdentity identity{service_->tcpProcessIdentity(),
                                        registration};

    std::array<char, 8> received{};
    ASSERT_TRUE(ControlClient::recvDataV2(
                    address_, reinterpret_cast<uint64_t>(target.data()),
                    received.data(), received.size(), identity)
                    .ok());
    EXPECT_EQ(received, target);

    std::array<char, 8> source{'o', 'v', 'e', 'r', 'w', 'r', 'i', 't'};
    auto denied = ControlClient::sendDataV2(
        address_, reinterpret_cast<uint64_t>(target.data()), source.data(),
        source.size(), identity);
    EXPECT_TRUE(denied.IsAddressNotRegistered()) << denied.ToString();
    EXPECT_EQ(target,
              (std::array<char, 8>{'r', 'e', 'a', 'd', 'o', 'n', 'l', 'y'}));

    EXPECT_TRUE(service_->tcpBufferRegistry().remove(registration).ok());
    ASSERT_TRUE(service_->tcpBufferRegistry()
                    .add(reinterpret_cast<uint64_t>(target.data()),
                         target.size(), kLocalReadWrite, &registration)
                    .ok());
    const TcpDataPlaneIdentity local_only{service_->tcpProcessIdentity(),
                                          registration};
    received.fill('z');
    auto local_read = ControlClient::recvDataV2(
        address_, reinterpret_cast<uint64_t>(target.data()), received.data(),
        received.size(), local_only);
    EXPECT_TRUE(local_read.IsAddressNotRegistered()) << local_read.ToString();
    EXPECT_EQ(received,
              (std::array<char, 8>{'z', 'z', 'z', 'z', 'z', 'z', 'z', 'z'}));
    auto local_write = ControlClient::sendDataV2(
        address_, reinterpret_cast<uint64_t>(target.data()), source.data(),
        source.size(), local_only);
    EXPECT_TRUE(local_write.IsAddressNotRegistered()) << local_write.ToString();
    EXPECT_EQ(target,
              (std::array<char, 8>{'r', 'e', 'a', 'd', 'o', 'n', 'l', 'y'}));
    EXPECT_TRUE(service_->tcpBufferRegistry().remove(registration).ok());
}

TEST(TcpTransportIdentityTest, PublishesAndResolvesRegistrationMetadata) {
    TcpTransport transport;
    auto metadata = makeP2PMetadata();
    uint16_t port = 0;
    ASSERT_TRUE(metadata->start(port).ok());
    std::string name = "127.0.0.1:" + std::to_string(port);
    ASSERT_TRUE(transport.install(name, metadata, nullptr).ok());

    std::array<char, 8> target{};
    BufferDesc buffer;
    buffer.addr = reinterpret_cast<uint64_t>(target.data());
    buffer.length = target.size();
    MemoryOptions options;
    options.perm = kGlobalReadWrite;
    ASSERT_TRUE(transport.addMemoryBuffer(buffer, options).ok());
    ASSERT_NE(buffer.transport_attrs.find(TransportType::TCP),
              buffer.transport_attrs.end());

    ASSERT_TRUE(metadata->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        segment.rpc_server_addr = name;
                        std::get<MemorySegmentDesc>(segment.detail)
                            .buffers.push_back(buffer);
                        return Status::OK();
                    })
                    .ok());
    std::optional<TcpDataPlaneIdentity> identity;
    auto resolved = TcpTransportTestPeer::resolve(
        transport, buffer.addr, buffer.length, LOCAL_SEGMENT_ID, identity);
    ASSERT_TRUE(resolved.ok()) << resolved.ToString();
    ASSERT_TRUE(identity.has_value());
    EXPECT_EQ(identity->process, metadata->tcpProcessIdentity());
    EXPECT_NE(identity->registration_id, 0u);

    const auto stale_identity = *identity;
    ASSERT_TRUE(metadata->segmentManager()
                    .updateLocal([](SegmentDesc& segment) {
                        std::get<MemorySegmentDesc>(segment.detail)
                            .buffers.back()
                            .transport_attrs.erase(TransportType::TCP);
                        return Status::OK();
                    })
                    .ok());
    auto missing_registration = TcpTransportTestPeer::resolve(
        transport, buffer.addr, buffer.length, LOCAL_SEGMENT_ID, identity);
    EXPECT_TRUE(missing_registration.IsAddressNotRegistered())
        << missing_registration.ToString();

    ASSERT_TRUE(metadata->segmentManager()
                    .updateLocal([](SegmentDesc& segment) {
                        std::get<MemorySegmentDesc>(segment.detail)
                            .transport_attrs.erase(
                                static_cast<int>(TransportType::TCP));
                        return Status::OK();
                    })
                    .ok());
    ASSERT_TRUE(TcpTransportTestPeer::resolve(transport, buffer.addr,
                                              buffer.length, LOCAL_SEGMENT_ID,
                                              identity)
                    .ok());
    EXPECT_FALSE(identity.has_value());

    ASSERT_TRUE(metadata->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        std::get<MemorySegmentDesc>(segment.detail)
                            .buffers.back()
                            .transport_attrs[TransportType::TCP] =
                            buffer.transport_attrs.at(TransportType::TCP);
                        return Status::OK();
                    })
                    .ok());
    auto partial = TcpTransportTestPeer::resolve(
        transport, buffer.addr, buffer.length, LOCAL_SEGMENT_ID, identity);
    EXPECT_TRUE(partial.IsNeedsRefreshCache()) << partial.ToString();

    ASSERT_TRUE(transport.removeMemoryBuffer(buffer).ok());
    TcpBufferRegistry::Lease stale;
    auto stale_status = metadata->tcpBufferRegistry().acquire(
        stale_identity.registration_id,
        reinterpret_cast<uint64_t>(target.data()), target.size(), true, &stale);
    EXPECT_TRUE(stale_status.IsNeedsRefreshCache()) << stale_status.ToString();
    EXPECT_TRUE(transport.uninstall().ok());
}

TEST(TcpTransportIdentityTest, RefreshesStaleRegistrationBeforeWriteReplay) {
    uint16_t server_port = 0;
    auto server_metadata = startUniqueTcpService(&server_port);
    ASSERT_NE(server_metadata, nullptr);
    std::string server_name = "127.0.0.1:" + std::to_string(server_port);
    TcpTransport server;
    ASSERT_TRUE(server.install(server_name, server_metadata, nullptr).ok());

    std::array<char, 8> target{};
    MemoryOptions options;
    options.perm = kGlobalReadWrite;
    BufferDesc first;
    first.addr = reinterpret_cast<uint64_t>(target.data());
    first.length = target.size();
    ASSERT_TRUE(server.addMemoryBuffer(first, options).ok());
    const std::string first_registration =
        first.transport_attrs.at(TransportType::TCP);
    ASSERT_TRUE(server_metadata->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        segment.name = server_name;
                        segment.rpc_server_addr = server_name;
                        std::get<MemorySegmentDesc>(segment.detail).buffers = {
                            first};
                        return Status::OK();
                    })
                    .ok());

    uint16_t client_port = 0;
    auto client_metadata = startUniqueTcpService(&client_port);
    ASSERT_NE(client_metadata, nullptr);
    TcpTransport client;
    std::string client_name = "127.0.0.1:" + std::to_string(client_port);
    ASSERT_TRUE(client.install(client_name, client_metadata, nullptr).ok());
    ASSERT_TRUE(client_metadata->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        segment.name = client_name;
                        segment.rpc_server_addr = client_name;
                        return Status::OK();
                    })
                    .ok());
    SegmentID target_id = 0;
    ASSERT_TRUE(client_metadata->segmentManager()
                    .openRemote(target_id, server_name)
                    .ok());

    std::array<char, 8> first_source{'f', 'i', 'r', 's', 't', '-', 'v', '2'};
    Request request{};
    request.opcode = Request::WRITE;
    request.source = first_source.data();
    request.target_id = target_id;
    request.target_offset = first.addr;
    request.length = first.length;
    ASSERT_TRUE(TcpTransportTestPeer::transfer(client, request).ok());
    EXPECT_EQ(target, first_source);

    ASSERT_TRUE(server.removeMemoryBuffer(first).ok());
    BufferDesc second;
    second.addr = reinterpret_cast<uint64_t>(target.data());
    second.length = target.size();
    ASSERT_TRUE(server.addMemoryBuffer(second, options).ok());
    ASSERT_NE(first_registration,
              second.transport_attrs.at(TransportType::TCP));
    ASSERT_TRUE(server_metadata->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        std::get<MemorySegmentDesc>(segment.detail).buffers = {
                            second};
                        return Status::OK();
                    })
                    .ok());

    target.fill(0);
    std::array<char, 8> second_source{'s', 'e', 'c', 'o', 'n', 'd', 'v', '2'};
    request.source = second_source.data();
    ASSERT_TRUE(TcpTransportTestPeer::transfer(client, request).ok());
    EXPECT_EQ(target, second_source);

    // Simulate a client snapshot captured before the server published the
    // per-buffer registration. The client must not fall back to v1 when the
    // process advertises v2 support; it refreshes the incomplete snapshot
    // locally and issues only the identified WRITE.
    ASSERT_TRUE(server_metadata->segmentManager()
                    .updateLocal([](SegmentDesc& segment) {
                        std::get<MemorySegmentDesc>(segment.detail)
                            .buffers.back()
                            .transport_attrs.erase(TransportType::TCP);
                        return Status::OK();
                    })
                    .ok());
    ASSERT_TRUE(
        client_metadata->segmentManager().invalidateRemote(target_id).ok());
    SegmentDescRef legacy_snapshot;
    ASSERT_TRUE(client_metadata->segmentManager()
                    .getRemoteCached(legacy_snapshot, target_id)
                    .ok());
    ASSERT_TRUE(server_metadata->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        std::get<MemorySegmentDesc>(segment.detail)
                            .buffers.back()
                            .transport_attrs[TransportType::TCP] =
                            second.transport_attrs.at(TransportType::TCP);
                        return Status::OK();
                    })
                    .ok());
    target.fill(0);
    std::array<char, 8> refreshed_source{'r', 'e', 'f', 'r',
                                         'e', 's', 'h', '2'};
    request.source = refreshed_source.data();
    ASSERT_TRUE(TcpTransportTestPeer::transfer(client, request).ok());
    EXPECT_EQ(target, refreshed_source);

    const std::string current_process_attr =
        server_metadata->segmentManager()
            .getLocal()
            ->getMemory()
            .transport_attrs.at(static_cast<int>(TransportType::TCP));
    auto wrong_process = server_metadata->tcpProcessIdentity();
    wrong_process.low ^= 1;
    ASSERT_TRUE(
        server_metadata->segmentManager()
            .updateLocal([&](SegmentDesc& segment) {
                std::get<MemorySegmentDesc>(segment.detail)
                    .transport_attrs[static_cast<int>(TransportType::TCP)] =
                    encodeTcpProcessAttr(wrong_process);
                return Status::OK();
            })
            .ok());
    ASSERT_TRUE(
        client_metadata->segmentManager().invalidateRemote(target_id).ok());
    SegmentDescRef stale_process_snapshot;
    ASSERT_TRUE(client_metadata->segmentManager()
                    .getRemoteCached(stale_process_snapshot, target_id)
                    .ok());
    ASSERT_TRUE(
        server_metadata->segmentManager()
            .updateLocal([&](SegmentDesc& segment) {
                std::get<MemorySegmentDesc>(segment.detail)
                    .transport_attrs[static_cast<int>(TransportType::TCP)] =
                    current_process_attr;
                return Status::OK();
            })
            .ok());

    target.fill(0);
    std::array<char, 8> third_source{'p', 'r', 'o', 'c', 'e', 's', 's', '2'};
    request.source = third_source.data();
    ASSERT_TRUE(TcpTransportTestPeer::transfer(client, request).ok());
    EXPECT_EQ(target, third_source);

    ASSERT_TRUE(client_metadata->segmentManager().closeRemote(target_id).ok());
    ASSERT_TRUE(server.removeMemoryBuffer(second).ok());
    EXPECT_TRUE(client.uninstall().ok());
    EXPECT_TRUE(server.uninstall().ok());
}

TEST(TcpTransportIdentityTest,
     RefreshesV2MetadataAfterPeerRollsBackToLegacyAtSameEndpoint) {
    std::array<char, 8> target{};
    LegacyTcpPeer legacy(target.data(), target.size());
    ASSERT_TRUE(legacy.available());

    uint16_t client_port = 0;
    auto client_metadata = startUniqueTcpService(&client_port);
    ASSERT_NE(client_metadata, nullptr);
    TcpTransport client;
    std::string client_name = "127.0.0.1:" + std::to_string(client_port);
    ASSERT_TRUE(client.install(client_name, client_metadata, nullptr).ok());
    ASSERT_TRUE(client_metadata->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        segment.name = client_name;
                        segment.rpc_server_addr = client_name;
                        return Status::OK();
                    })
                    .ok());

    SegmentID target_id = 0;
    ASSERT_TRUE(client_metadata->segmentManager()
                    .openRemote(target_id, legacy.address())
                    .ok());
    SegmentDescRef v2_snapshot;
    ASSERT_TRUE(client_metadata->segmentManager()
                    .getRemoteCached(v2_snapshot, target_id)
                    .ok());
    ASSERT_NE(v2_snapshot->getMemory().transport_attrs.find(
                  static_cast<int>(TransportType::TCP)),
              v2_snapshot->getMemory().transport_attrs.end());

    // The cached snapshot still selects SendDataV2, but the same endpoint no
    // longer has that handler. Its pre-dispatch rejection must invalidate the
    // snapshot and safely replay the WRITE through SendData.
    legacy.advertiseV2(false);
    std::array<char, 8> written{'f', 'a', 'l', 'l', 'b', 'a', 'c', 'k'};
    Request request{};
    request.opcode = Request::WRITE;
    request.source = written.data();
    request.target_id = target_id;
    request.target_offset = reinterpret_cast<uint64_t>(target.data());
    request.length = target.size();
    ASSERT_TRUE(TcpTransportTestPeer::transfer(client, request).ok());
    EXPECT_EQ(target, written);
    EXPECT_EQ(legacy.v1Writes(), 1);

    // Recreate a stale v2 cache entry for READ and verify the same endpoint
    // refreshes to the legacy snapshot before copying the response.
    legacy.advertiseV2(true);
    ASSERT_TRUE(
        client_metadata->segmentManager().invalidateRemote(target_id).ok());
    ASSERT_TRUE(client_metadata->segmentManager()
                    .getRemoteCached(v2_snapshot, target_id)
                    .ok());
    legacy.advertiseV2(false);
    target = {'r', 'e', 'a', 'd', '-', 'v', 'o', 'k'};
    std::array<char, 8> received{};
    request.opcode = Request::READ;
    request.source = received.data();
    ASSERT_TRUE(TcpTransportTestPeer::transfer(client, request).ok());
    EXPECT_EQ(received, target);
    EXPECT_EQ(legacy.v1Reads(), 1);

    EXPECT_TRUE(client_metadata->segmentManager().closeRemote(target_id).ok());
    EXPECT_TRUE(client.uninstall().ok());
}

TEST(TcpTransportIdentityTest, RealRpcLeasesDrainRemoveAndUninstall) {
    uint16_t port = 0;
    auto metadata = startUniqueTcpService(&port);
    ASSERT_NE(metadata, nullptr);
    TcpTransport transport;
    std::string name = "127.0.0.1:" + std::to_string(port);
    auto config = std::make_shared<Config>();
    config->set("transports/tcp/allow_unsafe_legacy_data_rpc", true);
    ASSERT_TRUE(transport.install(name, metadata, nullptr, config).ok());
    std::array<char, 8> target{};
    BufferDesc buffer;
    buffer.addr = reinterpret_cast<uint64_t>(target.data());
    buffer.length = target.size();
    MemoryOptions options;
    options.perm = kGlobalReadWrite;
    ASSERT_TRUE(transport.addMemoryBuffer(buffer, options).ok());
    uint64_t registration = 0;
    ASSERT_TRUE(decodeTcpRegistrationAttr(
        buffer.transport_attrs.at(TransportType::TCP), &registration));
    TcpDataPlaneIdentity identity{metadata->tcpProcessIdentity(), registration};
    std::array<char, 8> source{'l', 'e', 'a', 's', 'e', '-', 'v', '2'};

    std::promise<void> write_entered;
    std::promise<void> write_resume;
    auto write_resumed = write_resume.get_future().share();
    TcpBufferRegistryTestPeer::setAcquireHook(metadata->tcpBufferRegistry(),
                                              [&](uint64_t acquired) {
                                                  if (acquired != registration)
                                                      return;
                                                  write_entered.set_value();
                                                  write_resumed.wait();
                                              });
    auto write = std::async(std::launch::async, [&] {
        return ControlClient::sendDataV2(name, buffer.addr, source.data(),
                                         source.size(), identity);
    });
    if (write_entered.get_future().wait_for(std::chrono::seconds(5)) !=
        std::future_status::ready) {
        TcpBufferRegistryTestPeer::setAcquireHook(metadata->tcpBufferRegistry(),
                                                  nullptr);
        write_resume.set_value();
        (void)write.get();
        FAIL() << "V2 WRITE handler did not acquire its registration lease";
        return;
    }

    auto removing = std::async(std::launch::async, [&] {
        return transport.removeMemoryBuffer(buffer);
    });
    const auto remove_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (TcpBufferRegistryTestPeer::hasRegistration(
               metadata->tcpBufferRegistry(), registration) &&
           std::chrono::steady_clock::now() < remove_deadline) {
        std::this_thread::yield();
    }
    if (TcpBufferRegistryTestPeer::hasRegistration(
            metadata->tcpBufferRegistry(), registration)) {
        TcpBufferRegistryTestPeer::setAcquireHook(metadata->tcpBufferRegistry(),
                                                  nullptr);
        write_resume.set_value();
        (void)write.get();
        (void)removing.get();
        FAIL() << "remove did not retire the registration";
        return;
    }
    EXPECT_EQ(removing.wait_for(std::chrono::seconds(0)),
              std::future_status::timeout);
    auto stale = ControlClient::sendDataV2(name, buffer.addr, source.data(),
                                           source.size(), identity);
    EXPECT_TRUE(stale.IsNeedsRefreshCache()) << stale.ToString();

    write_resume.set_value();
    ASSERT_EQ(write.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    EXPECT_TRUE(write.get().ok());
    EXPECT_EQ(target, source);
    ASSERT_EQ(removing.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    EXPECT_TRUE(removing.get().ok());
    TcpBufferRegistryTestPeer::setAcquireHook(metadata->tcpBufferRegistry(),
                                              nullptr);

    ASSERT_TRUE(transport.addMemoryBuffer(buffer, options).ok());
    ASSERT_TRUE(decodeTcpRegistrationAttr(
        buffer.transport_attrs.at(TransportType::TCP), &registration));
    identity.registration_id = registration;
    ASSERT_TRUE(metadata->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        segment.type = SegmentType::Memory;
                        std::get<MemorySegmentDesc>(segment.detail).buffers = {
                            buffer};
                        return Status::OK();
                    })
                    .ok());
    std::array<char, 8> received{};
    std::atomic<bool> block_first_read{true};
    std::promise<void> read_entered;
    std::promise<void> read_resume;
    auto read_resumed = read_resume.get_future().share();
    TcpBufferRegistryTestPeer::setAcquireHook(
        metadata->tcpBufferRegistry(), [&](uint64_t acquired) {
            if (acquired != registration || !block_first_read.exchange(false))
                return;
            read_entered.set_value();
            read_resumed.wait();
        });
    auto read = std::async(std::launch::async, [&] {
        return ControlClient::recvData(name, buffer.addr, received.data(),
                                       received.size());
    });
    if (read_entered.get_future().wait_for(std::chrono::seconds(5)) !=
        std::future_status::ready) {
        TcpBufferRegistryTestPeer::setAcquireHook(metadata->tcpBufferRegistry(),
                                                  nullptr);
        read_resume.set_value();
        (void)read.get();
        FAIL() << "V1 READ handler did not acquire its registration lease";
        return;
    }

    auto uninstalling =
        std::async(std::launch::async, [&] { return transport.uninstall(); });
    EXPECT_EQ(uninstalling.wait_for(std::chrono::milliseconds(100)),
              std::future_status::ready);
    EXPECT_FALSE(
        TcpBufferRegistryTestPeer::closing(metadata->tcpBufferRegistry()));

    std::array<char, 8> concurrent_v1{};
    auto v1 = ControlClient::recvData(name, buffer.addr, concurrent_v1.data(),
                                      concurrent_v1.size());
    EXPECT_TRUE(v1.ok()) << v1.ToString();
    EXPECT_EQ(concurrent_v1, source);
    std::array<char, 8> concurrent_v2{};
    auto v2 = ControlClient::recvDataV2(name, buffer.addr, concurrent_v2.data(),
                                        concurrent_v2.size(), identity);
    EXPECT_TRUE(v2.ok()) << v2.ToString();
    EXPECT_EQ(concurrent_v2, source);

    read_resume.set_value();
    ASSERT_EQ(read.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    EXPECT_TRUE(read.get().ok());
    EXPECT_EQ(received, source);
    auto still_registered = uninstalling.get();
    EXPECT_TRUE(still_registered.IsInvalidArgument())
        << still_registered.ToString();
    TcpBufferRegistryTestPeer::setAcquireHook(metadata->tcpBufferRegistry(),
                                              nullptr);
    EXPECT_TRUE(transport.removeMemoryBuffer(buffer).ok());
    EXPECT_TRUE(
        metadata->segmentManager()
            .updateLocal([](SegmentDesc& segment) {
                std::get<MemorySegmentDesc>(segment.detail).buffers.clear();
                return Status::OK();
            })
            .ok());
    EXPECT_TRUE(transport.uninstall().ok());
}

TEST(TcpTransportIdentityTest, DestructorForceClosesLiveRegistrations) {
    uint16_t port = 0;
    auto metadata = startUniqueTcpService(&port);
    ASSERT_NE(metadata, nullptr);
    const std::string name = "127.0.0.1:" + std::to_string(port);
    std::array<char, 8> target{};
    std::array<char, 8> source{'d', 'e', 's', 't', 'r', 'o', 'y', '!'};
    BufferDesc buffer;
    buffer.addr = reinterpret_cast<uint64_t>(target.data());
    buffer.length = target.size();
    MemoryOptions options;
    options.perm = kGlobalReadWrite;
    TcpDataPlaneIdentity identity;

    {
        auto config = std::make_shared<Config>();
        config->set("transports/tcp/allow_unsafe_legacy_data_rpc", true);
        auto transport = std::make_unique<TcpTransport>();
        std::string mutable_name = name;
        ASSERT_TRUE(
            transport->install(mutable_name, metadata, nullptr, config).ok());
        ASSERT_TRUE(transport->addMemoryBuffer(buffer, options).ok());
        identity.process = metadata->tcpProcessIdentity();
        ASSERT_TRUE(decodeTcpRegistrationAttr(
            buffer.transport_attrs.at(TransportType::TCP),
            &identity.registration_id));
        ASSERT_TRUE(
            metadata->segmentManager()
                .updateLocal([&](SegmentDesc& segment) {
                    segment.type = SegmentType::Memory;
                    std::get<MemorySegmentDesc>(segment.detail).buffers = {
                        buffer};
                    return Status::OK();
                })
                .ok());
    }

    EXPECT_TRUE(
        TcpBufferRegistryTestPeer::closing(metadata->tcpBufferRegistry()));
    auto v2 = ControlClient::sendDataV2(name, buffer.addr, source.data(),
                                        source.size(), identity);
    EXPECT_TRUE(v2.IsNeedsRefreshCache()) << v2.ToString();
    auto v1 = ControlClient::sendData(name, buffer.addr, source.data(),
                                      source.size());
    EXPECT_TRUE(v1.IsNeedsRefreshCache()) << v1.ToString();
    Notification notification{"after-destroy", "must-not-call-transport"};
    auto notified = ControlClient::notify(name, notification);
    EXPECT_FALSE(notified.ok()) << notified.ToString();
    EXPECT_EQ(target, (std::array<char, 8>{}));

    // Reusing the same ControlService also reuses its process incarnation.
    // Terminal teardown must therefore discard every old registration, or a
    // replacement transport would revive a stale (process, registration) pair
    // for memory that may already belong to a different object.
    target.fill('r');
    TcpTransport replacement;
    std::string mutable_name = name;
    ASSERT_TRUE(replacement.install(mutable_name, metadata, nullptr).ok());
    auto stale_after_reinstall = ControlClient::sendDataV2(
        name, buffer.addr, source.data(), source.size(), identity);
    EXPECT_TRUE(stale_after_reinstall.IsNeedsRefreshCache())
        << stale_after_reinstall.ToString();
    EXPECT_TRUE(std::all_of(target.begin(), target.end(),
                            [](char value) { return value == 'r'; }));
    EXPECT_TRUE(replacement.uninstall().ok());
}

TEST(TcpBufferRegistryTest, RemoveDrainsLeaseAndRejectsNewAcquire) {
    TcpBufferRegistry registry;
    std::array<char, 8> target{};
    uint64_t registration = 0;
    ASSERT_TRUE(registry
                    .add(reinterpret_cast<uint64_t>(target.data()),
                         target.size(), kGlobalReadWrite, &registration)
                    .ok());

    TcpBufferRegistry::Lease lease;
    ASSERT_TRUE(registry
                    .acquire(registration,
                             reinterpret_cast<uint64_t>(target.data()),
                             target.size(), true, &lease)
                    .ok());
    auto removing = std::async(std::launch::async,
                               [&] { return registry.remove(registration); });

    Status acquire_after_retire;
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(1);
    do {
        TcpBufferRegistry::Lease candidate;
        acquire_after_retire = registry.acquire(
            registration, reinterpret_cast<uint64_t>(target.data()),
            target.size(), true, &candidate);
        if (acquire_after_retire.IsNeedsRefreshCache()) break;
        ASSERT_TRUE(acquire_after_retire.ok())
            << acquire_after_retire.ToString();
        std::this_thread::yield();
    } while (std::chrono::steady_clock::now() < deadline);
    EXPECT_TRUE(acquire_after_retire.IsNeedsRefreshCache())
        << acquire_after_retire.ToString();
    EXPECT_EQ(removing.wait_for(std::chrono::seconds(0)),
              std::future_status::timeout);

    lease.reset();
    ASSERT_EQ(removing.wait_for(std::chrono::seconds(1)),
              std::future_status::ready);
    EXPECT_TRUE(removing.get().ok());
}

TEST(TcpBufferRegistryTest, CloseRejectsNewAccessAndDrainsExistingLease) {
    TcpBufferRegistry registry;
    std::array<char, 8> target{};
    uint64_t registration = 0;
    ASSERT_TRUE(registry
                    .add(reinterpret_cast<uint64_t>(target.data()),
                         target.size(), kGlobalReadWrite, &registration)
                    .ok());
    TcpBufferRegistry::Lease lease;
    ASSERT_TRUE(registry
                    .acquire(registration,
                             reinterpret_cast<uint64_t>(target.data()),
                             target.size(), true, &lease)
                    .ok());

    auto closing = std::async(std::launch::async, [&] { registry.close(); });
    Status rejected;
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(1);
    do {
        TcpBufferRegistry::Lease candidate;
        rejected = registry.acquire(registration,
                                    reinterpret_cast<uint64_t>(target.data()),
                                    target.size(), true, &candidate);
        if (rejected.IsTooManyRequests()) break;
        ASSERT_TRUE(rejected.ok()) << rejected.ToString();
        std::this_thread::yield();
    } while (std::chrono::steady_clock::now() < deadline);
    EXPECT_TRUE(rejected.IsTooManyRequests()) << rejected.ToString();
    EXPECT_EQ(closing.wait_for(std::chrono::seconds(0)),
              std::future_status::timeout);

    lease.reset();
    ASSERT_EQ(closing.wait_for(std::chrono::seconds(1)),
              std::future_status::ready);
    closing.get();
    EXPECT_TRUE(registry
                    .add(reinterpret_cast<uint64_t>(target.data()),
                         target.size(), kGlobalReadWrite, &registration)
                    .IsTooManyRequests());
    EXPECT_TRUE(registry.remove(registration).ok());
    EXPECT_TRUE(registry.reopen().ok());
}

TEST(TcpTransportCapabilityTest, CpuPlatformCanSelectPeerGpu) {
    if (Platform::getLoader().type() != "cpu") {
        GTEST_SKIP() << "Requires a CPU-only build";
    }

    auto conf = std::make_shared<Config>();
    conf->set("transports/tcp/max_concurrent_tasks", 1);
    auto transport = std::make_shared<TcpTransport>();
    auto metadata = makeP2PMetadata();
    std::string name = "tcp-cpu-peer-gpu";
    ASSERT_TRUE(transport->install(name, metadata, nullptr, conf).ok());

    TransportSelector selector(conf);
    std::array<std::shared_ptr<Transport>, kSupportedTransportTypes>
        transports{};
    transports[TCP] = transport;
    const std::vector<TransportType> tcp_buffer{TCP};
    const std::vector<TransportType> no_tcp_buffer{};

    struct Case {
        const char* name;
        MemoryType local;
        MemoryType peer;
        bool peer_exports_tcp;
        TransportType expected;
    };
    const Case cases[] = {
        {"host_peer_host", MTYPE_CPU, MTYPE_CPU, true, TCP},
        {"host_peer_gpu", MTYPE_CPU, MTYPE_CUDA, true, TCP},
        {"host_peer_gpu_without_tcp", MTYPE_CPU, MTYPE_CUDA, false, UNSPEC},
        {"local_gpu_peer_host", MTYPE_CUDA, MTYPE_CPU, true, UNSPEC},
        {"local_gpu_peer_gpu", MTYPE_CUDA, MTYPE_CUDA, true, UNSPEC},
    };
    // These are selection metadata, not device allocations or GPU transfers.
    // READ and WRITE keep the same local/peer roles in SelectionContext.
    for (const auto& test : cases) {
        SCOPED_TRACE(test.name);
        SelectionContext context{};
        context.segment_type = SegmentType::Memory;
        context.local_memory_type = test.local;
        context.remote_memory_type = test.peer;
        context.buffer_transports =
            test.peer_exports_tcp ? &tcp_buffer : &no_tcp_buffer;
        context.transfer_size = 1;
        EXPECT_EQ(selector.select(context, transports, 0, TCP).transport,
                  test.expected);
    }

    EXPECT_TRUE(transport->uninstall().ok());
}

TEST(TcpSubBatchTest, PointerStabilityAfterReserve) {
    // allocateSubBatch reserves task_list to max_size so submitTransferTasks
    // can take stable TcpTask* after emplace.
    TcpSubBatch batch;
    batch.max_size = 8;
    batch.task_list.reserve(batch.max_size);

    batch.task_list.emplace_back();
    TcpTask* first = &batch.task_list[0];
    first->target_addr = 42;

    for (int i = 1; i < 8; ++i) {
        batch.task_list.emplace_back();
    }

    EXPECT_EQ(first->target_addr, 42u);
    EXPECT_EQ(&batch.task_list[0], first);
}

TEST(TcpRetryBackoffTest, ExponentialGrowthWithCap) {
    const uint64_t base = 100;
    const uint64_t cap = 2000;
    uint64_t delay = base;

    std::vector<uint64_t> delays;
    for (int attempt = 0; attempt < 6; ++attempt) {
        delays.push_back(delay);
        delay = nextTcpRetryDelay(delay, cap);
    }

    EXPECT_EQ(delays[0], 100u);
    EXPECT_EQ(delays[1], 200u);
    EXPECT_EQ(delays[2], 400u);
    EXPECT_EQ(delays[3], 800u);
    EXPECT_EQ(delays[4], 1600u);
    EXPECT_EQ(delays[5], 2000u);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
