// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_direct_runtime.h"
#include "client_service.h"
#include "file_storage.h"
#include "ssd_control_rpc.h"
#include "ssd_fixed_l2_requester.h"
#include "ssd_peer_directory.h"
#include "ssd_streaming_config.h"
#include "ssd_file_storage_owner_reader.h"
#include "ssd_transfer_target.h"
#include "environment_variables.h"
#include "environ.h"
#include "random.h"
#include <json/json.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <algorithm>
#include <limits>
#include <map>
#include <mutex>
#include <stdexcept>

namespace mooncake::ssd_offload {
namespace {
std::string Numeric(std::string host) {
    addrinfo hints{}, *list = nullptr;
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(host.c_str(), nullptr, &hints, &list) != 0) {
        hints.ai_family = AF_INET6;
        if (getaddrinfo(host.c_str(), nullptr, &hints, &list) != 0)
            throw std::invalid_argument(
                "Unable to resolve SSD control host during setup");
    }
    std::unique_ptr<addrinfo, decltype(&freeaddrinfo)> cleanup(list,
                                                               freeaddrinfo);
    char buffer[INET6_ADDRSTRLEN];
    const void* address =
        list->ai_family == AF_INET
            ? static_cast<const void*>(
                  &reinterpret_cast<sockaddr_in*>(list->ai_addr)->sin_addr)
            : static_cast<const void*>(
                  &reinterpret_cast<sockaddr_in6*>(list->ai_addr)->sin6_addr);
    if (!inet_ntop(list->ai_family, address, buffer, sizeof(buffer)))
        throw std::invalid_argument("Invalid SSD control address");
    return buffer;
}
using Peer = SsdPeerDirectoryEntry;
std::optional<Peer> Parse(const std::string& bytes) {
    if (bytes.size() > 16384) return {};
    Json::Value value;
    Json::CharReaderBuilder builder;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    std::string errors;
    if (!reader->parse(bytes.data(), bytes.data() + bytes.size(), &value,
                       &errors))
        return {};
    for (const auto* field :
         {"client_id_high", "client_id_low", "boot_epoch", "credential_high",
          "credential_low", "control_port"})
        if (!value[field].isUInt64()) return {};
    for (const auto* field : {"tenant", "transport_endpoint", "legacy_endpoint",
                              "control_host", "protocol"})
        if (!value[field].isString()) return {};
    if (!value["can_read_ssd"].isBool() || !value["control_port"].asUInt64() ||
        value["control_port"].asUInt64() > UINT16_MAX)
        return {};
    // An old owner did not advertise this field and can decode only its
    // historic 64-object batch. Never send it a larger unknown write attempt.
    uint64_t max_objects = 64;
    if (value.isMember("max_batch_objects")) {
        if (!value["max_batch_objects"].isUInt64() ||
            !value["max_batch_objects"].asUInt64() ||
            value["max_batch_objects"].asUInt64() > 65536)
            return {};
        max_objects = value["max_batch_objects"].asUInt64();
    }
    Peer peer;
    peer.fixed = {
        {value["client_id_high"].asUInt64(), value["client_id_low"].asUInt64(),
         value["boot_epoch"].asUInt64()},
        value["tenant"].asString(),
        value["transport_endpoint"].asString(),
        {value["credential_high"].asUInt64(),
         value["credential_low"].asUInt64()}};
    peer.protocol = value["protocol"].asString();
    if (peer.protocol != "tcp" && peer.protocol != "rdma") return {};
    auto host = value["control_host"].asString();
    in_addr v4{};
    in6_addr v6{};
    if (host.find('\0') != std::string::npos ||
        (inet_pton(AF_INET, host.c_str(), &v4) != 1 &&
         inet_pton(AF_INET6, host.c_str(), &v6) != 1))
        return {};
    peer.readable = value["can_read_ssd"].asBool();
    peer.route = {peer.fixed.identity,
                  peer.fixed.tenant_id,
                  value["legacy_endpoint"].asString(),
                  host,
                  uint16_t(value["control_port"].asUInt64()),
                  peer.fixed.transport_endpoint,
                  {1,
                   peer.fixed.identity,
                   peer.readable ? uint64_t{15} : uint64_t{0},
                   max_objects,
                   1024ULL * 1024 * 1024,
                   65536,
                   1024ULL * 1024 * 1024,
                   1024 * 1024,
                   {peer.protocol}}};
    return peer;
}
}  // namespace
struct SsdDirectRuntime::Impl {
    struct Region {
        void* base;
        size_t bytes;
        DestinationRegion region;
    };
    std::shared_ptr<TransferEngine> engine;
    SsdReadProtocolLimits wire;
    Peer self;
    std::shared_ptr<SsdPeerDirectory> directory;
    std::shared_ptr<SsdDirectReadOwner> owner;
    std::unique_ptr<SsdReadIntoRpcEndpoint> endpoint;
    std::unique_ptr<SsdControlRpcServer> server;
    std::unique_ptr<SsdFixedL2Requester> requester;
    mutable std::mutex mutex;
    std::map<uintptr_t, Region> regions;
    std::vector<Peer> peers;
    bool closing{false};
    bool closed{false};
};
bool SsdDirectRuntime::Enabled(const std::shared_ptr<Client>& client) {
    return client &&
           Environ::ReadOr(
               FileStorageEnvironmentVariables::MOONCAKE_OFFLOAD_SSD_PUSH,
               false) &&
           (client->GetProtocol() == "tcp" ||
            client->GetProtocol() == "rdma") &&
           !client->GetTransferEngine()->isUsingTent();
}
SsdDirectRuntime::SsdDirectRuntime(std::shared_ptr<Client> client,
                                   std::shared_ptr<FileStorage> storage,
                                   std::string legacy, std::string host)
    : impl_(std::make_unique<Impl>()) {
    impl_->engine = client->GetTransferEngine();
    StreamingReadConfig stream;
    stream.push_enabled = true;
    impl_->wire = stream.ProtocolLimits();
    impl_->self.protocol = client->GetProtocol();
    auto& self = impl_->self;
    self.fixed.identity = {client->getClientId().first,
                           client->getClientId().second, 0};
    if (!secureRandomBytes(std::as_writable_bytes(
            std::span(&self.fixed.identity.boot_epoch, 1))) ||
        !self.fixed.identity.boot_epoch ||
        !secureRandomBytes(
            std::as_writable_bytes(std::span(&self.fixed.credential, 1))) ||
        self.fixed.credential == WriteCredential{})
        throw std::runtime_error("SSD bootstrap entropy unavailable");
    self.fixed.tenant_id = client->tenant_id();
    self.fixed.transport_endpoint = client->GetTransportEndpoint();
    self.readable = storage && storage->SupportsOwnerStreamingRead();
    self.route = {self.fixed.identity,
                  self.fixed.tenant_id,
                  std::move(legacy),
                  Numeric(std::move(host)),
                  0,
                  self.fixed.transport_endpoint,
                  {1,
                   self.fixed.identity,
                   self.readable ? uint64_t{15} : uint64_t{0},
                   uint64_t(impl_->wire.max_objects),
                   1024ULL * 1024 * 1024,
                   65536,
                   1024ULL * 1024 * 1024,
                   1024 * 1024,
                   {self.protocol}}};
    if (const auto path =
            Environ::Read(FileStorageEnvironmentVariables::
                              MOONCAKE_OFFLOAD_SSD_PEER_DIRECTORY)) {
        // Deployment configuration, not an application protocol. This private
        // directory is the authority for peer credentials and reciprocal
        // bootstrap; public TE metadata never carries these secrets.
        impl_->directory = std::make_shared<SsdPeerDirectory>(*path);
    }
    SsdDirectReadOwner::TrustedPeerResolver resolver;
    if (impl_->directory)
        resolver = [directory = impl_->directory, tenant = self.fixed.tenant_id,
                    protocol = self.protocol](
                       PeerIdentity identity) -> std::optional<SsdFixedL2Peer> {
            auto resolved =
                directory->LookupRequester(identity, tenant, protocol);
            if (!resolved.peer) return {};
            return std::move(resolved.peer->fixed);
        };
    impl_->owner = std::make_shared<SsdDirectReadOwner>(
        self.fixed.identity, std::vector<SsdFixedL2Peer>{self.fixed},
        [engine = impl_->engine, protocol = self.protocol](auto views) {
            return protocol == "rdma"
                       ? PrepareSsdRdmaWriteTarget(engine, std::move(views))
                             .target
                       : PrepareSsdTcpWriteTarget(engine, std::move(views))
                             .target;
        },
        MakeFileStorageOwnerReader(std::move(storage)), SsdControlLimits{},
        impl_->wire, std::move(resolver));
    impl_->endpoint =
        std::make_unique<SsdReadIntoRpcEndpoint>(impl_->owner, impl_->wire);
    impl_->server = std::make_unique<SsdControlRpcServer>(
        2, 0,
        self.route.control_host.find(':') == std::string::npos ? "0.0.0.0"
                                                               : "::");
    impl_->server->register_handler<&SsdReadIntoRpcEndpoint::Call>(
        impl_->endpoint.get());
    impl_->server->async_start();
    if (impl_->server->get_errc())
        throw std::runtime_error("SSD direct RPC startup failed");
    self.route.control_port = impl_->server->port();
    DestinationRegistryLimits registrations;
    // Layer-first Fluent host pools can exceed 1024 individually registered
    // regions. Raise only the Store adapter; advanced API defaults stay fixed.
    registrations.max_regions = 65536;
    impl_->requester = std::make_unique<SsdFixedL2Requester>(
        self.fixed, self.protocol, MakeSsdRequesterControlCall(impl_->wire),
        registrations, impl_->wire);
    impl_->peers = {self};
    if (impl_->directory &&
        impl_->directory->Publish(self) != DestinationError::kOk)
        throw std::runtime_error("SSD trusted peer publication failed");
}
SsdDirectRuntime::~SsdDirectRuntime() { Close(); }
std::string SsdDirectRuntime::PeerInfo() const {
    Json::Value value;
    const auto& self = impl_->self;
    value["client_id_high"] = Json::UInt64(self.fixed.identity.client_id_high);
    value["client_id_low"] = Json::UInt64(self.fixed.identity.client_id_low);
    value["boot_epoch"] = Json::UInt64(self.fixed.identity.boot_epoch);
    value["credential_high"] = Json::UInt64(self.fixed.credential.high);
    value["credential_low"] = Json::UInt64(self.fixed.credential.low);
    value["tenant"] = self.fixed.tenant_id;
    value["transport_endpoint"] = self.fixed.transport_endpoint;
    value["legacy_endpoint"] = self.route.legacy_rpc_endpoint;
    value["control_host"] = self.route.control_host;
    value["control_port"] = Json::UInt64(self.route.control_port);
    value["protocol"] = self.protocol;
    value["can_read_ssd"] = self.readable;
    value["max_batch_objects"] =
        Json::UInt64(self.route.capability.max_batch_objects);
    value["read_requests"] = Json::UInt64(impl_->endpoint->read_requests());
    auto stats = impl_->requester->stats();
    value["pending_requests"] = Json::UInt64(stats.active_attempts);
    value["registered_l2_pools"] = Json::UInt64(stats.regions);
    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";
    return Json::writeString(builder, value);
}
int SsdDirectRuntime::ConfigurePeers(
    const std::vector<std::string>& documents) {
    if (documents.size() > 63) return -1;
    std::vector<Peer> peers{impl_->self};
    std::vector<SsdFixedL2Peer> fixed{impl_->self.fixed};
    for (const auto& doc : documents) {
        auto peer = Parse(doc);
        if (!peer || peer->fixed.tenant_id != impl_->self.fixed.tenant_id ||
            peer->protocol != impl_->self.protocol)
            return -1;
        fixed.push_back(peer->fixed);
        peers.push_back(std::move(*peer));
    }
    std::lock_guard lock(impl_->mutex);
    if (impl_->closing || impl_->requester->stats().active_attempts) return -1;
    if (impl_->owner->SetPeers(std::move(fixed)) != DestinationError::kOk)
        return -1;
    impl_->peers = std::move(peers);
    return 0;
}
int SsdDirectRuntime::ImportFixedPool(void* base, size_t bytes,
                                      std::shared_ptr<void> lifetime) {
    std::lock_guard lock(impl_->mutex);
    if (impl_->closing) return -1;
    if (!lifetime)
        lifetime = impl_->engine;  // External L2 mapping is permanently owned
                                   // by the caller.
    const auto address = reinterpret_cast<uint64_t>(base);
    auto registered = impl_->requester->Register(
        {address, bytes, address, bytes, DestinationMemory::kHost},
        std::move(lifetime));
    if (registered.error != DestinationError::kOk) return -1;
    try {
        impl_->regions.emplace(uintptr_t(base),
                               Impl::Region{base, bytes, registered.region});
    } catch (...) {
        impl_->requester->Revoke(registered.region);
        throw;
    }
    return 0;
}
int SsdDirectRuntime::RevokeFixedPool(void* base) {
    std::lock_guard lock(impl_->mutex);
    auto found = impl_->regions.find(uintptr_t(base));
    if (found == impl_->regions.end()) return 0;
    const auto revoked = impl_->requester->Revoke(found->second.region);
    if (revoked != DestinationError::kOk &&
        revoked != DestinationError::kNotFound)
        return -1;
    impl_->regions.erase(found);
    return 0;
}
bool SsdDirectRuntime::HasPending(void* base, size_t bytes) const {
    return impl_->requester->HasPendingWrite(reinterpret_cast<uint64_t>(base),
                                             bytes);
}
std::optional<tl::expected<void, ErrorCode>> SsdDirectRuntime::Read(
    const std::string& endpoint, const std::vector<SsdRuntimeObject>& objects,
    std::chrono::milliseconds timeout) {
    const auto start = std::chrono::steady_clock::now();
    if (timeout.count() <= 0) return tl::unexpected(ErrorCode::INVALID_PARAMS);
    // Reject overlapping destinations and route sparse small TCP batches to
    // legacy before constructing a wire request or exposing any destination
    // lease. Push still validates every registered region before submission.
    if (impl_->self.protocol == "tcp" && objects.size() >= 4) {
        struct Span {
            uintptr_t address;
            size_t length;
        };
        std::vector<Span> spans;
        size_t payload = 0, destinations = 0;
        bool eligible = true;
        for (const auto& object : objects) {
            if (object.source_offset != 0) {
                eligible = false;
                break;
            }
            size_t object_bytes = 0;
            for (const auto& slice : object.destinations) {
                const auto address = reinterpret_cast<uintptr_t>(slice.ptr);
                if (slice.size > object.expected_size - object_bytes ||
                    slice.size > std::numeric_limits<size_t>::max() - payload ||
                    slice.size >
                        std::numeric_limits<uintptr_t>::max() - address)
                    return tl::unexpected(ErrorCode::INVALID_PARAMS);
                object_bytes += slice.size;
                payload += slice.size;
                ++destinations;
                spans.push_back({address, slice.size});
            }
            if (object_bytes != object.expected_size) eligible = false;
        }
        if (eligible && payload <= 4 * 1024 * 1024 && destinations >= 4) {
            std::sort(spans.begin(), spans.end(),
                      [](const Span& a, const Span& b) {
                          return a.address < b.address;
                      });
            size_t adjacent = 0;
            for (size_t i = 1; i < spans.size(); ++i) {
                const auto end = spans[i - 1].address + spans[i - 1].length;
                if (spans[i].address < end)
                    return tl::unexpected(ErrorCode::INVALID_PARAMS);
                adjacent += spans[i].address == end;
            }
            if (adjacent * 4 < spans.size() - 1) return {};
        }
    }
    SsdPushPeerInfo route;
    std::vector<SsdObjectReadV1> request;
    {
        std::lock_guard lock(impl_->mutex);
        if (impl_->closing) return tl::unexpected(ErrorCode::INVALID_PARAMS);
        auto peer = std::find_if(
            impl_->peers.begin(), impl_->peers.end(), [&](const auto& p) {
                return p.readable && p.route.legacy_rpc_endpoint == endpoint;
            });
        if (peer != impl_->peers.end()) {
            route = peer->route;
        } else if (impl_->directory) {
            auto resolved = impl_->directory->LookupLegacy(
                endpoint, impl_->self.fixed.tenant_id, impl_->self.protocol);
            if (resolved.error != DestinationError::kOk) {
                if (resolved.error == DestinationError::kNotFound) return {};
                return tl::unexpected(ErrorCode::INVALID_PARAMS);
            }
            route = std::move(resolved.peer->route);
        } else {
            return {};
        }
        for (const auto& object : objects) {
            SsdReadRangeV1 range{object.source_offset, {}};
            for (const auto& slice : object.destinations) {
                const auto address = uintptr_t(slice.ptr);
                auto region = impl_->regions.upper_bound(address);
                if (region == impl_->regions.begin()) return {};
                --region;
                const auto offset = address - uintptr_t(region->second.base);
                if (offset > region->second.bytes ||
                    slice.size > region->second.bytes - offset)
                    return {};
                range.destinations.push_back(
                    {region->second.region, offset, slice.size});
            }
            request.push_back(
                {object.key, object.expected_size, {}, {std::move(range)}});
        }
    }
    const auto remaining =
        timeout - std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - start);
    if (remaining.count() <= 0) return tl::unexpected(ErrorCode::TRANSFER_FAIL);
    // The Store API borrows pages owned by an external allocator. Even an
    // error return lets that allocator reuse them immediately; keep uncertain
    // writes and their recovery inside Mooncake until destination-safe drain.
    auto result = impl_->requester->ReadAndDrain(
        route, std::move(request),
        std::min(remaining, std::chrono::milliseconds(60000)));
    if (result.error == DestinationError::kOk)
        return tl::expected<void, ErrorCode>{};
    if (!result.may_have_submitted) {
        if (result.error == DestinationError::kOverlap)
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        return {};
    }
    // A valid terminal rejection means no writes were accepted and safely
    // permits the existing legacy path. Unknown/partial writes never fall back.
    if (result.allow_fallback) return {};
    LOG(ERROR) << "SSD push failed control_error="
               << static_cast<int>(result.error)
               << " request=" << result.attempt.request_id
               << " terminal=" << bool(result.result);
    if (result.result)
        for (const auto& object : result.result->objects)
            LOG(ERROR) << "SSD push object=" << object.original_index
                       << " outcome=" << static_cast<int>(object.outcome)
                       << " valid_bytes=" << object.valid_bytes;
    return tl::unexpected(result.result ? ErrorCode::FILE_READ_FAIL
                                        : ErrorCode::INVALID_PARAMS);
}
int SsdDirectRuntime::Recover() {
    int pending = 0;
    for (const auto& attempt : impl_->requester->PendingAttempts()) {
        auto result =
            impl_->requester->Recover(attempt, std::chrono::seconds(1));
        if (!result.result) ++pending;
    }
    return pending;
}
int SsdDirectRuntime::Close() {
    {
        std::lock_guard lock(impl_->mutex);
        if (impl_->closed) return 0;
        impl_->closing = true;
    }
    // An engine token pins the MR, but cannot own an application tensor.
    // Keep the recovery entry point and services until all old writes drain.
    if (impl_->requester && impl_->requester->Close() != DestinationError::kOk)
        return -1;
    if (impl_->endpoint) impl_->endpoint->CloseAndDrain();
    if (impl_->server) impl_->server->stop();
    std::lock_guard lock(impl_->mutex);
    // Store owns the explicit pool registration even when the caller keeps
    // the TE facade alive. Remove writable MRs before acknowledging close.
    for (auto region = impl_->regions.begin();
         region != impl_->regions.end();) {
        if (impl_->engine->unregisterLocalMemory(region->second.base, true) !=
            0)
            return -1;
        region = impl_->regions.erase(region);
    }
    if (impl_->directory &&
        impl_->directory->Unpublish() != DestinationError::kOk)
        return -1;
    impl_->closed = true;
    return 0;
}
}  // namespace mooncake::ssd_offload
