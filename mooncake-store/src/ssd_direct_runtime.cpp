// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_direct_runtime.h"

#include <arpa/inet.h>
#include <glog/logging.h>
#include <json/json.h>
#include <netdb.h>

#include <algorithm>
#include <limits>
#include <map>
#include <mutex>
#include <stdexcept>
#include <utility>

#include "client_service.h"
#include "environ.h"
#include "environment_variables.h"
#include "file_storage.h"
#include "random.h"
#include "ssd_control_rpc.h"
#include "ssd_file_storage_owner_reader.h"
#include "ssd_fixed_l2_requester.h"
#include "ssd_peer_directory.h"
#include "ssd_streaming_config.h"
#include "ssd_transfer_target.h"

namespace mooncake::ssd_offload {
namespace {

constexpr size_t kMaxConfiguredPeers = 256;

std::string ResolveNumericControlHost(const std::string& host) {
    if (host.empty() || host.find('\0') != std::string::npos) {
        throw std::invalid_argument("Empty SSD control host during setup");
    }
    addrinfo hints{}, *list = nullptr;
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(host.c_str(), nullptr, &hints, &list) != 0) {
        hints.ai_family = AF_INET6;
        if (getaddrinfo(host.c_str(), nullptr, &hints, &list) != 0) {
            throw std::invalid_argument(
                "Unable to resolve SSD control host during setup");
        }
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
    if (!inet_ntop(list->ai_family, address, buffer, sizeof(buffer))) {
        throw std::invalid_argument("Invalid SSD control address");
    }
    std::string resolved(buffer);
    if (!IsNonWildcardNumericHost(resolved)) {
        throw std::invalid_argument(
            "SSD control host must be a non-wildcard private address");
    }
    return resolved;
}

using Peer = SsdPeerDirectoryEntry;

bool SameFixedBinding(const SsdFixedL2Peer& a, const SsdFixedL2Peer& b) {
    return a.identity == b.identity && a.tenant_id == b.tenant_id &&
           a.transport_endpoint == b.transport_endpoint &&
           a.credential == b.credential;
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
    // Keep server declared before endpoint so reverse destruction drains
    // endpoint worker threads while the coro_rpc server and I/O executor are
    // still alive (including if constructor setup throws).
    std::unique_ptr<SsdControlRpcServer> server;
    std::unique_ptr<SsdReadIntoRpcEndpoint> endpoint;
    std::unique_ptr<SsdFixedL2Requester> requester;

    mutable std::mutex mutex;
    std::map<uintptr_t, Region> regions;
    std::vector<Peer> peers;
    size_t in_flight_reads{0};
    bool closing{false};
    bool closed{false};

    bool CoversRangeLocked(const void* ptr, size_t bytes) const {
        if (!ptr) return false;
        const auto address = reinterpret_cast<uintptr_t>(ptr);
        if (bytes > std::numeric_limits<uintptr_t>::max() - address) {
            return false;
        }
        auto region = regions.upper_bound(address);
        if (region == regions.begin()) return false;
        --region;
        const auto base = reinterpret_cast<uintptr_t>(region->second.base);
        const auto offset = address - base;
        return offset <= region->second.bytes &&
               bytes <= region->second.bytes - offset;
    }

    std::optional<SsdPushPeerInfo> ResolveReadablePeer(
        const std::string& legacy_endpoint, DestinationError& error) const {
        error = DestinationError::kNotFound;
        {
            std::lock_guard lock(mutex);
            if (closing) {
                error = DestinationError::kClosing;
                return std::nullopt;
            }
            const auto found =
                std::find_if(peers.begin(), peers.end(), [&](const Peer& peer) {
                    return peer.readable &&
                           peer.route.legacy_rpc_endpoint == legacy_endpoint;
                });
            if (found != peers.end()) {
                error = DestinationError::kOk;
                return found->route;
            }
        }
        // Perform peer directory I/O outside impl_->mutex so concurrent
        // registrations and lookups never block on filesystem latency.
        if (directory) {
            auto resolved = directory->LookupLegacy(
                legacy_endpoint, self.fixed.tenant_id, self.protocol);
            error = resolved.error;
            if (resolved.error == DestinationError::kOk && resolved.peer) {
                return std::move(resolved.peer->route);
            }
        }
        return std::nullopt;
    }
};

bool SsdDirectRuntime::Enabled(const std::shared_ptr<Client>& client) {
    const auto engine = client ? client->GetTransferEngine() : nullptr;
    return client && engine &&
           Environ::ReadOr(
               FileStorageEnvironmentVariables::MOONCAKE_OFFLOAD_SSD_PUSH,
               false) &&
           client->GetProtocol() == "tcp" && !engine->isUsingTent();
}

SsdDirectRuntime::SsdDirectRuntime(std::shared_ptr<Client> client,
                                   std::shared_ptr<FileStorage> storage,
                                   std::string legacy, std::string host)
    : impl_(std::make_unique<Impl>()) {
    const auto engine = client ? client->GetTransferEngine() : nullptr;
    if (!client || !engine || client->GetProtocol() != "tcp" ||
        engine->isUsingTent()) {
        throw std::invalid_argument(
            "SSD direct runtime requires classic TCP client");
    }
    impl_->engine = engine;

    StreamingReadConfig stream;
    if (storage) {
        stream = storage->config_.streaming_read;
    }
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
        self.fixed.credential == WriteCredential{}) {
        throw std::runtime_error("SSD bootstrap entropy unavailable");
    }
    self.fixed.tenant_id = client->tenant_id();
    self.fixed.transport_endpoint = client->GetTransportEndpoint();
    self.readable = storage && storage->SupportsOwnerStreamingRead();
    self.route = {self.fixed.identity,
                  self.fixed.tenant_id,
                  std::move(legacy),
                  ResolveNumericControlHost(host),
                  0,
                  self.fixed.transport_endpoint,
                  {1,
                   self.fixed.identity,
                   self.readable ? uint64_t{15} : uint64_t{0},
                   static_cast<uint64_t>(impl_->wire.max_objects),
                   1024ULL * 1024 * 1024,
                   65536,
                   1024ULL * 1024 * 1024,
                   stream.slot_bytes,
                   {self.protocol}}};

    if (const auto path =
            Environ::Read(FileStorageEnvironmentVariables::
                              MOONCAKE_OFFLOAD_SSD_PEER_DIRECTORY)) {
        // Deployment-owned private directory for peer credentials and
        // reciprocal bootstrap; public TE metadata never carries secrets.
        impl_->directory = std::make_shared<SsdPeerDirectory>(*path);
    }

    if (self.readable) {
        SsdDirectReadOwner::TrustedPeerResolver resolver;
        if (impl_->directory) {
            resolver =
                [directory = impl_->directory, tenant = self.fixed.tenant_id,
                 protocol = self.protocol](
                    PeerIdentity identity) -> std::optional<SsdFixedL2Peer> {
                auto resolved =
                    directory->LookupRequester(identity, tenant, protocol);
                if (!resolved.peer) return std::nullopt;
                return std::move(resolved.peer->fixed);
            };
        }
        SsdControlLimits owner_limits;
        owner_limits.max_sessions = kMaxConfiguredPeers;
        impl_->owner = std::make_shared<SsdDirectReadOwner>(
            self.fixed.identity, std::vector<SsdFixedL2Peer>{self.fixed},
            [engine = impl_->engine](std::vector<ResolvedDestination> views) {
                return PrepareSsdTcpWriteTarget(engine, std::move(views))
                    .target;
            },
            MakeFileStorageOwnerReader(std::move(storage)), owner_limits,
            impl_->wire, std::move(resolver));
        // Bind strictly to the configured private control_host with a finite
        // idle connection timeout (no 0.0.0.0/:: wildcard bind or unbounded
        // slowloris sockets).
        impl_->server = std::make_unique<SsdControlRpcServer>(
            2, 0, self.route.control_host, std::chrono::seconds(60));
        impl_->endpoint =
            std::make_unique<SsdReadIntoRpcEndpoint>(impl_->owner, impl_->wire);
        impl_->server->register_handler<&SsdReadIntoRpcEndpoint::Call>(
            impl_->endpoint.get());
        impl_->server->async_start();
        if (impl_->server->get_errc()) {
            throw std::runtime_error("SSD direct RPC startup failed");
        }
        self.route.control_port = impl_->server->port();
    }

    DestinationRegistryLimits registrations;
    // Per-layer host pools can exceed 1024 individually registered regions.
    // Raise only the Store runtime limit while keeping core API defaults small.
    registrations.max_regions = 65536;
    impl_->requester = std::make_unique<SsdFixedL2Requester>(
        self.fixed, self.protocol, MakeSsdRequesterControlCall(impl_->wire),
        registrations, impl_->wire);
    impl_->peers = {self};

    if (impl_->directory &&
        impl_->directory->Publish(self) != DestinationError::kOk) {
        throw std::runtime_error("SSD trusted peer publication failed");
    }
}

SsdDirectRuntime::~SsdDirectRuntime() {
    (void)Close(/*force_quarantine=*/true);
}

std::string SsdDirectRuntime::PeerInfo() const {
    return EncodeSsdPeerDocument(impl_->self);
}

std::string SsdDirectRuntime::StatsJson() const {
    Json::Value value;
    const auto& self = impl_->self;
    value["can_read_ssd"] = self.readable;
    value["control_host"] = self.route.control_host;
    value["control_port"] = Json::UInt64(self.route.control_port);
    value["preferred_chunk_bytes"] =
        Json::UInt64(self.route.capability.preferred_chunk_bytes);
    value["read_requests"] =
        Json::UInt64(impl_->endpoint ? impl_->endpoint->read_requests() : 0);
    const auto stats = impl_->requester->stats();
    value["pending_requests"] = Json::UInt64(stats.active_attempts);
    value["registered_l2_pools"] = Json::UInt64(stats.regions);
    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";
    return Json::writeString(builder, value);
}

int SsdDirectRuntime::ConfigurePeers(
    const std::vector<std::string>& documents) {
    if (documents.size() > kMaxConfiguredPeers) return -1;
    std::vector<Peer> peers{impl_->self};
    std::vector<SsdFixedL2Peer> fixed{impl_->self.fixed};
    for (const auto& doc : documents) {
        auto peer = DecodeSsdPeerDocument(doc);
        if (!peer || peer->fixed.tenant_id != impl_->self.fixed.tenant_id ||
            peer->protocol != impl_->self.protocol) {
            return -1;
        }
        // Deduplicate cluster-wide peer lists that include self or repeats;
        // fail closed if the same identity carries a conflicting binding.
        const auto existing =
            std::find_if(peers.begin(), peers.end(), [&](const Peer& known) {
                return known.fixed.identity == peer->fixed.identity;
            });
        if (existing != peers.end()) {
            if (!SameFixedBinding(existing->fixed, peer->fixed) ||
                existing->readable != peer->readable ||
                existing->route.control_host != peer->route.control_host ||
                existing->route.control_port != peer->route.control_port ||
                existing->route.legacy_rpc_endpoint !=
                    peer->route.legacy_rpc_endpoint) {
                return -1;
            }
            continue;
        }
        if (fixed.size() >= kMaxConfiguredPeers) return -1;
        fixed.push_back(peer->fixed);
        peers.push_back(std::move(*peer));
    }
    std::lock_guard lock(impl_->mutex);
    if (impl_->closing) return -1;
    if (impl_->owner) {
        // Preflight against already-configured immutable bindings before
        // upserting so a conflict cannot partially update the owner table.
        for (const auto& candidate : fixed) {
            const auto prev = std::find_if(
                impl_->peers.begin(), impl_->peers.end(), [&](const Peer& old) {
                    return old.fixed.identity == candidate.identity;
                });
            if (prev != impl_->peers.end() &&
                !SameFixedBinding(prev->fixed, candidate)) {
                return -1;
            }
        }
        for (const auto& candidate : fixed) {
            if (impl_->owner->UpsertPeer(candidate) != DestinationError::kOk) {
                return -1;
            }
        }
    }
    impl_->peers = std::move(peers);
    return 0;
}

int SsdDirectRuntime::ImportFixedPool(void* base, size_t bytes,
                                      std::shared_ptr<void> lifetime) {
    std::lock_guard lock(impl_->mutex);
    if (impl_->closing) return -1;
    if (!lifetime) {
        lifetime = impl_->engine;
    }
    const auto address = reinterpret_cast<uint64_t>(base);
    auto registered = impl_->requester->Register(
        {address, bytes, address, bytes, DestinationMemory::kHost},
        std::move(lifetime));
    if (registered.error != DestinationError::kOk) return -1;
    try {
        impl_->regions.emplace(reinterpret_cast<uintptr_t>(base),
                               Impl::Region{base, bytes, registered.region});
    } catch (...) {
        impl_->requester->Revoke(registered.region);
        throw;
    }
    return 0;
}

int SsdDirectRuntime::RevokeFixedPool(void* base) {
    std::lock_guard lock(impl_->mutex);
    auto found = impl_->regions.find(reinterpret_cast<uintptr_t>(base));
    if (found == impl_->regions.end()) return 0;
    const auto revoked = impl_->requester->Revoke(found->second.region);
    if (revoked != DestinationError::kOk &&
        revoked != DestinationError::kNotFound) {
        return -1;
    }
    impl_->regions.erase(found);
    return 0;
}

bool SsdDirectRuntime::HasPending(void* base, size_t bytes) const {
    return impl_->requester->HasPendingWrite(reinterpret_cast<uint64_t>(base),
                                             bytes);
}

bool SsdDirectRuntime::HasPendingAttempts() const {
    {
        std::lock_guard lock(impl_->mutex);
        if (impl_->in_flight_reads > 0) return true;
    }
    return impl_->requester && !impl_->requester->PendingAttempts().empty();
}

bool SsdDirectRuntime::CanPush(const std::string& endpoint, const void* ptr,
                               size_t bytes) const {
    if (!ptr || bytes == 0) return false;
    {
        std::lock_guard lock(impl_->mutex);
        if (impl_->closing || !impl_->CoversRangeLocked(ptr, bytes)) {
            return false;
        }
    }
    DestinationError err = DestinationError::kNotFound;
    return impl_->ResolveReadablePeer(endpoint, err).has_value();
}

std::optional<tl::expected<void, ErrorCode>> SsdDirectRuntime::Read(
    const std::string& endpoint, const std::vector<SsdRuntimeObject>& objects,
    std::chrono::milliseconds timeout) {
    const auto start = std::chrono::steady_clock::now();
    if (timeout.count() <= 0) return tl::unexpected(ErrorCode::INVALID_PARAMS);

    DestinationError peer_err = DestinationError::kNotFound;
    auto resolved_route = impl_->ResolveReadablePeer(endpoint, peer_err);
    if (!resolved_route) {
        if (peer_err == DestinationError::kNotFound) return std::nullopt;
        LOG(WARNING) << "SSD push peer resolution failed for endpoint='"
                     << endpoint << "' error=" << static_cast<int>(peer_err);
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::vector<SsdObjectReadV1> request;
    request.reserve(objects.size());
    struct InFlightReadGuard {
        Impl* impl{nullptr};
        ~InFlightReadGuard() {
            if (!impl) return;
            std::lock_guard lock(impl->mutex);
            --impl->in_flight_reads;
        }
    } read_guard;
    {
        std::lock_guard lock(impl_->mutex);
        if (impl_->closing) return tl::unexpected(ErrorCode::INVALID_PARAMS);
        for (const auto& object : objects) {
            SsdReadRangeV1 range{object.source_offset, {}};
            range.destinations.reserve(object.destinations.size());
            for (const auto& slice : object.destinations) {
                const auto address = reinterpret_cast<uintptr_t>(slice.ptr);
                auto region = impl_->regions.upper_bound(address);
                if (region == impl_->regions.begin()) return std::nullopt;
                --region;
                const auto offset =
                    address - reinterpret_cast<uintptr_t>(region->second.base);
                if (offset > region->second.bytes ||
                    slice.size > region->second.bytes - offset) {
                    return std::nullopt;
                }
                range.destinations.push_back(
                    {region->second.region, offset, slice.size});
            }
            request.push_back(
                {object.key, object.expected_size, {}, {std::move(range)}});
        }
        ++impl_->in_flight_reads;
        read_guard.impl = impl_.get();
    }

    const auto remaining =
        timeout - std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - start);
    if (remaining.count() <= 0) return tl::unexpected(ErrorCode::TRANSFER_FAIL);

    const auto attempt_timeout =
        std::min(remaining, std::chrono::milliseconds(60000));
    SsdFixedL2DrainOptions drain_options;
    drain_options.recovery_timeout =
        std::min(std::chrono::milliseconds(1000), attempt_timeout);
    drain_options.retry_interval = std::chrono::milliseconds(20);
    drain_options.recovery_budget =
        std::min(std::chrono::milliseconds(1000), attempt_timeout);

    // Bounded recovery prevents dead/restarted/quarantined owners from hanging
    // read threads and blocking teardown. Any un-drained destination remains
    // pinned in DestinationRegistry until Recover() observes terminal drain.
    auto result = impl_->requester->ReadWithRecovery(
        *resolved_route, std::move(request), attempt_timeout, drain_options);
    if (result.error == DestinationError::kOk) {
        return tl::expected<void, ErrorCode>{};
    }
    if (!result.may_have_submitted) {
        if (result.error == DestinationError::kOverlap) {
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        return std::nullopt;
    }
    if (result.allow_fallback) return std::nullopt;

    LOG(ERROR) << "SSD push failed control_error="
               << static_cast<int>(result.error)
               << " request=" << result.attempt.request_id
               << " terminal=" << bool(result.result);
    if (result.result) {
        bool disk_failure = false;
        for (const auto& object : result.result->objects) {
            LOG(ERROR) << "SSD push object=" << object.original_index
                       << " outcome=" << static_cast<int>(object.outcome)
                       << " valid_bytes=" << object.valid_bytes;
            if (object.outcome == AttemptOutcome::kFailed) {
                disk_failure = true;
            }
        }
        return tl::unexpected(disk_failure ? ErrorCode::FILE_READ_FAIL
                                           : ErrorCode::TRANSFER_FAIL);
    }
    return tl::unexpected(ErrorCode::TRANSFER_FAIL);
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

int SsdDirectRuntime::Close(bool force_quarantine) {
    {
        std::lock_guard lock(impl_->mutex);
        if (impl_->closed) return 0;
        if (!force_quarantine) {
            // Non-destructive pre-check: if reads are still in flight or
            // pending attempts remain, return -1 WITHOUT marking closing or
            // calling requester->Close() so subsequent Recover()/Read() and
            // Close() retries remain completely healthy.
            if (impl_->in_flight_reads > 0 ||
                (impl_->requester &&
                 !impl_->requester->PendingAttempts().empty())) {
                return -1;
            }
        }
        impl_->closing = true;
    }
    // Requester Close() revokes drained regions and safely retains any
    // un-drained backing registrations in
    // DestinationRegistry::State::drain_hold when force_quarantine is true. TE
    // unregisterLocalMemory is owned by RealClient teardown after servers and
    // prefetcher have stopped.
    if (impl_->requester) {
        const auto err = impl_->requester->Close();
        if (!force_quarantine && err != DestinationError::kOk) {
            LOG(ERROR) << "SsdDirectRuntime requester close returned error="
                       << static_cast<int>(err);
        }
    }
    if (impl_->endpoint) impl_->endpoint->CloseAndDrain();
    if (impl_->server) impl_->server->stop();
    if (impl_->directory) {
        const auto unpub = impl_->directory->Unpublish();
        if (unpub != DestinationError::kOk) {
            LOG(WARNING) << "SsdPeerDirectory::Unpublish failed error="
                         << static_cast<int>(unpub);
        }
    }

    std::lock_guard lock(impl_->mutex);
    impl_->regions.clear();
    impl_->closed = true;
    return 0;
}

}  // namespace mooncake::ssd_offload
