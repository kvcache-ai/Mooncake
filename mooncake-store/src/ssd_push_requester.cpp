// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_push_requester.h"
#include "tenant_id.h"
#include <algorithm>
#include <atomic>
#include <arpa/inet.h>
#include <limits>
#include <mutex>
#include <stdexcept>

namespace mooncake::ssd_offload {
namespace {
using Clock = std::chrono::steady_clock;
bool ValidPeer(PeerIdentity p) {
    return (p.client_id_high || p.client_id_low) && p.boot_epoch;
}
bool Endpoint(const std::string& s) {
    return !s.empty() && s.size() <= 4096 && s.find('\0') == std::string::npos;
}
bool NumericHost(const std::string& s) {
    in_addr v4{};
    in6_addr v6{};
    return Endpoint(s) && ((inet_pton(AF_INET, s.c_str(), &v4) == 1 &&
                            v4.s_addr != INADDR_ANY) ||
                           (inet_pton(AF_INET6, s.c_str(), &v6) == 1 &&
                            !IN6_IS_ADDR_UNSPECIFIED(&v6)));
}
bool CallTimeout(std::chrono::milliseconds t) {
    return t.count() > 0 && t <= std::chrono::seconds(60);
}
bool ProtocolLimits(SsdReadProtocolLimits l) {
    return l.max_objects && l.max_objects <= 65536 && l.max_ranges &&
           l.max_ranges <= 1048576 && l.max_fragments &&
           l.max_fragments <= 1048576 && l.max_key_bytes &&
           l.max_payload_bytes && l.max_timeout_ms;
}
struct Budget {
    std::mutex mutex;
    uint64_t bytes{0}, max_bytes;
    size_t reads{0}, max_reads;
    explicit Budget(SsdPushRequesterLimits l)
        : max_bytes(l.max_owned_bytes), max_reads(l.max_owned_reads) {}
    bool Reserve(uint64_t amount) {
        std::lock_guard lock(mutex);
        if (reads >= max_reads || amount > max_bytes - bytes) return false;
        bytes += amount;
        ++reads;
        return true;
    }
    void Release(uint64_t amount) {
        std::lock_guard lock(mutex);
        bytes -= amount;
        --reads;
    }
};
struct OwnedBacking {
    std::shared_ptr<Budget> budget;
    std::vector<std::byte> bytes;
    OwnedBacking(std::shared_ptr<Budget> b, size_t size)
        : budget(std::move(b)), bytes(size) {}
    ~OwnedBacking() { budget->Release(bytes.size()); }
};
struct Runtime {
    std::shared_ptr<DestinationRegistration> registration;
    std::shared_ptr<SsdRequesterGrants> grants;
    std::shared_ptr<Budget> budget;
    std::atomic<bool> closing{false};
};
std::chrono::milliseconds Remaining(Clock::time_point deadline) {
    return std::max(std::chrono::milliseconds::zero(),
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        deadline - Clock::now()));
}
}  // namespace

struct SsdOwnedRead::Impl {
    mutable std::mutex mutex;
    std::shared_ptr<Runtime> runtime;
    std::shared_ptr<OwnedBacking> backing;
    std::vector<std::pair<size_t, size_t>> objects;
    SsdReadRequestV1 request;
    SsdGrantReferenceV1 reference;
    DestinationRegion region;
    SsdPushPeerInfo peer;
    SsdRequesterControlCall call;
    Clock::time_point deadline;
    std::optional<SsdReadResultV1> terminal;
    bool timed_out{false}, owner_forgotten{false}, local_forgotten{false},
        released{false};

    DestinationError Timeout() {
        timed_out = true;
        return runtime->grants->Timeout(reference);
    }
    SsdRequesterCallResult Call(SsdControlCommand command,
                                std::chrono::milliseconds timeout) noexcept {
        try {
            return call(
                peer.control_host, peer.control_port,
                {command, reference,
                 command == SsdControlCommand::kSubmit ? std::optional(request)
                                                       : std::nullopt},
                timeout);
        } catch (...) {
            // May have been accepted remotely before any exception.
            return {};
        }
    }
    DestinationError Apply(SsdRequesterCallResult reply,
                           SsdControlCommand command) {
        if (Clock::now() >= deadline) Timeout();
        auto fail = [&](DestinationError error) {
            Timeout();
            return error;
        };
        if (reply.error != DestinationError::kOk) return fail(reply.error);
        if (!reply.reply) return fail(DestinationError::kInvalidArgument);
        const auto& response = *reply.reply;
        if (response.command != command)
            return fail(DestinationError::kWrongAttempt);
        if (response.error != DestinationError::kOk &&
            !(command == SsdControlCommand::kSubmit &&
              response.error == DestinationError::kDuplicate))
            return fail(response.error);
        if (!response.snapshot)
            return command == SsdControlCommand::kSubmit
                       ? DestinationError::kOk
                       : fail(DestinationError::kInvalidArgument);
        const auto& state = *response.snapshot;
        if (state.error != DestinationError::kOk) return fail(state.error);
        if (!state.result)
            return state.state == SsdOwnerRequestState::kDrained
                       ? fail(DestinationError::kInvalidArgument)
                       : DestinationError::kOk;
        if (state.state != SsdOwnerRequestState::kDrained)
            return fail(DestinationError::kInvalidArgument);
        // Copy before logical completion; allocation failure cannot leave a
        // completed grant without its recoverable terminal response.
        auto result = *state.result;
        if (Clock::now() >= deadline) Timeout();
        const auto error = runtime->grants->Complete(reference, result);
        if (error != DestinationError::kOk &&
            error != DestinationError::kDuplicate)
            return fail(error);
        // Validation, allocation and lock contention are part of the local
        // deadline. A newly drained grant cannot publish a late logical Get.
        if (Clock::now() >= deadline) Timeout();
        terminal = std::move(result);
        return DestinationError::kOk;
    }
};
SsdOwnedRead::SsdOwnedRead(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}
SsdOwnedRead::~SsdOwnedRead() {
    try {
        if (!impl_->released) {
            if (!impl_->terminal) impl_->Timeout();
            impl_->runtime->registration->Revoke(impl_->region);
        }
    } catch (...) {
        // Registration retains backing even during allocation failure.
    }
}
DestinationError SsdOwnedRead::Poll(std::chrono::milliseconds timeout) {
    std::lock_guard lock(impl_->mutex);
    if (!CallTimeout(timeout)) return DestinationError::kInvalidArgument;
    if (impl_->released) return DestinationError::kClosing;
    if (impl_->terminal) return DestinationError::kOk;
    if (Clock::now() >= impl_->deadline) impl_->Timeout();
    try {
        return impl_->Apply(impl_->Call(SsdControlCommand::kQuery, timeout),
                            SsdControlCommand::kQuery);
    } catch (...) {
        impl_->Timeout();
        return DestinationError::kTransportError;
    }
}
DestinationError SsdOwnedRead::Timeout() {
    std::lock_guard lock(impl_->mutex);
    return impl_->released ? DestinationError::kClosing : impl_->Timeout();
}
DestinationAttemptSnapshot SsdOwnedRead::snapshot() const {
    std::lock_guard lock(impl_->mutex);
    if (impl_->released || impl_->local_forgotten)
        return {DestinationError::kClosing};
    if (!impl_->terminal && Clock::now() >= impl_->deadline) impl_->Timeout();
    auto state = impl_->runtime->grants->Inspect(impl_->reference);
    if (impl_->timed_out || impl_->runtime->closing) state.publishable = false;
    return state;
}
std::optional<std::span<const std::byte>> SsdOwnedRead::data(
    size_t index) const {
    std::lock_guard lock(impl_->mutex);
    if (impl_->released || impl_->local_forgotten || impl_->timed_out ||
        impl_->runtime->closing || !impl_->terminal ||
        index >= impl_->objects.size() ||
        !impl_->runtime->grants->Inspect(impl_->reference).publishable)
        return {};
    const auto [offset, size] = impl_->objects[index];
    return std::span<const std::byte>(impl_->backing->bytes)
        .subspan(offset, size);
}
DestinationError SsdOwnedRead::Release(std::chrono::milliseconds timeout) {
    std::lock_guard lock(impl_->mutex);
    if (!CallTimeout(timeout)) return DestinationError::kInvalidArgument;
    if (impl_->released) return DestinationError::kOk;
    if (!impl_->terminal && !impl_->local_forgotten) {
        // Timeout on an unexposed grant drains it locally and fences every
        // future Resolve. No owner ever received an address, so an admission
        // rejection need not leave this peer permanently occupied. A grant
        // which exposed addresses remains quarantined until a real owner ACK.
        const auto state = impl_->runtime->grants->Inspect(impl_->reference);
        if ((!impl_->timed_out && !impl_->runtime->closing) ||
            state.error != DestinationError::kOk ||
            state.state != DestinationAttemptState::kDrained)
            return DestinationError::kNotDrained;
    }
    if (!impl_->owner_forgotten) {
        auto call = impl_->Call(SsdControlCommand::kForget, timeout);
        if (call.error != DestinationError::kOk) return call.error;
        if (!call.reply) return DestinationError::kInvalidArgument;
        if (call.reply->command != SsdControlCommand::kForget)
            return DestinationError::kWrongAttempt;
        if (call.reply->error != DestinationError::kOk &&
            call.reply->error != DestinationError::kNotFound)
            return call.reply->error;
        impl_->owner_forgotten = true;
    }
    if (!impl_->local_forgotten) {
        const auto error = impl_->runtime->grants->Forget(
            impl_->peer.owner, impl_->reference.attempt.request_id);
        if (error != DestinationError::kOk) return error;
        impl_->local_forgotten = true;
    }
    const auto error = impl_->runtime->registration->Revoke(impl_->region);
    if (error != DestinationError::kOk && error != DestinationError::kNotFound)
        return error;
    impl_->backing.reset();
    impl_->released = true;
    return DestinationError::kOk;
}
bool SsdOwnedRead::released() const {
    std::lock_guard lock(impl_->mutex);
    return impl_->released;
}
SsdGrantReferenceV1 SsdOwnedRead::reference() const {
    std::lock_guard lock(impl_->mutex);
    return impl_->reference;
}
struct SsdPushRequester::Impl {
    struct Peer {
        SsdPushPeerInfo info;
        std::shared_ptr<SsdOwnedRead> active;
    };
    mutable std::mutex mutex;
    PeerIdentity requester;
    std::string tenant;
    std::shared_ptr<Runtime> runtime;
    std::vector<Peer> peers;
    SsdRequesterControlCall call;
    SsdPushRequesterLimits limits;
    uint64_t next_request{1};
};
SsdPushRequester::SsdPushRequester(
    PeerIdentity requester, std::string tenant, std::string endpoint,
    std::shared_ptr<DestinationRegistrationTransport> transport,
    std::vector<SsdPushPeerInfo> peers, SsdRequesterControlCall call,
    SsdPushRequesterLimits limits) {
    if (!ValidPeer(requester) || tenant.empty() || tenant.size() > 256 ||
        !TenantId(tenant).IsValid() || !Endpoint(endpoint) || !transport ||
        !call || peers.size() > 64 || !limits.max_owned_bytes ||
        !limits.max_owned_reads || limits.max_owned_reads > 65536 ||
        !ProtocolLimits(limits.protocol))
        throw std::invalid_argument("Invalid SSD push requester configuration");
    for (size_t i = 0; i < peers.size(); ++i) {
        const auto& p = peers[i];
        if (p.tenant_id != tenant || !Endpoint(p.legacy_rpc_endpoint) ||
            !NumericHost(p.control_host) || !p.control_port ||
            !Endpoint(p.transport_endpoint) ||
            !CanUseSsdPushV1(p.capability, p.owner, "tcp") ||
            std::any_of(
                peers.begin(), peers.begin() + i, [&](const auto& other) {
                    return p.owner == other.owner ||
                           p.legacy_rpc_endpoint == other.legacy_rpc_endpoint;
                }))
            throw std::invalid_argument("Invalid SSD owner bootstrap");
    }
    auto runtime = std::make_shared<Runtime>();
    runtime->budget = std::make_shared<Budget>(limits);
    runtime->registration = std::make_shared<DestinationRegistration>(
        requester, std::move(endpoint), std::move(transport),
        limits.registrations);
    runtime->grants = std::make_shared<SsdRequesterGrants>(
        requester, runtime->registration, limits.grants, limits.protocol);
    impl_ = std::make_unique<Impl>();
    impl_->requester = requester;
    impl_->tenant = std::move(tenant);
    impl_->runtime = std::move(runtime);
    impl_->call = std::move(call);
    impl_->limits = limits;
    for (auto& p : peers) impl_->peers.push_back({std::move(p), {}});
}
SsdPushRequester::~SsdPushRequester() {
    try {
        Close();
    } catch (...) { /* Registration destructor pins backing. */
    }
}
SsdPushStartResult SsdPushRequester::Start(
    const std::string& endpoint, const std::vector<SsdOwnedReadSpec>& specs,
    std::chrono::milliseconds timeout) {
    const auto deadline =
        Clock::now() +
        (CallTimeout(timeout) ? timeout : std::chrono::milliseconds::zero());
    std::unique_lock lock(impl_->mutex);
    if (impl_->runtime->closing) return {DestinationError::kClosing, false, {}};
    auto peer = std::find_if(
        impl_->peers.begin(), impl_->peers.end(),
        [&](const auto& p) { return p.info.legacy_rpc_endpoint == endpoint; });
    if (peer == impl_->peers.end())
        return {DestinationError::kNotSupported, false, {}};
    if (peer->active && !peer->active->released())
        return {DestinationError::kBusy, false, {}};
    const auto& limits = impl_->limits.protocol;
    if (!CallTimeout(timeout) ||
        uint64_t(timeout.count()) > limits.max_timeout_ms || specs.empty())
        return {DestinationError::kInvalidArgument, false, {}};
    if (specs.size() > limits.max_objects || specs.size() > limits.max_ranges ||
        specs.size() > limits.max_fragments ||
        specs.size() > peer->info.capability.max_batch_objects ||
        specs.size() > peer->info.capability.max_fragments)
        return {DestinationError::kLimit, false, {}};
    uint64_t total = 0;
    size_t key_bytes = impl_->tenant.size();
    if (key_bytes > limits.max_key_bytes)
        return {DestinationError::kLimit, false, {}};
    for (const auto& s : specs) {
        if (s.key.empty() || !s.length ||
            s.expected_object_size >
                uint64_t(std::numeric_limits<int64_t>::max()) ||
            s.source_offset > s.expected_object_size ||
            s.length > s.expected_object_size - s.source_offset)
            return {DestinationError::kInvalidArgument, false, {}};
        if (s.key.size() > limits.max_key_bytes - key_bytes ||
            total > limits.max_payload_bytes ||
            s.length > limits.max_payload_bytes - total)
            return {DestinationError::kLimit, false, {}};
        total += s.length;
        key_bytes += s.key.size();
    }
    if (total > std::numeric_limits<size_t>::max() ||
        total > peer->info.capability.max_batch_bytes ||
        total > peer->info.capability.max_inflight_bytes ||
        impl_->next_request == std::numeric_limits<uint64_t>::max() ||
        !impl_->runtime->budget->Reserve(total))
        return {DestinationError::kLimit, false, {}};
    std::shared_ptr<OwnedBacking> backing;
    try {
        backing = std::make_shared<OwnedBacking>(impl_->runtime->budget,
                                                 size_t(total));
    } catch (...) {
        impl_->runtime->budget->Release(total);
        return {DestinationError::kLimit, false, {}};
    }
    std::shared_ptr<SsdOwnedRead> read;
    bool prepared = false;
    try {
        auto state = std::make_unique<SsdOwnedRead::Impl>();
        state->runtime = impl_->runtime;
        state->backing = std::move(backing);
        state->deadline = deadline;
        state->peer = peer->info;
        state->call = impl_->call;
        state->request.requester = impl_->requester;
        state->request.attempt = {peer->info.owner, impl_->next_request++, 1};
        state->request.tenant_id = impl_->tenant;
        state->request.relative_timeout_ms = uint64_t(timeout.count());
        size_t offset = 0;
        for (const auto& spec : specs) {
            state->objects.emplace_back(offset, size_t(spec.length));
            state->request.objects.push_back(
                {spec.key,
                 spec.expected_object_size,
                 {},
                 {{spec.source_offset, {{{}, offset, spec.length}}}}});
            offset += size_t(spec.length);
        }
        read =
            std::shared_ptr<SsdOwnedRead>(new SsdOwnedRead(std::move(state)));
        auto& s = *read->impl_;
        const auto base = reinterpret_cast<uint64_t>(s.backing->bytes.data());
        auto region = impl_->runtime->registration->Register(
            {base, total, base, total}, "cpu:0", s.backing);
        if (region.error != DestinationError::kOk)
            return {region.error, false, {}};
        s.region = region.region;
        for (auto& object : s.request.objects)
            object.ranges[0].destinations[0].region = s.region;
        auto grant = impl_->runtime->grants->Prepare(s.request);
        if (grant.error != DestinationError::kOk)
            return {grant.error, false, {}};
        s.reference = grant.reference;
        prepared = true;
        if (Remaining(deadline).count() == 0) {
            s.Timeout();
            impl_->runtime->grants->Forget(s.peer.owner,
                                           s.reference.attempt.request_id);
            return {DestinationError::kClosing, false, {}};
        }
        peer->active = read;
    } catch (...) {
        if (prepared) {
            read->impl_->Timeout();
            impl_->runtime->grants->Forget(
                read->impl_->peer.owner,
                read->impl_->reference.attempt.request_id);
        }
        return {DestinationError::kLimit, false, {}};
    }
    lock.unlock();
    // The session owns the handle before crossing the uncertain network
    // boundary.
    std::lock_guard read_lock(read->impl_->mutex);
    DestinationError error;
    try {
        error = read->impl_->Apply(
            read->impl_->Call(SsdControlCommand::kSubmit, Remaining(deadline)),
            SsdControlCommand::kSubmit);
    } catch (...) {
        read->impl_->Timeout();
        error = DestinationError::kTransportError;
    }
    return {error, true, std::move(read)};
}
std::shared_ptr<SsdOwnedRead> SsdPushRequester::active_read(
    const std::string& endpoint) const {
    std::lock_guard lock(impl_->mutex);
    auto peer = std::find_if(
        impl_->peers.begin(), impl_->peers.end(),
        [&](const auto& p) { return p.info.legacy_rpc_endpoint == endpoint; });
    return peer == impl_->peers.end() ? nullptr : peer->active;
}
std::shared_ptr<SsdRequesterGrants> SsdPushRequester::grants() const {
    return impl_->runtime->grants;
}
SsdPushRequesterStats SsdPushRequester::stats() const {
    auto& budget = *impl_->runtime->budget;
    std::lock_guard lock(budget.mutex);
    return {budget.bytes, budget.reads,
            impl_->runtime->registration->pinned_regions()};
}
DestinationError SsdPushRequester::Close() {
    std::lock_guard lock(impl_->mutex);
    impl_->runtime->closing = true;
    impl_->runtime->grants->Close();
    return impl_->runtime->registration->Close();
}
}  // namespace mooncake::ssd_offload
