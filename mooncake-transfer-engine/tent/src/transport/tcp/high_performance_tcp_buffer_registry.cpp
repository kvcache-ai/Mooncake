// Copyright 2026 KVCache.AI
#include "tent/transport/tcp/high_performance_tcp_buffer_registry.h"

#include <limits>

namespace mooncake::tent {
namespace {

bool RangeEnd(uint64_t base, uint64_t length, uint64_t* end) {
    if (length == 0 || base > std::numeric_limits<uint64_t>::max() - length)
        return false;
    *end = base + length;
    return true;
}

Status LeaseError(HighPerformanceTcpBufferRegistry::AcquireFailure failure,
                  const char* message) {
    switch (failure) {
        case HighPerformanceTcpBufferRegistry::AcquireFailure::
            kStaleRegistration:
            return Status::NeedsRefreshCache(std::string(message) + LOC_MARK);
        case HighPerformanceTcpBufferRegistry::AcquireFailure::kShuttingDown:
            return Status::TooManyRequests(std::string(message) + LOC_MARK);
        default:
            return Status::AddressNotRegistered(std::string(message) +
                                                LOC_MARK);
    }
}

}  // namespace

HighPerformanceTcpBufferRegistry::Lease::Lease(std::shared_ptr<Entry> entry)
    : entry_(std::move(entry)) {}
HighPerformanceTcpBufferRegistry::Lease::Lease(Lease&& other) noexcept
    : entry_(std::move(other.entry_)) {}
HighPerformanceTcpBufferRegistry::Lease&
HighPerformanceTcpBufferRegistry::Lease::operator=(Lease&& other) noexcept {
    if (this != &other) {
        reset();
        entry_ = std::move(other.entry_);
    }
    return *this;
}
HighPerformanceTcpBufferRegistry::Lease::~Lease() { reset(); }
void HighPerformanceTcpBufferRegistry::Lease::reset() {
    if (!entry_) return;
    {
        std::lock_guard<std::mutex> lock(entry_->mutex);
        --entry_->active_leases;
    }
    entry_->drained.notify_all();
    entry_.reset();
}
void* HighPerformanceTcpBufferRegistry::Lease::data() const {
    return entry_ == nullptr ? nullptr : reinterpret_cast<void*>(entry_->base);
}
uint64_t HighPerformanceTcpBufferRegistry::Lease::length() const {
    return entry_ == nullptr ? 0 : entry_->length;
}

Status HighPerformanceTcpBufferRegistry::add(uint64_t base, uint64_t length,
                                             Permission permission,
                                             uint64_t* registration_id) {
    uint64_t end = 0;
    if (!RangeEnd(base, length, &end))
        return Status::InvalidArgument("invalid HP TCP buffer range" LOC_MARK);
    std::lock_guard<std::mutex> lock(registry_mutex_);
    const auto next = entries_.upper_bound(base);
    if (next != entries_.end() && end > next->second->base)
        return Status::InvalidArgument("overlapping HP TCP buffer" LOC_MARK);
    if (next != entries_.begin()) {
        const auto previous = std::prev(next);
        uint64_t previous_end = 0;
        if (!RangeEnd(previous->second->base, previous->second->length,
                      &previous_end) ||
            previous_end > base)
            return Status::InvalidArgument(
                "overlapping HP TCP buffer" LOC_MARK);
    }
    auto entry = std::make_shared<Entry>();
    entry->base = base;
    entry->length = length;
    entry->permission = permission;
    entry->registration_id = next_registration_id_++;
    if (entry->registration_id == 0)
        entry->registration_id = next_registration_id_++;
    entries_.emplace(base, entry);
    if (registration_id != nullptr) *registration_id = entry->registration_id;
    return Status::OK();
}

Status HighPerformanceTcpBufferRegistry::remove(uint64_t base,
                                                uint64_t length) {
    std::shared_ptr<Entry> entry;
    {
        std::lock_guard<std::mutex> lock(registry_mutex_);
        const auto it = entries_.find(base);
        if (it == entries_.end() || it->second->length != length)
            return Status::AddressNotRegistered(
                "HP TCP buffer not registered" LOC_MARK);
        entry = it->second;
        {
            std::lock_guard<std::mutex> entry_lock(entry->mutex);
            entry->closing = true;
        }
        entries_.erase(it);
    }
    std::unique_lock<std::mutex> lock(entry->mutex);
    entry->drained.wait(lock, [&] { return entry->active_leases == 0; });
    return Status::OK();
}

Status HighPerformanceTcpBufferRegistry::acquireLocalLease(uint64_t addr,
                                                           uint64_t length,
                                                           Lease* lease) {
    return acquire(addr, length, 0, HighPerformanceTcpOpcode::kRead, false,
                   lease, nullptr);
}
Status HighPerformanceTcpBufferRegistry::acquireRemoteLease(
    uint64_t addr, uint64_t length, uint64_t registration_id,
    HighPerformanceTcpOpcode opcode, Lease* lease, AcquireFailure* failure) {
    return acquire(addr, length, registration_id, opcode, true, lease, failure);
}
Status HighPerformanceTcpBufferRegistry::acquire(
    uint64_t addr, uint64_t length, uint64_t registration_id,
    HighPerformanceTcpOpcode opcode, bool remote, Lease* lease,
    AcquireFailure* failure) {
    if (failure != nullptr) *failure = AcquireFailure::kNone;
    if (lease == nullptr)
        return Status::InvalidArgument("HP TCP lease output is null" LOC_MARK);
    lease->reset();
    uint64_t end = 0;
    if (!RangeEnd(addr, length, &end)) {
        if (failure != nullptr) *failure = AcquireFailure::kRangeRejected;
        return Status::InvalidArgument("invalid HP TCP lease range" LOC_MARK);
    }
    std::lock_guard<std::mutex> registry_lock(registry_mutex_);
    const auto next = entries_.upper_bound(addr);
    if (next == entries_.begin()) {
        if (failure != nullptr) *failure = AcquireFailure::kRangeRejected;
        return LeaseError(AcquireFailure::kRangeRejected,
                          "HP TCP range not registered");
    }
    const auto entry = std::prev(next)->second;
    if (addr < entry->base || end > entry->base + entry->length) {
        if (failure != nullptr) *failure = AcquireFailure::kRangeRejected;
        return LeaseError(AcquireFailure::kRangeRejected,
                          "HP TCP range not registered");
    }
    std::lock_guard<std::mutex> entry_lock(entry->mutex);
    if (entry->closing) {
        if (failure != nullptr) *failure = AcquireFailure::kShuttingDown;
        return LeaseError(AcquireFailure::kShuttingDown,
                          "HP TCP buffer is closing");
    }
    if (remote && entry->registration_id != registration_id) {
        if (failure != nullptr) *failure = AcquireFailure::kStaleRegistration;
        return LeaseError(AcquireFailure::kStaleRegistration,
                          "stale HP TCP registration");
    }
    if (remote && (entry->permission == kLocalReadWrite ||
                   (opcode == HighPerformanceTcpOpcode::kWrite &&
                    entry->permission != kGlobalReadWrite))) {
        if (failure != nullptr) *failure = AcquireFailure::kPermissionDenied;
        return LeaseError(AcquireFailure::kPermissionDenied,
                          "HP TCP permission denied");
    }
    ++entry->active_leases;
    *lease = Lease(entry);
    return Status::OK();
}
bool HighPerformanceTcpBufferRegistry::tracks(uint64_t base,
                                              uint64_t length) const {
    std::lock_guard<std::mutex> lock(registry_mutex_);
    const auto it = entries_.find(base);
    return it != entries_.end() && it->second->length == length;
}
size_t HighPerformanceTcpBufferRegistry::size() const {
    std::lock_guard<std::mutex> lock(registry_mutex_);
    return entries_.size();
}
}  // namespace mooncake::tent
