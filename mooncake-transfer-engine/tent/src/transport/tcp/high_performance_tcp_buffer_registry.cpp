// Copyright 2026 KVCache.AI
#include "tent/transport/tcp/high_performance_tcp_buffer_registry.h"

#include <limits>
#include <string>
#include <utility>

namespace mooncake::tent {
namespace {

bool RangeEnd(uint64_t base, uint64_t length, uint64_t* end) {
    if (end == nullptr || length == 0 ||
        base > std::numeric_limits<uint64_t>::max() - length) {
        return false;
    }
    *end = base + length;
    return true;
}

Status LeaseError(HighPerformanceTcpStatus status, const char* message) {
    switch (status) {
        case HighPerformanceTcpStatus::kStaleRegistration:
            return Status::NeedsRefreshCache(std::string(message) + LOC_MARK);
        case HighPerformanceTcpStatus::kShuttingDown:
            return Status::TooManyRequests(std::string(message) + LOC_MARK);
        case HighPerformanceTcpStatus::kPermissionDenied:
        case HighPerformanceTcpStatus::kRangeRejected:
            return Status::AddressNotRegistered(std::string(message) +
                                                LOC_MARK);
        default:
            return Status::InternalError(std::string(message) + LOC_MARK);
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
        if (entry_->active_leases > 0) --entry_->active_leases;
    }
    entry_->drained.notify_all();
    entry_.reset();
}

void* HighPerformanceTcpBufferRegistry::Lease::data() const {
    return entry_ ? reinterpret_cast<void*>(entry_->base) : nullptr;
}

uint64_t HighPerformanceTcpBufferRegistry::Lease::base() const {
    return entry_ ? entry_->base : 0;
}

Status HighPerformanceTcpBufferRegistry::add(uint64_t base, uint64_t length,
                                             Permission permission,
                                             uint64_t* registration_id) {
    uint64_t end = 0;
    if (!RangeEnd(base, length, &end)) {
        return Status::InvalidArgument("invalid HP TCP buffer range" LOC_MARK);
    }
    if (permission != kLocalReadWrite && permission != kGlobalReadOnly &&
        permission != kGlobalReadWrite) {
        return Status::InvalidArgument(
            "invalid HP TCP buffer permission" LOC_MARK);
    }

    std::lock_guard<std::mutex> lock(registry_mutex_);
    if (closing_) {
        return Status::TooManyRequests(
            "HP TCP buffer registry is shutting down" LOC_MARK);
    }
    const auto next = entries_.upper_bound(base);
    if (next != entries_.end() && end > next->second->base) {
        return Status::InvalidArgument("overlapping HP TCP buffer" LOC_MARK);
    }
    if (next != entries_.begin()) {
        const auto previous = std::prev(next);
        uint64_t previous_end = 0;
        if (!RangeEnd(previous->second->base, previous->second->length,
                      &previous_end) ||
            previous_end > base) {
            return Status::InvalidArgument(
                "overlapping HP TCP buffer" LOC_MARK);
        }
    }

    if (next_registration_id_ == 0 ||
        next_registration_id_ == std::numeric_limits<uint64_t>::max()) {
        return Status::InternalError(
            "HP TCP registration id space exhausted" LOC_MARK);
    }
    const uint64_t id = next_registration_id_++;

    auto entry = std::make_shared<Entry>();
    entry->base = base;
    entry->length = length;
    entry->permission = permission;
    entry->registration_id = id;
    entries_.emplace(base, entry);
    if (registration_id != nullptr) *registration_id = id;
    return Status::OK();
}

Status HighPerformanceTcpBufferRegistry::remove(uint64_t base,
                                                uint64_t length) {
    std::shared_ptr<Entry> entry;
    {
        std::lock_guard<std::mutex> registry_lock(registry_mutex_);
        const auto it = entries_.find(base);
        if (it == entries_.end() || it->second->length != length) {
            return Status::AddressNotRegistered(
                "HP TCP buffer not registered" LOC_MARK);
        }
        entry = it->second;
        {
            std::lock_guard<std::mutex> entry_lock(entry->mutex);
            entry->closing = true;
        }
        // Hide the range before waiting so no new lease can race in.
        entries_.erase(it);
    }

    std::unique_lock<std::mutex> entry_lock(entry->mutex);
    entry->drained.wait(entry_lock, [&] { return entry->active_leases == 0; });
    return Status::OK();
}

void HighPerformanceTcpBufferRegistry::close() {
    std::lock_guard<std::mutex> lock(registry_mutex_);
    closing_ = true;
}

Status HighPerformanceTcpBufferRegistry::reopen() {
    std::lock_guard<std::mutex> lock(registry_mutex_);
    if (!entries_.empty()) {
        return Status::InvalidArgument(
            "cannot reopen HP TCP buffer registry while buffers remain "
            "registered" LOC_MARK);
    }
    closing_ = false;
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
    HighPerformanceTcpOpcode opcode, Lease* lease,
    HighPerformanceTcpStatus* wire_status) {
    return acquire(addr, length, registration_id, opcode, true, lease,
                   wire_status);
}

Status HighPerformanceTcpBufferRegistry::acquire(
    uint64_t addr, uint64_t length, uint64_t registration_id,
    HighPerformanceTcpOpcode opcode, bool remote, Lease* lease,
    HighPerformanceTcpStatus* wire_status) {
    if (wire_status != nullptr) *wire_status = HighPerformanceTcpStatus::kOk;
    if (lease == nullptr) {
        return Status::InvalidArgument("HP TCP lease output is null" LOC_MARK);
    }
    lease->reset();

    uint64_t end = 0;
    if (!RangeEnd(addr, length, &end)) {
        if (wire_status != nullptr)
            *wire_status = HighPerformanceTcpStatus::kRangeRejected;
        return Status::InvalidArgument("invalid HP TCP lease range" LOC_MARK);
    }

    std::lock_guard<std::mutex> registry_lock(registry_mutex_);
    if (closing_) {
        if (wire_status != nullptr)
            *wire_status = HighPerformanceTcpStatus::kShuttingDown;
        return LeaseError(HighPerformanceTcpStatus::kShuttingDown,
                          "HP TCP buffer registry is shutting down");
    }
    const auto next = entries_.upper_bound(addr);
    if (next == entries_.begin()) {
        if (wire_status != nullptr)
            *wire_status = HighPerformanceTcpStatus::kRangeRejected;
        return LeaseError(HighPerformanceTcpStatus::kRangeRejected,
                          "HP TCP range not registered");
    }
    const auto entry = std::prev(next)->second;
    uint64_t entry_end = 0;
    if (!RangeEnd(entry->base, entry->length, &entry_end) ||
        addr < entry->base || end > entry_end) {
        if (wire_status != nullptr)
            *wire_status = HighPerformanceTcpStatus::kRangeRejected;
        return LeaseError(HighPerformanceTcpStatus::kRangeRejected,
                          "HP TCP range not registered");
    }

    std::lock_guard<std::mutex> entry_lock(entry->mutex);
    if (entry->closing) {
        if (wire_status != nullptr)
            *wire_status = HighPerformanceTcpStatus::kShuttingDown;
        return LeaseError(HighPerformanceTcpStatus::kShuttingDown,
                          "HP TCP buffer is closing");
    }
    if (remote && entry->registration_id != registration_id) {
        if (wire_status != nullptr)
            *wire_status = HighPerformanceTcpStatus::kStaleRegistration;
        return LeaseError(HighPerformanceTcpStatus::kStaleRegistration,
                          "stale HP TCP registration");
    }
    if (remote && (entry->permission == kLocalReadWrite ||
                   (opcode == HighPerformanceTcpOpcode::kWrite &&
                    entry->permission != kGlobalReadWrite))) {
        if (wire_status != nullptr)
            *wire_status = HighPerformanceTcpStatus::kPermissionDenied;
        return LeaseError(HighPerformanceTcpStatus::kPermissionDenied,
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

}  // namespace mooncake::tent
