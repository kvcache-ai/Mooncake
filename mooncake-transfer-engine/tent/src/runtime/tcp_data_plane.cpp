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

#include "tent/runtime/tcp_data_plane.h"

#include <algorithm>
#include <charconv>
#include <limits>
#include <random>
#include <utility>
#include <vector>

namespace mooncake::tent {
namespace {

uint64_t randomUint64() {
    std::random_device device;
    std::seed_seq seed{device(), device(), device(), device(),
                       device(), device(), device(), device()};
    return std::mt19937_64(seed)();
}

bool contains(uint64_t base, uint64_t size, uint64_t addr, uint64_t length) {
    return size != 0 && length != 0 && base <= addr &&
           addr <= std::numeric_limits<uint64_t>::max() - length &&
           base <= std::numeric_limits<uint64_t>::max() - size &&
           addr + length <= base + size;
}

void encodeHex(uint64_t value, char* output) {
    char unpadded[16];
    auto result =
        std::to_chars(unpadded, unpadded + sizeof(unpadded), value, 16);
    const size_t length = static_cast<size_t>(result.ptr - unpadded);
    std::fill(output, output + 16 - length, '0');
    std::copy(unpadded, result.ptr, output + 16 - length);
}

bool decodeHex(std::string_view input, uint64_t* output) {
    if (output == nullptr || input.size() != 16) return false;
    auto result =
        std::from_chars(input.data(), input.data() + input.size(), *output, 16);
    return result.ec == std::errc{} &&
           result.ptr == input.data() + input.size();
}

}  // namespace

TcpProcessIdentity makeTcpProcessIdentity() {
    TcpProcessIdentity identity{randomUint64(), randomUint64()};
    if (identity.high == 0 && identity.low == 0) identity.low = 1;
    return identity;
}

std::string encodeTcpProcessAttr(const TcpProcessIdentity& identity) {
    std::string attr(35, '0');
    attr[0] = '2';
    attr[1] = ':';
    attr[18] = ':';
    encodeHex(identity.high, attr.data() + 2);
    encodeHex(identity.low, attr.data() + 19);
    return attr;
}

std::string encodeTcpRegistrationAttr(uint64_t registration_id) {
    std::string attr(18, '0');
    attr[0] = '2';
    attr[1] = ':';
    encodeHex(registration_id, attr.data() + 2);
    return attr;
}

bool decodeTcpProcessAttr(std::string_view attr, TcpProcessIdentity* identity) {
    return identity != nullptr && attr.size() == 35 && attr[0] == '2' &&
           attr[1] == ':' && attr[18] == ':' &&
           decodeHex(attr.substr(2, 16), &identity->high) &&
           decodeHex(attr.substr(19, 16), &identity->low) &&
           (identity->high != 0 || identity->low != 0);
}

bool decodeTcpRegistrationAttr(std::string_view attr,
                               uint64_t* registration_id) {
    return attr.size() == 18 && attr[0] == '2' && attr[1] == ':' &&
           decodeHex(attr.substr(2), registration_id) && *registration_id != 0;
}

TcpBufferRegistry::TcpBufferRegistry()
    : registration_namespace_(randomUint64()) {}

TcpBufferRegistry::Lease::Lease(std::shared_ptr<Entry> entry)
    : entry_(std::move(entry)) {}

TcpBufferRegistry::Lease::Lease(Lease&& other) noexcept
    : entry_(std::move(other.entry_)) {}

TcpBufferRegistry::Lease& TcpBufferRegistry::Lease::operator=(
    Lease&& other) noexcept {
    if (this != &other) {
        reset();
        entry_ = std::move(other.entry_);
    }
    return *this;
}

TcpBufferRegistry::Lease::~Lease() { reset(); }

void TcpBufferRegistry::Lease::reset() {
    if (!entry_) return;
    {
        std::lock_guard<std::mutex> lock(entry_->mutex);
        --entry_->active_leases;
    }
    entry_->drained.notify_all();
    entry_.reset();
}

Status TcpBufferRegistry::add(uint64_t base, uint64_t length,
                              Permission permission,
                              uint64_t* registration_id) {
    if (!contains(base, length, base, length) || registration_id == nullptr) {
        return Status::InvalidArgument(
            "invalid TCP buffer registration" LOC_MARK);
    }
    if (permission != kLocalReadWrite && permission != kGlobalReadOnly &&
        permission != kGlobalReadWrite) {
        return Status::InvalidArgument(
            "invalid TCP buffer permission" LOC_MARK);
    }

    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (closing_) {
        return Status::TooManyRequests(
            "TCP buffer registry is shutting down" LOC_MARK);
    }
    if (next_registration_sequence_ == 0) {
        return Status::InternalError(
            "TCP registration id space exhausted" LOC_MARK);
    }
    uint64_t id = registration_namespace_ ^ next_registration_sequence_++;
    if (id == 0) id = registration_namespace_ ^ next_registration_sequence_++;
    auto entry = std::make_shared<Entry>();
    entry->base = base;
    entry->length = length;
    entry->permission = permission;
    entries_.emplace(id, std::move(entry));
    *registration_id = id;
    return Status::OK();
}

Status TcpBufferRegistry::remove(uint64_t registration_id) {
    std::shared_ptr<Entry> entry;
    {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        auto it = entries_.find(registration_id);
        if (it == entries_.end()) {
            return Status::AddressNotRegistered(
                "TCP registration is not active" LOC_MARK);
        }
        entry = std::move(it->second);
        entries_.erase(it);
    }
    std::unique_lock<std::mutex> lock(entry->mutex);
    entry->drained.wait(lock, [&] { return entry->active_leases == 0; });
    return Status::OK();
}

Status TcpBufferRegistry::acquire(uint64_t registration_id, uint64_t addr,
                                  uint64_t length, bool write, Lease* lease) {
    if (lease == nullptr) {
        return Status::InvalidArgument("TCP lease output is null" LOC_MARK);
    }
    lease->reset();
    std::shared_ptr<Entry> entry;
    {
        std::shared_lock<std::shared_mutex> registry_lock(mutex_);
        if (closing_) {
            return Status::TooManyRequests(
                "TCP buffer registry is shutting down" LOC_MARK);
        }
        auto it = entries_.find(registration_id);
        if (it == entries_.end()) {
            return Status::NeedsRefreshCache("stale TCP registration" LOC_MARK);
        }
        entry = it->second;
        std::lock_guard<std::mutex> entry_lock(entry->mutex);
        if (!contains(entry->base, entry->length, addr, length) ||
            entry->permission == kLocalReadWrite ||
            (write && entry->permission != kGlobalReadWrite)) {
            return Status::AddressNotRegistered(
                "TCP registration does not permit requested access" LOC_MARK);
        }
        ++entry->active_leases;
        *lease = Lease(entry);
    }

    // The disabled fast path is one relaxed atomic load. Tests use the hook to
    // stop a real RPC after it has acquired its lease, making unregister and
    // uninstall lifetime barriers deterministic without slowing production
    // copies with a mutex or callback.
    if (acquire_hook_active_.load(std::memory_order_relaxed)) {
        std::function<void(uint64_t)> hook;
        {
            std::lock_guard<std::mutex> lock(acquire_hook_mutex_);
            hook = acquire_hook_for_test_;
        }
        if (hook) hook(registration_id);
    }
    return Status::OK();
}

void TcpBufferRegistry::close(bool discard_entries) {
    std::vector<std::shared_ptr<Entry>> entries;
    {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        closing_ = true;
        entries.reserve(entries_.size());
        for (const auto& [id, entry] : entries_) entries.push_back(entry);
    }
    for (const auto& entry : entries) {
        std::unique_lock<std::mutex> lock(entry->mutex);
        entry->drained.wait(lock, [&] { return entry->active_leases == 0; });
    }
    if (discard_entries) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        entries_.clear();
    }
}

Status TcpBufferRegistry::reopen() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    closing_ = false;
    return Status::OK();
}

bool TcpBufferRegistry::empty() {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return entries_.empty();
}

void TcpBufferRegistry::setAcquireHookForTest(
    std::function<void(uint64_t)> hook) {
    std::lock_guard<std::mutex> lock(acquire_hook_mutex_);
    acquire_hook_for_test_ = std::move(hook);
    acquire_hook_active_.store(static_cast<bool>(acquire_hook_for_test_),
                               std::memory_order_release);
}

}  // namespace mooncake::tent
