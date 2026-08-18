#include "gds_storage.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <limits>

#include <glog/logging.h>

namespace mooncake {

GdsStorageManager::Reservation::Reservation(
    GdsStorageManager* manager, std::string key, GdsDescriptor descriptor,
    offset_allocator::OffsetAllocationHandle&& allocation)
    : manager_(manager),
      key_(std::move(key)),
      descriptor_(std::move(descriptor)),
      allocation_(std::move(allocation)) {}

GdsStorageManager::Reservation::~Reservation() { Reset(); }

GdsStorageManager::Reservation::Reservation(Reservation&& other) noexcept
    : manager_(other.manager_),
      key_(std::move(other.key_)),
      descriptor_(std::move(other.descriptor_)),
      allocation_(std::move(other.allocation_)) {
    other.manager_ = nullptr;
}

GdsStorageManager::Reservation& GdsStorageManager::Reservation::operator=(
    Reservation&& other) noexcept {
    if (this == &other) {
        return *this;
    }
    Reset();
    manager_ = other.manager_;
    key_ = std::move(other.key_);
    descriptor_ = std::move(other.descriptor_);
    allocation_ = std::move(other.allocation_);
    other.manager_ = nullptr;
    return *this;
}

void GdsStorageManager::Reservation::Reset() {
    if (manager_) {
        manager_->Abort(*this);
    }
}

GdsStorageManager::GdsStorageManager(TransferEngine& engine,
                                     UUID owner_client_id)
    : engine_(engine), owner_client_id_(owner_client_id) {}

GdsStorageManager::~GdsStorageManager() {
    if (segment_handle_ != static_cast<SegmentHandle>(-1)) {
        const int rc = engine_.closeSegment(segment_handle_);
        if (rc != 0) {
            LOG(WARNING) << "Failed to close file offload segment, rc=" << rc;
        }
    }
}

tl::expected<uint64_t, ErrorCode> GdsStorageManager::AlignSize(uint64_t size) {
    if (size == 0 ||
        size > std::numeric_limits<uint64_t>::max() - (kAlignment - 1)) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return (size + kAlignment - 1) & ~(uint64_t{kAlignment} - 1);
}

tl::expected<void, ErrorCode> GdsStorageManager::Init(
    const std::string& file_path, uint64_t capacity,
    const std::string& transport) {
    namespace fs = std::filesystem;
    if (transport != "gds") {
        LOG(ERROR) << "Unsupported file offload transport: " << transport;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    const bool has_gds = engine_.isUsingTent() && engine_.hasTransport("gds");
    if (!has_gds) {
        LOG(ERROR) << "GDS offload requires an active Tent GDS transport";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (engine_.hasTransport("io_uring")) {
        LOG(ERROR) << "GDS offload requires Tent file fallback transports to be "
                      "disabled";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    const fs::path path(file_path);
    if (file_path.empty() || !path.is_absolute() || capacity == 0) {
        LOG(ERROR) << "File offload requires an absolute file path and a "
                      "non-zero capacity";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    const fs::path parent = path.parent_path();
    if (parent.empty() || !fs::exists(parent) || !fs::is_directory(parent)) {
        LOG(ERROR) << "File offload parent directory does not exist: "
                   << parent.string();
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    const int fd = ::open(file_path.c_str(), O_CLOEXEC | O_RDWR | O_CREAT |
                                                 O_TRUNC,
                          0644);
    if (fd < 0) {
        LOG(ERROR) << "Failed to create file offload backing file " << file_path
                   << ": " << std::strerror(errno);
        return tl::unexpected(ErrorCode::FILE_OPEN_FAIL);
    }

    int preallocate_rc = ::fallocate(fd, 0, 0, capacity);
    if (preallocate_rc != 0) {
        preallocate_rc = ::ftruncate(fd, capacity);
    }
    const int saved_errno = errno;
    ::close(fd);
    if (preallocate_rc != 0) {
        LOG(ERROR) << "Failed to preallocate file offload backing file "
                   << file_path
                   << ": " << std::strerror(saved_errno);
        return tl::unexpected(ErrorCode::FILE_WRITE_FAIL);
    }

    allocator_ = offset_allocator::OffsetAllocator::create(0, capacity);
    if (!allocator_) {
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }

    const std::string segment_name = "file://" + file_path;
    const SegmentHandle segment = engine_.openSegment(segment_name);
    if (segment == static_cast<SegmentHandle>(-1)) {
        LOG(ERROR) << "Failed to open Tent file offload segment "
                   << segment_name;
        allocator_.reset();
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    generation_ = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
    generation_ ^= owner_client_id_.first ^ owner_client_id_.second;
    if (generation_ == 0) {
        generation_ = 1;
    }
    storage_id_ = std::to_string(owner_client_id_.first) + "-" +
                  std::to_string(owner_client_id_.second);
    file_path_ = file_path;
    capacity_ = capacity;
    segment_handle_ = segment;
    LOG(INFO) << "Initialized GDS file offload, path=" << file_path_
              << ", capacity=" << capacity_;
    return {};
}

tl::expected<GdsStorageManager::Reservation, ErrorCode>
GdsStorageManager::Reserve(const std::string& key, uint64_t value_size) {
    auto allocated_size = AlignSize(value_size);
    if (!allocated_size || allocated_size.value() > capacity_) {
        return tl::unexpected(allocated_size
                                  ? ErrorCode::NO_AVAILABLE_HANDLE
                                  : allocated_size.error());
    }

    std::lock_guard<std::mutex> lock(mutex_);
    if (!allocator_ || reserved_keys_.count(key) != 0 ||
        entries_.count(key) != 0) {
        return tl::unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
    }
    auto allocation = allocator_->allocate(allocated_size.value());
    if (!allocation.has_value()) {
        return tl::unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }
    if (allocation->address() % kAlignment != 0) {
        LOG(ERROR) << "GDS allocator returned an unaligned extent";
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }

    reserved_keys_.insert(key);
    GdsDescriptor descriptor{owner_client_id_, storage_id_, generation_,
                             allocation->address(), value_size,
                             allocated_size.value()};
    return Reservation(this, key, std::move(descriptor),
                       std::move(allocation.value()));
}

bool GdsStorageManager::Owns(const Reservation& reservation) const {
    return reservation.manager_ == this && reservation.allocation_.has_value();
}

tl::expected<void, ErrorCode> GdsStorageManager::Commit(
    Reservation& reservation) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!Owns(reservation) || reserved_keys_.count(reservation.key_) == 0) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto allocation =
        std::make_shared<offset_allocator::OffsetAllocationHandle>(
            std::move(reservation.allocation_.value()));
    entries_.emplace(reservation.key_,
                     Entry{reservation.descriptor_, std::move(allocation),
                           true});
    reserved_keys_.erase(reservation.key_);
    reservation.manager_ = nullptr;
    reservation.allocation_.reset();
    return {};
}

void GdsStorageManager::Abort(Reservation& reservation) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!Owns(reservation)) {
        reservation.manager_ = nullptr;
        return;
    }
    reserved_keys_.erase(reservation.key_);
    reservation.allocation_.reset();
    reservation.manager_ = nullptr;
}

void GdsStorageManager::Preserve(Reservation& reservation) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!Owns(reservation)) {
        return;
    }
    auto allocation =
        std::make_shared<offset_allocator::OffsetAllocationHandle>(
            std::move(reservation.allocation_.value()));
    entries_.emplace(reservation.key_,
                     Entry{reservation.descriptor_, std::move(allocation),
                           false});
    reserved_keys_.erase(reservation.key_);
    reservation.manager_ = nullptr;
    reservation.allocation_.reset();
}

bool GdsStorageManager::MatchesStorage(const GdsDescriptor& descriptor) const {
    return descriptor.owner_client_id == owner_client_id_ &&
           descriptor.storage_id == storage_id_ &&
           descriptor.storage_generation == generation_;
}

tl::expected<GdsStorageManager::ReadLease, ErrorCode>
GdsStorageManager::AcquireRead(const std::string& key,
                               const GdsDescriptor& expected_descriptor) {
    if (!MatchesStorage(expected_descriptor)) {
        return tl::unexpected(ErrorCode::INVALID_REPLICA);
    }

    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = entries_.find(key);
    if (it == entries_.end() || !it->second.finalized) {
        return tl::unexpected(ErrorCode::INVALID_REPLICA);
    }

    const auto& current = it->second.descriptor;
    if (current.owner_client_id != expected_descriptor.owner_client_id ||
        current.storage_id != expected_descriptor.storage_id ||
        current.storage_generation != expected_descriptor.storage_generation ||
        current.value_offset != expected_descriptor.value_offset ||
        current.value_size != expected_descriptor.value_size ||
        current.allocated_size != expected_descriptor.allocated_size) {
        return tl::unexpected(ErrorCode::INVALID_REPLICA);
    }

    return ReadLease(it->second.allocation);
}

}  // namespace mooncake
