#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include <ylt/util/tl/expected.hpp>

#include "offset_allocator/offset_allocator.h"
#include "replica.h"
#include "transfer_engine.h"
#include "types.h"

namespace mooncake {

class GdsStorageManager {
   public:
    static constexpr size_t kAlignment = 4096;

    class ReadLease {
       public:
        ReadLease() = default;

        ReadLease(const ReadLease&) = delete;
        ReadLease& operator=(const ReadLease&) = delete;
        ReadLease(ReadLease&&) noexcept = default;
        ReadLease& operator=(ReadLease&&) noexcept = default;

        bool valid() const { return allocation_ != nullptr; }

       private:
        friend class GdsStorageManager;
        explicit ReadLease(
            std::shared_ptr<offset_allocator::OffsetAllocationHandle>
                allocation)
            : allocation_(std::move(allocation)) {}

        std::shared_ptr<offset_allocator::OffsetAllocationHandle> allocation_;
    };

    class Reservation {
       public:
        Reservation() = default;
        ~Reservation();

        Reservation(const Reservation&) = delete;
        Reservation& operator=(const Reservation&) = delete;
        Reservation(Reservation&& other) noexcept;
        Reservation& operator=(Reservation&& other) noexcept;

        const GdsDescriptor& descriptor() const { return descriptor_; }
        bool valid() const { return manager_ != nullptr; }

       private:
        friend class GdsStorageManager;
        Reservation(
            GdsStorageManager* manager, std::string key,
            GdsDescriptor descriptor,
            offset_allocator::OffsetAllocationHandle&& allocation);

        void Reset();

        GdsStorageManager* manager_{nullptr};
        std::string key_;
        GdsDescriptor descriptor_{};
        std::optional<offset_allocator::OffsetAllocationHandle> allocation_;
    };

    GdsStorageManager(TransferEngine& engine, UUID owner_client_id);
    ~GdsStorageManager();

    GdsStorageManager(const GdsStorageManager&) = delete;
    GdsStorageManager& operator=(const GdsStorageManager&) = delete;

    tl::expected<void, ErrorCode> Init(const std::string& file_path,
                                       uint64_t capacity,
                                       const std::string& transport = "gds");
    tl::expected<Reservation, ErrorCode> Reserve(const std::string& key,
                                                 uint64_t value_size);
    tl::expected<void, ErrorCode> Commit(Reservation& reservation);
    void Abort(Reservation& reservation);
    void Preserve(Reservation& reservation);
    tl::expected<ReadLease, ErrorCode> AcquireRead(
        const std::string& key, const GdsDescriptor& expected_descriptor);

    bool MatchesStorage(const GdsDescriptor& descriptor) const;

    SegmentHandle segment_handle() const { return segment_handle_; }
    const std::string& storage_id() const { return storage_id_; }
    uint64_t generation() const { return generation_; }
    const std::string& file_path() const { return file_path_; }

   private:
    struct Entry {
        GdsDescriptor descriptor;
        std::shared_ptr<offset_allocator::OffsetAllocationHandle> allocation;
        bool finalized{false};
    };

    static tl::expected<uint64_t, ErrorCode> AlignSize(uint64_t size);
    bool Owns(const Reservation& reservation) const;

    TransferEngine& engine_;
    UUID owner_client_id_;
    std::string file_path_;
    std::string storage_id_;
    uint64_t generation_{0};
    uint64_t capacity_{0};
    SegmentHandle segment_handle_{static_cast<SegmentHandle>(-1)};
    std::shared_ptr<offset_allocator::OffsetAllocator> allocator_;

    std::mutex mutex_;
    std::unordered_set<std::string> reserved_keys_;
    std::unordered_map<std::string, Entry> entries_;
};

}  // namespace mooncake
