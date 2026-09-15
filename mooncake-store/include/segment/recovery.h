#pragma once

#include <map>
#include <memory>
#include <span>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "replica.h"

namespace mooncake {

class SegmentPool;

struct RecoverySegment {
    std::string name;
    std::string endpoint;
    uint64_t capacity;
};

// Detached resource preparation used while the coordinator restores metadata.
// It owns placeholders and validates live ranges, including across chunks.
class SegmentRecovery final {
   public:
    static tl::expected<std::unique_ptr<SegmentRecovery>, ErrorCode> Create(
        std::vector<RecoverySegment> segments);
    tl::expected<std::unique_ptr<AllocatedBuffer>, ErrorCode> Restore(
        const Replica::Descriptor& descriptor, uint64_t object_size);

   private:
    friend class SegmentPool;
    friend class PreparedRemount;
    std::vector<RecoverySegment> segments_;
    std::unordered_map<std::string, size_t> aliases_;
    std::unordered_map<std::string, std::shared_ptr<BufferAllocatorBase>>
        allocators_;
    std::vector<std::map<uintptr_t, uint64_t>> ranges_;
    std::unordered_map<std::string, uint64_t> accounted_;
};

// NoF restore still has no resource lifecycle migration. This detached holder
// only keeps descriptor placeholders alive, as before the Pool cutover.
class NoFBufferRecovery final {
   public:
    tl::expected<std::unique_ptr<AllocatedBuffer>, ErrorCode> Restore(
        const Replica::Descriptor& descriptor, uint64_t object_size);

   private:
    std::unordered_map<std::string, std::shared_ptr<BufferAllocatorBase>>
        allocators_;
};

class RemountRequest final {
   public:
    bool NeedsBuffers() const { return needs_buffers_; }
    // The coordinator supplies opaque IDs, never metadata or Replica pointers.
    // false means the descriptor does not belong to this remount batch.
    tl::expected<bool, ErrorCode> AddBuffer(
        size_t binding_id, AllocatedBuffer::Descriptor descriptor);

   private:
    friend class SegmentPool;
    friend class PreparedRemount;
    struct Region {
        Segment segment;
        std::vector<size_t> bindings;
        std::vector<AllocatedBuffer::Descriptor> descriptors;
    };
    UUID client_id_;
    bool needs_buffers_{false};
    std::vector<Region> regions_;
    std::unordered_set<size_t> binding_ids_;
};

struct RestoredBuffer {
    size_t binding_id;
    std::unique_ptr<AllocatedBuffer> buffer;
};

// Owns the Pool write access until destruction. Prepare performs all fallible
// validation; commit publishes the resources and returns already-bound buffers.
class PreparedRemount final {
   public:
    ~PreparedRemount();
    PreparedRemount(PreparedRemount&&) noexcept;
    PreparedRemount& operator=(PreparedRemount&&) noexcept;
    PreparedRemount(const PreparedRemount&) = delete;
    PreparedRemount& operator=(const PreparedRemount&) = delete;
    std::vector<RestoredBuffer> Commit();

   private:
    friend class SegmentPool;
    struct State;
    explicit PreparedRemount(std::unique_ptr<State> state);
    std::unique_ptr<State> state_;
};

}  // namespace mooncake
