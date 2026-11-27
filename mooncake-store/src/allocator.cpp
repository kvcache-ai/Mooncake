// buffer_allocator.cpp
#include "allocator.h"

#include <glog/logging.h>

#include <algorithm>
#include <limits>
#include <memory>
#include <numeric>
#include <stdexcept>

#include "master_metric_manager.h"

namespace mooncake {
namespace {

bool IsValidCachelibLayout(size_t base, size_t size) noexcept {
    const size_t slab_count = size / sizeof(facebook::cachelib::Slab);
    return base != 0 && base % facebook::cachelib::Slab::kSize == 0 &&
           size >= facebook::cachelib::Slab::kSize &&
           size % facebook::cachelib::Slab::kSize == 0 &&
           slab_count <= std::numeric_limits<unsigned int>::max() &&
           base <= std::numeric_limits<size_t>::max() - size;
}

bool IsValidAllocation(const LiveAllocation& allocation,
                       size_t capacity) noexcept {
    return allocation.requested_size != 0 &&
           allocation.offset_from_base < capacity &&
           allocation.requested_size <= capacity - allocation.offset_from_base;
}

}  // namespace

struct SegmentLifetime::State {
    explicit State(ClientSessionSharedPtr owner) : session(std::move(owner)) {}

    std::atomic<bool> valid{true};
    // Retained directly by State and immutable from construction, so serving
    // and affiliation checks need neither an atomic shared_ptr load nor a
    // reference-count increment.
    const ClientSessionSharedPtr session;
};

SegmentLifetime::SegmentLifetime(ClientSessionSharedPtr session)
    : state_(std::make_shared<State>(std::move(session))) {}

bool SegmentLifetime::isValid() const noexcept {
    return !state_ || state_->valid.load(std::memory_order_acquire);
}

bool SegmentLifetime::isServing() const noexcept {
    return isValid() &&
           (!state_ || !state_->session || state_->session->IsServing());
}

const ClientSessionSharedPtr& SegmentLifetime::getClientSession()
    const noexcept {
    static const ClientSessionSharedPtr kNoSession;
    return state_ ? state_->session : kNoSession;
}

void SegmentLifetime::Invalidate() noexcept {
    if (state_) {
        state_->valid.store(false, std::memory_order_release);
    }
}

void BufferAllocatorBase::AttachUsageTracker(
    const std::shared_ptr<StorageUsageTracker>& usage_tracker) {
    if (!usage_tracker || usage_registration_) {
        return;
    }
    AttachUsageTracker(usage_tracker, capacity());
}

void BufferAllocatorBase::RecordAllocation(size_t bytes) noexcept {
    cur_size_.fetch_add(bytes, std::memory_order_relaxed);
    if (usage_registration_) {
        usage_registration_->AddUsedBytes(bytes);
    }
}

void BufferAllocatorBase::RecordDeallocation(size_t bytes) noexcept {
    cur_size_.fetch_sub(bytes, std::memory_order_relaxed);
    if (usage_registration_) {
        usage_registration_->RemoveUsedBytes(bytes);
    }
}

std::string AllocatedBuffer::getSegmentName() const noexcept {
    auto alloc = getAllocator();
    if (alloc) {
        return alloc->getSegmentName();
    }
    return std::string();
}

std::shared_ptr<const BufferOrigin> BufferAllocatorBase::GetBufferOrigin(
    const std::shared_ptr<BufferAllocatorBase>& allocator) {
    if (!allocator) {
        static const auto empty = std::make_shared<const BufferOrigin>();
        return empty;
    }
    std::call_once(allocator->origin_once_, [&] {
        auto root = std::make_shared<BufferOrigin>();
        root->allocator = allocator;
        root->offset_allocator = allocator->OffsetOwner();
        root->protocol = allocator->getTransportProtocol();
        allocator->origin_ = std::move(root);
    });
    return allocator->origin_;
}

std::shared_ptr<const BufferOrigin> BufferOrigin::ForTransport(
    const std::shared_ptr<const BufferOrigin>& source,
    std::string_view protocol, std::string_view cxl_endpoint,
    uintptr_t transfer_address_base) {
    const auto matches = [&](const BufferOrigin& origin) {
        return origin.protocol == protocol &&
               origin.cxl_endpoint == cxl_endpoint &&
               origin.transfer_address_base == transfer_address_base;
    };
    if (matches(*source)) return source;

    const auto make_variant = [&] {
        auto result = std::make_shared<BufferOrigin>(*source);
        result->protocol = protocol;
        result->cxl_endpoint = cxl_endpoint;
        result->transfer_address_base = transfer_address_base;
        return result;
    };
    // An already converted CXL buffer or an external release token can carry
    // private state. Do not build chains of cached overrides on these buffers.
    if (source->transfer_address_base || source->release_size)
        return make_variant();

    // Mounted CXL bindings and most standby endpoints use a single variant.
    // Once published it can be read concurrently without taking the mutex.
    auto& cache = source->transports;
    std::call_once(cache.first_once, [&] { cache.first = make_variant(); });
    if (matches(*cache.first)) return cache.first;

    std::lock_guard lock(cache.mutex);
    for (const auto& entry : cache.others) {
        auto result = entry.lock();
        if (result && matches(*result)) return result;
    }
    std::erase_if(cache.others,
                  [](const auto& entry) { return entry.expired(); });
    auto result = make_variant();
    cache.others.push_back(result);
    return result;
}

void AllocatedBuffer::ReleaseOffset() noexcept {
    using offset_allocator::OffsetAllocation;
    if (allocation_.isNoSpace()) return;
    const auto allocation = std::exchange(
        allocation_, OffsetAllocation{OffsetAllocation::NO_SPACE,
                                      OffsetAllocation::NO_SPACE});
    if (auto owner = origin_->offset_allocator.lock()) {
        owner->freeAllocation(allocation,
                              origin_->release_size.value_or(size_));
    }
}

AllocatedBuffer::~AllocatedBuffer() {
    if (auto owner = getAllocator()) {
        owner->deallocate(this);
    }
    // The inner allocator can outlive the mounting allocator.
    ReleaseOffset();
}

AllocatedBuffer::AllocatedBuffer(
    std::shared_ptr<BufferAllocatorBase> allocator, void* buffer_ptr,
    std::size_t size,
    std::optional<offset_allocator::OffsetAllocationHandle>&& offset_handle)
    : origin_(BufferAllocatorBase::GetBufferOrigin(allocator)),
      allocation_address_(buffer_ptr),
      size_(size) {
    if (offset_handle) {
        const auto& owner = offset_handle->m_allocator;
        if (owner.owner_before(origin_->offset_allocator) ||
            origin_->offset_allocator.owner_before(owner) ||
            offset_handle->size() != size) {
            auto origin = std::make_shared<BufferOrigin>(*origin_);
            origin->offset_allocator = owner;
            origin->release_size = offset_handle->size();
            origin_ = std::move(origin);
        }
        allocation_ = offset_handle->m_allocation;
        offset_handle->m_allocator.reset();
        offset_handle.reset();
    }
}

AllocatedBuffer::AllocatedBuffer(std::shared_ptr<BufferAllocatorBase> allocator,
                                 const AllocatedBuffer::Descriptor& descriptor)
    : AllocatedBuffer(
          std::move(allocator),
          reinterpret_cast<void*>(
              descriptor.buffer_address_ +
              (descriptor.protocol_ == "cxl" ? DEFAULT_CXL_BASE : 0)),
          descriptor.size_) {
    if (descriptor.protocol_ != origin_->protocol ||
        descriptor.protocol_ == "cxl") {
        const bool cxl = descriptor.protocol_ == "cxl";
        origin_ = BufferOrigin::ForTransport(
            origin_, descriptor.protocol_,
            cxl ? std::string_view(descriptor.transport_endpoint_)
                : std::string_view{},
            cxl ? DEFAULT_CXL_BASE : 0);
    }
}

void AllocatedBuffer::bindSegmentLifetime(SegmentLifetime lifetime) {
    auto bound = std::make_shared<BufferOrigin>(*origin_);
    bound->lifetime = std::move(lifetime);
    origin_ = std::move(bound);
}

std::string AllocatedBuffer::getTransportEndpoint(
    std::optional<SegmentLocality>* locality) const {
    auto alloc = getAllocator();
    std::string endpoint;
    if (alloc) {
        endpoint = alloc->getTransportEndpoint();
        if (locality) *locality = alloc->GetLocality();
    } else {
        // The owning segment was unmounted while this allocation still
        // exists (e.g. its client went offline and metadata cleanup is
        // still pending). Producing an empty endpoint is a defect signal:
        // callers that serialize or use this descriptor get an unusable
        // replica descriptor. Rate-limit: a stale-metadata sweep can hit
        // this once per leftover replica, which floods the log otherwise.
        LOG_EVERY_N(ERROR, 10000)
            << "allocator=expired_or_null in getTransportEndpoint";
    }

    if (origin_->protocol == "cxl") {
        endpoint = origin_->cxl_endpoint;
    }
    return endpoint;
}

// Implementation of get_descriptor
AllocatedBuffer::Descriptor AllocatedBuffer::get_descriptor(
    std::optional<SegmentLocality>* locality) const {
    return {static_cast<uint64_t>(size()), reinterpret_cast<uintptr_t>(data()),
            origin_->protocol, getTransportEndpoint(locality)};
}

void AllocatedBuffer::change_to_cxl(std::string_view client_segment_name) {
    origin_ = BufferOrigin::ForTransport(origin_, "cxl", client_segment_name,
                                         DEFAULT_CXL_BASE);
}

void* AllocatedBuffer::get_vaddr_from_cxl() { return allocation_address_; }

// Define operator<< using public accessors or get_descriptor if appropriate
std::ostream& operator<<(std::ostream& os, const AllocatedBuffer& buffer) {
    const auto allocator = buffer.getAllocator();
    return os << "AllocatedBuffer: { "
              << "segment_name: "
              << (allocator ? allocator->getSegmentName()
                            : std::string("<expired>"))
              << ", "
              << "size: " << buffer.size() << ", "
              << "buffer_ptr: " << static_cast<void*>(buffer.data()) << " }";
}

tl::expected<std::shared_ptr<CachelibBufferAllocator>, ErrorCode>
CachelibBufferAllocator::Create(std::string segment_name, size_t base,
                                size_t size, std::string transport_endpoint,
                                std::string transport_protocol,
                                ReplicaType replica_type) {
    if (!IsValidCachelibLayout(base, size)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // CacheLib's parameter-dependent constructor failures are covered by the
    // layout validation above. Do not catch allocation failures here: metadata
    // exhaustion follows the process-level fail-fast policy.
    return std::shared_ptr<CachelibBufferAllocator>(new CachelibBufferAllocator(
        std::move(segment_name), base, size, std::move(transport_endpoint),
        std::move(transport_protocol), replica_type));
}

tl::expected<std::shared_ptr<BufferAllocatorBase>, ErrorCode>
CreateBufferAllocator(BufferAllocatorType allocator_type,
                      std::string segment_name, size_t base, size_t size,
                      std::string transport_endpoint,
                      std::string transport_protocol,
                      ReplicaType replica_type) {
    switch (allocator_type) {
        case BufferAllocatorType::CACHELIB: {
            auto allocator = CachelibBufferAllocator::Create(
                std::move(segment_name), base, size,
                std::move(transport_endpoint), std::move(transport_protocol),
                replica_type);
            if (!allocator) {
                return tl::make_unexpected(allocator.error());
            }
            return std::shared_ptr<BufferAllocatorBase>(std::move(*allocator));
        }
        case BufferAllocatorType::OFFSET:
            // Offset construction has no parameter-dependent throwing path;
            // metadata allocation failures intentionally follow fail-fast.
            return std::shared_ptr<BufferAllocatorBase>(
                std::make_shared<OffsetBufferAllocator>(
                    std::move(segment_name), base, size,
                    std::move(transport_endpoint),
                    std::move(transport_protocol), replica_type));
        default:
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
}

CachelibBufferAllocator::CachelibBufferAllocator(std::string segment_name,
                                                 size_t base, size_t size,
                                                 std::string transport_endpoint,
                                                 std::string transport_protocol,
                                                 ReplicaType replica_type)
    : segment_name_(segment_name),
      base_(base),
      total_size_(size),
      transport_endpoint_(std::move(transport_endpoint)),
      transport_protocol_(std::move(transport_protocol)),
      replica_type_(replica_type) {
    VLOG(1) << "initializing_buffer_allocator segment_name=" << segment_name
            << " base_address=" << reinterpret_cast<void*>(base)
            << " size=" << size;

    // Calculate the size of the header region.
    header_region_size_ =
        sizeof(facebook::cachelib::SlabHeader) *
            static_cast<unsigned int>(size / sizeof(facebook::cachelib::Slab)) +
        1;
    header_region_start_ = std::make_unique<char[]>(header_region_size_);

    LOG_ASSERT(header_region_start_);

    // Initialize the CacheLib MemoryAllocator.
    memory_allocator_ = std::make_unique<facebook::cachelib::MemoryAllocator>(
        facebook::cachelib::MemoryAllocator::Config(
            facebook::cachelib::MemoryAllocator::generateAllocSizes()),
        reinterpret_cast<void*>(header_region_start_.get()),
        header_region_size_, reinterpret_cast<void*>(base), size);

    if (!memory_allocator_) {
        LOG(ERROR) << "status=failed_to_init_facebook_memory_allocator";
    }

    // Add the main pool to the allocator.
    pool_id_ = memory_allocator_->addPool("main", size);
    VLOG(1) << "buffer_allocator_initialized pool_id="
            << static_cast<int>(pool_id_);
}

CachelibBufferAllocator::~CachelibBufferAllocator() {
    if (replica_type_ == ReplicaType::MEMORY) {
        MasterMetricManager::instance().dec_allocated_mem_size(segment_name_,
                                                               size());
        MasterMetricManager::instance().remove_segment_metrics(segment_name_);
    } else if (replica_type_ == ReplicaType::NOF_SSD) {
        MasterMetricManager::instance().dec_allocated_nof_size(segment_name_,
                                                               size());
    }
};

std::unique_ptr<AllocatedBuffer> CachelibBufferAllocator::allocate(
    size_t size) {
    // CacheLib's free list can itself allocate. Prepare the complete owner
    // before taking a block so an origin/handle OOM needs no rollback.
    auto handle =
        std::make_unique<AllocatedBuffer>(shared_from_this(), nullptr, size);
    void* buffer = nullptr;
    try {
        // Allocate memory using CacheLib.
        size_t padding_size = std::max<size_t>(size, kMinSliceSize);
        buffer = memory_allocator_->allocate(pool_id_, padding_size);
        if (!buffer) {
            VLOG(1) << "allocation_failed size=" << size
                    << " segment=" << segment_name_
                    << " current_size=" << GetUsageBytes();
            return nullptr;
        }
    } catch (const std::exception& e) {
        LOG(ERROR) << "allocation_exception error=" << e.what();
        return nullptr;
    } catch (...) {
        LOG(ERROR) << "allocation_unknown_exception";
        return nullptr;
    }
    adoptBuffer(*handle, buffer);
    return handle;
}

void CachelibBufferAllocator::adoptBuffer(AllocatedBuffer& handle,
                                          void* buffer) {
    handle.allocation_address_ = buffer;
    const auto size = handle.size();
    RecordAllocation(size);
    if (replica_type_ == ReplicaType::MEMORY) {
        MasterMetricManager::instance().inc_allocated_mem_size(segment_name_,
                                                               size);
    } else if (replica_type_ == ReplicaType::NOF_SSD) {
        MasterMetricManager::instance().inc_allocated_nof_size(segment_name_,
                                                               size);
    }
    VLOG(1) << "allocation_succeeded size=" << size
            << " segment=" << segment_name_ << " address=" << buffer;
}

void CachelibBufferAllocator::deallocate(AllocatedBuffer* handle) {
    if (!handle->allocation_address_) return;
    try {
        // Deallocate memory using CacheLib.
        memory_allocator_->free(handle->allocation_address_);
        size_t freed_size =
            handle->size_;  // Store size before handle might become invalid
        RecordDeallocation(freed_size);
        if (replica_type_ == ReplicaType::MEMORY) {
            MasterMetricManager::instance().dec_allocated_mem_size(
                segment_name_, freed_size);
        } else if (replica_type_ == ReplicaType::NOF_SSD) {
            MasterMetricManager::instance().dec_allocated_nof_size(
                segment_name_, freed_size);
        }
        VLOG(1) << "deallocation_succeeded address="
                << handle->allocation_address_ << " size=" << freed_size
                << " segment=" << segment_name_;
    } catch (const std::exception& e) {
        LOG(ERROR) << "deallocation_exception error=" << e.what();
    } catch (...) {
        LOG(ERROR) << "deallocation_unknown_exception";
    }
}

std::optional<RestoredCachelibBufferAllocator> ImportCachelibBufferAllocator(
    std::string segment_name, size_t base, size_t size,
    std::string transport_endpoint, std::string transport_protocol,
    const std::vector<LiveAllocation>& allocations, ReplicaType replica_type) {
    if (replica_type != ReplicaType::MEMORY ||
        !IsValidCachelibLayout(base, size)) {
        return std::nullopt;
    }
    std::vector<MemoryAllocator::ImportedAllocation> imports;
    imports.reserve(allocations.size());
    for (const auto& allocation : allocations) {
        if (!IsValidAllocation(allocation, size) ||
            allocation.requested_size > UINT32_MAX) {
            return std::nullopt;
        }
        imports.push_back(
            {reinterpret_cast<void*>(base + allocation.offset_from_base),
             static_cast<uint32_t>(std::max<uint64_t>(allocation.requested_size,
                                                      kMinSliceSize))});
    }

    auto created = CachelibBufferAllocator::Create(
        std::move(segment_name), base, size, std::move(transport_endpoint),
        std::move(transport_protocol), replica_type);
    if (!created) {
        return std::nullopt;
    }
    auto allocator = std::move(*created);
    if (!allocator->memory_allocator_->importAllocations(allocator->pool_id_,
                                                         imports)) {
        return std::nullopt;
    }

    std::vector<std::unique_ptr<AllocatedBuffer>> buffers;
    buffers.reserve(allocations.size());
    for (const auto& allocation : allocations) {
        auto handle = std::make_unique<AllocatedBuffer>(
            allocator, nullptr, allocation.requested_size);
        allocator->adoptBuffer(
            *handle,
            reinterpret_cast<void*>(base + allocation.offset_from_base));
        buffers.push_back(std::move(handle));
    }
    return RestoredCachelibBufferAllocator{std::move(allocator),
                                           std::move(buffers)};
}

// OffsetBufferAllocator implementation
OffsetBufferAllocator::OffsetBufferAllocator(std::string segment_name,
                                             size_t base, size_t size,
                                             std::string transport_endpoint,
                                             std::string transport_protocol,
                                             ReplicaType replica_type)
    : segment_name_(segment_name),
      base_(base),
      total_size_(size),
      transport_endpoint_(std::move(transport_endpoint)),
      transport_protocol_(std::move(transport_protocol)),
      replica_type_(replica_type) {
    VLOG(1) << "initializing_offset_buffer_allocator segment_name="
            << segment_name << " base_address=" << reinterpret_cast<void*>(base)
            << " size=" << size;

    try {
        // 1k <= init_capacity <= 64k
        uint64_t init_capacity = size / 4096;
        init_capacity = std::max(init_capacity, static_cast<uint64_t>(1024));
        init_capacity =
            std::min(init_capacity, static_cast<uint64_t>(64 * 1024));
        // 1M <= max_capacity <= 64G / 1K = 64M
        uint64_t max_capacity = size / 1024;
        max_capacity =
            std::max(max_capacity, static_cast<uint64_t>(1024 * 1024));
        max_capacity =
            std::min(max_capacity, static_cast<uint64_t>(64 * 1024 * 1024));
        // Create the offset allocator
        offset_allocator_ = offset_allocator::OffsetAllocator::create(
            base, size, static_cast<uint32_t>(init_capacity),
            static_cast<uint32_t>(max_capacity));
        if (!offset_allocator_) {
            LOG(ERROR) << "status=failed_to_create_offset_allocator";
            throw std::runtime_error("Failed to create offset allocator");
        }

        VLOG(1) << "offset_buffer_allocator_initialized segment_name="
                << segment_name;
    } catch (const std::exception& e) {
        LOG(ERROR) << "offset_allocator_init_exception error=" << e.what();
        throw;
    }
}

OffsetBufferAllocator::~OffsetBufferAllocator() {
    if (replica_type_ == ReplicaType::MEMORY) {
        MasterMetricManager::instance().dec_allocated_mem_size(segment_name_,
                                                               size());
        MasterMetricManager::instance().remove_segment_metrics(segment_name_);
    } else if (replica_type_ == ReplicaType::NOF_SSD) {
        MasterMetricManager::instance().dec_allocated_nof_size(segment_name_,
                                                               size());
    }
};

std::unique_ptr<AllocatedBuffer> OffsetBufferAllocator::allocate(size_t size) {
    if (!offset_allocator_) {
        LOG(ERROR) << "allocator_status=not_initialized";
        return nullptr;
    }

    std::unique_ptr<AllocatedBuffer> allocated_buffer = nullptr;
    try {
        // Allocate memory using OffsetAllocator
        auto allocation_handle = offset_allocator_->allocate(size);
        if (!allocation_handle) {
            VLOG(1) << "allocation_failed size=" << size
                    << " segment=" << segment_name_
                    << " current_size=" << GetUsageBytes();
            return nullptr;
        }

        // Create AllocatedBuffer with the allocated memory
        void* buffer_ptr = allocation_handle->ptr();

        // Create a custom AllocatedBuffer that manages the
        // OffsetAllocationHandle
        allocated_buffer = std::make_unique<AllocatedBuffer>(
            shared_from_this(), buffer_ptr, size, std::move(allocation_handle));
        VLOG(1) << "allocation_succeeded size=" << size
                << " segment=" << segment_name_ << " address=" << buffer_ptr;
    } catch (const std::exception& e) {
        LOG(ERROR) << "allocation_exception error=" << e.what();
        return nullptr;
    } catch (...) {
        LOG(ERROR) << "allocation_unknown_exception";
        return nullptr;
    }

    RecordAllocation(size);
    if (replica_type_ == ReplicaType::MEMORY) {
        MasterMetricManager::instance().inc_allocated_mem_size(segment_name_,
                                                               size);
    } else if (replica_type_ == ReplicaType::NOF_SSD) {
        MasterMetricManager::instance().inc_allocated_nof_size(segment_name_,
                                                               size);
    }
    return allocated_buffer;
}

void OffsetBufferAllocator::deallocate(AllocatedBuffer* handle) {
    try {
        // Consume the allocation token once, before publishing the new usage.
        size_t freed_size = handle->size();
        handle->ReleaseOffset();
        RecordDeallocation(freed_size);
        if (replica_type_ == ReplicaType::MEMORY) {
            MasterMetricManager::instance().dec_allocated_mem_size(
                segment_name_, freed_size);
        } else if (replica_type_ == ReplicaType::NOF_SSD) {
            MasterMetricManager::instance().dec_allocated_nof_size(
                segment_name_, freed_size);
        }
        VLOG(1) << "deallocation_succeeded address=" << handle->data()
                << " size=" << freed_size << " segment=" << segment_name_;
    } catch (const std::exception& e) {
        LOG(ERROR) << "deallocation_exception error=" << e.what();
    } catch (...) {
        LOG(ERROR) << "deallocation_unknown_exception";
    }
}

size_t OffsetBufferAllocator::getLargestFreeRegion() const {
    if (!offset_allocator_) {
        return 0;
    }

    try {
        auto report = offset_allocator_->storageReport();
        return report.largestFreeRegion;
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to get storage report: " << e.what()
                   << " segment=" << segment_name_;
        return 0;
    } catch (...) {
        LOG(ERROR) << "Unknown error getting storage report"
                   << " segment=" << segment_name_;
        return 0;
    }
}

std::optional<RestoredOffsetBufferAllocator> ImportOffsetBufferAllocator(
    std::string segment_name, size_t base, size_t size,
    std::string transport_endpoint, std::string transport_protocol,
    const std::vector<LiveAllocation>& allocations, ReplicaType replica_type) {
    if (base > std::numeric_limits<size_t>::max() - size) {
        return std::nullopt;
    }
    const size_t end = base + size;
    auto allocator = std::make_shared<OffsetBufferAllocator>(
        std::move(segment_name), base, size, std::move(transport_endpoint),
        std::move(transport_protocol), replica_type);
    const auto offset_allocator = allocator->getOffsetAllocator();

    std::vector<size_t> order(allocations.size());
    std::iota(order.begin(), order.end(), 0);
    std::sort(order.begin(), order.end(), [&](size_t lhs, size_t rhs) {
        return allocations[lhs].offset_from_base <
               allocations[rhs].offset_from_base;
    });

    std::vector<std::unique_ptr<AllocatedBuffer>> buffers(allocations.size());
    std::vector<std::unique_ptr<AllocatedBuffer>> gaps;
    size_t cursor = base;

    auto fill_gap = [&](size_t gap) {
        while (gap != 0) {
            size_t low = 1;
            size_t high = gap;
            size_t request = 0;
            while (low <= high) {
                const size_t mid = low + (high - low) / 2;
                const uint64_t normalized =
                    offset_allocator->normalizedAllocationSize(mid);
                if (normalized != 0 && normalized <= gap) {
                    request = mid;
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            }
            if (request == 0) {
                return false;
            }
            const size_t occupied =
                offset_allocator->normalizedAllocationSize(request);
            if (occupied == 0 || occupied > gap) {
                return false;
            }
            auto filler = allocator->allocate(request);
            if (!filler || reinterpret_cast<size_t>(filler->data()) != cursor) {
                return false;
            }
            cursor += occupied;
            gap -= occupied;
            gaps.push_back(std::move(filler));
        }
        return true;
    };

    for (const size_t index : order) {
        const auto& allocation = allocations[index];
        if (!IsValidAllocation(allocation, size)) {
            return std::nullopt;
        }
        const size_t address = base + allocation.offset_from_base;
        if (address < cursor) {
            return std::nullopt;
        }
        const uint64_t occupied = offset_allocator->normalizedAllocationSize(
            allocation.requested_size);
        if (occupied == 0 || occupied > end - address ||
            !fill_gap(address - cursor)) {
            return std::nullopt;
        }
        auto buffer = allocator->allocate(allocation.requested_size);
        if (!buffer || reinterpret_cast<size_t>(buffer->data()) != address) {
            return std::nullopt;
        }
        cursor = address + occupied;
        buffers[index] = std::move(buffer);
    }

    gaps.clear();
    return RestoredOffsetBufferAllocator{std::move(allocator),
                                         std::move(buffers)};
}

SimpleAllocator::SimpleAllocator(size_t size) {
    LOG(INFO) << "initializing_simple_allocator size=" << size;

    try {
        // Allocate the base memory region
        base_ = std::aligned_alloc(facebook::cachelib::Slab::kSize, size);
        if (!base_) {
            LOG(ERROR) << "base_memory_allocation_failed size=" << size;
            throw std::bad_alloc();
        }

        // Calculate header region size similar to BufferAllocator
        header_region_size_ = sizeof(facebook::cachelib::SlabHeader) *
                                  static_cast<unsigned int>(
                                      size / sizeof(facebook::cachelib::Slab)) +
                              1;

        header_region_start_ = std::make_unique<char[]>(header_region_size_);
        if (!header_region_start_) {
            std::free(base_);
            LOG(ERROR) << "header_region_allocation_failed size="
                       << header_region_size_;
            throw std::bad_alloc();
        }

        // Initialize CacheLib memory allocator
        memory_allocator_ =
            std::make_unique<facebook::cachelib::MemoryAllocator>(
                facebook::cachelib::MemoryAllocator::Config(
                    facebook::cachelib::MemoryAllocator::generateAllocSizes()),
                header_region_start_.get(), header_region_size_, base_, size);

        if (!memory_allocator_) {
            std::free(base_);
            LOG(ERROR) << "cachelib_memory_allocator_init_failed";
            throw std::runtime_error("Failed to initialize memory allocator");
        }

        // Add main memory pool
        pool_id_ = memory_allocator_->addPool("main", size);
        LOG(INFO) << "simple_allocator_initialized pool_id="
                  << static_cast<int>(pool_id_);

    } catch (const std::exception& e) {
        if (base_) {
            std::free(base_);
            base_ = nullptr;
        }
        LOG(ERROR) << "simple_allocator_init_exception error=" << e.what();
        throw;
    }
}

SimpleAllocator::~SimpleAllocator() {
    try {
        if (memory_allocator_) {
            memory_allocator_.reset();
        }
        if (base_) {
            std::free(base_);
            base_ = nullptr;
        }
        LOG(INFO) << "simple_allocator_destroyed status=success";
    } catch (const std::exception& e) {
        LOG(ERROR) << "simple_allocator_destruction_exception error="
                   << e.what();
    }
}

void* SimpleAllocator::allocate(size_t size) {
    if (!memory_allocator_) {
        LOG(ERROR) << "allocator_status=not_initialized";
        return nullptr;
    }

    try {
        size_t padding_size = std::max<size_t>(size, kMinSliceSize);
        void* ptr = memory_allocator_->allocate(pool_id_, padding_size);
        if (!ptr) {
            // This allocator is used in client side. Though the failure will
            // not cause any critical issue, it generally indicates the local
            // buffer is not large enough. So we log it as warning.
            LOG(WARNING) << "allocation_failed size=" << size;
            return nullptr;
        }
        VLOG(1) << "allocation_succeeded size=" << size << " address=" << ptr;
        return ptr;
    } catch (const std::exception& e) {
        LOG(ERROR) << "allocation_exception error=" << e.what();
        return nullptr;
    } catch (...) {
        LOG(ERROR) << "allocation_unknown_exception";
        return nullptr;
    }
}

void SimpleAllocator::deallocate(void* ptr, size_t size) {
    if (!memory_allocator_ || !ptr) {
        LOG(WARNING) << "invalid_deallocation_request allocator="
                     << (memory_allocator_ ? "valid" : "null")
                     << " ptr=" << (ptr ? "valid" : "null");
        return;
    }

    try {
        memory_allocator_->free(ptr);
        VLOG(1) << "deallocation_succeeded size=" << size << " address=" << ptr;
    } catch (const std::exception& e) {
        LOG(ERROR) << "deallocation_exception error=" << e.what();
    } catch (...) {
        LOG(ERROR) << "deallocation_unknown_exception";
    }
}

}  // namespace mooncake
