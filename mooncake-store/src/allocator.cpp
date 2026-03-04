// buffer_allocator.cpp
#include "allocator.h"

#include <glog/logging.h>

#include <memory>

#include "master_metric_manager.h"

namespace mooncake {

std::string AllocatedBuffer::getSegmentName() const noexcept {
    auto alloc = allocator_.lock();
    if (alloc) {
        return alloc->getSegmentName();
    }
    return std::string();
}

AllocatedBuffer::~AllocatedBuffer() {
    // Note: This is an edge case. If the 'weak_ptr' is released, the segment
    // has already been deallocated at this point, and its memory usage details
    // (capacity/allocated) no longer need to be maintained.
    auto alloc = allocator_.lock();
    if (alloc) {
        alloc->deallocate(this);
        VLOG(1) << "buf_handle_deallocated size=" << size_;
    }
}

// Implementation of get_descriptor
AllocatedBuffer::Descriptor AllocatedBuffer::get_descriptor() const {
    auto alloc = allocator_.lock();
    std::string endpoint;
    if (alloc) {
        endpoint = alloc->getTransportEndpoint();
    } else {
        LOG(ERROR) << "allocator=expired_or_null in get_descriptor";
    }
    return {static_cast<uint64_t>(size()),
            reinterpret_cast<uintptr_t>(buffer_ptr_), endpoint};
}

// Define operator<< using public accessors or get_descriptor if appropriate
std::ostream& operator<<(std::ostream& os, const AllocatedBuffer& buffer) {
    return os << "AllocatedBuffer: { "
              << "segment_name: "
              << (buffer.allocator_.lock()
                      ? buffer.allocator_.lock()->getSegmentName()
                      : std::string("<expired>"))
              << ", "
              << "size: " << buffer.size() << ", "
              << "buffer_ptr: " << static_cast<void*>(buffer.data()) << " }";
}

// Removed allocated_bytes parameter and member initialization
CachelibBufferAllocator::CachelibBufferAllocator(std::string segment_name,
                                                 size_t base, size_t size,
                                                 std::string transport_endpoint,
                                                 const UUID& segment_id)
    : segment_name_(segment_name),
      base_(base),
      total_size_(size),
      cur_size_(0),
      transport_endpoint_(std::move(transport_endpoint)),
      segment_id_(segment_id) {
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
    MasterMetricManager::instance().dec_allocated_mem_size(segment_name_,
                                                           cur_size_);
};

std::unique_ptr<AllocatedBuffer> CachelibBufferAllocator::allocate(
    size_t size) {
    void* buffer = nullptr;
    try {
        // Allocate memory using CacheLib.
        size_t padding_size = std::max(size, kMinSliceSize);
        buffer = memory_allocator_->allocate(pool_id_, padding_size);
        if (!buffer) {
            VLOG(1) << "allocation_failed size=" << size
                    << " segment=" << segment_name_
                    << " current_size=" << cur_size_;
            return nullptr;
        }
    } catch (const std::exception& e) {
        LOG(ERROR) << "allocation_exception error=" << e.what();
        return nullptr;
    } catch (...) {
        LOG(ERROR) << "allocation_unknown_exception";
        return nullptr;
    }
    VLOG(1) << "allocation_succeeded size=" << size
            << " segment=" << segment_name_ << " address=" << buffer;
    cur_size_.fetch_add(size);
    MasterMetricManager::instance().inc_allocated_mem_size(segment_name_, size);
    return std::make_unique<AllocatedBuffer>(shared_from_this(), buffer, size,
                                             segment_id_);
}

void CachelibBufferAllocator::deallocate(AllocatedBuffer* handle) {
    try {
        // Deallocate memory using CacheLib.
        memory_allocator_->free(handle->buffer_ptr_);
        size_t freed_size =
            handle->size_;  // Store size before handle might become invalid
        cur_size_.fetch_sub(freed_size);
        MasterMetricManager::instance().dec_allocated_mem_size(segment_name_,
                                                               freed_size);
        VLOG(1) << "deallocation_succeeded address=" << handle->buffer_ptr_
                << " size=" << freed_size << " segment=" << segment_name_;
    } catch (const std::exception& e) {
        LOG(ERROR) << "deallocation_exception error=" << e.what();
    } catch (...) {
        LOG(ERROR) << "deallocation_unknown_exception";
    }
}

// OffsetBufferAllocator implementation
OffsetBufferAllocator::OffsetBufferAllocator(std::string segment_name,
                                             size_t base, size_t size,
                                             std::string transport_endpoint,
                                             const UUID& segment_id)
    : segment_name_(segment_name),
      base_(base),
      total_size_(size),
      cur_size_(0),
      transport_endpoint_(std::move(transport_endpoint)),
      segment_id_(segment_id) {
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
    MasterMetricManager::instance().dec_allocated_mem_size(segment_name_,
                                                           cur_size_);
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
                    << " current_size=" << cur_size_;
            return nullptr;
        }

        // Create AllocatedBuffer with the allocated memory
        void* buffer_ptr = allocation_handle->ptr();

        // Create a custom AllocatedBuffer that manages the
        // OffsetAllocationHandle
        allocated_buffer = std::make_unique<AllocatedBuffer>(
            shared_from_this(), buffer_ptr, size, segment_id_,
            std::move(allocation_handle));
        VLOG(1) << "allocation_succeeded size=" << size
                << " segment=" << segment_name_ << " address=" << buffer_ptr;
    } catch (const std::exception& e) {
        LOG(ERROR) << "allocation_exception error=" << e.what();
        return nullptr;
    } catch (...) {
        LOG(ERROR) << "allocation_unknown_exception";
        return nullptr;
    }

    cur_size_.fetch_add(size);
    MasterMetricManager::instance().inc_allocated_mem_size(segment_name_, size);
    return allocated_buffer;
}

void OffsetBufferAllocator::deallocate(AllocatedBuffer* handle) {
    try {
        // The OffsetAllocator handles deallocation automatically through RAII
        // when the OffsetAllocationHandle goes out of scope
        size_t freed_size = handle->size();
        handle->offset_handle_.reset();
        cur_size_.fetch_sub(freed_size);
        MasterMetricManager::instance().dec_allocated_mem_size(segment_name_,
                                                               freed_size);
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
        size_t padding_size = std::max(size, kMinSliceSize);
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
