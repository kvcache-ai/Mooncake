// buffer_allocator.cpp
#include "allocator.h"

#include <glog/logging.h>

#include <memory>

#include "master_metric_manager.h"

namespace mooncake {

AllocatedBuffer::~AllocatedBuffer() {
    auto alloc = allocator_.lock();
    if (alloc) {
        alloc->deallocate(this);
        VLOG(1) << "buf_handle_deallocated segment_name=" << segment_name_
                << " size=" << size_;
    } else {
        MasterMetricManager::instance().dec_allocated_size(size_);
        VLOG(1) << "allocator=expired_or_null in buf_handle_destructor";
    }
}

// Removed allocated_bytes parameter and member initialization
CachelibBufferAllocator::CachelibBufferAllocator(std::string segment_name,
                                                 size_t base, size_t size)
    : segment_name_(segment_name),
      base_(base),
      total_size_(size),
      cur_size_(0) {
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

CachelibBufferAllocator::~CachelibBufferAllocator() = default;

std::unique_ptr<AllocatedBuffer> CachelibBufferAllocator::allocate(
    size_t size) {
    void* buffer = nullptr;
    try {
        // Allocate memory using CacheLib.
        size_t padding_size = std::max(size, kMinSliceSize);
        buffer = memory_allocator_->allocate(pool_id_, padding_size);
        if (!buffer) {
            LOG(WARNING) << "allocation_failed size=" << size
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
    MasterMetricManager::instance().inc_allocated_size(size);
    return std::make_unique<AllocatedBuffer>(shared_from_this(), segment_name_,
                                             buffer, size);
}

void CachelibBufferAllocator::deallocate(AllocatedBuffer* handle) {
    try {
        // Deallocate memory using CacheLib.
        memory_allocator_->free(handle->buffer_ptr_);
        handle->status = BufStatus::UNREGISTERED;
        size_t freed_size =
            handle->size_;  // Store size before handle might become invalid
        cur_size_.fetch_sub(freed_size);
        MasterMetricManager::instance().dec_allocated_size(freed_size);
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
                                             size_t base, size_t size)
    : segment_name_(segment_name),
      base_(base),
      total_size_(size),
      cur_size_(0) {
    VLOG(1) << "initializing_offset_buffer_allocator segment_name="
            << segment_name << " base_address=" << reinterpret_cast<void*>(base)
            << " size=" << size;

    try {
        uint32_t max_allocs = size < (1ull << 32)
                                  ? size / 4096
                                  : 1024 * 1024;  // min(size / 4K, 1M)
        max_allocs =
            std::max(max_allocs, 1024u * 64u);  // at least 64K allocations
        // Create the offset allocator
        offset_allocator_ =
            offset_allocator::OffsetAllocator::create(base, size, max_allocs);
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

OffsetBufferAllocator::~OffsetBufferAllocator() = default;

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
            LOG(WARNING) << "allocation_failed size=" << size
                         << " segment=" << segment_name_
                         << " current_size=" << cur_size_;
            return nullptr;
        }

        // Create AllocatedBuffer with the allocated memory
        void* buffer_ptr = allocation_handle->ptr();

        // Create a custom AllocatedBuffer that manages the
        // OffsetAllocationHandle
        allocated_buffer = std::make_unique<AllocatedBuffer>(
            shared_from_this(), segment_name_, buffer_ptr, size,
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
    MasterMetricManager::instance().inc_allocated_size(size);
    return allocated_buffer;
}

void OffsetBufferAllocator::deallocate(AllocatedBuffer* handle) {
    try {
        // The OffsetAllocator handles deallocation automatically through RAII
        // when the OffsetAllocationHandle goes out of scope
        size_t freed_size = handle->size();
        handle->offset_handle_.reset();
        handle->status = BufStatus::UNREGISTERED;
        cur_size_.fetch_sub(freed_size);
        MasterMetricManager::instance().dec_allocated_size(freed_size);
        VLOG(1) << "deallocation_succeeded address=" << handle->data()
                << " size=" << freed_size << " segment=" << segment_name_;
    } catch (const std::exception& e) {
        LOG(ERROR) << "deallocation_exception error=" << e.what();
    } catch (...) {
        LOG(ERROR) << "deallocation_unknown_exception";
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
