// buffer_allocator.cpp
#include "allocator.h"

#include <glog/logging.h>

namespace mooncake {

BufHandle::BufHandle(std::shared_ptr<BufferAllocator> allocator,
                     std::string segment_name, uint64_t size, void* buffer)
    : segment_id(0),
      segment_name(segment_name),
      size(size),
      status(BufStatus::INIT),
      buffer(buffer),
      allocator_(allocator) {
    VLOG(1) << "buf_handle_created segment_id=" << segment_id
            << " size=" << size << " buffer_address=" << buffer;
}

bool BufHandle::isAllocatorValid() const { return !allocator_.expired(); }

BufHandle::~BufHandle() {
    auto alloc = allocator_.lock();
    if (alloc) {
        alloc->deallocate(this);
        VLOG(1) << "buf_handle_deallocated segment_id=" << segment_id
                << " size=" << size;
    } else {
        LOG(WARNING) << "allocator=expired_or_null in buf_handle_destructor";
    }
}

BufferAllocator::BufferAllocator(std::string segment_name, size_t base,
                                 size_t size)
    : segment_name_(segment_name), base_(base), total_size_(size) {
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

BufferAllocator::~BufferAllocator() = default;

std::shared_ptr<BufHandle> BufferAllocator::allocate(size_t size) {
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
    // Create and return a new BufHandle.
    cur_size_.fetch_add(size);
    return std::make_shared<BufHandle>(shared_from_this(), segment_name_, size,
                                       buffer);
}

void BufferAllocator::deallocate(BufHandle* handle) {
    try {
        // Deallocate memory using CacheLib.
        memory_allocator_->free(handle->buffer);
        handle->status = BufStatus::UNREGISTERED;
        cur_size_.fetch_sub(handle->size);
        VLOG(1) << "deallocation_succeeded address=" << handle->buffer
                << " size=" << handle->size << " segment=" << segment_name_;
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
