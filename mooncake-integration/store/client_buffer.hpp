#pragma once

#include <optional>

#include "offset_allocator/offset_allocator.hpp"

namespace mooncake {

class BufferHandle;

class ClientBufferAllocator
    : public std::enable_shared_from_this<ClientBufferAllocator> {
   public:
    static std::shared_ptr<ClientBufferAllocator> create(size_t size) {
        return std::shared_ptr<ClientBufferAllocator>(
            new ClientBufferAllocator(size));
    }

    ~ClientBufferAllocator();

    void* getBase() const { return reinterpret_cast<void*>(buffer_.get()); }

    std::optional<BufferHandle> allocate(size_t size);

   private:
    ClientBufferAllocator(size_t size);
    std::shared_ptr<offset_allocator::OffsetAllocator> allocator_;

    std::unique_ptr<char[]> buffer_;
};

class BufferHandle {
   public:
    BufferHandle(std::shared_ptr<ClientBufferAllocator> allocator,
                 offset_allocator::OffsetAllocationHandle handle, void* ptr,
                 size_t size);
    ~BufferHandle();

    BufferHandle(BufferHandle&&) = default;  // 允许移动
    BufferHandle& operator=(BufferHandle&&) = default;

    void* ptr() const;
    size_t size() const;

   private:
    std::shared_ptr<ClientBufferAllocator> allocator_;
    offset_allocator::OffsetAllocationHandle handle_;
    void* ptr_;
    size_t size_;
};

}  // namespace mooncake