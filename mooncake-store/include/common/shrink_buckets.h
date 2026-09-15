#pragma once

#include <cstddef>

namespace mooncake {

// erase() never returns bucket memory, so a container that once held far more
// entries than it does now keeps its high-water bucket array forever.
// ShrinkBucketsIfSparse rehashes a container down to roughly twice its live
// size once the bucket array is both large enough to matter and less than a
// quarter full. The bucket floor avoids rehash churn on small containers; the
// 2x headroom keeps a freshly shrunk container from growing again right away.
// Rehashing invalidates iterators: callers must hold the lock guarding the
// container and must not be iterating it.
inline constexpr size_t kShrinkMinBucketCount = 1024;

template <typename UnorderedContainer>
void ShrinkBucketsIfSparse(UnorderedContainer& container) {
    if (container.bucket_count() > kShrinkMinBucketCount &&
        container.size() < container.bucket_count() / 4) {
        container.rehash(container.size() * 2);
    }
}

}  // namespace mooncake
