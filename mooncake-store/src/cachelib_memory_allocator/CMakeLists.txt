cmake_minimum_required(VERSION 3.12)
project(cachelib_memory_allocator)

set(CMAKE_CXX_STANDARD 17)

include_directories("include")
include_directories("fake_include")

include_directories("cachelib_memory_allocator/include")
include_directories("cachelib_memory_allocator/fake_include")

add_library (cachelib_memory_allocator
        STATIC
        AllocationClass.cpp
        MemoryAllocator.cpp
        MemoryPool.cpp
        MemoryPoolManager.cpp
        Slab.cpp
        SlabAllocator.cpp
)
