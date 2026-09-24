project(cachelib_memory_allocator)

set(CMAKE_CXX_STANDARD 17)

add_library(
  cachelib_memory_allocator STATIC
  AllocationClass.cpp MemoryAllocator.cpp MemoryPool.cpp MemoryPoolManager.cpp
  Slab.cpp SlabAllocator.cpp)
target_include_directories(
  cachelib_memory_allocator
  PUBLIC ${MOONCAKE_STORE_INCLUDE_ROOT}/cachelib_memory_allocator
         ${MOONCAKE_STORE_INCLUDE_ROOT}/cachelib_memory_allocator/include
         ${MOONCAKE_STORE_INCLUDE_ROOT}/cachelib_memory_allocator/fake_include)
