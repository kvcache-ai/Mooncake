project(cachelib_memory_allocator)

add_library(
  cachelib_memory_allocator STATIC
  AllocationClass.cpp MemoryAllocator.cpp MemoryPool.cpp MemoryPoolManager.cpp
  Slab.cpp SlabAllocator.cpp)

target_include_directories(
  cachelib_memory_allocator
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../include/cachelib_memory_allocator>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../include/cachelib_memory_allocator/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../include/cachelib_memory_allocator/fake_include>
)
set_target_properties(
  cachelib_memory_allocator
  PROPERTIES CXX_STANDARD 17
             CXX_STANDARD_REQUIRED ON
             POSITION_INDEPENDENT_CODE ON)
