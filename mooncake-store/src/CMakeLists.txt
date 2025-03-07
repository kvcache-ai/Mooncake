# Find Python package
add_subdirectory(cachelib_memory_allocator)

set(CACHE_ALLOCATOR_SOURCES
    allocator.cpp
    client.cpp
    master.pb.cpp
    master.grpc.pb.cpp
    master_service.cpp
    types.cpp
    utils.cpp
)

# The cache_allocator library
include_directories(${Python3_INCLUDE_DIRS})
add_library(cache_allocator ${CACHE_ALLOCATOR_SOURCES})
target_link_libraries(cache_allocator PUBLIC transfer_engine glog gflags)

# Master binary
add_executable(mooncake_master master.cpp)
target_link_libraries(mooncake_master PRIVATE
    cache_allocator
    cachelib_memory_allocator
    grpc 
    grpc++
    protobuf
    pthread
)

install(TARGETS mooncake_master DESTINATION bin)
