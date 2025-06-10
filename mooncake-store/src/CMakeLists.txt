# Find Python package
add_subdirectory(cachelib_memory_allocator)

set(MOONCAKE_STORE_SOURCES
    allocator.cpp
    master_service.cpp
    client.cpp
    types.cpp
    master_client.cpp
    utils.cpp
    master_metric_manager.cpp
)

if (USE_KV_EVENT)
    list(APPEND MOONCAKE_STORE_SOURCES kv_cache_publisher.cpp)
endif()

# The cache_allocator library
include_directories(${Python3_INCLUDE_DIRS})
add_library(mooncake_store ${MOONCAKE_STORE_SOURCES})

target_link_libraries(mooncake_store PUBLIC transfer_engine glog::glog gflags::gflags)


if (USE_KV_EVENT)
    target_link_libraries(mooncake_store PUBLIC zmq)
endif()

# Master binary
add_executable(mooncake_master master.cpp)
target_link_libraries(mooncake_master PRIVATE
    mooncake_store
    cachelib_memory_allocator
    pthread
)

install(TARGETS mooncake_master DESTINATION bin)
