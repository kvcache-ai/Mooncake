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
    etcd_helper.cpp
    ha_helper.cpp
    segment.cpp
    transfer_task.cpp
    etcd_helper.cpp
    ha_helper.cpp
)

# The cache_allocator library
include_directories(${Python3_INCLUDE_DIRS})
add_library(mooncake_store ${MOONCAKE_STORE_SOURCES})
target_link_libraries(mooncake_store PUBLIC transfer_engine ${ETCD_WRAPPER_LIB} glog::glog gflags::gflags)
if (STORE_USE_ETCD)
    add_dependencies(mooncake_store build_etcd_wrapper)
endif()

# Master binary
add_executable(mooncake_master master.cpp)
target_link_libraries(mooncake_master PRIVATE
    mooncake_store
    cachelib_memory_allocator
    pthread
    ${ETCD_WRAPPER_LIB}
)
if (STORE_USE_ETCD)
    add_dependencies(mooncake_master build_etcd_wrapper)
endif()

install(TARGETS mooncake_master DESTINATION bin)
