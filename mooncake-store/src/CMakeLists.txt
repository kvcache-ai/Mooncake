# Find Python package
add_subdirectory(cachelib_memory_allocator)

set(MOONCAKE_STORE_SOURCES
    allocator.cpp
    master_service.cpp
    client.cpp
    client_metric.cpp
    types.cpp
    master_client.cpp
    utils.cpp
    master_metric_manager.cpp
    storage_backend.cpp
    thread_pool.cpp
    etcd_helper.cpp
    ha_helper.cpp
    segment.cpp
    transfer_task.cpp
    etcd_helper.cpp
    ha_helper.cpp
    rpc_service.cpp
    offset_allocator.cpp
    posix_file.cpp
    client_buffer.cpp
    pybind_client.cpp
    http_metadata_server.cpp
)

set(EXTRA_LIBS "")

if(USE_3FS)
  add_subdirectory(hf3fs)
  list(APPEND MOONCAKE_STORE_SOURCES  ${HF3FS_SOURCES})
  find_library(HF3FS_API_LIB hf3fs_api_shared PATHS /usr/lib NO_DEFAULT_PATH)
  if(NOT HF3FS_API_LIB)
    message(FATAL_ERROR "hf3fs_api_shared library not found in /usr/lib")
  endif()
  set(EXTRA_LIBS ${HF3FS_API_LIB})
endif()

# The cache_allocator library
include_directories(${Python3_INCLUDE_DIRS})
add_library(mooncake_store ${MOONCAKE_STORE_SOURCES})
target_link_libraries(mooncake_store PUBLIC transfer_engine cachelib_memory_allocator ${ETCD_WRAPPER_LIB} glog::glog gflags::gflags  
    ${EXTRA_LIBS}
)
if (STORE_USE_ETCD)
    add_dependencies(mooncake_store build_etcd_wrapper)
endif()

# Master binary
add_executable(mooncake_master master.cpp)
target_link_libraries(mooncake_master PRIVATE
    mooncake_store
    cachelib_memory_allocator
    pthread
    mooncake_common
    ${ETCD_WRAPPER_LIB}
)
if (STORE_USE_ETCD)
    add_dependencies(mooncake_master build_etcd_wrapper)
endif()

install(TARGETS mooncake_master DESTINATION bin)