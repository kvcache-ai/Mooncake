cmake_minimum_required(VERSION 3.12)

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


# The distributed_object_store module for Python
include_directories(vllm)
pybind11_add_module(distributed_object_store vllm/distributed_object_store.cpp ${CACHE_ALLOCATOR_SOURCES})
# set_target_properties(distributed_object_store PROPERTIES NO_SONAME ON)
target_link_libraries(distributed_object_store PUBLIC transfer_engine glog gflags cache_allocator cachelib_memory_allocator)

install(TARGETS distributed_object_store DESTINATION ${PYTHON_SYS_PATH}/)

# install(TARGETS distributed_object_store DESTINATION .)

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