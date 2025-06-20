add_executable(clientctl clientctl.cpp)
target_link_libraries(clientctl PUBLIC
    mooncake_store
    cachelib_memory_allocator
    glog
    pthread
)