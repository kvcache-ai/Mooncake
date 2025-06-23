add_executable(chaosctl chaosctl.cpp)
target_link_libraries(chaosctl PUBLIC
    mooncake_store
    cachelib_memory_allocator
    glog
    pthread
)

add_executable(chaosclient chaosclient.cpp)
target_link_libraries(chaosclient PUBLIC
    mooncake_store
    cachelib_memory_allocator
    glog
    pthread
)