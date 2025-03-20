add_executable(buffer_allocator_test buffer_allocator_test.cpp)
target_link_libraries(buffer_allocator_test PUBLIC cache_allocator cachelib_memory_allocator gtest gtest_main pthread)
add_test(NAME buffer_allocator_test COMMAND buffer_allocator_test)

add_executable(master_service_test master_service_test.cpp)
target_link_libraries(master_service_test PUBLIC cache_allocator cachelib_memory_allocator glog gtest gtest_main pthread)
add_test(NAME master_service_test COMMAND master_service_test)

find_package(PkgConfig REQUIRED)
pkg_check_modules(GRPCPP REQUIRED grpc++)
pkg_check_modules(GRPC REQUIRED grpc)

add_executable(client_integration_test client_integration_test.cpp)
target_link_libraries(client_integration_test PUBLIC 
    cache_allocator 
    cachelib_memory_allocator 
    glog 
    gtest 
    ${GRPC_LDFLAGS}
    ${GRPCPP_LDFLAGS}
    protobuf 
    gtest_main 
    pthread
)
add_test(NAME client_integration_test COMMAND client_integration_test)

add_executable(stress_workload_test stress_workload_test.cpp)
target_link_libraries(stress_workload_test PUBLIC 
    cache_allocator 
    cachelib_memory_allocator 
    glog 
    gtest 
    ${GRPC_LDFLAGS}
    ${GRPCPP_LDFLAGS}
    protobuf 
    gtest_main 
    pthread
)