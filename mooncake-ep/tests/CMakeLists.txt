add_executable(ep_jit_smoke_test ep_jit_smoke_test.cpp)
target_include_directories(ep_jit_smoke_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_link_libraries(ep_jit_smoke_test PUBLIC mooncake_ep_device gtest
                                               gtest_main pthread CUDA::cudart)
add_test(NAME ep_jit_smoke_test COMMAND ep_jit_smoke_test)
