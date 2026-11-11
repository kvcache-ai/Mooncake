# Host-only tests of Mooncake EP code shared by every backend. They need no GPU
# and no EP build, so the top level adds them whenever unit tests are on.
add_executable(ep_dispatch_slots_test dispatch_slots_test.cpp)
target_include_directories(ep_dispatch_slots_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_link_libraries(ep_dispatch_slots_test PRIVATE gtest gtest_main pthread)
add_test(NAME ep_dispatch_slots_test COMMAND ep_dispatch_slots_test)

add_executable(
  ep_jit_compile_failure_test
  jit_compile_failure_test.cpp jit_test_cuda_stubs.cpp ../src/jit/jit_cache.cpp
  ../src/jit/jit_runtime.cpp ../src/jit/jit_utils.cpp
  ../src/jit/nvcc_compiler.cpp)
target_include_directories(
  ep_jit_compile_failure_test
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/cuda_stubs
          ${CMAKE_CURRENT_SOURCE_DIR}/test_config
          ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_link_libraries(ep_jit_compile_failure_test PRIVATE gtest pthread
                                                          ${CMAKE_DL_LIBS})
add_test(NAME ep_jit_compile_failure_test COMMAND ep_jit_compile_failure_test)
