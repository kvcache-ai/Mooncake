if(NOT USE_CUDA OR NOT UNIX)
  return()
endif()

set(cuda_plugin_context_source
    ${CMAKE_CURRENT_LIST_DIR}/../plugins/cuda/cuda_plugin.cpp)
set(cuda_plugin_context_wrap_options)
foreach(
  symbol
  cuInit
  cuPointerGetAttribute
  cuCtxGetCurrent
  cuCtxSetCurrent
  cudaGetDevice
  cudaSetDevice
  cudaMemcpy)
  list(APPEND cuda_plugin_context_wrap_options "-Wl,--wrap=${symbol}")
endforeach()

add_library(cuda_plugin_context_mock SHARED ${cuda_plugin_context_source})
target_link_libraries(cuda_plugin_context_mock PRIVATE tent_common CUDA::cudart
                                                       CUDA::cuda_driver)
target_link_options(cuda_plugin_context_mock PRIVATE
                    ${cuda_plugin_context_wrap_options})
set_target_properties(
  cuda_plugin_context_mock
  PROPERTIES LIBRARY_OUTPUT_DIRECTORY
             "${CMAKE_CURRENT_BINARY_DIR}/cuda_plugin_context_plugins")

add_executable(
  tent_cuda_plugin_context_test
  ${CMAKE_CURRENT_LIST_DIR}/cuda_plugin_context_test.cpp
  ${cuda_plugin_context_source}
  ${CMAKE_CURRENT_LIST_DIR}/../src/runtime/memory_prober.cpp)
target_link_libraries(
  tent_cuda_plugin_context_test
  PRIVATE tent_common
          gtest
          gtest_main
          CUDA::cudart
          CUDA::cuda_driver
          numa
          ${CMAKE_DL_LIBS})
target_link_options(tent_cuda_plugin_context_test PRIVATE
                    ${cuda_plugin_context_wrap_options})
target_compile_definitions(
  tent_cuda_plugin_context_test
  PRIVATE
    TENT_CUDA_PLUGIN_CONTEXT_MOCK
    CUDA_PLUGIN_CONTEXT_TEST_DIR="$<TARGET_FILE_DIR:cuda_plugin_context_mock>")
set_target_properties(tent_cuda_plugin_context_test PROPERTIES ENABLE_EXPORTS
                                                               ON)
add_dependencies(tent_cuda_plugin_context_test cuda_plugin_context_mock)
add_test(NAME tent_cuda_plugin_context_test
         COMMAND tent_cuda_plugin_context_test)

add_executable(
  tent_cuda_plugin_context_gpu_test
  ${CMAKE_CURRENT_LIST_DIR}/cuda_plugin_context_test.cpp
  ${cuda_plugin_context_source})
target_link_libraries(
  tent_cuda_plugin_context_gpu_test PRIVATE tent_common gtest gtest_main
                                            CUDA::cudart CUDA::cuda_driver)
add_test(NAME tent_cuda_plugin_context_gpu_test
         COMMAND tent_cuda_plugin_context_gpu_test)
