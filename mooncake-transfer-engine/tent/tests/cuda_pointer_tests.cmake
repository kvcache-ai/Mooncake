# The fault-injection targets require the Linux linker's --wrap support.
if(NOT USE_CUDA OR NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
  return()
endif()

# Exercise the real platform and plugin methods without transfer transports.
add_library(
  cuda_pointer_test_runtime STATIC
  ${CMAKE_CURRENT_LIST_DIR}/../src/runtime/platform.cpp
  ${CMAKE_CURRENT_LIST_DIR}/../src/runtime/topology.cpp
  ${CMAKE_CURRENT_LIST_DIR}/../src/runtime/memory_prober.cpp)
target_link_libraries(cuda_pointer_test_runtime
                      PUBLIC platform_cuda tent_common glog::glog numa ibverbs)

set(cuda_pointer_wrapped_symbols
    cuInit
    cuDeviceGet
    cuPointerGetAttributes
    cuPointerGetAttribute
    cuCtxGetCurrent
    cuCtxSetCurrent
    cudaGetDeviceCount
    cudaRuntimeGetVersion
    cudaGetDevice
    cudaSetDevice
    cudaStreamCreateWithFlags
    cudaStreamDestroy
    cudaStreamSynchronize
    cudaMemcpyAsync
    cudaMemcpy
    cudaMalloc
    cudaFree
    numa_free
    numa_move_pages)
set(cuda_pointer_wrap_options)
foreach(symbol IN LISTS cuda_pointer_wrapped_symbols)
  list(APPEND cuda_pointer_wrap_options "-Wl,--wrap=${symbol}")
endforeach()

add_library(cuda_pointer_test_plugin SHARED
            ${CMAKE_CURRENT_LIST_DIR}/../plugins/cuda/cuda_plugin.cpp)
target_link_libraries(cuda_pointer_test_plugin PRIVATE tent_common CUDA::cudart
                                                       CUDA::cuda_driver)
target_link_options(cuda_pointer_test_plugin PRIVATE
                    ${cuda_pointer_wrap_options})
set_target_properties(
  cuda_pointer_test_plugin
  PROPERTIES LIBRARY_OUTPUT_DIRECTORY
             "${CMAKE_CURRENT_BINARY_DIR}/cuda_pointer_plugins")

add_executable(tent_cuda_pointer_test
               ${CMAKE_CURRENT_LIST_DIR}/cuda_pointer_test.cpp)
target_link_libraries(tent_cuda_pointer_test PRIVATE gtest_main
                                                     ${CMAKE_DL_LIBS})
if(TARGET tent_link_group)
  target_link_libraries(tent_cuda_pointer_test PRIVATE tent_link_group)
  target_compile_definitions(tent_cuda_pointer_test
                             PRIVATE TENT_TEST_SEGMENT_TRACKER)
else()
  target_link_libraries(tent_cuda_pointer_test
                        PRIVATE cuda_pointer_test_runtime)
endif()
target_link_options(tent_cuda_pointer_test PRIVATE ${cuda_pointer_wrap_options})
set_target_properties(tent_cuda_pointer_test PROPERTIES ENABLE_EXPORTS ON)
target_compile_definitions(
  tent_cuda_pointer_test
  PRIVATE
    CUDA_POINTER_TEST_PLUGIN_DIR="$<TARGET_FILE_DIR:cuda_pointer_test_plugin>")
add_dependencies(tent_cuda_pointer_test cuda_pointer_test_plugin)
add_test(NAME tent_cuda_pointer_test
         COMMAND tent_cuda_pointer_test
                 --gtest_filter=-CudaPointerTest.*Registration*DoesNotPublish)
add_test(
  NAME tent_cuda_pointer_abi_mismatch_test
  COMMAND
    ${CMAKE_COMMAND} -E env TENT_TEST_CUDA_ABI_MISMATCH=1
    $<TARGET_FILE:tent_cuda_pointer_test>
    --gtest_filter=CudaPointerInitTest.AbiMismatchStopsPlatformProbeAndCopy)
add_test(
  NAME tent_cuda_pointer_init_error_test
  COMMAND
    ${CMAKE_COMMAND} -E env TENT_TEST_CUDA_INIT_ERROR=1
    $<TARGET_FILE:tent_cuda_pointer_test> --gtest_filter=CudaPointerInitTest.*)
add_test(
  NAME tent_cuda_pointer_no_device_test
  COMMAND
    ${CMAKE_COMMAND} -E env TENT_TEST_CUDA_NO_DEVICE=1
    $<TARGET_FILE:tent_cuda_pointer_test>
    --gtest_filter=CudaPointerInitTest.NoVisibleDevicePreservesHostOnlyOperation
)

add_executable(
  tent_cuda_pointer_gpu_test
  ${CMAKE_CURRENT_LIST_DIR}/cuda_pointer_gpu_test.cpp
  ${CMAKE_CURRENT_LIST_DIR}/../plugins/cuda/cuda_plugin.cpp)
target_link_libraries(tent_cuda_pointer_gpu_test
                      PRIVATE cuda_pointer_test_runtime gtest gtest_main)
add_test(NAME tent_cuda_pointer_gpu_test COMMAND tent_cuda_pointer_gpu_test)

if(TARGET tent_link_group)
  add_test(NAME tent_cuda_pointer_registration_test
           COMMAND tent_cuda_pointer_test
                   --gtest_filter=CudaPointerTest.*Registration*DoesNotPublish)
endif()
