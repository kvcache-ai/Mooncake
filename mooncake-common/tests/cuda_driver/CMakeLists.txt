cmake_minimum_required(VERSION 3.18)
project(mooncake_cuda_driver_test LANGUAGES CXX)

enable_testing()
find_package(Threads REQUIRED)
find_package(
  Python3
  COMPONENTS Interpreter
  REQUIRED)
find_path(CUDA_DRIVER_INCLUDE_DIR cuda.h
          HINTS ${CUDAToolkit_INCLUDE_DIRS} "$ENV{CUDA_HOME}/include"
                /usr/local/cuda/include REQUIRED)

add_executable(cuda_driver_probe probe.cpp)
add_library(cuda_driver_import SHARED import.cpp)
add_library(fake_cuda_driver SHARED fake_driver.cpp)
add_library(fake_cuda_driver_missing SHARED fake_driver.cpp)
target_compile_definitions(fake_cuda_driver_missing
                           PRIVATE MISSING_CUDA_SYMBOLS)

foreach(target cuda_driver_probe cuda_driver_import fake_cuda_driver
               fake_cuda_driver_missing)
  target_include_directories(
    ${target} PRIVATE "${CUDA_DRIVER_INCLUDE_DIR}"
                      "${CMAKE_CURRENT_LIST_DIR}/../../include")
  set_target_properties(${target} PROPERTIES CXX_STANDARD 11
                                             CXX_STANDARD_REQUIRED YES)
endforeach()

target_link_libraries(cuda_driver_probe PRIVATE ${CMAKE_DL_LIBS}
                                                Threads::Threads)
target_link_libraries(cuda_driver_import PRIVATE ${CMAKE_DL_LIBS})
set_target_properties(
  fake_cuda_driver fake_cuda_driver_missing
  PROPERTIES PREFIX "lib"
             OUTPUT_NAME "cuda.so.1"
             SUFFIX "")
set_target_properties(
  fake_cuda_driver PROPERTIES LIBRARY_OUTPUT_DIRECTORY
                              "${CMAKE_CURRENT_BINARY_DIR}/complete")
set_target_properties(
  fake_cuda_driver_missing PROPERTIES LIBRARY_OUTPUT_DIRECTORY
                                      "${CMAKE_CURRENT_BINARY_DIR}/missing")

add_test(
  NAME cuda_driver_loading
  COMMAND
    "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_LIST_DIR}/run_tests.py" --probe
    "$<TARGET_FILE:cuda_driver_probe>" --import-library
    "$<TARGET_FILE:cuda_driver_import>" --complete
    "$<TARGET_FILE_DIR:fake_cuda_driver>" --missing
    "$<TARGET_FILE_DIR:fake_cuda_driver_missing>")
