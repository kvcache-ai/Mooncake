# Portable codec/binding tests without Store, CUDA, RDMA or a running master.
cmake_minimum_required(VERSION 3.16)
project(mooncake_codec_tests LANGUAGES CXX)
enable_testing()
get_filename_component(MOONCAKE_ROOT "${CMAKE_CURRENT_LIST_DIR}/../../.."
                       ABSOLUTE)
add_subdirectory(${MOONCAKE_ROOT}/mooncake-store/src/codec codec)
add_executable(kv_codec_test kv_codec_test.cpp)
target_link_libraries(kv_codec_test PRIVATE mooncake_kv_codec)
add_test(NAME codec_buffers COMMAND kv_codec_test)
option(CODEC_TEST_PYTHON "Build and test the Python codec binding" ON)
if(NOT CODEC_TEST_PYTHON)
  return()
endif()
find_package(
  Python3
  COMPONENTS Interpreter Development.Module
  REQUIRED)
set(PYBIND11_FINDPYTHON ON)
add_subdirectory(${MOONCAKE_ROOT}/extern/pybind11 pybind11 EXCLUDE_FROM_ALL)
pybind11_add_module(_kv_codec
                    ${MOONCAKE_ROOT}/mooncake-integration/store/kv_codec_py.cpp)
target_link_libraries(_kv_codec PRIVATE mooncake_kv_codec)
set_target_properties(
  _kv_codec PROPERTIES LIBRARY_OUTPUT_DIRECTORY
                       "${CMAKE_CURRENT_BINARY_DIR}/python/mooncake")
add_test(
  NAME codec_store_python
  COMMAND
    ${CMAKE_COMMAND} -E env
    "PYTHONPATH=${MOONCAKE_ROOT}/python:${CMAKE_CURRENT_BINARY_DIR}/python"
    ${Python3_EXECUTABLE} -m pytest
    ${MOONCAKE_ROOT}/python/tests/store/test_codec_store.py -q)
