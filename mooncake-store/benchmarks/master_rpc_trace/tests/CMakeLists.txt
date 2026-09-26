cmake_minimum_required(VERSION 3.16)
project(master_rpc_trace_tests LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
include(
  ${CMAKE_CURRENT_SOURCE_DIR}/../../../../mooncake-common/FindJsonCpp.cmake)
find_package(Threads REQUIRED)
find_package(GTest REQUIRED)
enable_testing()
add_executable(master_rpc_trace_test trace_test.cpp ../trace.cpp)
target_include_directories(master_rpc_trace_test PRIVATE ..)
target_link_libraries(
  master_rpc_trace_test PRIVATE JsonCpp::JsonCpp GTest::gtest_main
                                Threads::Threads)
add_test(NAME master_rpc_trace_test COMMAND master_rpc_trace_test)
set_tests_properties(master_rpc_trace_test PROPERTIES TIMEOUT 20)
find_package(Python3 COMPONENTS Interpreter)
if(Python3_Interpreter_FOUND)
  add_test(NAME master_rpc_trace_monitor_test
           COMMAND ${Python3_EXECUTABLE} -m unittest discover -s
                   ${CMAKE_CURRENT_SOURCE_DIR} -p test_monitor.py -v)
endif()
