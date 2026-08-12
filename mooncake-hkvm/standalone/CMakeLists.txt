# Standalone build of the HKVM DiffServer library + example, WITHOUT the
# PyTorch/CUDA dependencies that the mooncake-hkvm root CMakeLists pulls in.
#
# Use this when you only need the diff server:
#   cmake -S standalone -B build-standalone
#   cmake --build build-standalone -j
cmake_minimum_required(VERSION 3.16)
project(mooncake_hkvm_diff_standalone CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_path(ZMQ_INCLUDE_DIR
  NAMES zmq.h
  PATHS /usr/include /usr/local/include /opt/homebrew/include)
find_library(ZMQ_LIBRARY
  NAMES zmq libzmq
  PATHS /usr/lib /usr/local/lib /usr/lib64 /opt/homebrew/lib)
find_path(MSGPACK_INCLUDE_DIR
  NAMES msgpack.hpp
  PATHS /usr/include /usr/local/include /opt/homebrew/include)

if(NOT ZMQ_INCLUDE_DIR OR NOT ZMQ_LIBRARY)
  message(FATAL_ERROR "libzmq not found. Install it (e.g. `brew install zeromq` / `apt install libzmq3-dev`).")
endif()
if(NOT MSGPACK_INCLUDE_DIR)
  message(FATAL_ERROR "msgpack-c not found. Install it (e.g. `brew install msgpack-cxx` / `apt install libmsgpack-dev`).")
endif()

add_library(mooncake_hkvm_diff STATIC
  ${CMAKE_CURRENT_SOURCE_DIR}/../src/diff_server.cpp)
target_include_directories(mooncake_hkvm_diff PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/../include
  ${ZMQ_INCLUDE_DIR}
  ${MSGPACK_INCLUDE_DIR})
target_compile_definitions(mooncake_hkvm_diff PRIVATE MOONCAKE_ENABLE_KV_EVENTS=1)
target_link_libraries(mooncake_hkvm_diff PUBLIC ${ZMQ_LIBRARY})

add_executable(mooncake_hkvm_diff_example
  ${CMAKE_CURRENT_SOURCE_DIR}/../examples/diff_server_main.cpp)
target_link_libraries(mooncake_hkvm_diff_example PRIVATE mooncake_hkvm_diff)
