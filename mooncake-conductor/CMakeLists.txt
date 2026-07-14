cmake_minimum_required(VERSION 3.16)

if(NOT GLOBAL_CONFIG)
  project(mooncake-conductor CXX)
  set(CMAKE_CXX_STANDARD 20)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
  include(../mooncake-common/FindJsonCpp.cmake)
  include(../mooncake-common/FindGLOG.cmake)
endif()

find_package(cppzmq REQUIRED)
find_package(msgpack-cxx REQUIRED)
# In the top-level build mooncake-common/common.cmake has already found
# yalantinglibs; its config.cmake mutates the imported target and cannot run
# twice (the target belongs to the parent scope), so guard it.
if(NOT TARGET yalantinglibs::yalantinglibs)
  find_package(yalantinglibs CONFIG REQUIRED)
endif()
find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED COMPONENTS Crypto)

# ThreadSanitizer build for the concurrency tests (task 6.7): cmake
# -DCONDUCTOR_CPP_TSAN=ON .. Run with:
# TSAN_OPTIONS="suppressions=<src>/tests/tsan.supp" ctest (suppressions cover
# uninstrumented libzmq/glog internals only).
option(CONDUCTOR_CPP_TSAN "Build mooncake-conductor with ThreadSanitizer" OFF)
if(CONDUCTOR_CPP_TSAN)
  add_compile_options(-fsanitize=thread -g)
  add_link_options(-fsanitize=thread)
  message(STATUS "conductor-cpp: ThreadSanitizer enabled")
endif()

add_library(
  conductor_cpp_core STATIC
  src/common/utils.cpp
  src/prefixindex/hash_strategy.cpp
  src/prefixindex/prefix_indexer.cpp
  src/zmq/msg_decoder.cpp
  src/zmq/zmq_client.cpp
  src/kvevent/event_handler.cpp
  src/kvevent/event_manager.cpp
  src/kvevent/config.cpp)

target_include_directories(conductor_cpp_core
                           PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

target_link_libraries(
  conductor_cpp_core
  PUBLIC OpenSSL::Crypto
         cppzmq
         msgpack-cxx
         yalantinglibs::yalantinglibs
         glog::glog
         JsonCpp::JsonCpp
         Threads::Threads)

if(TARGET asio_shared)
  target_link_libraries(conductor_cpp_core PUBLIC asio_shared ibverbs)
endif()

add_executable(mooncake_conductor src/main.cpp)
target_link_libraries(mooncake_conductor PRIVATE conductor_cpp_core)

install(TARGETS mooncake_conductor DESTINATION bin)

if(BUILD_UNIT_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
