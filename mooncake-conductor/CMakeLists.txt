cmake_minimum_required(VERSION 3.16)

if(NOT GLOBAL_CONFIG)
  project(mooncake-conductor CXX)
  set(CMAKE_CXX_STANDARD 20)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
  include(../mooncake-common/FindJsonCpp.cmake)
  include(../mooncake-common/FindGLOG.cmake)
  # Match the top-level common.cmake policy: keep debuginfo by default.
  if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "RelWithDebInfo")
  endif()
endif()

find_package(Threads REQUIRED)

add_library(conductor_cpp_core STATIC src/common/utils.cpp)

# mooncake-common headers are consumed directly instead of linking
# mooncake_common: the helpers conductor needs (ascii_string.h,
# integer_parser.h) are header-only, so this reuses them without taking on that
# target's asio/yaml-cpp/yalantinglibs dependencies. Keeps the standalone build
# resolvable against glog/JsonCpp/Threads alone.
target_include_directories(
  conductor_cpp_core
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
         ${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-common/include)

target_link_libraries(conductor_cpp_core PUBLIC glog::glog JsonCpp::JsonCpp
                                                Threads::Threads)

if(BUILD_UNIT_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
