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
find_package(OpenSSL REQUIRED COMPONENTS Crypto)
option(WITH_CONDUCTOR_GO "build Go bindings for mooncake conductor" OFF)
option(WITH_CONDUCTOR_C_SHARED
       "build the conductor client C ABI shared library" OFF)

# Standalone builds own their dependencies; the top-level build provides this
# target before adding Conductor and Store.
if(NOT TARGET Mooncake::libzmq)
  include(${CMAKE_CURRENT_SOURCE_DIR}/../cmake/Dependencies.cmake)
  mooncake_provide_libzmq()
endif()

# msgpack-cxx is header-only; older distributions have no CMake config.

find_package(msgpack-cxx QUIET)
if(TARGET msgpack-cxx)
  set(CONDUCTOR_MSGPACK_LIBS msgpack-cxx)
  message(STATUS "conductor: msgpack-cxx via package config")
else()
  find_path(CONDUCTOR_MSGPACK_INCLUDE_DIR msgpack.hpp)
  if(NOT CONDUCTOR_MSGPACK_INCLUDE_DIR)
    message(
      FATAL_ERROR
        "msgpack C++ headers (msgpack.hpp) not found; install libmsgpack-cxx-dev or libmsgpack-dev"
    )
  endif()
  set(CONDUCTOR_MSGPACK_INCLUDES ${CONDUCTOR_MSGPACK_INCLUDE_DIR})
  message(
    STATUS
      "conductor: msgpack-cxx via headers in ${CONDUCTOR_MSGPACK_INCLUDE_DIR}")
endif()

# In the top-level build mooncake-common/common.cmake has already found
# yalantinglibs; Its config.cmake mutates the imported target and cannot run
# twice, because the target belongs to the parent scope. Guard it so both the
# top-level and standalone paths configure.
if(NOT TARGET yalantinglibs::yalantinglibs)
  find_package(yalantinglibs CONFIG REQUIRED)
endif()

add_library(
  conductor_cpp_core STATIC
  src/common/utils.cpp
  src/client/types.cpp
  src/client/conductor_client.cpp
  src/prefixindex/hash_strategy.cpp
  src/prefixindex/prefix_indexer.cpp
  src/zmq/msg_decoder.cpp
  src/zmq/msgpack_reader.cpp
  src/zmq/zmq_client.cpp
  src/kvevent/object_key_parser.cpp
  src/kvevent/config.cpp
  src/kvevent/event_handler.cpp
  src/kvevent/conductor_service.cpp
  src/kvevent/rpc_service.cpp
  src/kvevent/event_manager.cpp)

# mooncake-common headers are consumed directly instead of linking
# mooncake_common: the helpers conductor needs (ascii_string.h,
# integer_parser.h) are header-only, so this reuses them without taking on that
# target's asio/yaml-cpp/yalantinglibs dependencies. Keeps the standalone build
# resolvable against glog/JsonCpp/Threads alone.
target_include_directories(
  conductor_cpp_core
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
         ${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-common/include
         ${CONDUCTOR_MSGPACK_INCLUDES})

target_link_libraries(
  conductor_cpp_core
  PUBLIC OpenSSL::Crypto
         Mooncake::libzmq
         ${CONDUCTOR_MSGPACK_LIBS}
         yalantinglibs::yalantinglibs
         glog::glog
         JsonCpp::JsonCpp
         Threads::Threads)

# The top-level build compiles yalantinglibs against its own asio; picking that
# up keeps this target's coroutine HTTP server ABI-consistent with the parent.
if(TARGET asio_shared)
  target_link_libraries(conductor_cpp_core PUBLIC asio_shared ibverbs)
endif()

add_executable(mooncake_conductor src/main.cpp)
target_link_libraries(mooncake_conductor PRIVATE conductor_cpp_core)

# Stage 3's pybind module links conductor_cpp_core into a shared object.
set_target_properties(conductor_cpp_core PROPERTIES POSITION_INDEPENDENT_CODE
                                                    ON)

# The /query hash chain prefers the low-level SHA256_* API, which hashes through
# a stack context instead of allocating an EVP context per call. Those
# declarations are hidden in OpenSSL no-deprecated builds, so probe for them and
# fall back to the EVP backend when unavailable. FIPS deployments should force
# the EVP path explicitly with -DCONDUCTOR_FORCE_EVP_SHA256=ON.
option(CONDUCTOR_FORCE_EVP_SHA256 "Force the EVP SHA-256 backend (e.g. FIPS)"
       OFF)
if(NOT CONDUCTOR_FORCE_EVP_SHA256)
  include(CheckCXXSourceCompiles)
  set(CMAKE_REQUIRED_INCLUDES ${OPENSSL_INCLUDE_DIR})
  set(CMAKE_REQUIRED_LIBRARIES OpenSSL::Crypto)
  check_cxx_source_compiles(
    "
    #define OPENSSL_SUPPRESS_DEPRECATED
    #include <openssl/sha.h>
    int main() { SHA256_CTX c; return SHA256_Init(&c); }
  "
    CONDUCTOR_HAS_LOWLEVEL_SHA256)
  unset(CMAKE_REQUIRED_INCLUDES)
  unset(CMAKE_REQUIRED_LIBRARIES)
endif()
if(CONDUCTOR_HAS_LOWLEVEL_SHA256)
  target_compile_definitions(conductor_cpp_core
                             PRIVATE CONDUCTOR_HAS_LOWLEVEL_SHA256=1)
  message(STATUS "conductor: SHA-256 backend = low-level SHA256_*")
else()
  message(
    STATUS
      "conductor: SHA-256 backend = EVP (low-level API unavailable or forced)")
endif()

if(WITH_CONDUCTOR_C_SHARED OR WITH_CONDUCTOR_GO)
  if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "Conductor C shared library currently supports Linux")
  endif()
  include(GNUInstallDirs)
  add_library(mooncake_conductor_client SHARED src/client/conductor_c.cpp)
  target_link_libraries(mooncake_conductor_client PRIVATE conductor_cpp_core)
  set_target_properties(
    mooncake_conductor_client
    PROPERTIES VERSION 1.0.0
               SOVERSION 1
               INSTALL_RPATH "$ORIGIN"
               LINK_DEPENDS
               "${CMAKE_CURRENT_SOURCE_DIR}/src/client/conductor_c.map")
  target_link_options(
    mooncake_conductor_client
    PRIVATE
    "LINKER:--version-script=${CMAKE_CURRENT_SOURCE_DIR}/src/client/conductor_c.map"
  )
  install(TARGETS mooncake_conductor_client
          LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
  install(FILES include/conductor/client/conductor_c.h
          DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/conductor/client)
endif()

if(WITH_CONDUCTOR_GO)
  find_program(CONDUCTOR_GO_EXECUTABLE go)
  if(NOT CONDUCTOR_GO_EXECUTABLE)
    message(FATAL_ERROR "WITH_CONDUCTOR_GO=ON requires the Go compiler")
  endif()
  add_custom_target(
    build_conductor_go ALL
    COMMAND
      ${CMAKE_COMMAND} -E env "GO=${CONDUCTOR_GO_EXECUTABLE}" bash
      ${CMAKE_CURRENT_SOURCE_DIR}/go/build.sh ${CMAKE_CURRENT_BINARY_DIR}
      ${CMAKE_CURRENT_BINARY_DIR}/mooncake-conductor-go-example
    DEPENDS mooncake_conductor_client
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/go
    VERBATIM)
endif()

if(BUILD_UNIT_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

install(TARGETS mooncake_conductor DESTINATION bin)
