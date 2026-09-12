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

add_library(conductor_cpp_core STATIC src/common/utils.cpp
                                      src/prefixindex/hash_strategy.cpp)

# mooncake-common headers are consumed directly instead of linking
# mooncake_common: the helpers conductor needs (ascii_string.h,
# integer_parser.h) are header-only, so this reuses them without taking on that
# target's asio/yaml-cpp/yalantinglibs dependencies. Keeps the standalone build
# resolvable against glog/JsonCpp/Threads alone.
target_include_directories(
  conductor_cpp_core
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
         ${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-common/include)

target_link_libraries(
  conductor_cpp_core PUBLIC OpenSSL::Crypto glog::glog JsonCpp::JsonCpp
                            Threads::Threads)

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

if(BUILD_UNIT_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
