cmake_minimum_required(VERSION 3.16)
project(mooncake-pg)

if(USE_CUDA OR BUILD_UNIT_TESTS)
  find_package(OpenSSL REQUIRED COMPONENTS Crypto)
endif()

include_directories(include)
add_subdirectory(src)
if(BUILD_UNIT_TESTS)
  add_subdirectory(tests)
endif()
