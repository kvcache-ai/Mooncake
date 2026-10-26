cmake_minimum_required(VERSION 3.24)
project(mooncake_cuobject_prototype LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
include(CTest)
find_package(CURL REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(glog REQUIRED)
find_package(GTest REQUIRED)
find_package(Threads REQUIRED)
get_filename_component(MOONCAKE_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../.."
                       ABSOLUTE)
include(FetchContent)
FetchContent_Declare(
  ylt_headers
  URL https://github.com/alibaba/yalantinglibs/archive/7801bc9ad9021781f15217552214e325a1cf7373.tar.gz
  URL_HASH
    SHA256=2a4b93c256c09fa84e1507bf4d5b33571ee47a4e2c65316ca255680ad3cdfc1e
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE)
FetchContent_GetProperties(ylt_headers)
if(NOT ylt_headers_POPULATED)
  FetchContent_Populate(ylt_headers)
endif()

set(STORE "${MOONCAKE_ROOT}/mooncake-store")
add_library(
  store_s3 STATIC
  ${STORE}/src/storage/distributed/rest_object_storage_adapter.cpp
  ${STORE}/src/storage/distributed/s3_adapter.cpp
  ${STORE}/src/storage/distributed/object_storage_xml.cpp
  ${STORE}/src/config/s3_adapter_config.cpp
  ${STORE}/src/types.cpp
  ${MOONCAKE_ROOT}/mooncake-common/src/environ.cpp)
target_include_directories(
  store_s3
  PUBLIC ${STORE}/include
         ${STORE}/src
         ${STORE}/include/cachelib_memory_allocator
         ${STORE}/include/cachelib_memory_allocator/include
         ${STORE}/include/cachelib_memory_allocator/fake_include
         ${MOONCAKE_ROOT}/mooncake-common/include
         ${MOONCAKE_ROOT}/mooncake-common/src)
target_include_directories(
  store_s3 SYSTEM
  PUBLIC ${ylt_headers_SOURCE_DIR}/include
         ${ylt_headers_SOURCE_DIR}/include/ylt/thirdparty
         ${ylt_headers_SOURCE_DIR}/include/ylt/standalone)
target_link_libraries(store_s3 PUBLIC CURL::libcurl OpenSSL::Crypto glog::glog
                                      Threads::Threads)
add_library(cuobject_prototype STATIC adapter.cpp)
target_include_directories(cuobject_prototype
                           PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(cuobject_prototype PUBLIC store_s3)
add_executable(cuobject_prototype_test adapter_test.cpp)
target_link_libraries(cuobject_prototype_test PRIVATE cuobject_prototype
                                                      GTest::gtest_main)
add_test(NAME cuobject_prototype_test COMMAND cuobject_prototype_test)
add_executable(s3_regression_test ${STORE}/tests/s3_adapter_test.cpp
                                  ${STORE}/tests/s3_adapter_config_test.cpp)
target_link_libraries(s3_regression_test PRIVATE store_s3 GTest::gtest_main)
add_test(NAME s3_regression_test COMMAND s3_regression_test)
option(WITH_CUOBJECT "Build the real SDK smoke test and benchmark" ON)
if(WITH_CUOBJECT)
  find_path(CUOBJECT_INCLUDE_DIR cuobjclient.h HINTS /usr/local/cuda/include
                                                     REQUIRED)
  find_library(CUOBJECT_LIBRARY cuobjclient HINTS /usr/local/cuda/lib64
                                                  REQUIRED)
  add_library(cuobject_sdk STATIC sdk.cpp)
  target_include_directories(cuobject_sdk SYSTEM
                             PRIVATE ${CUOBJECT_INCLUDE_DIR})
  target_link_libraries(cuobject_sdk PUBLIC cuobject_prototype
                                            ${CUOBJECT_LIBRARY})
  add_executable(cuobject_benchmark benchmark.cpp)
  target_link_libraries(cuobject_benchmark PRIVATE cuobject_sdk)
endif()
