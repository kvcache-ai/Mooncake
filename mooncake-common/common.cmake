set(CMAKE_C_STANDARD 99)
set(CMAKE_CXX_STANDARD 20)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -Wall -Wextra -Wno-unused-parameter -fPIC")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -Wall -Wextra -Wno-unused-parameter -fPIC")

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fcoroutines")
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -fno-tree-slp-vectorize")
endif()

set(CMAKE_C_FLAGS_RELEASE "-O3")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

set(CMAKE_C_FLAGS_DEBUG "-O0")
set(CMAKE_CXX_FLAGS_DEBUG "-O0")

set(CMAKE_BUILD_TYPE "Release")

# Necessary if you are using Alibaba Cloud eRDMA
add_definitions(-DCONFIG_ERDMA)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(ENABLE_CCACHE "Whether to open ccache" OFF)
find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND AND ENABLE_CCACHE)
  message(STATUS "Building with CCACHE enabled")
  set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif()

add_compile_definitions(GLOG_USE_GLOG_EXPORT)

option(BUILD_EXAMPLES "Build examples" ON)

option(BUILD_UNIT_TESTS "Build uint tests" ON)
option(USE_CUDA "option for using gpu direct" OFF)
option(USE_NVMEOF "option for using NVMe over Fabric" OFF)
option(USE_TCP "option for using TCP transport" ON)
option(USE_CXL "option for using cxl protocol" OFF)
option(USE_ETCD "option for enable etcd as metadata server" OFF)
option(USE_ETCD_LEGACY "option for enable etcd based on etcd-cpp-api-v3" OFF)
option(USE_REDIS "option for enable redis as metadata server" OFF)
option(USE_HTTP "option for enable http as metadata server" ON)
option(WITH_RUST_EXAMPLE "build the Rust interface and sample code for the transfer engine" OFF)
option(WITH_METRICS "enable metrics and metrics reporting thread" ON)


option(USE_LRU_MASTER "option for using LRU in master service" OFF)
set(LRU_MAX_CAPACITY 1000)

if (USE_LRU_MASTER)
  add_compile_definitions(USE_LRU_MASTER)
  add_compile_definitions(LRU_MAX_CAPACITY)
endif()


if (USE_CUDA)
  add_compile_definitions(USE_CUDA)
  message(STATUS "CUDA support is enabled")

  if (USE_NVMEOF)
    add_compile_definitions(USE_NVMEOF)
    message(STATUS "NVMe-oF support is enabled")
  endif()

  include_directories(/usr/local/cuda/include)
  link_directories(/usr/local/cuda/lib /usr/local/cuda/lib64)
elseif(USE_NVMEOF)
  message(FATAL_ERROR "Cannot enable USE_NVMEOF without USE_CUDA")
endif()

if (USE_TCP)
  add_compile_definitions(USE_TCP)
endif()

if (USE_REDIS)
  add_compile_definitions(USE_REDIS)
  message(STATUS "Redis as metadata server support is enabled")
endif()

if (USE_HTTP)
  add_compile_definitions(USE_HTTP)
  message(STATUS "Http as metadata server support is enabled")
endif()

if (NOT USE_ETCD AND NOT USE_REDIS AND NOT USE_HTTP)
  message(STATUS "None of USE_ETCD, USE_REDIS, USE_HTTP is selected, only \"P2PHANDSHAKE\" is supported as metadata server")
endif()

if (WITH_METRICS)
  add_compile_definitions(WITH_METRICS)
  message(STATUS "metrics is enabled")
endif()


set(GFLAGS_USE_TARGET_NAMESPACE "true")
find_package(gflags REQUIRED)
find_package(glog REQUIRED)
find_package(yalantinglibs CONFIG REQUIRED)
