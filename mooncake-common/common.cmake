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

if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror=thread-safety")
endif()

option(ENABLE_ASAN "enable address sanitizer" OFF)

if (ENABLE_ASAN)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=leak")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=leak")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
endif()

# keep debuginfo by default
if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "RelWithDebInfo")
endif()

message(CMAKE_BUILD_TYPE ": ${CMAKE_BUILD_TYPE}")

# Necessary if you are using Alibaba Cloud eRDMA
add_definitions(-DCONFIG_ERDMA)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(ENABLE_SCCACHE "Whether to open sccache" OFF)
if (ENABLE_SCCACHE)
  find_program(SCCACHE sccache REQUIRED)
endif()
if(SCCACHE AND ENABLE_SCCACHE)
  message(STATUS "Building with SCCACHE enabled")
  set(CMAKE_C_COMPILER_LAUNCHER ${SCCACHE})
  set(CMAKE_CXX_COMPILER_LAUNCHER ${SCCACHE})
endif()

add_compile_definitions(GLOG_USE_GLOG_EXPORT)
add_compile_options(-fno-tree-slp-vectorize)

option(BUILD_EXAMPLES "Build examples" ON)

option(BUILD_UNIT_TESTS "Build unit tests" ON)
option(USE_CUDA "option for enabling gpu features" OFF)
option(USE_NVMEOF "option for using NVMe over Fabric" OFF)
option(USE_TCP "option for using TCP transport" ON)
option(USE_ASCEND "option for using npu with HCCL" OFF)
option(USE_ASCEND_DIRECT "option for using ascend npu with adxl engine" OFF)
option(USE_ASCEND_HETEROGENEOUS "option for transferring between ascend npu and gpu" OFF)
option(USE_MNNVL "option for using Multi-Node NVLink transport" OFF)
option(USE_CXL "option for using CXL protocol" OFF)
option(USE_ETCD "option for enable etcd as metadata server" OFF)
option(USE_ETCD_LEGACY "option for enable etcd based on etcd-cpp-api-v3" OFF)
option(USE_REDIS "option for enable redis as metadata server" OFF)
option(USE_HTTP "option for enable http as metadata server" ON)
option(WITH_RUST_EXAMPLE "build the Rust interface and sample code for the transfer engine" OFF)
option(WITH_METRICS "enable metrics and metrics reporting thread" ON)
option(USE_3FS "option for using 3FS storage backend" OFF)
option(WITH_NVIDIA_PEERMEM "disable to support RDMA without nvidia-peermem. If WITH_NVIDIA_PEERMEM=OFF then USE_CUDA=ON is required." ON)

option(USE_LRU_MASTER "option for using LRU in master service" OFF)
set(LRU_MAX_CAPACITY 1000)

if (USE_LRU_MASTER)
  add_compile_definitions(USE_LRU_MASTER)
  add_compile_definitions(LRU_MAX_CAPACITY)
endif()


if (USE_NVMEOF)
  set(USE_CUDA ON)
  add_compile_definitions(USE_NVMEOF)
  message(STATUS "NVMe-oF support is enabled")
endif()

if (USE_MNNVL)
  set(USE_CUDA ON)
  add_compile_definitions(USE_MNNVL)
  message(STATUS "Multi-Node NVLink support is enabled")
endif()

if (USE_CUDA)
  add_compile_definitions(USE_CUDA)
  message(STATUS "CUDA support is enabled")
  include_directories(/usr/local/cuda/include)
  link_directories(
    /usr/local/cuda/lib
    /usr/local/cuda/lib64
  )
endif()

if (USE_CXL)
  add_compile_definitions(USE_CXL)
  message(STATUS "CXL support is enabled")
endif()

if (USE_TCP)
  add_compile_definitions(USE_TCP)
endif()

if (USE_ASCEND OR USE_ASCEND_DIRECT)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DOPEN_BUILD_PROJECT ")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DOPEN_BUILD_PROJECT ")

  if (EXISTS "/usr/local/Ascend/latest")
    file(GLOB ASCEND_TOOLKIT_ROOT "/usr/local/Ascend/latest/*-linux")
  else()
    file(GLOB ASCEND_TOOLKIT_ROOT "/usr/local/Ascend/ascend-toolkit/latest/*-linux")
  endif()
  set(ASCEND_LIB_DIR "${ASCEND_TOOLKIT_ROOT}/lib64")
  set(ASCEND_DEVLIB_DIR "${ASCEND_TOOLKIT_ROOT}/devlib")
  set(ASCEND_INCLUDE_DIR "${ASCEND_TOOLKIT_ROOT}/include")
  add_compile_options(-Wno-ignored-qualifiers)
  include_directories(/usr/local/include /usr/include ${ASCEND_INCLUDE_DIR})
  link_directories(${ASCEND_LIB_DIR} ${ASCEND_DEVLIB_DIR})
endif()

if (USE_ASCEND)
  add_compile_definitions(USE_ASCEND)
endif()

if (USE_ASCEND_DIRECT)
  add_compile_definitions(USE_ASCEND_DIRECT)
endif()

if (USE_ASCEND_HETEROGENEOUS)
  file(GLOB ASCEND_TOOLKIT_ROOT "/usr/local/Ascend/ascend-toolkit/latest/*-linux")
  set(ASCEND_LIB_DIR "${ASCEND_TOOLKIT_ROOT}/lib64")
  set(ASCEND_INCLUDE_DIR "${ASCEND_TOOLKIT_ROOT}/include")
  add_compile_definitions(USE_ASCEND_HETEROGENEOUS)
  include_directories(/usr/local/include /usr/include ${ASCEND_INCLUDE_DIR})
  link_directories(${ASCEND_LIB_DIR})
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

if(USE_3FS)
  add_compile_definitions(USE_3FS)
  message(STATUS "3FS storage backend is enabled")
endif()

if(WITH_NVIDIA_PEERMEM)
  add_compile_definitions(WITH_NVIDIA_PEERMEM)
endif()

set(GFLAGS_USE_TARGET_NAMESPACE "true")
find_package(yaml-cpp REQUIRED)
find_package(gflags REQUIRED)
find_package(yalantinglibs CONFIG REQUIRED)
add_compile_definitions(YLT_ENABLE_IBV)
