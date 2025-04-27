cmake_minimum_required(VERSION 3.16)
project(mooncake CXX C)

# indicates cmake is invoked from top-level dir
set(GLOBAL_CONFIG "true")

option(ENABLE_CCACHE "Whether to open ccache" OFF)
find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND AND ENABLE_CCACHE)
  message(STATUS "Building with CCACHE enabled")
  set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif()

option(BUILD_EXAMPLES "Build examples" ON)

# unit test
option(BUILD_UNIT_TESTS "Build unit tests" ON)
if (BUILD_UNIT_TESTS)
  enable_testing()
endif()

set(CMAKE_C_STANDARD 99)
set(CMAKE_CXX_STANDARD 20)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -Wall -Wextra -Wno-unused-parameter -fPIC")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -Wall -Wextra -Wno-unused-parameter -fPIC")

set(CMAKE_C_FLAGS_RELEASE "-O3")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

set(CMAKE_C_FLAGS_DEBUG "-O0")
set(CMAKE_CXX_FLAGS_DEBUG "-O0")

set(CMAKE_BUILD_TYPE "Release")

# Necessary if you are using Alibaba Cloud eRDMA
add_definitions(-DCONFIG_ERDMA)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(USE_CUDA "option for using gpu direct" OFF)
option(USE_NVMEOF "option for using NVMe over Fabric" OFF)
option(USE_CXL "option for using cxl protocol" OFF)
option(USE_ETCD "option for enable etcd as metadata server" ON)
option(USE_ETCD_LEGACY "option for enable etcd based on etcd-cpp-api-v3" OFF)
option(USE_REDIS "option for enable redis as metadata server" OFF)
option(USE_HTTP "option for enable http as metadata server" ON)
option(USE_LRU_MASTER "option for using LRU in master service" OFF)

option(WITH_STORE "build mooncake store library and sample code" ON)
option(WITH_P2P_STORE "build p2p store library and sample code" OFF)
option(WITH_RUST_EXAMPLE "build the Rust interface and sample code for the transfer engine" OFF)

add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/extern/pybind11)
set(PYTHON_EXECUTABLE "python3")
execute_process(
    COMMAND ${PYTHON_EXECUTABLE} -c "import sys; print(sys.path[-1])"
    OUTPUT_VARIABLE PYTHON_SYS_PATH
)
string(STRIP ${PYTHON_SYS_PATH} PYTHON_SYS_PATH)

if (USE_LRU_MASTER)
  add_compile_definitions(USE_LRU_MASTER)
  add_compile_definitions(LRU_MAX_CAPACITY=1000)
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

if (USE_REDIS)
  add_compile_definitions(USE_REDIS)
  message(STATUS "Redis as metadata server support is enabled")
endif()

if (USE_HTTP)
  add_compile_definitions(USE_HTTP)
  message(STATUS "Http as metadata server support is enabled")
endif()

if (USE_ETCD)
  add_compile_definitions(USE_ETCD)
  if (USE_ETCD_LEGACY)
    add_compile_definitions(USE_ETCD_LEGACY)
    message(STATUS "etcd as metadata server support is enabled (etcd-cpp-api-v3)")
  else()
    message(STATUS "etcd as metadata server support is enabled (go package)")
  endif()
endif()

if (NOT USE_ETCD AND NOT USE_REDIS AND NOT USE_HTTP)
  message(STATUS "None of USE_ETCD, USE_REDIS, USE_HTTP is selected, only \"P2PHANDSHAKE\" is supported as metadata server")
endif()

add_subdirectory(mooncake-common)
include_directories(mooncake-common/etcd)

add_subdirectory(mooncake-transfer-engine)
include_directories(mooncake-transfer-engine/include)

if (WITH_STORE)
  message(STATUS "Mooncake Store will be build")
  add_subdirectory(mooncake-store)
  include_directories(mooncake-store/include)
endif()

add_subdirectory(mooncake-integration)

if (WITH_P2P_STORE AND USE_CUDA)
  message(FATAL_ERROR "Cannot enable both WITH_P2P_STORE and USE_CUDA at the same time.")
endif()

if (WITH_P2P_STORE)
  add_subdirectory(mooncake-p2p-store)
  message(STATUS "P2P Store will be build")
endif()
