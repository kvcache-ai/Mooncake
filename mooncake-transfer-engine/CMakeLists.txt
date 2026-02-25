cmake_minimum_required(VERSION 3.16)
project(mooncake-transfer-engine)

if (NOT GLOBAL_CONFIG)
  if (USE_ETCD)
    message(FATAL_ERROR "Cannot enable USE_ETCD while building transfer engine independently")
  endif()
  include(../mooncake-common/FindJsonCpp.cmake)
  include(../mooncake-common/FindGLOG.cmake)
  include(../mooncake-common/common.cmake)

  # Build mooncake_common for standalone transfer engine build.
  # transfer_engine links against the mooncake_common target.
  add_subdirectory(../mooncake-common ${CMAKE_CURRENT_BINARY_DIR}/mooncake-common)

  # Build asio_shared library for standalone build
  # Set compile definitions before add_subdirectory to ensure proper compilation
  add_compile_definitions(ASIO_SEPARATE_COMPILATION ASIO_DYN_LINK)
  add_subdirectory(../mooncake-asio ${CMAKE_CURRENT_BINARY_DIR}/mooncake-asio)
endif() # GLOBAL_CONFIG

if (USE_ASCEND)
  include(../mooncake-common/FindMpi.cmake)
  file(GLOB ASCEND_TOOLKIT_ROOT "/usr/local/Ascend/ascend-toolkit/latest/*-linux")
  set(ASCEND_INCLUDE_DIR "${ASCEND_TOOLKIT_ROOT}/include")

  include_directories(/usr/local/include
                      /usr/include
                      ${ASCEND_INCLUDE_DIR}
                      ${ASCEND_INCLUDE_DIR}/hccl
                      ${ASCEND_INCLUDE_DIR}/experiment
                      ${ASCEND_INCLUDE_DIR}/experiment/hccl
                      ${ASCEND_INCLUDE_DIR}/experiment/slog/toolchain
                      ${ASCEND_INCLUDE_DIR}/experiment/metadef/common/util/error_manager
                      ${ASCEND_INCLUDE_DIR}/experiment/runtime
                      ${ASCEND_INCLUDE_DIR}/experiment/msprof
                      ${MPI_INCLUDE_DIR}
                      /usr/local/Ascend/ascend-toolkit/latest/tools/hccl_test/common/src/
                      )
endif()

if (USE_ASCEND_HETEROGENEOUS)
  file(GLOB ASCEND_TOOLKIT_ROOT "/usr/local/Ascend/ascend-toolkit/latest/*-linux")
  set(ASCEND_INCLUDE_DIR "${ASCEND_TOOLKIT_ROOT}/include")
  include_directories(/usr/local/include
                      /usr/include
                      ${ASCEND_INCLUDE_DIR})
endif()

include_directories(../mooncake-common/include)
include_directories(include)
add_subdirectory(include)
add_subdirectory(src)

if (BUILD_UNIT_TESTS)
  add_subdirectory(tests)
endif()

if (BUILD_EXAMPLES)
  add_subdirectory(example)
endif()

if (WITH_RUST_EXAMPLE)
  add_subdirectory(rust)
endif()

# Non-peermem requires cuMem allocated memory pointers
if (USE_MNNVL OR NOT WITH_NVIDIA_PEERMEM)
  add_subdirectory(nvlink-allocator)
endif()

if (USE_TENT)
  add_subdirectory(tent)
  add_subdirectory(benchmark)
endif()