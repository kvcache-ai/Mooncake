cmake_minimum_required(VERSION 3.16)
project(mooncake CXX C)

# indicates cmake is invoked from top-level dir
set(GLOBAL_CONFIG "true")

include(mooncake-common/FindJsonCpp.cmake)
include(mooncake-common/FindGLOG.cmake)
include(mooncake-common/common.cmake)
# unit test
if (BUILD_UNIT_TESTS)
  enable_testing()
endif()

option(WITH_TE "build mooncake transfer engine and sample code" ON)
option(WITH_STORE "build mooncake store library and sample code" ON)
option(WITH_P2P_STORE "build p2p store library and sample code" OFF)
option(WITH_RUST_EXAMPLE "build the Rust interface and sample code for the transfer engine" OFF)
option(WITH_EP "build mooncake with expert parallelism support" OFF)

add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/extern/pybind11)
set(PYTHON_EXECUTABLE "python3")
execute_process(
    COMMAND ${PYTHON_EXECUTABLE} -c "import sys; print(sys.path[-1])"
    OUTPUT_VARIABLE PYTHON_SYS_PATH
)
string(STRIP ${PYTHON_SYS_PATH} PYTHON_SYS_PATH)

if (USE_ETCD)
  add_compile_definitions(USE_ETCD)
  if (USE_ETCD_LEGACY)
    add_compile_definitions(USE_ETCD_LEGACY)
    message(STATUS "etcd as metadata server support is enabled (etcd-cpp-api-v3)")
  else()
    message(STATUS "etcd as metadata server support is enabled (go package)")
  endif()
endif()
option(STORE_USE_ETCD "build mooncake store with etcd" OFF)
if (STORE_USE_ETCD)
  add_compile_definitions(STORE_USE_ETCD)
endif()

option(STORE_USE_JEMALLOC "Use jemalloc in mooncake store master" OFF)

# Define ASIO macros before adding mooncake-asio subdirectory
add_compile_definitions(ASIO_SEPARATE_COMPILATION ASIO_DYN_LINK)
add_subdirectory(mooncake-asio)

add_subdirectory(mooncake-common)
include_directories(mooncake-common/etcd)
include_directories(mooncake-common/include)

if (WITH_TE)
  add_subdirectory(mooncake-transfer-engine)
  include_directories(mooncake-transfer-engine/include)
endif()

if (WITH_STORE)
  message(STATUS "Mooncake Store will be built")
  add_subdirectory(mooncake-store)
  include_directories(mooncake-store/include)
endif()

if (WITH_EP)
  message(STATUS "WITH_EP enabled: building Mooncake EP and PG Python extensions")
  find_package(CUDAToolkit REQUIRED)
  message(STATUS "Detected CUDA version: ${CUDAToolkit_VERSION}")

  # EP_TORCH_VERSIONS: semicolon-separated list of PyTorch versions to build for.
  # Can be set via -DEP_TORCH_VERSIONS="2.9.1;2.8.0" or the EP_TORCH_VERSIONS env var.
  # Empty means build with the currently-installed torch.
  if(NOT EP_TORCH_VERSIONS)
    set(EP_TORCH_VERSIONS "$ENV{EP_TORCH_VERSIONS}")
  endif()
  set(EP_TORCH_VERSIONS "${EP_TORCH_VERSIONS}" CACHE STRING
      "PyTorch versions for EP/PG extensions, semicolon-separated (empty = use currently-installed torch)")

  # TORCH_CUDA_ARCH_LIST forwarded to the torch CUDA extension build.
  if(NOT TORCH_CUDA_ARCH_LIST)
    set(TORCH_CUDA_ARCH_LIST "$ENV{TORCH_CUDA_ARCH_LIST}")
  endif()
  if(NOT TORCH_CUDA_ARCH_LIST)
    set(TORCH_CUDA_ARCH_LIST "8.0;9.0")
  endif()
  set(TORCH_CUDA_ARCH_LIST "${TORCH_CUDA_ARCH_LIST}" CACHE STRING
      "CUDA arch list for EP/PG extension builds (e.g. \"8.0;9.0\")")

  # Staging directory: EP/PG .so files are placed here during make and later
  # injected into the wheel AFTER auditwheel, so patchelf never touches the
  # CUDA fatbins (which would cause cudaErrorInvalidKernelImage at runtime).
  set(EP_PG_STAGING_DIR "${CMAKE_BINARY_DIR}/ep_pg_staging")

  # Convert semicolon-separated lists to pipe-separated strings so they survive
  # CMake's COMMAND list-splitting (semicolons are CMake list separators).
  string(REPLACE ";" "|" _ep_torch_versions_pipe "${EP_TORCH_VERSIONS}")
  string(REPLACE ";" "|" _torch_cuda_arch_list_pipe "${TORCH_CUDA_ARCH_LIST}")

  add_custom_target(mooncake_ep_ext ALL
    COMMAND ${CMAKE_COMMAND} -E make_directory "${EP_PG_STAGING_DIR}"
    COMMAND ${CMAKE_COMMAND}
      "-DSOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/mooncake-ep"
      "-DEP_CUDA_MAJOR=${CUDAToolkit_VERSION_MAJOR}"
      "-DEP_TORCH_VERSIONS=${_ep_torch_versions_pipe}"
      "-DTORCH_CUDA_ARCH_LIST=${_torch_cuda_arch_list_pipe}"
      "-DSTAGING_DIR=${EP_PG_STAGING_DIR}"
      "-DENGINE_SO_PATH=$<TARGET_FILE:engine>"
      -P "${CMAKE_CURRENT_SOURCE_DIR}/mooncake-ep/BuildEpExt.cmake"
    COMMENT "Building Mooncake EP Python extension(s)"
    DEPENDS engine
    VERBATIM
  )

  add_custom_target(mooncake_pg_ext ALL
    COMMAND ${CMAKE_COMMAND} -E make_directory "${EP_PG_STAGING_DIR}"
    COMMAND ${CMAKE_COMMAND}
      "-DSOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/mooncake-pg"
      "-DEP_CUDA_MAJOR=${CUDAToolkit_VERSION_MAJOR}"
      "-DEP_TORCH_VERSIONS=${_ep_torch_versions_pipe}"
      "-DTORCH_CUDA_ARCH_LIST=${_torch_cuda_arch_list_pipe}"
      "-DSTAGING_DIR=${EP_PG_STAGING_DIR}"
      "-DENGINE_SO_PATH=$<TARGET_FILE:engine>"
      -P "${CMAKE_CURRENT_SOURCE_DIR}/mooncake-pg/BuildPgExt.cmake"
    COMMENT "Building Mooncake PG Python extension(s)"
    DEPENDS engine mooncake_ep_ext
    VERBATIM
  )
endif()

add_subdirectory(mooncake-integration)

if (WITH_P2P_STORE)
  add_subdirectory(mooncake-p2p-store)
  message(STATUS "P2P Store will be built")
endif()
