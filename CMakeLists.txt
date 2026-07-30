cmake_minimum_required(VERSION 3.16)
project(mooncake CXX C)

# indicates cmake is invoked from top-level dir
set(GLOBAL_CONFIG "true")
option(WITH_STORE "build mooncake store library and sample code" ON)

set(CMAKE_C_STANDARD 99)
set(CMAKE_CXX_STANDARD 20)

include(cmake/dependencies/MooncakeDependencies.cmake)

include(mooncake-common/common.cmake)
# unit test
if(BUILD_UNIT_TESTS)
  enable_testing()
endif()

option(WITH_TE "build mooncake transfer engine and sample code" ON)
option(WITH_STORE_GO "build Go bindings for mooncake store" OFF)
option(WITH_P2P_STORE "build p2p store library and sample code" OFF)
option(WITH_RUST_EXAMPLE
       "build the Rust interface and sample code for the transfer engine" OFF)
option(WITH_STORE_RUST "build the Rust bindings for the Mooncake Store" ON)
option(
  WITH_STORE_C_SHARED
  "Deprecated compatibility switch; Store now always builds libmooncake_store.so"
  OFF)
if(WITH_STORE_C_SHARED)
  message(
    STATUS
      "WITH_STORE_C_SHARED is retained for compatibility; mooncake_store is already the canonical shared SDK target"
  )
endif()
option(WITH_EP "build mooncake with expert parallelism support" OFF)
option(USE_NOF "build mooncake store with NoF SSD pool support" OFF)
option(MOONCAKE_ENABLE_TEST_FAILPOINTS
       "Enable file-handshake failpoints for integration tests" OFF)
if(MOONCAKE_ENABLE_TEST_FAILPOINTS)
  add_compile_definitions(MOONCAKE_ENABLE_TEST_FAILPOINTS)
endif()
option(MOONCAKE_ENABLE_OPLOG_PERF_METRICS
       "Enable detailed batch OpLog performance metrics" OFF)

if(WITH_TE OR WITH_EP)
  include(${CMAKE_CURRENT_SOURCE_DIR}/mooncake-common/SetupPython.cmake)
  if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/extern/pybind11/CMakeLists.txt")
    add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/extern/pybind11)
  else()
    execute_process(
      COMMAND ${PYTHON_EXECUTABLE} -m pybind11 --cmakedir
      OUTPUT_VARIABLE _pybind11_cmake_dir
      OUTPUT_STRIP_TRAILING_WHITESPACE ERROR_QUIET)
    if(_pybind11_cmake_dir)
      set(pybind11_DIR "${_pybind11_cmake_dir}")
    endif()
    find_package(pybind11 CONFIG REQUIRED)
  endif()
  execute_process(
    COMMAND ${PYTHON_EXECUTABLE} -c "import sys; print(sys.path[-1])"
    OUTPUT_VARIABLE PYTHON_SYS_PATH)
  string(STRIP ${PYTHON_SYS_PATH} PYTHON_SYS_PATH)
endif()

if(USE_ETCD)
  add_compile_definitions(USE_ETCD)
  if(USE_ETCD_LEGACY)
    add_compile_definitions(USE_ETCD_LEGACY)
    message(
      STATUS "etcd as metadata server support is enabled (etcd-cpp-api-v3)")
  else()
    message(STATUS "etcd as metadata server support is enabled (go package)")
  endif()
endif()
option(STORE_USE_ETCD "build mooncake store with etcd" OFF)
if(STORE_USE_ETCD)
  add_compile_definitions(STORE_USE_ETCD)
endif()
option(STORE_USE_REDIS "build mooncake store with redis" OFF)
if(STORE_USE_REDIS)
  add_compile_definitions(STORE_USE_REDIS)
endif()
option(STORE_USE_K8S_LEASE
       "build mooncake store with K8s Lease leader election" OFF)
if(STORE_USE_K8S_LEASE)
  if(STORE_USE_ETCD)
    message(
      FATAL_ERROR
        "STORE_USE_K8S_LEASE and STORE_USE_ETCD cannot be enabled together because both build Go c-shared HA backends."
    )
  endif()
  if(USE_ETCD AND NOT USE_ETCD_LEGACY)
    message(
      FATAL_ERROR
        "STORE_USE_K8S_LEASE cannot be enabled with non-legacy USE_ETCD because both build Go c-shared libraries in the same process."
    )
  endif()
  add_compile_definitions(STORE_USE_K8S_LEASE)
endif()

if(USE_NOF)
  add_compile_definitions(USE_NOF)
else()
  message(STATUS "USE_NOF=OFF, NoF SSD pool support is disabled")
endif()

option(STORE_USE_JEMALLOC "Use jemalloc in mooncake store master" OFF)

add_subdirectory(mooncake-common)
include_directories(mooncake-common/etcd)
include_directories(mooncake-common/k8s-lease)
include_directories(mooncake-common/include)

if(WITH_TE)
  add_subdirectory(mooncake-transfer-engine)
  include_directories(mooncake-transfer-engine/include)
endif()

if(WITH_STORE)
  message(STATUS "Mooncake Store will be built")
  add_subdirectory(mooncake-store)
endif()

if(WITH_STORE_RUST)
  if(TARGET mooncake_store)
    message(STATUS "Mooncake Store Rust bindings will be built")
    add_subdirectory(mooncake-store/rust)
  elseif(WITH_STORE)
    message(
      STATUS
        "Mooncake Store Rust bindings are disabled because the Store SDK requires WITH_TE=ON"
    )
  else()
    message(STATUS "Mooncake Store Rust bindings are disabled (WITH_STORE=OFF)")
  endif()
endif()

option(EP_USE_IDE "Enable intelligent indexing for IDEs" OFF)
if(WITH_EP)
  if(EP_USE_IDE)
    message(WARNING "EP_USE_IDE enabled. DO NOT USE IN PRODUCTION!")
    add_subdirectory(mooncake-ep)
    include_directories(mooncake-ep/include)
    add_subdirectory(mooncake-pg)
    include_directories(mooncake-pg/include)
  else()
    message(
      STATUS "WITH_EP enabled: building Mooncake EP and PG Python extensions")
    if(USE_CUDA)
      find_package(CUDAToolkit REQUIRED)
      message(STATUS "Detected CUDA version: ${CUDAToolkit_VERSION}")
    endif()

    # EP_TORCH_VERSIONS: semicolon-separated list of PyTorch versions to build
    # for. Can be set via -DEP_TORCH_VERSIONS="2.9.1;2.8.0" or the
    # EP_TORCH_VERSIONS env var. Empty means build with the currently-installed
    # torch.
    if(NOT EP_TORCH_VERSIONS)
      set(EP_TORCH_VERSIONS "$ENV{EP_TORCH_VERSIONS}")
    endif()
    set(EP_TORCH_VERSIONS
        "${EP_TORCH_VERSIONS}"
        CACHE
          STRING
          "PyTorch versions for EP/PG extensions, semicolon-separated (empty = use currently-installed torch)"
    )

    # TORCH_CUDA_ARCH_LIST forwarded to the torch CUDA extension build.
    if(NOT TORCH_CUDA_ARCH_LIST)
      set(TORCH_CUDA_ARCH_LIST "$ENV{TORCH_CUDA_ARCH_LIST}")
    endif()
    if(NOT TORCH_CUDA_ARCH_LIST)
      set(TORCH_CUDA_ARCH_LIST "8.0;9.0")
    endif()
    set(TORCH_CUDA_ARCH_LIST
        "${TORCH_CUDA_ARCH_LIST}"
        CACHE STRING
              "CUDA arch list for EP/PG extension builds (e.g. \"8.0;9.0\")")

    # Staging directory: EP/PG .so files are placed here during make and later
    # injected into the wheel AFTER auditwheel, so patchelf never touches the
    # CUDA fatbins (which would cause cudaErrorInvalidKernelImage at runtime).
    set(EP_PG_STAGING_DIR "${CMAKE_BINARY_DIR}/ep_pg_staging")

    # Convert semicolon-separated lists to pipe-separated strings so they
    # survive CMake's COMMAND list-splitting (semicolons are CMake list
    # separators).
    string(REPLACE ";" "|" _ep_torch_versions_pipe "${EP_TORCH_VERSIONS}")
    string(REPLACE ";" "|" _torch_cuda_arch_list_pipe "${TORCH_CUDA_ARCH_LIST}")

    add_custom_target(
      mooncake_ep_ext ALL
      COMMAND ${CMAKE_COMMAND} -E make_directory "${EP_PG_STAGING_DIR}"
      COMMAND
        ${CMAKE_COMMAND} "-DSOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/mooncake-ep"
        "-DEP_CUDA_MAJOR=${CUDAToolkit_VERSION_MAJOR}"
        "-DEP_CUDA_MINOR=${CUDAToolkit_VERSION_MINOR}"
        "-DEP_TORCH_VERSIONS=${_ep_torch_versions_pipe}"
        "-DTORCH_CUDA_ARCH_LIST=${_torch_cuda_arch_list_pipe}"
        "-DSTAGING_DIR=${EP_PG_STAGING_DIR}"
        "-DENGINE_SO_PATH=$<TARGET_FILE:engine>"
        "-DPython3_EXECUTABLE=${Python3_EXECUTABLE}"
        "-DEP_USE_MUSA=$<IF:$<BOOL:${USE_MUSA}>,1,0>"
        "-DEP_USE_MACA=$<IF:$<BOOL:${USE_MACA}>,1,0>" -P
        "${CMAKE_CURRENT_SOURCE_DIR}/mooncake-ep/BuildEpExt.cmake"
      COMMENT "Building Mooncake EP Python extension(s)"
      DEPENDS engine
      VERBATIM)

    add_custom_target(
      mooncake_pg_ext ALL
      COMMAND ${CMAKE_COMMAND} -E make_directory "${EP_PG_STAGING_DIR}"
      COMMAND
        ${CMAKE_COMMAND} "-DSOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/mooncake-pg"
        "-DEP_CUDA_MAJOR=${CUDAToolkit_VERSION_MAJOR}"
        "-DEP_CUDA_MINOR=${CUDAToolkit_VERSION_MINOR}"
        "-DEP_TORCH_VERSIONS=${_ep_torch_versions_pipe}"
        "-DTORCH_CUDA_ARCH_LIST=${_torch_cuda_arch_list_pipe}"
        "-DSTAGING_DIR=${EP_PG_STAGING_DIR}"
        "-DENGINE_SO_PATH=$<TARGET_FILE:engine>"
        "-DPython3_EXECUTABLE=${Python3_EXECUTABLE}"
        "-DEP_USE_MUSA=$<IF:$<BOOL:${USE_MUSA}>,1,0>"
        "-DEP_USE_MACA=$<IF:$<BOOL:${USE_MACA}>,1,0>" -P
        "${CMAKE_CURRENT_SOURCE_DIR}/mooncake-pg/BuildPgExt.cmake"
      COMMENT "Building Mooncake PG Python extension(s)"
      DEPENDS engine mooncake_ep_ext
      VERBATIM)
  endif()
endif()

if(WITH_TE OR TARGET mooncake_store)
  add_subdirectory(mooncake-integration)
endif()

if(WITH_STORE_GO AND TARGET mooncake_store)
  add_custom_target(build_store_go DEPENDS mooncake_store)
  add_custom_command(
    TARGET build_store_go
    COMMAND bash build.sh ${CMAKE_BINARY_DIR} ${CMAKE_CURRENT_BINARY_DIR}
            ${USE_ETCD} ${USE_REDIS} ${USE_HTTP} ${USE_ETCD_LEGACY}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/mooncake-store/go)
  set_property(TARGET build_store_go PROPERTY EXCLUDE_FROM_ALL FALSE)
  message(STATUS "Mooncake Store Go bindings will be built")
endif()

if(WITH_P2P_STORE)
  add_subdirectory(mooncake-p2p-store)
  message(STATUS "P2P Store will be built")
endif()
