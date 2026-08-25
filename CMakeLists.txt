cmake_minimum_required(VERSION 3.16)
project(mooncake CXX C)

# indicates cmake is invoked from top-level dir
set(GLOBAL_CONFIG "true")

if(SKBUILD)
  add_subdirectory(python)
endif()

include(mooncake-common/FindJsonCpp.cmake)
include(mooncake-common/FindGLOG.cmake)
include(mooncake-common/common.cmake)
# unit test
if(BUILD_UNIT_TESTS)
  enable_testing()
endif()

option(WITH_TE "build mooncake transfer engine and sample code" ON)
option(WITH_STORE "build mooncake store library and sample code" ON)
option(WITH_STORE_GO "build Go bindings for mooncake store" OFF)
option(WITH_P2P_STORE "build p2p store library and sample code" OFF)
option(WITH_RUST_EXAMPLE
       "build the Rust interface and sample code for the transfer engine" OFF)
option(WITH_STORE_RUST "build the Rust bindings for the Mooncake Store" ON)
option(
  WITH_STORE_C_SHARED
  "build a self-contained libmooncake_store.so exposing only the store_c.h C ABI (for dlopen consumers)"
  OFF)
option(WITH_EP "build mooncake with expert parallelism support" OFF)
option(USE_NOF "build mooncake store with NoF SSD pool support" OFF)
option(MOONCAKE_ENABLE_TEST_FAILPOINTS
       "Enable file-handshake failpoints for integration tests" OFF)
if(MOONCAKE_ENABLE_TEST_FAILPOINTS)
  add_compile_definitions(MOONCAKE_ENABLE_TEST_FAILPOINTS)
endif()
option(MOONCAKE_ENABLE_OPLOG_PERF_METRICS
       "Enable detailed batch OpLog performance metrics" OFF)

include(${CMAKE_CURRENT_SOURCE_DIR}/mooncake-common/SetupPython.cmake)
if(SKBUILD)
  set(PYBIND11_FINDPYTHON ON)
  find_package(pybind11 CONFIG REQUIRED)
else()
  add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/extern/pybind11)
endif()
execute_process(
  COMMAND ${PYTHON_EXECUTABLE} -c "import sys; print(sys.path[-1])"
  OUTPUT_VARIABLE PYTHON_SYS_PATH)
string(STRIP ${PYTHON_SYS_PATH} PYTHON_SYS_PATH)

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
option(ENABLE_KV_EVENTS
       "Build master KV events ZMQ publisher (requires libzmq when ON)" OFF)
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

include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/Dependencies.cmake)
if(WITH_STORE)
  mooncake_provide_zstd()
  mooncake_provide_xxhash()
  if(ENABLE_KV_EVENTS)
    mooncake_provide_libzmq()
  endif()
endif()
if(WITH_STORE OR (WITH_TE AND USE_TENT))
  mooncake_provide_liburing()
endif()
if((WITH_TE AND USE_REDIS) OR (WITH_STORE AND STORE_USE_REDIS))
  mooncake_provide_hiredis(REQUIRED)
elseif(WITH_TE AND USE_TENT)
  mooncake_provide_hiredis()
endif()

# Define ASIO macros before building targets that include ASIO headers.
add_compile_definitions(ASIO_SEPARATE_COMPILATION ASIO_DYN_LINK)

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
  include_directories(mooncake-store/include)
endif()

if(WITH_STORE_RUST)
  if(NOT WITH_STORE)
    message(FATAL_ERROR "WITH_STORE_RUST=ON requires WITH_STORE=ON")
  endif()
  message(STATUS "Mooncake Store Rust bindings will be built")
  add_subdirectory(mooncake-store/rust)
endif()

option(EP_USE_IDE "Enable intelligent indexing for IDEs" OFF)
if(WITH_EP)
  add_subdirectory(mooncake-pg)
  include_directories(mooncake-pg/include)

  if(EP_USE_IDE)
    message(WARNING "EP_USE_IDE enabled. DO NOT USE IN PRODUCTION!")
    add_subdirectory(mooncake-ep)
    include_directories(mooncake-ep/include)
    add_library(
      mooncake_pg_torch_ide OBJECT
      mooncake-pg/torch/src/mooncake_backend.cpp
      mooncake-pg/torch/src/pg_py.cpp mooncake-pg/torch/src/work_handles.cpp)
    # Reuse Mooncake EP's dependency include paths for clangd indexing.
    target_include_directories(
      mooncake_pg_torch_ide
      PRIVATE mooncake-pg/torch/include mooncake-pg/include
              $<TARGET_PROPERTY:_ep,INCLUDE_DIRECTORIES>)
  else()
    message(
      STATUS
        "WITH_EP enabled: building Mooncake EP natively and PG via setup.py")
    if(USE_CUDA)
      find_package(CUDAToolkit REQUIRED)
      message(STATUS "Detected CUDA version: ${CUDAToolkit_VERSION}")
    endif()

    add_subdirectory(mooncake-ep)
    include_directories(mooncake-ep/include)

    # Keep generated Torch extensions and device libraries in backend-owned
    # build directories. The legacy release path injects the staged device
    # artifacts after auditwheel; scikit-build-core installs them directly into
    # its private wheel tree.
    set(EP_PG_STAGING_DIR "${CMAKE_BINARY_DIR}/ep_pg_staging")
    file(REMOVE_RECURSE "${EP_PG_STAGING_DIR}")
    file(MAKE_DIRECTORY "${EP_PG_STAGING_DIR}")

    # Stage the runtime JIT source bundle for the legacy wheel script as well
    # as the scikit-build install path.  The authoritative files stay under
    # mooncake-pg/torch; no package-tree mirror is maintained.
    add_custom_target(
      mooncake_pg_jit_sources_stage ALL
      COMMAND ${CMAKE_COMMAND} -E make_directory
              "${EP_PG_STAGING_DIR}/_pg_jit"
      COMMAND ${CMAKE_COMMAND} -E copy_if_different
              "${CMAKE_CURRENT_SOURCE_DIR}/mooncake-pg/torch/src/pg_py.cpp"
              "${EP_PG_STAGING_DIR}/_pg_jit/pg_py.cpp"
      COMMAND ${CMAKE_COMMAND} -E copy_if_different
              "${CMAKE_CURRENT_SOURCE_DIR}/mooncake-pg/torch/src/mooncake_backend.cpp"
              "${EP_PG_STAGING_DIR}/_pg_jit/mooncake_backend.cpp"
      COMMAND ${CMAKE_COMMAND} -E copy_if_different
              "${CMAKE_CURRENT_SOURCE_DIR}/mooncake-pg/torch/src/work_handles.cpp"
              "${EP_PG_STAGING_DIR}/_pg_jit/work_handles.cpp"
      COMMAND ${CMAKE_COMMAND} -E copy_if_different
              "${CMAKE_CURRENT_SOURCE_DIR}/mooncake-pg/torch/include/mooncake_backend.h"
              "${EP_PG_STAGING_DIR}/_pg_jit/mooncake_backend.h"
      COMMAND ${CMAKE_COMMAND} -E copy_if_different
              "${CMAKE_CURRENT_SOURCE_DIR}/mooncake-pg/torch/include/work_handles.h"
              "${EP_PG_STAGING_DIR}/_pg_jit/work_handles.h"
      COMMAND ${CMAKE_COMMAND} -E copy_if_different
              "${CMAKE_CURRENT_SOURCE_DIR}/mooncake-pg/torch/include/torch_utils.h"
              "${EP_PG_STAGING_DIR}/_pg_jit/torch_utils.h"
      COMMAND ${CMAKE_COMMAND} -E copy_if_different
              "${CMAKE_CURRENT_SOURCE_DIR}/mooncake-pg/include/pg_utils.h"
              "${EP_PG_STAGING_DIR}/_pg_jit/pg_utils.h"
      COMMAND ${CMAKE_COMMAND} -E copy_if_different
              "${CMAKE_CURRENT_SOURCE_DIR}/mooncake-pg/include/mooncake_pg.h"
              "${EP_PG_STAGING_DIR}/_pg_jit/mooncake_pg.h"
      COMMENT "Staging Mooncake PG Torch JIT sources")

    if(USE_CUDA)
      add_custom_target(
        mooncake_ep_device_stage ALL
        COMMAND ${CMAKE_COMMAND} -E make_directory "${EP_PG_STAGING_DIR}"
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
                "$<TARGET_FILE:mooncake_ep_device>" "${EP_PG_STAGING_DIR}/"
        COMMENT "Staging Mooncake EP CUDA device library"
        DEPENDS mooncake_ep_device
        VERBATIM)
    endif()

    add_custom_target(
      mooncake_pg_device_stage ALL
      COMMAND ${CMAKE_COMMAND} -E make_directory "${EP_PG_STAGING_DIR}"
      COMMAND ${CMAKE_COMMAND} -E copy_if_different
              "$<TARGET_FILE:mooncake_pg_device>" "${EP_PG_STAGING_DIR}/"
      COMMENT "Staging Mooncake PG device library"
      DEPENDS mooncake_pg_device
      VERBATIM)
  endif()
endif()

add_subdirectory(mooncake-integration)

if(WITH_STORE_GO AND WITH_STORE)
  add_custom_target(build_store_go DEPENDS mooncake_store transfer_engine)
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
