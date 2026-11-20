file(GLOB SOURCES "*.cpp")
set(PYTHON_PACKAGE_NAME "mooncake")
if(SKBUILD)
  # scikit-build-core sets CMAKE_INSTALL_PREFIX to its private wheel tree. Keep
  # every Python artifact relative to that prefix.
  set(MOONCAKE_PYTHON_INSTALL_DIR "${PYTHON_PACKAGE_NAME}")
else()
  include(${CMAKE_CURRENT_LIST_DIR}/../mooncake-common/SetupPython.cmake)
  execute_process(
    COMMAND ${PYTHON_EXECUTABLE} -c
            "import sys; print([s for s in sys.path if 'packages' in s][0])"
    OUTPUT_VARIABLE PYTHON_SYS_PATH)
  string(STRIP ${PYTHON_SYS_PATH} PYTHON_SYS_PATH)

  if("${PYTHON_SYS_PATH}" STREQUAL "")
    message(FATAL_ERROR "Python path is empty! Please check the python env.")
  endif()
  set(MOONCAKE_PYTHON_INSTALL_DIR "${PYTHON_SYS_PATH}/${PYTHON_PACKAGE_NAME}")
endif()

if(WITH_STORE OR WITH_STORE_P2P)
  include_directories("../mooncake-store/include")
  include_directories("../mooncake-store/include/cachelib_memory_allocator")

  include_directories(
    "../mooncake-store/include/cachelib_memory_allocator/include")
  include_directories(
    "../mooncake-store/include/cachelib_memory_allocator/fake_include")
endif()

include_directories("/usr/include/jsoncpp")
include_directories("./")
include_directories("../mooncake-transfer-engine/include")

find_package(
  Python3
  COMPONENTS Interpreter Development.Module
  REQUIRED)

execute_process(
  COMMAND ${Python3_EXECUTABLE} -c
          "import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX'))"
  OUTPUT_VARIABLE PYTHON_EXT_SUFFIX
  OUTPUT_STRIP_TRAILING_WHITESPACE)

set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)

message(
  STATUS "Mooncake Python install directory: ${MOONCAKE_PYTHON_INSTALL_DIR}")

# TODO(UPG-T0004, replace-in=UPG2.3): Reconcile the TE/TENT and shared-segment
# bindings, allocator gates, and independent packages with U's real providers.
# Source: management 3 UPG1-F01-3.1 (F01-T113/A01), 8e6f32a1957b0ccfe082f4add18d55260a08fc08:mooncake-integration/CMakeLists.txt.
if(WITH_TE)
  pybind11_add_module(
    engine ${SOURCES} ${CACHE_ALLOCATOR_SOURCES}
    transfer_engine/transfer_engine_py.cpp)
  set_target_properties(engine PROPERTIES INSTALL_RPATH "$ORIGIN")

  target_link_libraries(engine PRIVATE $<TARGET_OBJECTS:rpc_communicator>)

  target_include_directories(engine PRIVATE ${Python3_INCLUDE_DIRS})
  if(USE_ASCEND_DIRECT)
    target_link_libraries(engine PUBLIC ascendcl transfer_engine glog::glog
                                        gflags::gflags)
  else()
    target_link_libraries(engine PUBLIC transfer_engine glog::glog
                                        gflags::gflags)
  endif()

  if(USE_CUDA)
    find_package(CUDAToolkit REQUIRED)
    target_link_libraries(engine PRIVATE CUDA::cudart)
  endif()
endif()

set(ALLOCATOR_SO_PATH
    "${CMAKE_BINARY_DIR}/mooncake-transfer-engine/nvlink-allocator/nvlink_allocator.so"
)

set(MOONCAKE_HAS_NVLINK_ALLOCATOR OFF)
if(WITH_TE
   AND (USE_MNNVL OR NOT WITH_NVIDIA_PEERMEM)
   AND (USE_CUDA OR USE_HIP OR USE_MUSA))
  set(MOONCAKE_HAS_NVLINK_ALLOCATOR ON)
endif()

if(MOONCAKE_HAS_NVLINK_ALLOCATOR)
  message(
    STATUS
      "GPU IPC support is enabled, nvlink_allocator.so will be installed in the Python package"
  )
  install(
    FILES "${ALLOCATOR_SO_PATH}"
    DESTINATION ${MOONCAKE_PYTHON_INSTALL_DIR}
    COMPONENT python)
endif()

function(add_store_binding target module source store_library)
  pybind11_add_module(
    ${target}
    ${SOURCES}
    ${CACHE_ALLOCATOR_SOURCES}
    ${source}
    integration_utils.h)
  set_target_properties(
    ${target} PROPERTIES OUTPUT_NAME "${module}" INSTALL_RPATH "$ORIGIN")
  target_link_libraries(
    ${target} PUBLIC transfer_engine glog::glog gflags::gflags
                    ${store_library} cachelib_memory_allocator)
  if(USE_ASCEND_DIRECT)
    target_link_libraries(${target} PUBLIC ascendcl)
  endif()
  install(TARGETS ${target} LIBRARY DESTINATION ${MOONCAKE_PYTHON_INSTALL_DIR}
                             COMPONENT python)
endfunction()

set(store_cli_targets)
if(WITH_STORE)
  # TODO(UPG-T0003, replace-in=UPG2.2): Receive U's central BufferPool binding and wrapper.
  # TODO(UPG-T0005, replace-in=UPG2.2): Receive U's central Engram sources and includes.
  # Source: management 3 UPG1-F01-3.1 (F01-T110/A01, F01-T114/A01),
  # 8e6f32a1957b0ccfe082f4add18d55260a08fc08:mooncake-integration/CMakeLists.txt.
  add_store_binding(store _store store/store_py.cpp mooncake_store)
  list(APPEND store_cli_targets mooncake_master mooncake_client)
endif()
if(WITH_STORE_P2P)
  add_store_binding(store_p2p _store_p2p store/p2p_store_py.cpp mooncake_store_p2p)
  list(APPEND store_cli_targets mooncake_p2p_master mooncake_p2p_client)
endif()

if(store_cli_targets)
  if(SKBUILD)
    set(store_cli_rpath "$ORIGIN")
  else()
    set(store_cli_rpath "$ORIGIN;$ORIGIN/../lib")
  endif()
  set_target_properties(${store_cli_targets} PROPERTIES INSTALL_RPATH "${store_cli_rpath}")
  install(TARGETS ${store_cli_targets}
          RUNTIME DESTINATION ${MOONCAKE_PYTHON_INSTALL_DIR} COMPONENT python)
endif()

if(NOT SKBUILD)
  file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/${PYTHON_PACKAGE_NAME}/__init__.py
       "# Auto-generated by CMake\n")
endif()

if(MOONCAKE_HAS_NVLINK_ALLOCATOR)
  message(STATUS "allocator.py will be installed in the Python package")
  install(
    FILES "${CMAKE_CURRENT_SOURCE_DIR}/allocator.py"
    DESTINATION ${MOONCAKE_PYTHON_INSTALL_DIR}
    COMPONENT python)
endif()

if(USE_INTRA_NVLINK)
  message(
    STATUS "USE_INTRA_NVLINK is enabled, IntraNode nvlink is now activated")
endif()

install(
  FILES "${CMAKE_CURRENT_SOURCE_DIR}/../python/mooncake/async_store.py"
  DESTINATION ${MOONCAKE_PYTHON_INSTALL_DIR}
  COMPONENT python)

# scikit-build-core consumes these legacy modules through python/CMakeLists.txt.
# Retain the direct CMake install surface only outside PEP 517 builds.
if(NOT SKBUILD)
  install(
    FILES
      "${CMAKE_CURRENT_SOURCE_DIR}/../python/mooncake/__init__.py"
      "${CMAKE_CURRENT_SOURCE_DIR}/../python/mooncake/py.typed"
      "${CMAKE_CURRENT_SOURCE_DIR}/../python/mooncake/http_metadata_server.py"
      "${CMAKE_CURRENT_SOURCE_DIR}/../python/mooncake/_launcher.py"
      "${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-wheel/mooncake/cli_bench.py"
      "${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-wheel/mooncake/transfer_engine_topology_dump.py"
      "${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-wheel/mooncake/mooncake_connector_v1.py"
      "${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-wheel/mooncake/vllm_v1_proxy_server.py"
    DESTINATION ${MOONCAKE_PYTHON_INSTALL_DIR})

  install(
    DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/../python/mooncake/store/"
    DESTINATION "${MOONCAKE_PYTHON_INSTALL_DIR}/store"
    COMPONENT python
    FILES_MATCHING
    PATTERN "*.py")

  # TODO(UPG-T0012, replace-in=UPG2.2): Reconcile installed central CLI,
  # config, and REST providers with U.
  # Source: management 3 UPG1-F01-3.1 (F01-T115/A01),
  # 8e6f32a1957b0ccfe082f4add18d55260a08fc08:mooncake-integration/CMakeLists.txt.
  if(WITH_STORE OR WITH_STORE_P2P)
    install(
      FILES
        "${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-wheel/mooncake/mooncake_store_service.py"
        "${CMAKE_CURRENT_SOURCE_DIR}/../python/mooncake/mooncake_config.py"
      DESTINATION ${MOONCAKE_PYTHON_INSTALL_DIR})
  endif()

  if(WITH_STORE)
    install(
      FILES
        "${CMAKE_CURRENT_SOURCE_DIR}/../python/mooncake/cli.py"
        "${CMAKE_CURRENT_SOURCE_DIR}/../python/mooncake/cli_client.py"
      DESTINATION ${MOONCAKE_PYTHON_INSTALL_DIR})

    # TODO(UPG-T0013, replace-in=UPG2.2): Receive U's centralized NoF tools.
    # TODO(UPG-T0046, replace-in=UPG1.4): Tie NoF wheel contents to the
    # centralized provider and keep them out of P2P-only packages.
    # Source: management 3 UPG1-F01-3.1 (F01-T116/A01),
    # 8e6f32a1957b0ccfe082f4add18d55260a08fc08:mooncake-integration/CMakeLists.txt.
  endif()
endif()

if(WITH_TE)
  install(TARGETS asio_shared LIBRARY DESTINATION ${MOONCAKE_PYTHON_INSTALL_DIR}
                                      COMPONENT python)
endif()

if(WITH_TE)
  install(TARGETS engine LIBRARY DESTINATION ${MOONCAKE_PYTHON_INSTALL_DIR}
                                 COMPONENT python)
endif()

if(SKBUILD)
  if(WITH_TE AND BUILD_EXAMPLES)
    set_target_properties(transfer_engine_bench PROPERTIES INSTALL_RPATH
                                                           "$ORIGIN")
    install(TARGETS transfer_engine_bench
            RUNTIME DESTINATION ${MOONCAKE_PYTHON_INSTALL_DIR} COMPONENT python)
  endif()

  if((USE_ETCD AND NOT USE_ETCD_LEGACY) OR STORE_USE_ETCD)
    install(
      FILES "${CMAKE_BINARY_DIR}/mooncake-common/etcd/libetcd_wrapper.so"
      DESTINATION ${MOONCAKE_PYTHON_INSTALL_DIR}
      COMPONENT python)
  endif()

  # TODO(UPG-T0014, replace-in=UPG2.2): Receive U's gated K8s lease runtime
  # and verify that enabled packages can load the actual library.
  # Source: management 3 UPG1-F01-3.1 (F01-T117/A01),
  # 8e6f32a1957b0ccfe082f4add18d55260a08fc08:mooncake-integration/CMakeLists.txt.

  if(USE_ASCEND_DIRECT)
    install(TARGETS ascend_transport
            LIBRARY DESTINATION ${MOONCAKE_PYTHON_INSTALL_DIR} COMPONENT python)
  endif()

  if(USE_ASCEND)
    install(TARGETS ascend_transport_mem
            LIBRARY DESTINATION ${MOONCAKE_PYTHON_INSTALL_DIR} COMPONENT python)
  endif()

  if(BUILD_SHARED_LIBS)
    set(_mooncake_python_shared_targets mooncake_common)
    if(WITH_TE)
      list(APPEND _mooncake_python_shared_targets transfer_engine)
    endif()
    if(WITH_STORE)
      list(APPEND _mooncake_python_shared_targets mooncake_store)
    endif()
    if(WITH_STORE_P2P)
      list(APPEND _mooncake_python_shared_targets mooncake_store_p2p)
    endif()
    install(TARGETS ${_mooncake_python_shared_targets}
            LIBRARY DESTINATION ${MOONCAKE_PYTHON_INSTALL_DIR} COMPONENT python)
  endif()
endif()
