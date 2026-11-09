file(GLOB SOURCES "*.cpp")
# TODO(F01-T111, replace-in=F01-3.2): Replace this ordinary-install-only wiring
# with U's SKBUILD/SetupPython split and complete python-component installs for
# enabled native modules, executables, and runtime libraries.
# Owner: F01-3.2. Audit remaining U differences by feature/UPG2 owner before
# removing this marker; deferred native features are not provided by this task.
set(PYTHON_PACKAGE_NAME "mooncake")
set(PYTHON_EXECUTABLE "python3")
execute_process(
    COMMAND ${PYTHON_EXECUTABLE} -c "import sys; print([s for s in sys.path if 'packages' in s][0])"
    OUTPUT_VARIABLE PYTHON_SYS_PATH
)
string(STRIP ${PYTHON_SYS_PATH} PYTHON_SYS_PATH)

if("${PYTHON_SYS_PATH}" STREQUAL "")
    message(FATAL_ERROR "Python path is empty! Please check the python env.")
endif()
set(MOONCAKE_PYTHON_INSTALL_DIR "${PYTHON_SYS_PATH}/${PYTHON_PACKAGE_NAME}")

if (WITH_STORE)
    include_directories("../mooncake-store/include")
    include_directories("../mooncake-store/include/cachelib_memory_allocator")

    include_directories("../mooncake-store/include/cachelib_memory_allocator/include")
    include_directories("../mooncake-store/include/cachelib_memory_allocator/fake_include")
endif()

include_directories("/usr/include/jsoncpp")
include_directories("./")
include_directories("../mooncake-transfer-engine/include")

find_package(Python3 COMPONENTS Interpreter Development REQUIRED)

set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)

message(STATUS "Mooncake Python install directory: ${MOONCAKE_PYTHON_INSTALL_DIR}")

# TODO(F01-T113, replace-in=UPG2-F01-public-build): Receive U's shared-segment,
# TE/TENT/device bindings, allocator gates, and independent EP/Conductor/Reshard
# package wiring with their real providers, including legacy-wheel consumers.
# Owner: UPG2 public-build integration. Close only after the per-component
# source, target, install, and package checks in the F01-3.1 U-diff ledger.
if (WITH_TE)
    pybind11_add_module(engine ${SOURCES} ${CACHE_ALLOCATOR_SOURCES}
        transfer_engine/transfer_engine_py.cpp
    )
    set_target_properties(engine PROPERTIES
        INSTALL_RPATH "$ORIGIN"
    )

    target_link_libraries(engine PRIVATE
        $<TARGET_OBJECTS:rpc_communicator>
    )

    target_link_libraries(engine PRIVATE ${Python3_LIBRARIES})
    target_include_directories(engine PRIVATE ${Python3_INCLUDE_DIRS})
    if (USE_ASCEND_DIRECT)
        target_link_libraries(engine PUBLIC
            ascendcl
            transfer_engine
            glog::glog
            gflags::gflags
        )
    else()
        target_link_libraries(engine PUBLIC
            transfer_engine
            glog::glog
            gflags::gflags
        )
    endif()

    if (USE_CUDA)
        find_package(CUDAToolkit REQUIRED)
        target_link_libraries(engine PRIVATE CUDA::cudart)
    endif()
endif()


set(ALLOCATOR_SO_PATH "${CMAKE_BINARY_DIR}/mooncake-transfer-engine/nvlink-allocator/nvlink_allocator.so")
if(USE_MNNVL)
    message(STATUS "USE_MNNVL is enabled, nvlink_allocator.so will be installed in the Python package")
    install(FILES
        "${ALLOCATOR_SO_PATH}"
        DESTINATION ${MOONCAKE_PYTHON_INSTALL_DIR}
    )
endif()

# TODO(F01-T114, replace-in=UPG1-F15): Add Engram binding sources/includes and
# structured/range/fast-copy Python package consumers with the approved native
# features. Owner: F15 (S58, W01-W04, W06, W07); close after all assigned items.
# F01-T110 also covers buffer_pool.cpp and buffer_pool.py installation in S13.
if (WITH_STORE)
    pybind11_add_module(store ${SOURCES} ${CACHE_ALLOCATOR_SOURCES}
        store/store_py.cpp
        integration_utils.h
    )
    set_target_properties(store PROPERTIES
        OUTPUT_NAME "_store"
        INSTALL_RPATH "$ORIGIN"
    )
    if (USE_ASCEND_DIRECT)
        target_link_libraries(store PUBLIC
            ascendcl
            transfer_engine
            glog::glog
            gflags::gflags
            mooncake_store
            cachelib_memory_allocator
        )
    else()
        target_link_libraries(store PUBLIC
            transfer_engine
            glog::glog
            gflags::gflags
            mooncake_store
            cachelib_memory_allocator
        )
    endif()
endif()

if (USE_MNNVL)
    message(STATUS "USE_MNNVL is enabled, allocator.py will be installed in the Python package")
    install(FILES
        "${CMAKE_CURRENT_SOURCE_DIR}/allocator.py"
        DESTINATION ${MOONCAKE_PYTHON_INSTALL_DIR}
    )
endif()

if(USE_INTRA_NVLINK)
    message(STATUS "USE_INTRA_NVLINK is enabled, IntraNode nvlink is now activated")
endif()

install(
  FILES "${CMAKE_CURRENT_SOURCE_DIR}/../python/mooncake/async_store.py"
  DESTINATION ${MOONCAKE_PYTHON_INSTALL_DIR}
  COMPONENT python)

install(
  FILES
    "${CMAKE_CURRENT_SOURCE_DIR}/../python/mooncake/__init__.py"
    "${CMAKE_CURRENT_SOURCE_DIR}/../python/mooncake/py.typed"
  DESTINATION ${MOONCAKE_PYTHON_INSTALL_DIR})

install(
  DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/../python/mooncake/store/"
  DESTINATION "${MOONCAKE_PYTHON_INSTALL_DIR}/store"
  COMPONENT python
  FILES_MATCHING
  PATTERN "*.py")

#Install Python scripts from mooncake - wheel / mooncake / directory
install(FILES
    "${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-wheel/mooncake/http_metadata_server.py"
    "${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-wheel/mooncake/cli_bench.py"
    "${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-wheel/mooncake/transfer_engine_topology_dump.py"
    "${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-wheel/mooncake/mooncake_connector_v1.py"
    "${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-wheel/mooncake/vllm_v1_proxy_server.py"
    DESTINATION ${MOONCAKE_PYTHON_INSTALL_DIR}
)

# TODO(F01-T115, replace-in=UPG1-F02-B05): Install U's canonical Store CLI,
# client/config and launcher modules and update legacy collection. The REST
# sync_main entry is already connected; verify service behavior in F02-B05.
# Owner: F02-B05.
if (WITH_STORE)
    install(FILES
        "${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-wheel/mooncake/mooncake_store_service.py"
        "${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-wheel/mooncake/mooncake_config.py"
        "${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-wheel/mooncake/cli.py"
        DESTINATION ${MOONCAKE_PYTHON_INSTALL_DIR}
    )
endif()

# TODO(F01-T116, replace-in=UPG1-F10-S41): Install and collect U's NoF
# administration, SSD register/unregister, and SPDK target tools with their
# native providers and USE_NOF gate. Owner: F10-S41; verify installed commands.
# TODO(F01-T117, replace-in=UPG1-F13-S57): Add U's conditional K8s lease runtime
# library install and legacy-wheel collection with the lease backend.
# Owner: F13-S57; verify enabled/disabled package contents and library loading.
if (WITH_STORE)
    install(TARGETS store LIBRARY DESTINATION ${MOONCAKE_PYTHON_INSTALL_DIR}
                                  COMPONENT python)
endif()

if (WITH_TE)
    install(TARGETS engine DESTINATION ${MOONCAKE_PYTHON_INSTALL_DIR})
    install(TARGETS asio_shared DESTINATION ${MOONCAKE_PYTHON_INSTALL_DIR})
endif()
