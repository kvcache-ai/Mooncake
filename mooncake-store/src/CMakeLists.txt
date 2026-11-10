add_subdirectory(cachelib_memory_allocator)

# Sources used by either architecture, grouped by their Store role.
set(MOONCAKE_STORE_COMMON_SHARED_SOURCES
    common_types.cpp
    utils.cpp
    common/file_util.cpp
    common/network.cpp
    etcd_helper.cpp
    thread_pool.cpp
    allocator.cpp
    offset_allocator.cpp
)

set(MOONCAKE_STORE_COMMON_CLIENT_SOURCES
    common/client_buffer_allocation.cpp
    common/mmap_buffer.cpp
    client_resources.cpp
    posix_file.cpp
    client_buffer.cpp
    real_client.cpp
    dummy_client.cpp
    shm_helper.cpp
)

set(EXTRA_LIBS "")

if(USE_3FS)
  # TODO(F10-S38-T001, replace-in=UPG1-F10-S38): Reconcile this client-side
  # ThreeFSFile wiring with U's shared HF3FS resource manager and DFS adapter
  # when the approved descriptor-based DFS consumers are implemented.
  # Owner: F10-S38. Keep USE_3FS source/header/library conditions aligned;
  # this existing wiring does not provide descriptor-based DFS support.
  # Remove this marker after the actual migration or approved exclusion.
  add_subdirectory(hf3fs)
  list(APPEND MOONCAKE_STORE_COMMON_CLIENT_SOURCES ${HF3FS_SOURCES})
  find_library(HF3FS_API_LIB hf3fs_api_shared PATHS /usr/lib NO_DEFAULT_PATH)
  if(NOT HF3FS_API_LIB)
    message(FATAL_ERROR "hf3fs_api_shared library not found in /usr/lib")
  endif()
  set(EXTRA_LIBS ${HF3FS_API_LIB})
endif()

set(MOONCAKE_STORE_INCLUDE_DIRS
    ${MOONCAKE_STORE_INCLUDE_ROOT}/mooncake-store/proto
    ${MOONCAKE_STORE_INCLUDE_ROOT} ${CMAKE_CURRENT_BINARY_DIR}/../include
    ${ETCD_WRAPPER_INCLUDE})

# Sources used by both the Master and Client sides of Store.
add_library(mooncake_store_shared_objects OBJECT
            ${MOONCAKE_STORE_COMMON_SHARED_SOURCES})
set_target_properties(mooncake_store_shared_objects
                      PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(mooncake_store_shared_objects
                           PRIVATE ${MOONCAKE_STORE_INCLUDE_DIRS})
target_link_libraries(
  mooncake_store_shared_objects
  PRIVATE cachelib_memory_allocator
          glog::glog
          gflags::gflags
          mooncake_common
          $<$<PLATFORM_ID:Linux>:numa>
          asio_shared
          yalantinglibs::yalantinglibs)
if(STORE_USE_ETCD)
  add_dependencies(mooncake_store_shared_objects build_etcd_wrapper)
endif()

# Sources used by mooncake_master and the complete mooncake_store library.
# The selected architecture supplies this target's sources below.
add_library(mooncake_store_master_objects OBJECT)
set_target_properties(mooncake_store_master_objects
                      PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(
  mooncake_store_master_objects PRIVATE ${MOONCAKE_STORE_INCLUDE_DIRS}
                                        ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(
  mooncake_store_master_objects
  PRIVATE mooncake_store_shared_objects
          cachelib_memory_allocator
          glog::glog
          gflags::gflags
          JsonCpp::JsonCpp
          yaml-cpp
          mooncake_common
          asio_shared
          yalantinglibs::yalantinglibs)
if(STORE_USE_ETCD)
  add_dependencies(mooncake_store_master_objects build_etcd_wrapper)
endif()

# Sources used only by Store clients and the complete mooncake_store library.
add_library(mooncake_store_client_objects OBJECT
            ${MOONCAKE_STORE_COMMON_CLIENT_SOURCES})
set_target_properties(mooncake_store_client_objects
                      PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(
  mooncake_store_client_objects PRIVATE ${MOONCAKE_STORE_INCLUDE_DIRS}
                                        ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(
  mooncake_store_client_objects
  PRIVATE mooncake_store_shared_objects
          transfer_engine
          cachelib_memory_allocator
          glog::glog
          gflags::gflags
          JsonCpp::JsonCpp
          yaml-cpp
          mooncake_common
          $<$<PLATFORM_ID:Linux>:numa>
          asio_shared
          yalantinglibs::yalantinglibs)
if(STORE_USE_ETCD)
  add_dependencies(mooncake_store_client_objects build_etcd_wrapper)
endif()

if (USE_ASCEND_DIRECT)
    target_include_directories(mooncake_store_client_objects PRIVATE ${ASCEND_INCLUDE_DIR})
    set(ACL_RUNTIME_HEADER_PATH "${ASCEND_INCLUDE_DIR}/acl/acl_rt.h")
    if(EXISTS "${ACL_RUNTIME_HEADER_PATH}")
        file(READ "${ACL_RUNTIME_HEADER_PATH}" ACL_RUNTIME_CONTENT)
        if("${ACL_RUNTIME_CONTENT}" MATCHES "ACL_MEM_P2P_HUGE1G")
            message(STATUS "ACL_MEM_P2P_HUGE1G exist.")
            target_compile_definitions(mooncake_store_client_objects PRIVATE ASCEND_SUPPORT_FABRIC_MEM)
        else()
            message(STATUS "ACL_MEM_P2P_HUGE1G is not exist.")
        endif()
    else()
        message(WARNING "Acl runtime header file is not exist")
    endif()
endif()

# Reduced Store library linked by mooncake_master.
add_library(
  mooncake_store_master STATIC $<TARGET_OBJECTS:mooncake_store_shared_objects>
                               $<TARGET_OBJECTS:mooncake_store_master_objects>)
target_include_directories(mooncake_store_master
                           PUBLIC ${MOONCAKE_STORE_INCLUDE_DIRS})
target_link_libraries(
  mooncake_store_master
  PRIVATE cachelib_memory_allocator
          ${ETCD_WRAPPER_LIB}
          glog::glog
          mooncake_common
          asio_shared
          yalantinglibs::yalantinglibs
          gflags::gflags
          JsonCpp::JsonCpp
          yaml-cpp
          $<$<PLATFORM_ID:Linux>:numa>)

# Complete public Store library.
add_library(
  mooncake_store
  $<TARGET_OBJECTS:mooncake_store_shared_objects>
  $<TARGET_OBJECTS:mooncake_store_master_objects>
  $<TARGET_OBJECTS:mooncake_store_client_objects>)
target_include_directories(mooncake_store PUBLIC ${MOONCAKE_STORE_INCLUDE_DIRS})
target_link_libraries(
  mooncake_store
  PUBLIC cachelib_memory_allocator
         ${ETCD_WRAPPER_LIB}
         glog::glog
         ${EXTRA_LIBS}
         mooncake_common
         asio_shared
         transfer_engine
         yalantinglibs::yalantinglibs
  PRIVATE gflags::gflags JsonCpp::JsonCpp yaml-cpp
          $<$<PLATFORM_ID:Linux>:numa>)
if(USE_ASCEND_DIRECT)
  target_link_libraries(mooncake_store PRIVATE ascendcl)
endif()

if (BUILD_SHARED_LIBS)
  set_target_properties(mooncake_store PROPERTIES INSTALL_RPATH "$ORIGIN")
  install(TARGETS mooncake_store DESTINATION lib)
endif()

# Store Master executable.
add_executable(mooncake_master)
set(MASTER_EXTRA_INCS)
set(MASTER_EXTRA_LIBS)

if (STORE_USE_JEMALLOC)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(JEMALLOC REQUIRED jemalloc)
    list(APPEND MASTER_EXTRA_INCS ${JEMALLOC_INCLUDE_DIRS})
    list(APPEND MASTER_EXTRA_LIBS ${JEMALLOC_STATIC_LIBRARIES})
endif()

target_include_directories(mooncake_master PRIVATE ${MASTER_EXTRA_INCS})
target_link_libraries(
  mooncake_master
  PRIVATE mooncake_store_master
          cachelib_memory_allocator
          pthread
          ibverbs
          mooncake_common
          glog::glog
          gflags::gflags
          JsonCpp::JsonCpp
          yalantinglibs::yalantinglibs
          ${ETCD_WRAPPER_LIB}
          ${MASTER_EXTRA_LIBS}
          asio_shared)
if(STORE_USE_ETCD)
  add_dependencies(mooncake_master build_etcd_wrapper)
endif()
target_compile_options(mooncake_master PRIVATE -Os)
target_link_options(mooncake_master PRIVATE -Os -s)

# Store Client executable.
add_executable(mooncake_client)
target_link_libraries(
  mooncake_client
  PRIVATE mooncake_store transfer_engine asio_shared gflags::gflags
          yalantinglibs::yalantinglibs)
target_compile_options(mooncake_client PRIVATE -Os)
target_link_options(mooncake_client PRIVATE -Os -s)

# TODO(F01-T070, replace-in=UPG2-F01-public-build): Export common headers through
# mooncake_common's PUBLIC include interface as in U. Owner: UPG2 public build.
# Remove this temporary include block and marker after that interface is adopted.
target_include_directories(mooncake_store_client_objects
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-common/include)
target_include_directories(mooncake_store
                           PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-common/include)
target_include_directories(mooncake_master
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-common/include)

# TODO(F01-T071, replace-in=UPG2-F01-public-build): Export TE headers through
# transfer_engine's PUBLIC include interface as in U. Owner: UPG2 public build.
# Remove this temporary include block and marker after that interface is adopted.
target_include_directories(mooncake_store_client_objects
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-transfer-engine/include)
target_include_directories(mooncake_store
                           PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-transfer-engine/include)

# Select the implementation of the standard Store targets.
if(MOONCAKE_STORE_CLIENT_ARCH STREQUAL "centralized")
    # Sources and entry points of the selected centralized implementation.
    set(MOONCAKE_STORE_CENTRALIZED_MASTER_SOURCES
        master_service.cpp
        master_metric_manager.cpp
        ha_helper.cpp
        segment.cpp
        rpc_service.cpp
        http_metadata_server.cpp
        task_manager.cpp
    )

    set(MOONCAKE_STORE_CENTRALIZED_CLIENT_SOURCES
        client_service.cpp
        centralized_real_client.cpp
        client_metric.cpp
        master_client.cpp
        transfer_task.cpp
        file_storage.cpp
        storage_backend.cpp
    )

    target_sources(mooncake_store_master_objects PRIVATE
        ${MOONCAKE_STORE_CENTRALIZED_MASTER_SOURCES})
    target_sources(mooncake_store_client_objects PRIVATE
        ${MOONCAKE_STORE_CENTRALIZED_CLIENT_SOURCES})
    target_sources(mooncake_master PRIVATE master.cpp)
    target_sources(mooncake_client PRIVATE real_client_main.cpp)
else()
    include(p2p/CMakeLists.txt)
endif()

install(TARGETS mooncake_master mooncake_client DESTINATION bin)
