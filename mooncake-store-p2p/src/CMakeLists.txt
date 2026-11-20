# Store foundations used by the P2P Master and Client.
set(MOONCAKE_STORE_P2P_FOUNDATION_SOURCES
    common_types.cpp
    utils.cpp
    common/file_util.cpp
    common/network.cpp
    etcd_helper.cpp
    thread_pool.cpp
    allocator.cpp
    offset_allocator.cpp
)

set(MOONCAKE_STORE_P2P_CLIENT_FOUNDATION_SOURCES
    common/client_buffer_allocation.cpp
    common/mmap_buffer.cpp
    client_resources.cpp
    config/client_auto_port_config.cpp
    posix_file.cpp
    client_buffer.cpp
    shm_helper.cpp
)

set(STORE_COMMON_SRC "${MOONCAKE_STORE_SOURCE_ROOT}/src")
list(TRANSFORM MOONCAKE_STORE_P2P_FOUNDATION_SOURCES PREPEND "${STORE_COMMON_SRC}/")
list(TRANSFORM MOONCAKE_STORE_P2P_CLIENT_FOUNDATION_SOURCES PREPEND "${STORE_COMMON_SRC}/")

set(EXTRA_LIBS "")

if(USE_3FS)
  add_subdirectory("${STORE_COMMON_SRC}/hf3fs" hf3fs)
  list(APPEND MOONCAKE_STORE_P2P_CLIENT_FOUNDATION_SOURCES ${HF3FS_SOURCES})
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
add_library(mooncake_store_p2p_shared_objects OBJECT
            ${MOONCAKE_STORE_P2P_FOUNDATION_SOURCES})
set_target_properties(mooncake_store_p2p_shared_objects
                      PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(mooncake_store_p2p_shared_objects
                           PRIVATE ${MOONCAKE_STORE_INCLUDE_DIRS})
target_link_libraries(
  mooncake_store_p2p_shared_objects
  PRIVATE cachelib_memory_allocator
          glog::glog
          gflags::gflags
          mooncake_common
          $<$<PLATFORM_ID:Linux>:numa>
          asio_shared
          yalantinglibs::yalantinglibs)
if(STORE_USE_ETCD)
  add_dependencies(mooncake_store_p2p_shared_objects build_etcd_wrapper)
endif()

# The backend supplies its Master sources after declaring the object target.
add_library(mooncake_store_p2p_master_objects OBJECT)
set_target_properties(mooncake_store_p2p_master_objects
                      PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(
  mooncake_store_p2p_master_objects PRIVATE ${MOONCAKE_STORE_INCLUDE_DIRS} ${STORE_COMMON_SRC})
target_link_libraries(
  mooncake_store_p2p_master_objects
  PRIVATE mooncake_store_p2p_shared_objects
          cachelib_memory_allocator
          glog::glog
          gflags::gflags
          JsonCpp::JsonCpp
          yaml-cpp
          mooncake_common
          asio_shared
          yalantinglibs::yalantinglibs)
if(STORE_USE_ETCD)
  add_dependencies(mooncake_store_p2p_master_objects build_etcd_wrapper)
endif()

# Shared client sources and the backend-specific client objects.
add_library(mooncake_store_p2p_client_objects OBJECT
            ${MOONCAKE_STORE_P2P_CLIENT_FOUNDATION_SOURCES})
set_target_properties(mooncake_store_p2p_client_objects
                      PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(
  mooncake_store_p2p_client_objects PRIVATE ${MOONCAKE_STORE_INCLUDE_DIRS} ${STORE_COMMON_SRC})
target_link_libraries(
  mooncake_store_p2p_client_objects
  PRIVATE mooncake_store_p2p_shared_objects
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
  add_dependencies(mooncake_store_p2p_client_objects build_etcd_wrapper)
endif()

if (USE_ASCEND_DIRECT)
    target_include_directories(mooncake_store_p2p_client_objects PRIVATE ${ASCEND_INCLUDE_DIR})
    set(ACL_RUNTIME_HEADER_PATH "${ASCEND_INCLUDE_DIR}/acl/acl_rt.h")
    if(EXISTS "${ACL_RUNTIME_HEADER_PATH}")
        file(READ "${ACL_RUNTIME_HEADER_PATH}" ACL_RUNTIME_CONTENT)
        if("${ACL_RUNTIME_CONTENT}" MATCHES "ACL_MEM_P2P_HUGE1G")
            message(STATUS "ACL_MEM_P2P_HUGE1G exist.")
            target_compile_definitions(mooncake_store_p2p_client_objects PRIVATE ASCEND_SUPPORT_FABRIC_MEM)
        else()
            message(STATUS "ACL_MEM_P2P_HUGE1G is not exist.")
        endif()
    else()
        message(WARNING "Acl runtime header file is not exist")
    endif()
endif()

# Reduced Store library linked by the Master executable.
add_library(
  mooncake_store_p2p_master STATIC
  $<TARGET_OBJECTS:mooncake_store_p2p_shared_objects>
  $<TARGET_OBJECTS:mooncake_store_p2p_master_objects>)
target_include_directories(mooncake_store_p2p_master
                           PUBLIC ${MOONCAKE_STORE_INCLUDE_DIRS})
target_link_libraries(
  mooncake_store_p2p_master
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
  mooncake_store_p2p
  $<TARGET_OBJECTS:mooncake_store_p2p_shared_objects>
  $<TARGET_OBJECTS:mooncake_store_p2p_master_objects>
  $<TARGET_OBJECTS:mooncake_store_p2p_client_objects>)
target_include_directories(mooncake_store_p2p PUBLIC ${MOONCAKE_STORE_INCLUDE_DIRS})
target_link_libraries(
  mooncake_store_p2p
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
  target_link_libraries(mooncake_store_p2p PRIVATE ascendcl)
endif()

if (BUILD_SHARED_LIBS)
  set_target_properties(mooncake_store_p2p PROPERTIES INSTALL_RPATH "$ORIGIN")
  install(TARGETS mooncake_store_p2p DESTINATION lib)
endif()

# Store Master executable.
add_executable(mooncake_p2p_master)
set(MASTER_EXTRA_INCS)
set(MASTER_EXTRA_LIBS)

if (STORE_USE_JEMALLOC)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(JEMALLOC REQUIRED jemalloc)
    list(APPEND MASTER_EXTRA_INCS ${JEMALLOC_INCLUDE_DIRS})
    list(APPEND MASTER_EXTRA_LIBS ${JEMALLOC_STATIC_LIBRARIES})
endif()

target_include_directories(mooncake_p2p_master PRIVATE ${MASTER_EXTRA_INCS})
target_link_libraries(
  mooncake_p2p_master
  PRIVATE mooncake_store_p2p_master
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
  add_dependencies(mooncake_p2p_master build_etcd_wrapper)
endif()
target_compile_options(mooncake_p2p_master PRIVATE -Os)
target_link_options(mooncake_p2p_master PRIVATE -Os -s)

# Store Client executable.
add_executable(mooncake_p2p_client)
target_link_libraries(
  mooncake_p2p_client
  PRIVATE mooncake_store_p2p transfer_engine asio_shared gflags::gflags
          yalantinglibs::yalantinglibs)
target_compile_options(mooncake_p2p_client PRIVATE -Os)
target_link_options(mooncake_p2p_client PRIVATE -Os -s)

# Export the header paths that the baseline dependency targets omit.
target_include_directories(mooncake_store_p2p_client_objects
                           PRIVATE ${MOONCAKE_STORE_SOURCE_ROOT}/../mooncake-common/include)
target_include_directories(mooncake_store_p2p
                           PUBLIC ${MOONCAKE_STORE_SOURCE_ROOT}/../mooncake-common/include)
target_include_directories(mooncake_p2p_master
                           PRIVATE ${MOONCAKE_STORE_SOURCE_ROOT}/../mooncake-common/include)

target_include_directories(mooncake_store_p2p_client_objects
                           PRIVATE ${MOONCAKE_STORE_SOURCE_ROOT}/../mooncake-transfer-engine/include)
target_include_directories(mooncake_store_p2p
                           PUBLIC ${MOONCAKE_STORE_SOURCE_ROOT}/../mooncake-transfer-engine/include)

install(TARGETS mooncake_p2p_master mooncake_p2p_client DESTINATION bin)

include("${CMAKE_CURRENT_SOURCE_DIR}/p2p/CMakeLists.txt")

set_target_properties(mooncake_p2p_master mooncake_p2p_client
                      PROPERTIES INSTALL_RPATH "$ORIGIN/../lib")
