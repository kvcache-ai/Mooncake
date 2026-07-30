add_subdirectory(cachelib_memory_allocator)

set(MOONCAKE_STORE_SDK_SOURCES
    allocator.cpp
    master_service.cpp
    master_snapshot_manager.cpp
    master_snapshot_repository.cpp
    client_service.cpp
    client_metric.cpp
    types.cpp
    master_client.cpp
    utils.cpp
    mmap_arena.cpp
    master_metric_manager.cpp
    storage_backend.cpp
    thread_pool.cpp
    etcd_helper.cpp
    segment.cpp
    transfer_task.cpp
    tenant_quota.cpp
    tenant_quota_policy_store.cpp
    rpc_service.cpp
    master_admin_service.cpp
    offset_allocator.cpp
    posix_file.cpp
    client_buffer.cpp
    aligned_client_buffer.cpp
    real_client.cpp
    registered_pinned_memory.cpp
    dummy_client.cpp
    uds_transport.cpp
    shm_helper.cpp
    http_metadata_server.cpp
    file_storage.cpp
    serialize/serializer.cpp
    storage/distributed/distributed_storage_backend.cpp
    device/accelerator_device.cpp
    device/accelerator_registry.cpp
    device/runtime_accelerator.cpp
    device/cuda_like_accelerator_device.cpp
    device/hip_accelerator_device.cpp
    device/ascend_accelerator_device.cpp
    device/sunrise_accelerator_device.cpp
    ha/leadership/leader_coordinator_factory.cpp
    ha/leadership/backends/etcd/etcd_leader_coordinator.cpp
    ha/common/redis/redis_connection.cpp
    ha/leadership/backends/redis/redis_leader_coordinator.cpp
    ha/leadership/master_service_supervisor.cpp
    ha/kv/etcd_ha_kv_backend.cpp
    ha/standby_controller.cpp
    ha/snapshot/catalog_backed_snapshot_provider.cpp
    ha/snapshot/master_snapshot_codec.cpp
    ha/snapshot/object/snapshot_object_store.cpp
    ha/snapshot/object/backends/local/local_file_snapshot_object_store.cpp
    ha/snapshot/object/backends/s3/s3_snapshot_object_store.cpp
    ha/snapshot/catalog/backends/embedded/embedded_snapshot_catalog_store.cpp
    ha/snapshot/catalog/backends/redis/redis_snapshot_catalog_store.cpp
    utils/type_util.cpp
    utils/file_util.cpp
    task_manager.cpp
    local_hot_cache.cpp
    ha/oplog/oplog_types.cpp
    ha/oplog/oplog_batch_codec.cpp
    ha/oplog/oplog_batch_storage.cpp
    ha/oplog/oplog_batch_standby_reader.cpp
    ha/oplog/oplog_batch_types.cpp
    ha/oplog/ordered_oplog_writer.cpp
    ha/oplog/oplog_test_failpoint.cpp
    ha/oplog/oplog_applier.cpp
    hot_standby_service.cpp
    metadata_store.cpp
    standby_state_machine.cpp
    ha_metric_manager.cpp
    k8s_lease_helper.cpp
    store_c.cpp
    memory_alloc.cpp
    ssd_register_client.cpp
    engram/engram_store.cpp)

# Master is deliberately compiled from a server-only source set. In particular,
# it contains no Client/SDK, Transfer Engine, registered-memory, or accelerator
# implementation translation units.
set(MOONCAKE_STORE_MASTER_SOURCES
    allocator.cpp
    master_service.cpp
    master_snapshot_manager.cpp
    master_snapshot_repository.cpp
    types.cpp
    utils.cpp
    mmap_arena.cpp
    master_metric_manager.cpp
    thread_pool.cpp
    etcd_helper.cpp
    segment.cpp
    tenant_quota.cpp
    tenant_quota_policy_store.cpp
    rpc_service.cpp
    master_admin_service.cpp
    offset_allocator.cpp
    posix_file.cpp
    http_metadata_server.cpp
    serialize/serializer.cpp
    ha/leadership/leader_coordinator_factory.cpp
    ha/leadership/backends/etcd/etcd_leader_coordinator.cpp
    ha/common/redis/redis_connection.cpp
    ha/leadership/backends/redis/redis_leader_coordinator.cpp
    ha/leadership/master_service_supervisor.cpp
    ha/kv/etcd_ha_kv_backend.cpp
    ha/standby_controller.cpp
    ha/snapshot/catalog_backed_snapshot_provider.cpp
    ha/snapshot/master_snapshot_codec.cpp
    ha/snapshot/object/snapshot_object_store.cpp
    ha/snapshot/object/backends/local/local_file_snapshot_object_store.cpp
    ha/snapshot/object/backends/s3/s3_snapshot_object_store.cpp
    ha/snapshot/catalog/backends/embedded/embedded_snapshot_catalog_store.cpp
    ha/snapshot/catalog/backends/redis/redis_snapshot_catalog_store.cpp
    utils/type_util.cpp
    utils/file_util.cpp
    task_manager.cpp
    ha/oplog/oplog_types.cpp
    ha/oplog/oplog_batch_codec.cpp
    ha/oplog/oplog_batch_storage.cpp
    ha/oplog/oplog_batch_standby_reader.cpp
    ha/oplog/oplog_batch_types.cpp
    ha/oplog/ordered_oplog_writer.cpp
    ha/oplog/oplog_test_failpoint.cpp
    ha/oplog/oplog_applier.cpp
    hot_standby_service.cpp
    metadata_store.cpp
    standby_state_machine.cpp
    ha_metric_manager.cpp
    k8s_lease_helper.cpp)

set(STORE_COMMON_OPTIONAL_LIBS "")
set(STORE_SDK_OPTIONAL_LIBS "")
set(SPDK_STATIC_LIBS "")
set(MASTER_EXTRA_INCS "")

# Find AWS SDK
find_package(AWSSDK QUIET COMPONENTS s3)
if(AWSSDK_FOUND)
  message(STATUS "AWS SDK found: ${AWSSDK_VERSION}")
  set(HAVE_AWS_SDK TRUE)
  # Add S3 related source files
  list(APPEND MOONCAKE_STORE_SDK_SOURCES utils/s3_helper.cpp)
  list(APPEND MOONCAKE_STORE_MASTER_SOURCES utils/s3_helper.cpp)
else()
  message(STATUS "AWS SDK not found, S3 functionality will be disabled")
  set(HAVE_AWS_SDK FALSE)
endif()

# If AWS SDK is found, add it to both Store boundaries.
if(HAVE_AWS_SDK)
  list(APPEND STORE_COMMON_OPTIONAL_LIBS ${AWSSDK_LINK_LIBRARIES})
  if(AWS_S3_LIB AND AWS_CORE_LIB)
    list(APPEND STORE_COMMON_OPTIONAL_LIBS ${AWS_S3_LIB} ${AWS_CORE_LIB})
  endif()
endif()

set(KV_EVENTS_ZMQ_FOUND FALSE)
if(ENABLE_KV_EVENTS)
  mooncake_enable_bundled_libzmq()
  list(APPEND STORE_COMMON_OPTIONAL_LIBS Mooncake::zmq)
  list(APPEND MOONCAKE_STORE_SDK_SOURCES kv_event/kv_event_publisher.cpp)
  list(APPEND MOONCAKE_STORE_MASTER_SOURCES kv_event/kv_event_publisher.cpp)
  set(KV_EVENTS_ZMQ_FOUND TRUE)
endif()

if(USE_3FS)
  add_subdirectory(hf3fs)
  list(APPEND MOONCAKE_STORE_SDK_SOURCES ${HF3FS_SOURCES})
  list(APPEND MOONCAKE_STORE_SDK_SOURCES storage/distributed/hf3fs_adapter.cpp)
  find_library(
    HF3FS_API_LIB hf3fs_api_shared
    PATHS /usr/lib
    NO_DEFAULT_PATH)
  if(NOT HF3FS_API_LIB)
    message(FATAL_ERROR "hf3fs_api_shared library not found in /usr/lib")
  endif()
  list(APPEND STORE_SDK_OPTIONAL_LIBS ${HF3FS_API_LIB})
endif()

# io_uring support remains available on Linux, now from the fixed source tree.
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  mooncake_enable_bundled_liburing()
  message(STATUS "io_uring: Enabled for mooncake_store (bundled liburing)")
  list(APPEND MOONCAKE_STORE_SDK_SOURCES uring_file.cpp)
  list(APPEND STORE_SDK_OPTIONAL_LIBS Mooncake::uring)
else()
  message(STATUS "io_uring: Disabled (unsupported platform)")
endif()

if(STORE_USE_REDIS)
  list(APPEND STORE_COMMON_OPTIONAL_LIBS Mooncake::hiredis)
endif()

if(STORE_USE_K8S_LEASE)
  list(APPEND MOONCAKE_STORE_SDK_SOURCES
       ha/leadership/backends/k8s/k8s_leader_coordinator.cpp)
  list(APPEND MOONCAKE_STORE_MASTER_SOURCES
       ha/leadership/backends/k8s/k8s_leader_coordinator.cpp)
  list(APPEND STORE_COMMON_OPTIONAL_LIBS ${K8S_LEASE_WRAPPER_LIB})
  set_source_files_properties(
    k8s_lease_helper.cpp
    PROPERTIES
      OBJECT_DEPENDS
      "${CMAKE_BINARY_DIR}/mooncake-common/k8s-lease/libk8s_lease_wrapper.h")
endif()

if(USE_NOF)
  add_subdirectory(spdk)
  list(APPEND MOONCAKE_STORE_SDK_SOURCES ${SPDK_WRAPPER_SOURCES})
  list(APPEND MOONCAKE_STORE_MASTER_SOURCES ${SPDK_WRAPPER_SOURCES})

  set(SPDK_INCLUDE_DIR
      "/usr/local/include"
      CACHE PATH "SPDK include directory")
  set(DPDK_INCLUDE_DIR
      "/usr/local/include"
      CACHE PATH "DPDK include directory")
  set(SPDK_LIB_DIR
      "/usr/local/lib"
      CACHE PATH "SPDK static library directory")
  set(DPDK_LIB_DIR
      "/usr/local/lib"
      CACHE PATH "DPDK static library directory")

  set(SPDK_STATIC_LIBS
      -Wl,--whole-archive
      ${SPDK_LIB_DIR}/libspdk_event.a
      ${SPDK_LIB_DIR}/libspdk_rdma.a
      ${SPDK_LIB_DIR}/libspdk_nvme.a
      ${SPDK_LIB_DIR}/libspdk_sock_posix.a
      -Wl,--no-whole-archive
      ${SPDK_LIB_DIR}/libspdk_vfio_user.a
      ${SPDK_LIB_DIR}/libspdk_dma.a
      ${SPDK_LIB_DIR}/libspdk_init.a
      ${SPDK_LIB_DIR}/libspdk_env_dpdk.a
      ${SPDK_LIB_DIR}/libspdk_log.a
      ${SPDK_LIB_DIR}/libspdk_util.a
      ${SPDK_LIB_DIR}/libspdk_blob.a
      ${SPDK_LIB_DIR}/libspdk_blobfs.a
      ${SPDK_LIB_DIR}/libspdk_bdev.a
      ${SPDK_LIB_DIR}/libspdk_conf.a
      ${SPDK_LIB_DIR}/libspdk_json.a
      ${SPDK_LIB_DIR}/libspdk_jsonrpc.a
      ${SPDK_LIB_DIR}/libspdk_sock.a
      ${SPDK_LIB_DIR}/libspdk_thread.a
      ${SPDK_LIB_DIR}/libspdk_trace.a
      ${SPDK_LIB_DIR}/libspdk_vmd.a
      ${SPDK_LIB_DIR}/libspdk_rpc.a
      ${DPDK_LIB_DIR}/librte_eal.a
      ${DPDK_LIB_DIR}/librte_mempool.a
      ${DPDK_LIB_DIR}/librte_ring.a
      ${DPDK_LIB_DIR}/librte_bus_pci.a
      ${DPDK_LIB_DIR}/librte_pci.a
      ${DPDK_LIB_DIR}/librte_timer.a
      ${DPDK_LIB_DIR}/librte_telemetry.a
      ${DPDK_LIB_DIR}/librte_kvargs.a
      ${DPDK_LIB_DIR}/librte_mbuf.a
      -Wl,--whole-archive
      ${DPDK_LIB_DIR}/librte_mempool_ring.a
      -Wl,--no-whole-archive
      ${DPDK_LIB_DIR}/librte_power.a
      ${DPDK_LIB_DIR}/librte_ethdev.a
      ${DPDK_LIB_DIR}/librte_net.a
      ${DPDK_LIB_DIR}/librte_vhost.a
      ${DPDK_LIB_DIR}/librte_cryptodev.a
      ${DPDK_LIB_DIR}/librte_hash.a
      ${DPDK_LIB_DIR}/librte_rcu.a
      pthread
      dl
      numa
      rt
      m
      uuid
      crypto
      aio
      z
      isal
      elf
      ibverbs
      rdmacm
      ssl
      bsd)
endif()

set(_store_public_include_dirs
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/../include>
    $<INSTALL_INTERFACE:include/mooncake-store>)
set(_store_internal_include_dirs
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/cachelib_memory_allocator
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/cachelib_memory_allocator/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/cachelib_memory_allocator/fake_include
    ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-common/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-common/etcd
    ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-common/k8s-lease
    ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-transfer-engine/include
    ${ETCD_WRAPPER_INCLUDE}
    ${K8S_LEASE_WRAPPER_INCLUDE})

function(_mooncake_configure_store_target target)
  set_target_properties(${target} PROPERTIES POSITION_INDEPENDENT_CODE ON)
  target_include_directories(${target} PRIVATE ${_store_public_include_dirs}
                                               ${_store_internal_include_dirs})
  target_link_libraries(
    ${target}
    PRIVATE cachelib_memory_allocator
            mooncake_common
            Mooncake::xxhash
            Mooncake::zstd
            Mooncake::jsoncpp
            Mooncake::yaml_cpp
            Mooncake::asio
            Mooncake::glog
            Mooncake::gflags
            yalantinglibs::yalantinglibs)
  if(KV_EVENTS_ZMQ_FOUND)
    target_compile_definitions(${target} PRIVATE MOONCAKE_ENABLE_KV_EVENTS=1)
  endif()
  if(MOONCAKE_ENABLE_OPLOG_PERF_METRICS)
    target_compile_definitions(${target}
                               PRIVATE MOONCAKE_ENABLE_OPLOG_PERF_METRICS)
  endif()
  if(HAVE_AWS_SDK)
    target_compile_definitions(${target} PRIVATE HAVE_AWS_SDK)
  endif()
  if(USE_NOF)
    target_include_directories(${target} PRIVATE ${SPDK_INCLUDE_DIR}
                                                 ${DPDK_INCLUDE_DIR})
  endif()
endfunction()

# Compile the server/core implementation once. The same PIC objects feed the
# standalone Master and the SDK DSO, avoiding the previous duplicate build of
# every server translation unit when both products are requested.
add_library(mooncake_store_server_objects OBJECT
            ${MOONCAKE_STORE_MASTER_SOURCES})
_mooncake_configure_store_target(mooncake_store_server_objects)
target_link_libraries(
  mooncake_store_server_objects
  PRIVATE ${ETCD_WRAPPER_LIB} ${STORE_COMMON_OPTIONAL_LIBS} ${SPDK_STATIC_LIBS})

# Keeping the Master implementation in a distinct archive makes accidental
# client/Transfer Engine coupling visible as a link failure.
add_library(mooncake_store_master_impl STATIC
            $<TARGET_OBJECTS:mooncake_store_server_objects>)
set_target_properties(mooncake_store_master_impl
                      PROPERTIES POSITION_INDEPENDENT_CODE ON)

include(CheckPIESupported)
check_pie_supported(LANGUAGES CXX)
add_executable(mooncake_master master.cpp)
target_include_directories(
  mooncake_master PRIVATE ${_store_public_include_dirs}
                          ${_store_internal_include_dirs} ${MASTER_EXTRA_INCS})

set(MASTER_EXTRA_LIBS "")
if(STORE_USE_JEMALLOC)
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(JEMALLOC REQUIRED jemalloc)
  target_include_directories(mooncake_master PRIVATE ${JEMALLOC_INCLUDE_DIRS})
  list(APPEND MASTER_EXTRA_LIBS ${JEMALLOC_STATIC_LIBRARIES})
endif()

target_link_libraries(
  mooncake_master
  PRIVATE mooncake_store_master_impl
          cachelib_memory_allocator
          mooncake_common
          Mooncake::xxhash
          Mooncake::zstd
          Mooncake::jsoncpp
          Mooncake::yaml_cpp
          Mooncake::asio
          Mooncake::glog
          Mooncake::gflags
          yalantinglibs::yalantinglibs
          Threads::Threads
          numa
          ${ETCD_WRAPPER_LIB}
          ${STORE_COMMON_OPTIONAL_LIBS}
          ${SPDK_STATIC_LIBS}
          ${MASTER_EXTRA_LIBS})
target_link_options(mooncake_master PRIVATE "LINKER:--as-needed")
set_target_properties(mooncake_master PROPERTIES POSITION_INDEPENDENT_CODE ON)

if(STORE_USE_ETCD)
  add_dependencies(mooncake_store_server_objects build_etcd_wrapper)
  add_dependencies(mooncake_store_master_impl build_etcd_wrapper)
  add_dependencies(mooncake_master build_etcd_wrapper)
endif()
if(STORE_USE_K8S_LEASE)
  add_dependencies(mooncake_store_server_objects build_k8s_lease_wrapper)
  add_dependencies(mooncake_store_master_impl build_k8s_lease_wrapper)
  add_dependencies(mooncake_master build_k8s_lease_wrapper)
endif()

# The SDK necessarily uses Transfer Engine. If it is absent, keep the Master
# available and do not create a fake "-ltransfer_engine" link item.
if(TARGET transfer_engine)
  set(MOONCAKE_STORE_CLIENT_SOURCES ${MOONCAKE_STORE_SDK_SOURCES})
  list(REMOVE_ITEM MOONCAKE_STORE_CLIENT_SOURCES
       ${MOONCAKE_STORE_MASTER_SOURCES})
  add_library(mooncake_store_sdk_objects OBJECT
              ${MOONCAKE_STORE_CLIENT_SOURCES})
  _mooncake_configure_store_target(mooncake_store_sdk_objects)
  # RDMA RPC support belongs to the client/SDK boundary.  Do not set this on the
  # directory: doing so also changes Master RPC types and pulls verbs into the
  # server process.
  target_compile_definitions(
    mooncake_store_sdk_objects PRIVATE YLT_ENABLE_IBV MOONCAKE_USE_ETCD
                                       CMAKE_INCLUDE)
  # Do not link the OBJECT target to transfer_engine: that creates an
  # unnecessary build-order edge and serializes the two largest compile groups.
  # The final DSO owns the real link edge below; all compile usage requirements
  # needed here are target-scoped explicitly.

  if(TARGET Mooncake::uring)
    target_compile_definitions(mooncake_store_sdk_objects PRIVATE USE_URING)
    target_link_libraries(mooncake_store_sdk_objects PRIVATE Mooncake::uring)
  endif()

  add_library(
    mooncake_store SHARED $<TARGET_OBJECTS:mooncake_store_server_objects>
                          $<TARGET_OBJECTS:mooncake_store_sdk_objects>)
  add_library(Mooncake::store ALIAS mooncake_store)
  if(WITH_STORE_C_SHARED)
    add_custom_target(mooncake_store_c DEPENDS mooncake_store)
  endif()
  set_target_properties(
    mooncake_store
    PROPERTIES OUTPUT_NAME mooncake_store
               POSITION_INDEPENDENT_CODE ON
               BUILD_RPATH "$ORIGIN"
               INSTALL_RPATH "$ORIGIN")
  target_include_directories(
    mooncake_store
    PUBLIC ${_store_public_include_dirs}
    INTERFACE
      ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-transfer-engine/include
      ${CMAKE_CURRENT_SOURCE_DIR}/../include/cachelib_memory_allocator
      ${CMAKE_CURRENT_SOURCE_DIR}/../include/cachelib_memory_allocator/include
      ${CMAKE_CURRENT_SOURCE_DIR}/../include/cachelib_memory_allocator/fake_include
      ${MOONCAKE_ASIO_INCLUDE_DIRS}
      ${MOONCAKE_JSONCPP_INCLUDE_DIRS})
  # Preserve source compatibility for existing in-tree C++ consumers while
  # store_c.h remains the stable cross-language ABI.
  target_compile_definitions(mooncake_store INTERFACE YLT_ENABLE_IBV)
  target_link_libraries(
    mooncake_store
    PRIVATE transfer_engine
            cachelib_memory_allocator
            mooncake_common
            Mooncake::xxhash
            Mooncake::zstd
            Mooncake::jsoncpp
            Mooncake::yaml_cpp
            Mooncake::asio
            Mooncake::glog
            Mooncake::gflags
            yalantinglibs::yalantinglibs
            ${ETCD_WRAPPER_LIB}
            ${STORE_COMMON_OPTIONAL_LIBS}
            ${STORE_SDK_OPTIONAL_LIBS}
            ${SPDK_STATIC_LIBS})
  # Existing C++ consumers (including the current pybind implementation) use
  # LOG() in code compiled outside this DSO. Keep that compile/link requirement
  # on the single project target instead of duplicating a dependency list in
  # every consumer. The bundled implementation absorbed by this DSO remains
  # local; consumers get the same fixed static target when they instantiate the
  # public C++ surface themselves.
  target_link_libraries(mooncake_store INTERFACE Mooncake::glog)
  if(UNIX AND NOT APPLE)
    target_link_options(mooncake_store PRIVATE "LINKER:--no-undefined")
  endif()
  if(UNIX AND NOT APPLE)
    # Keep inline C++ state and vendored symbols bound to this DSO. Without
    # symbolic binding, a consumer that includes Store headers can preempt an
    # inline variable and register its destructor in both ELF objects.
    target_link_options(
      mooncake_store
      PRIVATE
      "LINKER:-Bsymbolic"
      "LINKER:--exclude-libs=libmooncake_xxhash.a:libmooncake_zstd.a:libmooncake_jsoncpp.a:libmooncake_yaml_cpp.a:libmooncake_asio.a"
      "LINKER:--version-script=${CMAKE_CURRENT_SOURCE_DIR}/store_c.map")
    set_property(
      TARGET mooncake_store
      APPEND
      PROPERTY LINK_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/store_c.map")
  endif()

  find_package(CUDAToolkit QUIET)
  if(CUDAToolkit_FOUND)
    message(STATUS "mooncake_store: CUDAToolkit detected, enabling D2H staging")
    target_compile_definitions(mooncake_store_sdk_objects PRIVATE USE_CUDA)
    target_include_directories(mooncake_store_sdk_objects
                               PRIVATE ${CUDAToolkit_INCLUDE_DIRS})
    target_link_libraries(mooncake_store PRIVATE CUDA::cudart)
  else()
    find_package(hip QUIET)
    if(hip_FOUND)
      message(STATUS "mooncake_store: HIP detected, enabling D2H staging")
      target_compile_definitions(mooncake_store_sdk_objects PRIVATE USE_HIP)
      target_link_libraries(mooncake_store PRIVATE hip::host)
    endif()
  endif()

  if(USE_ASCEND
     OR USE_ASCEND_DIRECT
     OR USE_UBSHMEM)
    target_include_directories(mooncake_store_sdk_objects
                               PRIVATE $ENV{ASCEND_PATH}/include)
    target_link_libraries(mooncake_store PRIVATE ascendcl)
  endif()
  if(USE_SUNRISE)
    target_link_libraries(
      mooncake_store PRIVATE ${MC_TANGRT_ROOT}/lib/libtangrt_shared.so
                             ${MC_TANGRT_ROOT}/lib/libptml_shared.so dl)
  endif()

  if(STORE_USE_ETCD)
    add_dependencies(mooncake_store_sdk_objects build_etcd_wrapper)
    add_dependencies(mooncake_store build_etcd_wrapper)
  endif()
  if(STORE_USE_K8S_LEASE)
    add_dependencies(mooncake_store_sdk_objects build_k8s_lease_wrapper)
    add_dependencies(mooncake_store build_k8s_lease_wrapper)
  endif()

  add_executable(mooncake_client real_client_main.cpp)
  # real_client_main uses verbs helpers from the public C++ client headers. Keep
  # that system edge on the executable; language bindings link only Store.
  target_link_libraries(mooncake_client PRIVATE mooncake_store ibverbs)
  set_target_properties(mooncake_client PROPERTIES POSITION_INDEPENDENT_CODE ON)

  install(
    TARGETS mooncake_store
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION bin)
  install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/../include/store_c.h
          DESTINATION include/mooncake-store)
  install(TARGETS mooncake_client DESTINATION bin)
else()
  message(
    STATUS
      "Mooncake Store Client/SDK is disabled because Transfer Engine is not being built; mooncake_master remains available"
  )
endif()

string(TOUPPER "${CMAKE_BUILD_TYPE}" CMAKE_BUILD_TYPE_UPPER)
if(CMAKE_BUILD_TYPE_UPPER STREQUAL "RELEASE")
  target_compile_options(mooncake_master PRIVATE -Os)
  target_link_options(mooncake_master PRIVATE -Os -s)
  if(TARGET mooncake_client)
    target_compile_options(mooncake_client PRIVATE -Os)
    target_link_options(mooncake_client PRIVATE -Os -s)
  endif()
endif()

install(TARGETS mooncake_master DESTINATION bin)
