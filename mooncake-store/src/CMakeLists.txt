add_subdirectory(cachelib_memory_allocator)
add_subdirectory(local_ssd)

set(MOONCAKE_STORE_SHARED_SOURCES
    types.cpp
    utils.cpp
    mmap_arena.cpp
    thread_pool.cpp
    etcd_helper.cpp
    uds_transport.cpp
    storage/distributed/distributed_storage_backend.cpp
    storage/distributed/posix_fs_adapter.cpp
    utils/file_util.cpp)

set(MOONCAKE_STORE_MASTER_SOURCES
    allocation_strategy.cpp
    allocator.cpp
    master_service.cpp
    master_snapshot_manager.cpp
    master_snapshot_repository.cpp
    master_metric_manager.cpp
    segment.cpp
    tenant_quota.cpp
    tenant_quota_ledger.cpp
    tenant_quota_policy_store.cpp
    rpc_service.cpp
    master_admin_service.cpp
    offset_allocator.cpp
    http_metadata_server.cpp
    serialize/serializer.cpp
    storage/distributed/dfs_global_allocator.cpp
    ha/leadership/leader_coordinator_factory.cpp
    ha/leadership/backends/etcd/etcd_leader_coordinator.cpp
    ha/common/redis/redis_connection.cpp
    ha/leadership/backends/redis/redis_leader_coordinator.cpp
    ha/leadership/master_service_supervisor.cpp
    ha/kv/etcd_ha_kv_backend.cpp
    ha/standby_controller.cpp
    ha/standby_metadata_store.cpp
    ha/snapshot/catalog_backed_snapshot_provider.cpp
    ha/snapshot/batch_oplog/codec.cpp
    ha/snapshot/batch_oplog/metadata.cpp
    ha/snapshot/batch_oplog/writer.cpp
    ha/snapshot/master_snapshot_codec.cpp
    ha/snapshot/local_ssd_codec.cpp
    ha/snapshot/object/snapshot_object_store.cpp
    ha/snapshot/object/backends/local/local_file_snapshot_object_store.cpp
    ha/snapshot/object/backends/s3/s3_snapshot_object_store.cpp
    ha/snapshot/catalog/backends/embedded/embedded_snapshot_catalog_store.cpp
    ha/snapshot/catalog/backends/redis/redis_snapshot_catalog_store.cpp
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
    ha_metric_manager.cpp)

set(MOONCAKE_STORE_CLIENT_SOURCES
    client_buffer_allocation.cpp
    client_service.cpp
    client_metric.cpp
    master_client.cpp
    storage_backend.cpp
    nvme_kv_backend.cpp
    nvme_kv_connector.cpp
    nvme_kv_executor_io_uring.cpp
    nvme_kv_executor_ioctl.cpp
    nvme_kv_executor_util.cpp
    nvme_kv_key_codec.cpp
    nvme_kv_key_conflict_policy.cpp
    nvme_kv_object_layout.cpp
    transfer_task.cpp
    posix_file.cpp
    client_buffer.cpp
    aligned_client_buffer.cpp
    real_client.cpp
    registered_pinned_memory.cpp
    dummy_client.cpp
    shm_helper.cpp
    file_storage.cpp
    config/file_storage_config.cpp
    device/accelerator_device.cpp
    device/accelerator_registry.cpp
    device/runtime_accelerator.cpp
    device/cuda_like_accelerator_device.cpp
    device/cuda_ipc_buffer.cpp
    device/hip_accelerator_device.cpp
    device/ascend_accelerator_device.cpp
    device/sunrise_accelerator_device.cpp
    local_hot_cache.cpp
    store_c.cpp
    memory_alloc.cpp
    ssd_register_client.cpp
    engram/engram_store.cpp)

if(BUILD_UNIT_TESTS)
  list(APPEND MOONCAKE_STORE_CLIENT_SOURCES nvme_kv_executor_stub.cpp)
endif()

set(EXTRA_LIBS "")
set(SPDK_STATIC_LIBS "")

# Find AWS SDK
find_package(AWSSDK QUIET COMPONENTS s3)
if(AWSSDK_FOUND)
  message(STATUS "AWS SDK found: ${AWSSDK_VERSION}")
  set(HAVE_AWS_SDK TRUE)
  # Add S3 related source files
  list(APPEND MOONCAKE_STORE_MASTER_SOURCES utils/s3_helper.cpp)
else()
  message(STATUS "AWS SDK not found, S3 functionality will be disabled")
  set(HAVE_AWS_SDK FALSE)
endif()

# If AWS SDK is found, add it to EXTRA_LIBS
if(HAVE_AWS_SDK)
  list(APPEND EXTRA_LIBS ${AWSSDK_LINK_LIBRARIES})
  add_definitions(-DHAVE_AWS_SDK)
  if(AWS_S3_LIB AND AWS_CORE_LIB)
    list(APPEND EXTRA_LIBS ${AWS_S3_LIB} ${AWS_CORE_LIB})
    add_definitions(-DHAVE_AWS_SDK)
  endif()
endif()

set(KV_EVENTS_ZMQ_FOUND FALSE)
if(ENABLE_KV_EVENTS)
  list(APPEND MOONCAKE_STORE_MASTER_SOURCES kv_event/kv_event_publisher.cpp)
  set(KV_EVENTS_ZMQ_FOUND TRUE)
endif()

if(USE_3FS)
  add_subdirectory(hf3fs)
  list(APPEND MOONCAKE_STORE_SHARED_SOURCES ${HF3FS_SOURCES})
  list(APPEND MOONCAKE_STORE_SHARED_SOURCES
       storage/distributed/hf3fs_adapter.cpp)
  find_library(
    HF3FS_API_LIB hf3fs_api_shared
    PATHS /usr/lib
    NO_DEFAULT_PATH)
  if(NOT HF3FS_API_LIB)
    message(FATAL_ERROR "hf3fs_api_shared library not found in /usr/lib")
  endif()
  list(APPEND EXTRA_LIBS ${HF3FS_API_LIB})
endif()

set(MOONCAKE_HAVE_NVME_URING_CMD OFF)
if(TARGET Mooncake::liburing)
  list(APPEND MOONCAKE_STORE_CLIENT_SOURCES uring_file.cpp)

  include(CheckCXXSourceCompiles)
  get_target_property(MOONCAKE_LIBURING_INCLUDE_DIRS Mooncake::liburing
                      INTERFACE_INCLUDE_DIRECTORIES)
  set(CMAKE_REQUIRED_INCLUDES_SAVE ${CMAKE_REQUIRED_INCLUDES})
  set(CMAKE_REQUIRED_INCLUDES ${MOONCAKE_LIBURING_INCLUDE_DIRS})
  check_cxx_source_compiles(
    "
#include <liburing.h>
#include <linux/nvme_ioctl.h>
int main() {
    struct nvme_uring_cmd cmd{};
    io_uring_sqe sqe{};
    sqe.cmd_op = NVME_URING_CMD_IO;
    (void)sqe.cmd;
    io_uring_cqe cqe{};
    (void)cqe.big_cqe;
    unsigned flags = IORING_OP_URING_CMD | IORING_SETUP_CQE32;
    return static_cast<int>(flags) + cmd.opcode;
}
"
    MOONCAKE_NVME_URING_CMD_COMPILES)
  set(CMAKE_REQUIRED_INCLUDES ${CMAKE_REQUIRED_INCLUDES_SAVE})
  if(MOONCAKE_NVME_URING_CMD_COMPILES)
    set(MOONCAKE_HAVE_NVME_URING_CMD ON)
    message(STATUS "io_uring: NVMe uring command support enabled")
  else()
    message(
      STATUS
        "io_uring: NVMe uring command support unavailable, NVMe KV io_uring executor disabled"
    )
  endif()
endif()

list(APPEND MOONCAKE_STORE_MASTER_SOURCES k8s_lease_helper.cpp)
if(STORE_USE_K8S_LEASE)
  list(APPEND MOONCAKE_STORE_MASTER_SOURCES
       ha/leadership/backends/k8s/k8s_leader_coordinator.cpp)
  list(APPEND EXTRA_LIBS ${K8S_LEASE_WRAPPER_LIB})
  set_source_files_properties(
    k8s_lease_helper.cpp
    PROPERTIES
      OBJECT_DEPENDS
      "${CMAKE_BINARY_DIR}/mooncake-common/k8s-lease/libk8s_lease_wrapper.h")
endif()

if(USE_NOF)
  add_subdirectory(spdk)
  list(APPEND MOONCAKE_STORE_CLIENT_SOURCES ${SPDK_WRAPPER_SOURCES})
  # master_service.cpp references SpdkWrapper::GetInstance() under #ifdef
  # USE_NOF (ProbeNofSegment trampoline + SetNoFProbeFnForTesting); without
  # this, linking mooncake_store_master fails with "undefined reference to
  # SpdkWrapper::GetInstance()" whenever USE_NOF=ON.
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
      ${SPDK_LIB_DIR}/libisal.a
      ${SPDK_LIB_DIR}/libisal_crypto.a
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

# Resolve target-independent configuration before defining the Store targets.
set(MOONCAKE_STORE_INCLUDE_DIRS
    ${MOONCAKE_STORE_INCLUDE_ROOT}/mooncake-store/proto
    ${MOONCAKE_STORE_INCLUDE_ROOT} ${CMAKE_CURRENT_BINARY_DIR}/../include
    ${ETCD_WRAPPER_INCLUDE})

set(MASTER_EXTRA_INCS)
set(MASTER_EXTRA_LIBS)
if(STORE_USE_JEMALLOC)
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(JEMALLOC REQUIRED jemalloc)
  list(APPEND MASTER_EXTRA_INCS ${JEMALLOC_INCLUDE_DIRS})
  list(APPEND MASTER_EXTRA_LIBS ${JEMALLOC_STATIC_LIBRARIES})
endif()

include(CheckPIESupported)
check_pie_supported(LANGUAGES CXX)

string(TOUPPER "${CMAKE_BUILD_TYPE}" CMAKE_BUILD_TYPE_UPPER)

# Store Client sources call GPU runtime APIs directly. Detect the available
# runtime independently from the Transfer Engine feature flags so that each
# target below can declare its own compile and link requirements.
find_package(CUDAToolkit QUIET)
if(NOT CUDAToolkit_FOUND)
  find_package(hip QUIET)
endif()

# Sources used by both the Master and Client sides of Store.
add_library(mooncake_store_shared_objects OBJECT
            ${MOONCAKE_STORE_SHARED_SOURCES})
set_target_properties(mooncake_store_shared_objects
                      PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(mooncake_store_shared_objects
                           PRIVATE ${MOONCAKE_STORE_INCLUDE_DIRS})
target_link_libraries(
  mooncake_store_shared_objects
  PRIVATE cachelib_memory_allocator
          transfer_metadata_plugin
          glog::glog
          gflags::gflags
          mooncake_common
          numa
          asio_shared
          yalantinglibs::yalantinglibs)
if(STORE_USE_ETCD)
  add_dependencies(mooncake_store_shared_objects build_etcd_wrapper)
endif()
if(USE_UB)
  target_link_libraries(mooncake_store_shared_objects PRIVATE ub_allocator)
endif()

# Sources used by mooncake_master and the complete mooncake_store library.
add_library(mooncake_store_master_objects OBJECT
            ${MOONCAKE_STORE_MASTER_SOURCES})
set_target_properties(mooncake_store_master_objects
                      PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(mooncake_store_master_objects
                           PRIVATE ${MOONCAKE_STORE_INCLUDE_DIRS})
target_link_libraries(
  mooncake_store_master_objects
  PRIVATE mooncake_store_shared_objects
          mooncake_local_ssd
          transfer_metadata_plugin
          cachelib_memory_allocator
          glog::glog
          gflags::gflags
          JsonCpp::JsonCpp
          yaml-cpp
          mooncake_common
          Mooncake::zstd
          Mooncake::xxhash
          asio_shared
          yalantinglibs::yalantinglibs)
if(KV_EVENTS_ZMQ_FOUND)
  target_compile_definitions(mooncake_store_master_objects
                             PRIVATE MOONCAKE_ENABLE_KV_EVENTS=1)
  target_link_libraries(mooncake_store_master_objects PRIVATE Mooncake::libzmq)
endif()
if(MOONCAKE_ENABLE_OPLOG_PERF_METRICS)
  target_compile_definitions(mooncake_store_master_objects
                             PRIVATE MOONCAKE_ENABLE_OPLOG_PERF_METRICS)
endif()
if(STORE_USE_REDIS)
  target_link_libraries(mooncake_store_master_objects PRIVATE Mooncake::hiredis)
endif()
if(STORE_USE_ETCD)
  add_dependencies(mooncake_store_master_objects build_etcd_wrapper)
endif()
if(STORE_USE_K8S_LEASE)
  add_dependencies(mooncake_store_master_objects build_k8s_lease_wrapper)
endif()
if(USE_NOF)
  # master_service.cpp includes spdk/spdk_wrapper.h, which transitively pulls in
  # <spdk/env.h> / <spdk/nvme.h>; with the SPDK wrapper sources now in
  # mooncake_store_master_objects, the master_objects translation unit needs to
  # find SPDK/DPDK headers at compile time.
  target_include_directories(mooncake_store_master_objects
                             PRIVATE ${SPDK_INCLUDE_DIR} ${DPDK_INCLUDE_DIR})
endif()

# Sources used only by Store clients and the complete mooncake_store library.
add_library(mooncake_store_client_objects OBJECT
            ${MOONCAKE_STORE_CLIENT_SOURCES})
set_target_properties(mooncake_store_client_objects
                      PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(mooncake_store_client_objects
                           PRIVATE ${MOONCAKE_STORE_INCLUDE_DIRS})
target_link_libraries(
  mooncake_store_client_objects
  PRIVATE mooncake_store_shared_objects
          transfer_engine
          cachelib_memory_allocator
          glog::glog
          gflags::gflags
          JsonCpp::JsonCpp
          yaml-cpp
          Mooncake::xxhash
          mooncake_common
          numa
          asio_shared
          yalantinglibs::yalantinglibs)
if(BUILD_UNIT_TESTS)
  target_compile_definitions(mooncake_store_client_objects
                             PRIVATE MOONCAKE_ENABLE_NVME_KV_TEST_STUB=1)
endif()
if(BUILD_UNIT_TESTS AND USE_NOF)
  # Expose the file-scope pipeline_io_cb test hook in the client-side static
  # archive only when both BUILD_UNIT_TESTS and USE_NOF are on. Keeps production
  # binaries free of the test-only symbol while letting
  # nof_segment_pipeline_test link against it.
  target_compile_definitions(mooncake_store_client_objects
                             PRIVATE MOONCAKE_TEST_PIPELINE_IO=1)
endif()
if(USE_NOF)
  target_include_directories(mooncake_store_client_objects
                             PRIVATE ${SPDK_INCLUDE_DIR} ${DPDK_INCLUDE_DIR})
endif()
if(TARGET Mooncake::liburing)
  target_compile_definitions(mooncake_store_client_objects PRIVATE USE_URING)
  target_link_libraries(mooncake_store_client_objects
                        PRIVATE Mooncake::liburing)
  if(MOONCAKE_HAVE_NVME_URING_CMD)
    target_compile_definitions(mooncake_store_client_objects
                               PRIVATE MOONCAKE_HAVE_NVME_URING_CMD=1)
  endif()
endif()
if(STORE_USE_ETCD)
  add_dependencies(mooncake_store_client_objects build_etcd_wrapper)
endif()
if(CUDAToolkit_FOUND)
  message(STATUS "mooncake_store: CUDAToolkit detected, enabling D2H staging")
  target_compile_definitions(mooncake_store_client_objects PRIVATE USE_CUDA)
  target_include_directories(mooncake_store_client_objects
                             PRIVATE ${CUDAToolkit_INCLUDE_DIRS})
elseif(hip_FOUND)
  message(STATUS "mooncake_store: HIP detected, enabling D2H staging")
  target_compile_definitions(mooncake_store_client_objects PRIVATE USE_HIP)
endif()
if(USE_ASCEND
   OR USE_ASCEND_DIRECT
   OR USE_UBSHMEM)
  target_include_directories(mooncake_store_client_objects
                             PRIVATE $ENV{ASCEND_PATH}/include)
endif()

# Reduced Store library linked by mooncake_master.
add_library(
  mooncake_store_master STATIC $<TARGET_OBJECTS:mooncake_store_shared_objects>
                               $<TARGET_OBJECTS:mooncake_store_master_objects>)
target_include_directories(mooncake_store_master
                           PUBLIC ${MOONCAKE_STORE_INCLUDE_DIRS})
target_link_libraries(
  mooncake_store_master
  PRIVATE mooncake_local_ssd
          cachelib_memory_allocator
          ${ETCD_WRAPPER_LIB}
          glog::glog
          ${EXTRA_LIBS}
          mooncake_common
          Mooncake::zstd
          asio_shared
          yalantinglibs::yalantinglibs
          transfer_metadata_plugin
          gflags::gflags
          JsonCpp::JsonCpp
          yaml-cpp
          Mooncake::xxhash
          numa)
if(KV_EVENTS_ZMQ_FOUND)
  target_link_libraries(mooncake_store_master PRIVATE Mooncake::libzmq)
endif()
if(STORE_USE_REDIS)
  target_link_libraries(mooncake_store_master PRIVATE Mooncake::hiredis)
endif()
if(USE_UB)
  target_link_libraries(mooncake_store_master PRIVATE ub_allocator)
endif()
if(USE_NOF)
  # The master OBJECT lib now contains SPDK wrapper sources (master_service.cpp
  # calls SpdkWrapper::GetInstance() under #ifdef USE_NOF); the master static
  # archive therefore needs to resolve SPDK symbols at link time.
  target_link_libraries(mooncake_store_master PRIVATE ${SPDK_STATIC_LIBS})
endif()

# Complete public Store library.
add_library(
  mooncake_store
  $<TARGET_OBJECTS:mooncake_store_shared_objects>
  $<TARGET_OBJECTS:mooncake_store_master_objects>
  $<TARGET_OBJECTS:mooncake_store_client_objects>)
target_include_directories(mooncake_store PUBLIC ${MOONCAKE_STORE_INCLUDE_DIRS})
target_link_libraries(
  mooncake_store
  PUBLIC mooncake_local_ssd
         cachelib_memory_allocator
         ${ETCD_WRAPPER_LIB}
         glog::glog
         ${EXTRA_LIBS}
         ${SPDK_STATIC_LIBS}
         mooncake_common
         Mooncake::zstd
         asio_shared
         transfer_engine
         yalantinglibs::yalantinglibs
  PRIVATE gflags::gflags JsonCpp::JsonCpp yaml-cpp Mooncake::xxhash numa)
if(KV_EVENTS_ZMQ_FOUND)
  target_link_libraries(mooncake_store PRIVATE Mooncake::libzmq)
endif()
if(STORE_USE_REDIS)
  target_link_libraries(mooncake_store PRIVATE Mooncake::hiredis)
endif()
if(TARGET Mooncake::liburing)
  target_compile_definitions(mooncake_store PUBLIC USE_URING)
  target_link_libraries(mooncake_store PUBLIC Mooncake::liburing)
endif()
if(CUDAToolkit_FOUND)
  target_link_libraries(mooncake_store PRIVATE CUDA::cudart)
elseif(hip_FOUND)
  target_link_libraries(mooncake_store PRIVATE hip::host)
endif()
if(USE_ASCEND
   OR USE_ASCEND_DIRECT
   OR USE_UBSHMEM)
  target_link_libraries(mooncake_store PRIVATE ascendcl)
endif()
if(USE_SUNRISE)
  target_link_libraries(
    mooncake_store PRIVATE ${MC_TANGRT_ROOT}/lib/libtangrt_shared.so
                           ${MC_TANGRT_ROOT}/lib/libptml_shared.so dl)
endif()
if(BUILD_SHARED_LIBS)
  install(TARGETS mooncake_store DESTINATION lib)
endif()

# Store Master executable.
add_executable(mooncake_master master.cpp)
set_target_properties(mooncake_master PROPERTIES POSITION_INDEPENDENT_CODE ON)
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
if(USE_SUNRISE)
  target_link_directories(mooncake_master PRIVATE ${MC_TANGRT_ROOT}/lib)
endif()
if(USE_NOF)
  # The SPDK wrapper symbols that mooncake_store_master references (e.g.
  # spdk_nvme_ns_cmd_read/write, spdk_nvme_transport_id_parse) are NOT pulled in
  # transitively through mooncake_store_master because the static archive only
  # carries the object files.  The master executable must add the SPDK static
  # libs explicitly so the final link resolves the undefined references.
  target_include_directories(mooncake_master PRIVATE ${SPDK_INCLUDE_DIR}
                                                     ${DPDK_INCLUDE_DIR})
  target_link_libraries(mooncake_master PRIVATE ${SPDK_STATIC_LIBS})
endif()

if(STORE_USE_ETCD)
  add_dependencies(mooncake_master build_etcd_wrapper)
endif()
if(CMAKE_BUILD_TYPE_UPPER STREQUAL "RELEASE")
  target_compile_options(mooncake_master PRIVATE -Os)
  target_link_options(mooncake_master PRIVATE -Os -s)
endif()
install(TARGETS mooncake_master DESTINATION bin)

# Store Client executable.
add_executable(mooncake_client real_client_main.cpp)
set_target_properties(mooncake_client PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_link_libraries(
  mooncake_client PRIVATE mooncake_store transfer_engine asio_shared
                          gflags::gflags yalantinglibs::yalantinglibs)
if(CUDAToolkit_FOUND)
  target_compile_definitions(mooncake_client PRIVATE USE_CUDA)
  target_include_directories(mooncake_client
                             PRIVATE ${CUDAToolkit_INCLUDE_DIRS})
  target_link_libraries(mooncake_client PRIVATE CUDA::cudart)
elseif(hip_FOUND)
  target_compile_definitions(mooncake_client PRIVATE USE_HIP)
  target_link_libraries(mooncake_client PRIVATE hip::host)
endif()
if(USE_ASCEND
   OR USE_ASCEND_DIRECT
   OR USE_UBSHMEM)
  target_include_directories(mooncake_client PRIVATE $ENV{ASCEND_PATH}/include)
  target_link_libraries(mooncake_client PRIVATE ascendcl)
endif()
if(USE_SUNRISE)
  target_link_libraries(
    mooncake_client PRIVATE ${MC_TANGRT_ROOT}/lib/libtangrt_shared.so
                            ${MC_TANGRT_ROOT}/lib/libptml_shared.so dl)
endif()
if(CMAKE_BUILD_TYPE_UPPER STREQUAL "RELEASE")
  target_compile_options(mooncake_client PRIVATE -Os)
  target_link_options(mooncake_client PRIVATE -Os -s)
endif()
install(TARGETS mooncake_client DESTINATION bin)

# ---------------------------------------------------------------------------
# Optional: a self-contained libmooncake_store.so exporting only the store_c.h C
# ABI, for consumers that dlopen the store (e.g. the Rust crate's `dlopen`
# feature). Whole-archives the static mooncake_store + asio_static (objects are
# -fPIC, see mooncake-common/common.cmake) and localizes everything else via the
# version script; only system/toolkit libs (and the Go etcd/k8s backends when
# enabled) stay DT_NEEDED.
# ---------------------------------------------------------------------------
if(WITH_STORE_C_SHARED)
  # ELF/GNU-ld features (--whole-archive, --as-needed, version script, $ORIGIN);
  # supported by GNU ld, gold, and lld. Linux only.
  if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "WITH_STORE_C_SHARED currently supports Linux/ELF only "
                        "(got CMAKE_SYSTEM_NAME=${CMAKE_SYSTEM_NAME}).")
  endif()

  # We whole-archive mooncake_store and reuse its name, so require it to be
  # static (add_library(mooncake_store) follows BUILD_SHARED_LIBS, which some
  # backends force ON).
  get_target_property(_mooncake_store_type mooncake_store TYPE)
  if(NOT _mooncake_store_type STREQUAL "STATIC_LIBRARY")
    message(
      FATAL_ERROR
        "WITH_STORE_C_SHARED requires mooncake_store to be a STATIC_LIBRARY "
        "(got ${_mooncake_store_type}); configure with BUILD_SHARED_LIBS=OFF.")
  endif()

  add_library(mooncake_store_c SHARED store_c_shared.cpp)
  set_target_properties(
    mooncake_store_c
    PROPERTIES OUTPUT_NAME mooncake_store
               # Separate dir so this .so does not shadow the static
               # libmooncake_store.a on the Rust `link` backend's -L path.
               LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/shared"
               # $ORIGIN so the installed .so finds co-installed deps (CMake
               # strips the build RUNPATH on install).
               INSTALL_RPATH "$ORIGIN"
               # Relink when the version script (a bare link flag) changes.
               LINK_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/store_c.map")
  add_dependencies(mooncake_store_c mooncake_store asio_static)
  # Whole-archive the static lib + asio_static via file paths (no interface-dep
  # propagation); re-list the mooncake_store target for its transitive deps.
  # --as-needed drops the now-redundant transitive libasio.so.
  target_link_libraries(
    mooncake_store_c
    PRIVATE -Wl,--whole-archive "$<TARGET_FILE:mooncake_store>"
            "$<TARGET_FILE:asio_static>" -Wl,--no-whole-archive -Wl,--as-needed
            mooncake_store)
  # Export only the C ABI; the version script localizes every other symbol
  # pulled from the static archives. Strip debug info in Release (it dominates
  # the .so size).
  target_link_options(
    mooncake_store_c PRIVATE
    "-Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/store_c.map"
    "$<$<CONFIG:Release>:-Wl,-s>")
  install(TARGETS mooncake_store_c DESTINATION lib)
endif()
