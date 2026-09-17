add_subdirectory(../common/cachelib_memory_allocator cachelib_memory_allocator)
add_subdirectory(../common/local_ssd local_ssd)

set(MOONCAKE_STORE_SHARED_SOURCES
    ../common/types.cpp
    ../common/network.cpp
    ../common/thread_pool.cpp
    ../common/etcd_helper.cpp
    ../common/uds_transport.cpp
    ../common/config/distributed_storage_config.cpp
    ../common/config/rpc_protocol_config.cpp
    ../common/storage/distributed/distributed_storage_backend.cpp
    ../common/storage/distributed/posix_fs_adapter.cpp
    ../common/file_util.cpp)

set(MOONCAKE_STORE_MASTER_SOURCES
    ../master/allocation_strategy.cpp
    ../master/allocator.cpp
    ../master/config/ha_cluster_namespace_config.cpp
    ../master/config/local_file_snapshot_config.cpp
    ../master/config/redis_connection_config.cpp
    ../master/config/s3_client_config.cpp
    ../master/segment_allocator_registration.cpp
    ../master/client_offboarding.cpp
    ../master/master_service.cpp
    ../master/master_snapshot_manager.cpp
    ../master/master_snapshot_repository.cpp
    ../master/master_metric_manager.cpp
    ../master/segment/region_driver.cpp
    ../master/placement/index.cpp
    ../master/placement/replica_allocator.cpp
    ../master/segment/catalog.cpp
    ../master/segment/pool.cpp
    ../master/segment/pool_mount.cpp
    ../master/segment/snapshot.cpp
    ../master/ha/snapshot/allocator_snapshot_codec.cpp
    ../master/ha/snapshot/store_resource_snapshot_codec.cpp
    ../master/segment/pool_write_access.cpp
    ../master/segment/pool_read_access.cpp
    ../master/segment.cpp
    ../master/tenant_quota.cpp
    ../master/tenant_quota_ledger.cpp
    ../master/tenant_quota_policy_store.cpp
    ../master/rpc_service.cpp
    ../master/master_admin_service.cpp
    ../master/offset_allocator.cpp
    ../master/http_metadata_server.cpp
    ../master/serialize/serializer.cpp
    ../master/storage/distributed/dfs_global_allocator.cpp
    ../master/ha/leadership/leader_coordinator_factory.cpp
    ../master/ha/leadership/backends/etcd/etcd_leader_coordinator.cpp
    ../master/ha/common/redis/redis_connection.cpp
    ../master/ha/leadership/backends/redis/redis_leader_coordinator.cpp
    ../master/ha/leadership/master_service_supervisor.cpp
    ../master/ha/kv/etcd_ha_kv_backend.cpp
    ../master/ha/standby_controller.cpp
    ../master/ha/standby_metadata_store.cpp
    ../master/ha/snapshot/catalog_backed_snapshot_provider.cpp
    ../master/ha/snapshot/batch_oplog/codec.cpp
    ../master/ha/snapshot/batch_oplog/metadata.cpp
    ../master/ha/snapshot/batch_oplog/batch_oplog_snapshot_provider.cpp
    ../master/ha/snapshot/batch_oplog/writer.cpp
    ../master/ha/snapshot/batch_oplog/batch_oplog_snapshot_publisher.cpp
    ../master/ha/snapshot/batch_oplog/batch_oplog_snapshot_coordinator.cpp
    ../master/ha/snapshot/batch_oplog/batch_oplog_snapshot_gc.cpp
    ../master/ha/snapshot/batch_oplog/batch_oplog_pruning_coordinator.cpp
    ../master/ha/snapshot/snapshot_maintenance_lease.cpp
    ../master/ha/snapshot/master_snapshot_codec.cpp
    ../master/ha/snapshot/local_ssd_codec.cpp
    ../master/ha/snapshot/object/snapshot_object_store.cpp
    ../master/ha/snapshot/object/backends/local/local_file_snapshot_object_store.cpp
    ../master/ha/snapshot/object/backends/s3/s3_snapshot_object_store.cpp
    ../master/ha/snapshot/catalog/backends/embedded/embedded_snapshot_catalog_store.cpp
    ../master/ha/snapshot/catalog/backends/redis/redis_snapshot_catalog_store.cpp
    ../master/task_manager.cpp
    ../master/ha/oplog/oplog_types.cpp
    ../master/ha/oplog/oplog_batch_codec.cpp
    ../master/ha/oplog/oplog_batch_storage.cpp
    ../master/ha/oplog/oplog_batch_standby_reader.cpp
    ../master/ha/oplog/oplog_batch_types.cpp
    ../master/ha/oplog/ordered_oplog_writer.cpp
    ../master/ha/oplog/oplog_test_failpoint.cpp
    ../master/ha/oplog/oplog_applier.cpp
    ../master/hot_standby_service.cpp
    ../master/metadata_store.cpp
    ../master/standby_state_machine.cpp
    ../master/ha_metric_manager.cpp)

set(MOONCAKE_STORE_CLIENT_SOURCES
    ../client/common/client_buffer_allocation.cpp
    ../client/mmap_arena.cpp
    ../client/config/hugepage_config.cpp
    ../client/config/mmap_arena_config.cpp
    ../client/common/mmap_buffer.cpp
    ../client/client_service.cpp
    ../client/client_metric.cpp
    ../client/master_client.cpp
    ../client/storage_backend.cpp
    ../client/nvme_kv/backend.cpp
    ../client/nvme_kv/connector.cpp
    ../client/nvme_kv/config/connector_config.cpp
    ../client/nvme_kv/config/io_concurrency_config.cpp
    ../client/nvme_kv/config/u32_parser.cpp
    ../client/nvme_kv/executor_io_uring.cpp
    ../client/nvme_kv/executor_ioctl.cpp
    ../client/nvme_kv/executor_util.cpp
    ../client/nvme_kv/key_codec.cpp
    ../client/nvme_kv/key_conflict_policy.cpp
    ../client/nvme_kv/object_layout.cpp
    ../client/transfer_task.cpp
    ../client/config/fileread_worker_pool_config.cpp
    ../client/posix_file.cpp
    ../client/client_buffer.cpp
    ../client/aligned_client_buffer.cpp
    ../client/real_client.cpp
    ../client/config/registered_pinned_memory_config.cpp
    ../client/registered_pinned_memory.cpp
    ../client/dummy_client.cpp
    ../client/shm_helper.cpp
    ../client/file_storage.cpp
    ../client/config/client_host_identity_config.cpp
    ../client/config/client_metric_config.cpp
    ../client/config/file_storage_config.cpp
    ../client/config/bucket_backend_config.cpp
    ../client/config/file_per_key_config.cpp
    ../client/config/client_auto_port_config.cpp
    ../client/config/client_auto_discovery_config.cpp
    ../client/config/cxl_segment_config.cpp
    ../client/config/client_numa_config.cpp
    ../client/config/local_hot_cache_config.cpp
    ../client/config/nof_register_config.cpp
    ../client/config/offset_allocator_backend_config.cpp
    ../client/config/replica_selection_config.cpp
    ../client/config/rpc_timeout_config.cpp
    ../common/config/spdk_controller_config.cpp
    ../client/config/transfer_submitter_config.cpp
    ../client/device/accelerator_device.cpp
    ../client/device/accelerator_registry.cpp
    ../client/device/runtime_accelerator.cpp
    ../client/device/cuda_like_accelerator_device.cpp
    ../client/device/cuda_ipc_buffer.cpp
    ../client/device/hip_accelerator_device.cpp
    ../client/device/ascend_accelerator_device.cpp
    ../client/device/sunrise_accelerator_device.cpp
    ../client/local_hot_cache.cpp
    ../client/store_c.cpp
    ../client/memory_alloc.cpp
    ../client/ssd_register_client.cpp
    ../client/engram/engram_store.cpp)

if(BUILD_UNIT_TESTS)
  list(APPEND MOONCAKE_STORE_CLIENT_SOURCES ../client/nvme_kv/executor_stub.cpp)
endif()
set(EXTRA_LIBS "")
set(SPDK_STATIC_LIBS "")

# RISC-V toolchains may implement 16-byte atomic operations in libatomic rather
# than emitting native instructions. Boost.Lockfree uses these operations in the
# Store master, so detect and propagate the runtime library in RISC-V mode.
if(USE_RISCV)
  include(CheckCXXSourceCompiles)
  check_cxx_source_compiles(
    "
alignas(16) volatile unsigned __int128 value = 0;
int main(int argc, char**) {
  __atomic_store_n(&value, static_cast<unsigned __int128>(argc),
                   __ATOMIC_SEQ_CST);
  return static_cast<int>(__atomic_load_n(&value, __ATOMIC_SEQ_CST));
}
"
    MOONCAKE_HAS_NATIVE_16BYTE_ATOMICS)
  if(NOT MOONCAKE_HAS_NATIVE_16BYTE_ATOMICS)
    set(CMAKE_REQUIRED_LIBRARIES_SAVE ${CMAKE_REQUIRED_LIBRARIES})
    set(CMAKE_REQUIRED_LIBRARIES atomic)
    check_cxx_source_compiles(
      "
alignas(16) volatile unsigned __int128 value = 0;
int main(int argc, char**) {
  __atomic_store_n(&value, static_cast<unsigned __int128>(argc),
                   __ATOMIC_SEQ_CST);
  return static_cast<int>(__atomic_load_n(&value, __ATOMIC_SEQ_CST));
}
"
      MOONCAKE_HAS_LIBATOMIC_16BYTE_ATOMICS)
    set(CMAKE_REQUIRED_LIBRARIES ${CMAKE_REQUIRED_LIBRARIES_SAVE})
    if(MOONCAKE_HAS_LIBATOMIC_16BYTE_ATOMICS)
      list(APPEND EXTRA_LIBS atomic)
      message(STATUS "16-byte atomics: using libatomic")
    else()
      message(FATAL_ERROR "16-byte atomic operations are not supported")
    endif()
  endif()
endif()

# Alibaba Cloud OSS adapter (REST API with OSS V4 signing).
find_package(CURL QUIET)
find_package(OpenSSL QUIET)
if(CURL_FOUND AND OpenSSL_FOUND)
  message(STATUS "Alibaba Cloud OSS adapter: Enabled")
  set(MOONCAKE_OSS_ADAPTER_ENABLED
      TRUE
      CACHE INTERNAL "Whether the Alibaba Cloud OSS adapter is built")
  list(APPEND MOONCAKE_STORE_CLIENT_SOURCES
       ../client/config/oss_adapter_config.cpp
       ../client/storage/distributed/oss_adapter.cpp)
  list(APPEND EXTRA_LIBS CURL::libcurl OpenSSL::Crypto)
  add_definitions(-DHAVE_OSS_ADAPTER)
else()
  message(
    STATUS "Alibaba Cloud OSS adapter: Disabled (requires CURL and OpenSSL)")
  set(MOONCAKE_OSS_ADAPTER_ENABLED
      FALSE
      CACHE INTERNAL "Whether the Alibaba Cloud OSS adapter is built")
endif()

# Find AWS SDK
find_package(AWSSDK QUIET COMPONENTS s3)
if(AWSSDK_FOUND)
  message(STATUS "AWS SDK found: ${AWSSDK_VERSION}")
  set(HAVE_AWS_SDK TRUE)
  # Add S3 related source files
  list(APPEND MOONCAKE_STORE_MASTER_SOURCES ../master/common/s3_helper.cpp)
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
  list(APPEND MOONCAKE_STORE_MASTER_SOURCES
       ../master/kv_event/kv_event_publisher.cpp)
  set(KV_EVENTS_ZMQ_FOUND TRUE)
endif()

if(USE_3FS)
  add_subdirectory(../common/hf3fs hf3fs)
  list(APPEND MOONCAKE_STORE_SHARED_SOURCES ${HF3FS_SOURCES})
  list(APPEND MOONCAKE_STORE_SHARED_SOURCES
       ../common/storage/distributed/hf3fs_adapter.cpp)
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
  list(APPEND MOONCAKE_STORE_CLIENT_SOURCES ../client/uring_file.cpp)

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

list(APPEND MOONCAKE_STORE_MASTER_SOURCES ../master/k8s_lease_helper.cpp)
if(STORE_USE_K8S_LEASE)
  list(APPEND MOONCAKE_STORE_MASTER_SOURCES
       ../master/ha/leadership/backends/k8s/k8s_leader_coordinator.cpp)
  list(APPEND EXTRA_LIBS ${K8S_LEASE_WRAPPER_LIB})
  set_source_files_properties(
    ../master/k8s_lease_helper.cpp
    PROPERTIES
      OBJECT_DEPENDS
      "${CMAKE_BINARY_DIR}/mooncake-common/k8s-lease/libk8s_lease_wrapper.h")
endif()

if(USE_NOF)
  add_subdirectory(../common/spdk spdk)
  list(APPEND MOONCAKE_STORE_CLIENT_SOURCES ${SPDK_WRAPPER_SOURCES})

  # The master's NoF heartbeat probe also goes through SpdkWrapper.
  list(APPEND MOONCAKE_STORE_MASTER_SOURCES ${SPDK_WRAPPER_SOURCES}
       ../common/config/spdk_controller_config.cpp)

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

  set(SPDK_ISA_L_LIB_DIR
      "${CMAKE_SOURCE_DIR}/extern/spdk/isa-l/.libs"
      CACHE PATH "isa-l static library directory")
  if(NOT EXISTS "${SPDK_ISA_L_LIB_DIR}/libisal.a")
    message(
      FATAL_ERROR
        "isa-l not found at ${SPDK_ISA_L_LIB_DIR}/libisal.a. Run "
        "`sudo bash dependencies.sh --with-spdk`, or set -DSPDK_ISA_L_LIB_DIR.")
  endif()

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
      -L${SPDK_ISA_L_LIB_DIR}
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

# Store Client accelerator staging follows the explicitly selected build
# backend. In particular, non-CUDA wheels are built in a CUDA toolchain image,
# so SDK presence must not enable CUDA support by itself.
if(USE_CUDA)
  find_package(CUDAToolkit REQUIRED)
elseif(USE_HIP)
  find_package(hip REQUIRED)
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
target_include_directories(
  mooncake_store_master_objects
  PRIVATE ${MOONCAKE_STORE_INCLUDE_DIRS} ${CMAKE_CURRENT_SOURCE_DIR}/../common
          ${CMAKE_CURRENT_SOURCE_DIR}/../master)
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

# Sources used only by Store clients and the complete mooncake_store library.
add_library(mooncake_store_client_objects OBJECT
            ${MOONCAKE_STORE_CLIENT_SOURCES})
set_target_properties(mooncake_store_client_objects
                      PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(
  mooncake_store_client_objects
  PRIVATE ${MOONCAKE_STORE_INCLUDE_DIRS} ${CMAKE_CURRENT_SOURCE_DIR}/../common
          ${CMAKE_CURRENT_SOURCE_DIR}/../client)
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
if(USE_CUDA)
  message(STATUS "mooncake_store: CUDA enabled, enabling D2H staging")
  target_compile_definitions(mooncake_store_client_objects PRIVATE USE_CUDA)
  target_include_directories(mooncake_store_client_objects
                             PRIVATE ${CUDAToolkit_INCLUDE_DIRS})
elseif(USE_HIP)
  message(STATUS "mooncake_store: HIP enabled, enabling D2H staging")
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
if(USE_NOF)
  target_include_directories(mooncake_store_master_objects
                             PRIVATE ${SPDK_INCLUDE_DIR} ${DPDK_INCLUDE_DIR})
endif()
target_link_libraries(
  mooncake_store_master
  PRIVATE ${SPDK_STATIC_LIBS}
          mooncake_local_ssd
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
if(USE_CUDA)
  target_link_libraries(mooncake_store PRIVATE CUDA::cudart)
elseif(USE_HIP)
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
add_executable(mooncake_master ../master/master.cpp)
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

if(STORE_USE_ETCD)
  add_dependencies(mooncake_master build_etcd_wrapper)
endif()
if(CMAKE_BUILD_TYPE_UPPER STREQUAL "RELEASE")
  target_compile_options(mooncake_master PRIVATE -Os)
  target_link_options(mooncake_master PRIVATE -Os -s)
endif()
install(TARGETS mooncake_master DESTINATION bin)

# Store Client executable.
add_executable(mooncake_client ../client/real_client_main.cpp)
set_target_properties(mooncake_client PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_link_libraries(
  mooncake_client PRIVATE mooncake_store transfer_engine asio_shared
                          gflags::gflags yalantinglibs::yalantinglibs)
if(USE_CUDA)
  target_compile_definitions(mooncake_client PRIVATE USE_CUDA)
  target_include_directories(mooncake_client
                             PRIVATE ${CUDAToolkit_INCLUDE_DIRS})
  target_link_libraries(mooncake_client PRIVATE CUDA::cudart)
elseif(USE_HIP)
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

  add_library(mooncake_store_c SHARED ../client/store_c_shared.cpp)
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
               LINK_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/../client/store_c.map")
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
    "-Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/../client/store_c.map"
    "$<$<CONFIG:Release>:-Wl,-s>")
  install(TARGETS mooncake_store_c DESTINATION lib)
endif()
