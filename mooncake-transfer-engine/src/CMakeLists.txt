file(GLOB ENGINE_SOURCES "*.cpp")
add_subdirectory(common)
add_subdirectory(transport)

# EFA library path is set globally via common.cmake (LIBFABRIC_LIB_DIR)

set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

if(USE_HIP)
  hipify_files(ENGINE_SOURCES)
endif()

add_library(transfer_engine ${ENGINE_SOURCES} $<TARGET_OBJECTS:transport>)
if(BUILD_SHARED_LIBS)
  install(TARGETS transfer_engine DESTINATION lib)
endif()

add_compile_definitions(transfer_engine PUBLIC MOONCAKE_USE_ETCD CMAKE_INCLUDE)
if(USE_ETCD)
  if(USE_ETCD_LEGACY)
    if(USE_STATIC_ETCD_CPP_API)
      find_package(PkgConfig REQUIRED)
      pkg_check_modules(GRPCPP REQUIRED grpc++)
      pkg_check_modules(GRPC REQUIRED grpc)
      target_link_libraries(
        transfer_engine PUBLIC etcd-cpp-api-core protobuf ${GRPC_LDFLAGS}
                               ${GRPCPP_LDFLAGS})
    else()
      target_link_libraries(transfer_engine PUBLIC etcd-cpp-api)
    endif()
  else()
    add_dependencies(transfer_engine build_etcd_wrapper)
    set(ETCD_WRAPPER_LIB
        ${CMAKE_CURRENT_BINARY_DIR}/../../mooncake-common/etcd/libetcd_wrapper.so
    )
    target_link_libraries(transfer_engine PRIVATE ${ETCD_WRAPPER_LIB})
  endif()
endif()
if(USE_REDIS)
  target_link_libraries(transfer_engine PUBLIC hiredis)
endif()
if(USE_HTTP)
  find_package(CURL REQUIRED)
  target_link_libraries(transfer_engine PUBLIC ${CURL_LIBRARIES})
endif()
target_link_libraries(
  transfer_engine
  PUBLIC base
         transport
         rdma_transport
         ibverbs
         glog::glog
         gflags::gflags
         pthread
         JsonCpp::JsonCpp
         numa
         asio_shared
         mooncake_common
         yalantinglibs::yalantinglibs)

if(USE_BAREX)
  target_link_libraries(transfer_engine PUBLIC barex_transport)
endif()

if(USE_CUDA)
  target_include_directories(transfer_engine PRIVATE /usr/local/cuda/include)
  target_link_libraries(transfer_engine PUBLIC cuda cudart rt)
  if(USE_NVMEOF)
    target_link_libraries(transfer_engine PUBLIC nvmeof_transport cufile)
  endif()
endif()

if(USE_MUSA)
  target_include_directories(transfer_engine PRIVATE /usr/local/musa/include)
  target_link_libraries(transfer_engine PUBLIC musa musart rt)
endif()

if(USE_HIP)
  target_include_directories(transfer_engine PRIVATE ${HIP_INCLUDE_DIRS})
  target_link_libraries(transfer_engine PUBLIC hip::host rt)
endif()

if (USE_ASCEND)
  target_link_libraries(transfer_engine PUBLIC ascendcl hccl ascend_transport MPI::MPI)
endif()

if (USE_ASCEND_DIRECT)
  target_link_libraries(transfer_engine PUBLIC ascend_transport)
endif()

if (USE_UBSHMEM)
  target_link_libraries(transfer_engine PUBLIC ascend_transport)
endif()

if (USE_ASCEND_HETEROGENEOUS)
  file(GLOB ASCEND_TOOLKIT_ROOT "/usr/local/Ascend/ascend-toolkit/latest/*-linux")
  set(ASCEND_LIB_DIR "${ASCEND_TOOLKIT_ROOT}/lib64")
  link_directories(${ASCEND_LIB_DIR})
  target_link_libraries(transfer_engine PUBLIC ascendcl ascend_transport)
endif()

if(USE_TENT)
  add_compile_definitions(transfer_engine PUBLIC USE_TENT)
  target_link_libraries(transfer_engine PUBLIC tent)
endif()

if(USE_INTRA_NVLINK)
  message(STATUS "Enabled USE_INTRA_NVLINK support")
  target_compile_definitions(transfer_engine PUBLIC USE_INTRA_NVLINK)
endif()

if(USE_EFA)
  message(STATUS "Enabled USE_EFA (AWS Elastic Fabric Adapter) support")
  target_link_libraries(transfer_engine PUBLIC fabric efa_transport)
endif()
