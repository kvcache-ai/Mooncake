file(GLOB ENGINE_SOURCES "*.cpp")
add_subdirectory(common)
add_subdirectory(transport)

SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

add_library(transfer_engine ${ENGINE_SOURCES} $<TARGET_OBJECTS:transport>)
if (BUILD_SHARED_LIBS)
  install(TARGETS transfer_engine DESTINATION lib)
endif()

add_compile_definitions(transfer_engine PUBLIC MOONCAKE_USE_ETCD)
if (USE_ETCD)
  if (USE_ETCD_LEGACY)
    if (USE_STATIC_ETCD_CPP_API)
      find_package(PkgConfig REQUIRED)
      pkg_check_modules(GRPCPP REQUIRED grpc++)
      pkg_check_modules(GRPC REQUIRED grpc)
      target_link_libraries(transfer_engine PUBLIC etcd-cpp-api-core protobuf ${GRPC_LDFLAGS} ${GRPCPP_LDFLAGS})
    else()
      target_link_libraries(transfer_engine PUBLIC etcd-cpp-api)
    endif()
  else ()
    add_dependencies(transfer_engine build_etcd_wrapper)
    set(ETCD_WRAPPER_LIB ${CMAKE_CURRENT_BINARY_DIR}/../../mooncake-common/etcd/libetcd_wrapper.so)
    target_link_libraries(transfer_engine PRIVATE ${ETCD_WRAPPER_LIB})
  endif()
endif()
if (USE_REDIS)
  target_link_libraries(transfer_engine PUBLIC hiredis)
endif()
if (USE_HTTP)
  find_package(CURL REQUIRED)
  target_link_libraries(transfer_engine PUBLIC ${CURL_LIBRARIES})
endif()
target_link_libraries(
    transfer_engine
    PUBLIC
    base transport rdma_transport ibverbs glog::glog gflags::gflags pthread JsonCpp::JsonCpp numa yalantinglibs::yalantinglibs
    )

if (USE_CUDA)
  target_include_directories(transfer_engine PRIVATE /usr/local/cuda/include)
  target_link_libraries(transfer_engine PUBLIC cuda cudart rt)
  if (USE_NVMEOF)
    target_link_libraries(transfer_engine PUBLIC nvmeof_transport cufile)
  endif()
endif()

if (USE_ASCEND)
  target_link_libraries(transfer_engine PUBLIC ascendcl hccl ascend_transport MPI::MPI)
endif()

if (USE_ASCEND_DIRECT)
  find_library(
          FOUND_METADEF
          NAMES libmetadef.so
          PATHS ${ASCEND_LIB_DIR}
          NO_DEFAULT_PATH
          NO_CMAKE_FIND_ROOT_PATH
  )
  if(FOUND_METADEF)
    message(STATUS "Found library: ${FOUND_METADEF}")
    target_link_libraries(transfer_engine PUBLIC ascend_transport ascendcl llm_datadist metadef)
  else()
    target_link_libraries(transfer_engine PUBLIC ascend_transport ascendcl llm_datadist graph_base graph)
  endif()
endif()

if (USE_ASCEND_HETEROGENEOUS)
  file(GLOB ASCEND_TOOLKIT_ROOT "/usr/local/Ascend/ascend-toolkit/latest/*-linux")
  set(ASCEND_LIB_DIR "${ASCEND_TOOLKIT_ROOT}/lib64")
  link_directories(${ASCEND_LIB_DIR})
  target_link_libraries(transfer_engine PUBLIC ascendcl ascend_transport)
endif()
