file(GLOB TEV1_ENGINE_SOURCES "*.cpp")

add_subdirectory(common)
add_subdirectory(memory)
add_subdirectory(metadata)
add_subdirectory(transport)
add_subdirectory(utility)

SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

add_library(transfer_engine_v1 ${TEV1_ENGINE_SOURCES} $<TARGET_OBJECTS:transport_v1> $<TARGET_OBJECTS:metadata_v1>)
if (BUILD_SHARED_LIBS)
  install(TARGETS transfer_engine_v1 DESTINATION lib)
endif()

add_compile_definitions(transfer_engine_v1 PUBLIC MOONCAKE_USE_ETCD)
if (USE_ETCD)
  add_dependencies(transfer_engine_v1 build_etcd_wrapper)
  set(ETCD_WRAPPER_LIB ${CMAKE_CURRENT_BINARY_DIR}/../../../mooncake-common/etcd/libetcd_wrapper.so)
  target_link_libraries(transfer_engine_v1 PRIVATE ${ETCD_WRAPPER_LIB})
endif()
if (USE_REDIS)
  target_link_libraries(transfer_engine_v1 PUBLIC hiredis)
endif()
if (USE_HTTP)
  find_package(CURL REQUIRED)
  target_link_libraries(transfer_engine_v1 PUBLIC ${CURL_LIBRARIES})
endif()
target_link_libraries(
    transfer_engine_v1
    PUBLIC
    common_v1 memory_v1 utility_v1 transport_v1 transport_rdma_v1 ibverbs glog gflags pthread jsoncpp numa boost_thread
    )

if (USE_CUDA)
  target_include_directories(transfer_engine_v1 PRIVATE /usr/local/cuda/include)
  target_link_libraries(transfer_engine_v1 PUBLIC cuda cudart rt cufile)
endif()

target_link_libraries(transfer_engine_v1 PUBLIC uring)
