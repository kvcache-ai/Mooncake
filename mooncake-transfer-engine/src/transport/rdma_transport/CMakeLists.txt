file(GLOB RDMA_SOURCES "*.cpp")

add_library(rdma_transport OBJECT ${RDMA_SOURCES})
target_link_libraries(
  rdma_transport
  PUBLIC gflags::gflags glog::glog
  PRIVATE JsonCpp::JsonCpp pthread)

if(USE_ASCEND_RDMA)
  target_compile_definitions(rdma_transport PRIVATE USE_ASCEND_RDMA)
  target_include_directories(
    rdma_transport PRIVATE ${ASCEND_INCLUDE_DIR}
                           ${ASCEND_INCLUDE_DIR}/experiment/ascend_hal)
  target_link_directories(rdma_transport PUBLIC ${ASCEND_LIB_DIR}
                          ${ASCEND_TOOLKIT_ROOT}/devlib)
  target_link_libraries(rdma_transport PUBLIC ascendcl ascend_hal)
  message(STATUS "Ascend UB segment registration for RDMA is enabled")
endif()

if(USE_MLX5DV)
  target_compile_definitions(rdma_transport PRIVATE USE_MLX5DV)
endif()
