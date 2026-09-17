file(GLOB RDMA_SOURCES "*.cpp")

add_library(rdma_transport OBJECT ${RDMA_SOURCES})
target_link_libraries(
  rdma_transport
  PUBLIC gflags::gflags glog::glog
  PRIVATE JsonCpp::JsonCpp pthread)

if(USE_1825)
  target_compile_definitions(rdma_transport PRIVATE USE_1825)
  target_link_libraries(rdma_transport PUBLIC ${CMAKE_DL_LIBS})
  message(STATUS "Ascend UB segment registration for RDMA is enabled")
endif()

if(USE_MLX5DV)
  target_compile_definitions(rdma_transport PRIVATE USE_MLX5DV)
endif()
