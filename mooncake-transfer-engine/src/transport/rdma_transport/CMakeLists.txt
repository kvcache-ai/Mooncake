file(GLOB RDMA_SOURCES "*.cpp")

add_library(rdma_transport OBJECT ${RDMA_SOURCES})
target_link_libraries(rdma_transport PRIVATE JsonCpp::JsonCpp glog::glog
                                             pthread)

if(USE_MLX5DV)
  target_compile_definitions(rdma_transport PRIVATE USE_MLX5DV)
endif()

if(USE_TCP)
  target_link_libraries(rdma_transport PRIVATE tcp_transport)
endif()
