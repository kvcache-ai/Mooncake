file(GLOB RDMA_SOURCES "*.cpp")

add_library(rdma_transport OBJECT ${RDMA_SOURCES})
target_link_libraries(rdma_transport PRIVATE JsonCpp::JsonCpp glog::glog pthread)

if (USE_CUDA)
  target_link_libraries(rdma_transport PRIVATE cuda_loader)
endif()
