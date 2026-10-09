file(GLOB RDMA_TWOSIDED_SOURCES "*.cpp")

add_library(rdma_twosided OBJECT ${RDMA_TWOSIDED_SOURCES})
target_link_libraries(rdma_twosided PRIVATE JsonCpp::JsonCpp glog::glog pthread)
