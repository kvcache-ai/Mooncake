file(GLOB RDMA_SOURCES "*.cpp")

add_library(rdma_ngxl_transport OBJECT ${RDMA_SOURCES})
# target_link_libraries(rdma_ngxl_transport PUBLIC ngxl_transport)