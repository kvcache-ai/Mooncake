file(GLOB CXL_SOURCES "*.cpp")

add_library(cxl_transport OBJECT ${CXL_SOURCES})
# target_link_libraries(rdma_transport PUBLIC transport)