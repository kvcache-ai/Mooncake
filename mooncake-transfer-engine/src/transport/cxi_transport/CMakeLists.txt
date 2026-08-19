file(GLOB CXI_SOURCES "*.cpp")
add_library(cxi_transport OBJECT ${CXI_SOURCES})

# Link against libfabric (fabric) instead of ibverbs for AWS EFA
target_link_libraries(cxi_transport PRIVATE fabric glog::glog)

target_include_directories(
  cxi_transport
  PRIVATE
    ${CMAKE_SOURCE_DIR}/mooncake-transfer-engine/include
    ${CMAKE_SOURCE_DIR}/mooncake-transfer-engine/include/transport/cxi_transport
    ${CMAKE_SOURCE_DIR}/mooncake-common/include ${LIBFABRIC_INCLUDE_DIR})

# libfabric library path is set globally via common.cmake (LIBFABRIC_LIB_DIR)
