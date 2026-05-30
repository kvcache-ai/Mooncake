file(GLOB EFA_SOURCES "*.cpp")
add_library(efa_transport OBJECT ${EFA_SOURCES})

# Link against libfabric (fabric) instead of ibverbs for AWS EFA
target_link_libraries(efa_transport PRIVATE fabric glog::glog)

target_include_directories(efa_transport PRIVATE
    ${CMAKE_SOURCE_DIR}/mooncake-transfer-engine/include
    ${CMAKE_SOURCE_DIR}/mooncake-transfer-engine/include/transport/efa_transport
    ${CMAKE_SOURCE_DIR}/mooncake-common/include
    ${LIBFABRIC_INCLUDE_DIR}
)

# libfabric library path is set globally via common.cmake (LIBFABRIC_LIB_DIR)
