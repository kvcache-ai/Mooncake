file(GLOB TEV1_XPORT_SOURCES "*.cpp")

add_subdirectory(rdma)
add_library(transport_v1 OBJECT ${TEV1_XPORT_SOURCES} $<TARGET_OBJECTS:transport_rdma_v1>)

add_subdirectory(shm)
target_sources(transport_v1 PUBLIC $<TARGET_OBJECTS:transport_shm_v1>)

add_subdirectory(tcp)
target_sources(transport_v1 PUBLIC $<TARGET_OBJECTS:transport_tcp_v1>)