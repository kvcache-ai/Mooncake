file(GLOB XPORT_V1_SOURCES "*.cpp")

add_subdirectory(rdma)
add_library(transport_v1 OBJECT ${XPORT_V1_SOURCES} $<TARGET_OBJECTS:transport_rdma_v1>)

add_subdirectory(shm)
target_sources(transport_v1 PUBLIC $<TARGET_OBJECTS:transport_shm_v1>)