file(GLOB TEV1_XPORT_SOURCES "*.cpp")

add_subdirectory(rdma)
add_library(transport_v1 OBJECT ${TEV1_XPORT_SOURCES} $<TARGET_OBJECTS:transport_rdma_v1>)

add_subdirectory(shm)
target_sources(transport_v1 PUBLIC $<TARGET_OBJECTS:transport_shm_v1>)

add_subdirectory(tcp)
target_sources(transport_v1 PUBLIC $<TARGET_OBJECTS:transport_tcp_v1>)

if (USE_CUDA)
  add_subdirectory(nvlink)
  target_sources(transport_v1 PUBLIC $<TARGET_OBJECTS:transport_nvlink_v1>)

  add_subdirectory(mnnvl)
  target_sources(transport_v1 PUBLIC $<TARGET_OBJECTS:transport_mnnvl_v1>)
endif()

if (USE_GDS)
  add_subdirectory(gds)
  target_sources(transport_v1 PUBLIC $<TARGET_OBJECTS:transport_gds_v1>)
endif()

add_subdirectory(io_uring)
target_sources(transport_v1 PUBLIC $<TARGET_OBJECTS:transport_io_uring_v1>)
