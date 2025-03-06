file(GLOB XPORT_SOURCES "*.cpp")

add_subdirectory(rdma_transport)
add_library(transport OBJECT ${XPORT_SOURCES} $<TARGET_OBJECTS:rdma_transport>)

add_subdirectory(tcp_transport)
target_sources(transport PUBLIC $<TARGET_OBJECTS:tcp_transport>)

add_subdirectory(shm_transport)
target_sources(transport PUBLIC $<TARGET_OBJECTS:shm_transport>)

if (USE_NVMEOF)
  add_subdirectory(nvmeof_transport)
  target_sources(transport PUBLIC $<TARGET_OBJECTS:nvmeof_transport>)
endif()

if (USE_CXL)
  add_subdirectory(cxl_transport)
  target_sources(transport PUBLIC $<TARGET_OBJECTS:cxl_transport>)
endif()