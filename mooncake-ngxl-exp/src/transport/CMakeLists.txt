file(GLOB XPORT_SOURCES "*.cpp")

add_subdirectory(rdma_transport)
add_library(ngxl_transport OBJECT ${XPORT_SOURCES} $<TARGET_OBJECTS:rdma_ngxl_transport>)

add_subdirectory(tcp_transport)
target_sources(ngxl_transport PUBLIC $<TARGET_OBJECTS:tcp_ngxl_transport>)

add_subdirectory(shm_transport)
target_sources(ngxl_transport PUBLIC $<TARGET_OBJECTS:shm_ngxl_transport>)

if (USE_NVMEOF)
  add_subdirectory(nvmeof_transport)
  target_sources(ngxl_transport PUBLIC $<TARGET_OBJECTS:nvmeof_ngxl_transport>)
endif()
