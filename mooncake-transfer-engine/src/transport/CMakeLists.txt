file(GLOB XPORT_SOURCES "*.cpp")

add_subdirectory(rdma_transport)
add_library(transport OBJECT ${XPORT_SOURCES} $<TARGET_OBJECTS:rdma_transport>)
target_link_libraries(transport PRIVATE JsonCpp::JsonCpp yalantinglibs::yalantinglibs glog::glog pthread)

if (USE_TCP)
  add_subdirectory(tcp_transport)
  target_sources(transport PUBLIC $<TARGET_OBJECTS:tcp_transport>)
endif()

if (USE_NVMEOF)
  add_subdirectory(nvmeof_transport)
  target_sources(transport PUBLIC $<TARGET_OBJECTS:nvmeof_transport>)
endif()

if (USE_CXL)
  add_subdirectory(cxl_transport)
  target_sources(transport PUBLIC $<TARGET_OBJECTS:cxl_transport>)
endif()

if (USE_ASCEND)
  add_subdirectory(ascend_transport)
  target_sources(transport PUBLIC $<TARGET_OBJECTS:ascend_transport>)
endif()
