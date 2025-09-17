file(GLOB XPORT_SOURCES "*.cpp")

# Find Python - pybind11 is already configured at the root level
find_package(Python3 COMPONENTS Interpreter Development REQUIRED)

add_subdirectory(rdma_transport)
add_subdirectory(coro_rpc_connector)

add_library(transport OBJECT ${XPORT_SOURCES} $<TARGET_OBJECTS:rdma_transport> $<TARGET_OBJECTS:coro_rpc_connector>)
target_link_libraries(transport PRIVATE JsonCpp::JsonCpp yalantinglibs::yalantinglibs glog::glog pthread ${Python3_LIBRARIES})
target_include_directories(transport PRIVATE ${Python3_INCLUDE_DIRS})

# Add pybind11 headers if available
if(TARGET pybind11::headers)
    target_link_libraries(transport PRIVATE pybind11::headers)
endif()

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

if (USE_ASCEND OR USE_ASCEND_DIRECT)
  add_subdirectory(ascend_transport)
  target_sources(transport PUBLIC $<TARGET_OBJECTS:ascend_transport>)
endif()

if (USE_MNNVL)
  add_subdirectory(nvlink_transport)
  target_sources(transport PUBLIC $<TARGET_OBJECTS:nvlink_transport>)
endif()