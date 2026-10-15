if(USE_MEMFABRIC)
  add_library(
    tent_xport_memfabric STATIC memfabric_transport.cpp memfabric_under_api.cpp
                                memfabric_allocator.cpp)
  target_link_libraries(tent_xport_memfabric PUBLIC tent_interface dl ascendcl)
  target_include_directories(
    tent_xport_memfabric PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../../../include)
endif()
