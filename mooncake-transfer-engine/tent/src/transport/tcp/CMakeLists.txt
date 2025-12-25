file(GLOB XPORT_SOURCES "*.cpp")
add_library(tent_xport_tcp STATIC ${XPORT_SOURCES})
target_link_libraries(tent_xport_tcp PUBLIC tent_rpc tent_common)
