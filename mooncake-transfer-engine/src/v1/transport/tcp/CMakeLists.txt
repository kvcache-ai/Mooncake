file(GLOB TEV1_XPORT_TCP_SOURCES "*.cpp")
add_library(transport_tcp_obj OBJECT ${TEV1_XPORT_TCP_SOURCES})

add_library(transport_tcp_static STATIC $<TARGET_OBJECTS:transport_tcp_obj>)
target_link_libraries(transport_tcp_static PUBLIC tent_common tent_rpc)
