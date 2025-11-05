file(GLOB TEV1_XPORT_TCP_SOURCES "*.cpp")
add_library(transport_shm_obj OBJECT ${TEV1_XPORT_TCP_SOURCES})

add_library(transport_shm_static STATIC $<TARGET_OBJECTS:transport_shm_obj>)
target_link_libraries(transport_shm_static PUBLIC tent_common tent_rpc)
