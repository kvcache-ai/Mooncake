if (USE_ASCEND OR USE_ASCEND_DIRECT)
    file(GLOB TEV1_XPORT_ASCEND_DIRECT_SOURCES "*.cpp")
    add_library(transport_ascend_direct_obj OBJECT ${TEV1_XPORT_ASCEND_DIRECT_SOURCES})

    add_library(transport_ascend_direct_static STATIC $<TARGET_OBJECTS:transport_ascend_direct_obj>)
    target_link_libraries(transport_ascend_direct_static PUBLIC tent_common)
    target_link_libraries(transport_ascend_direct_static PUBLIC ascendcl cann_hixl metadef)
endif()
