if (USE_CANN)
    file(GLOB TEV1_XPORT_HIXL_SOURCES "*.cpp")
    add_library(transport_hixl_obj OBJECT ${TEV1_XPORT_HIXL_SOURCES})

    add_library(transport_hixl_static STATIC $<TARGET_OBJECTS:transport_hixl_obj>)
    target_link_libraries(transport_hixl_static PUBLIC tent_common)
    target_link_libraries(transport_hixl_static PUBLIC ascendcl cann_hixl metadef)
endif()