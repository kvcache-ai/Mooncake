set(CMAKE_POSITION_INDEPENDENT_CODE ON)

find_package(HIP REQUIRED)

set(TENT_ROCM_PLUGIN_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/rocm_plugin.cpp
)

add_library(tent_rocm_plugin SHARED ${TENT_ROCM_PLUGIN_SOURCES})

target_link_libraries(tent_rocm_plugin
    PRIVATE hip::host tent_plugin
)

set_target_properties(tent_rocm_plugin PROPERTIES
    OUTPUT_NAME "tent_rocm"
)

install(TARGETS tent_rocm_plugin
    LIBRARY DESTINATION lib/tent_plugins
    RUNTIME DESTINATION lib/tent_plugins
)
