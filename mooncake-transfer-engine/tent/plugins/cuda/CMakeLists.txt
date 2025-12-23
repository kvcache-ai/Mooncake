set(CMAKE_POSITION_INDEPENDENT_CODE ON)

find_package(CUDAToolkit)

if (NOT CUDAToolkit_FOUND)
    message(STATUS "CUDA Toolkit not found, skipping TENT CUDA device plugin.")
    return()
endif()

set(TENT_CUDA_PLUGIN_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/cuda_plugin.cpp
)

add_library(tent_cuda_plugin SHARED ${TENT_CUDA_PLUGIN_SOURCES})

target_link_libraries(tent_cuda_plugin
    PRIVATE CUDA::cudart tent_plugin
)

set_target_properties(tent_cuda_plugin PROPERTIES
    OUTPUT_NAME "tent_cuda"
)

install(TARGETS tent_cuda_plugin
    LIBRARY DESTINATION lib/tent_plugins
    RUNTIME DESTINATION lib/tent_plugins  # Windows/部分平台
)
