if(TENT_USE_PLUGINS)
    # Plugin mode: Build cross-platform dynamic plugins
    message(STATUS "Building platform backends as dynamic plugins")

    # Set common include directory for all platform plugins
    set(TENT_PLATFORM_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../include)
    set(PLATFORM_PLUGIN_OUTPUT_DIR "${CMAKE_BINARY_DIR}/lib/tent/platform")

    file(GLOB BACKEND_SOURCES "*.cpp")

    # Find available platforms and build plugins
    if(CUDAToolkit_FOUND)
        message(STATUS "  Building platform plugin: CUDA")
        add_library(tent_platform_cuda SHARED ${BACKEND_SOURCES})
        target_compile_definitions(tent_platform_cuda PRIVATE USE_CUDA TENT_BUILD_PLUGIN)
        target_include_directories(tent_platform_cuda PUBLIC
            ${CUDAToolkit_INCLUDE_DIRS}
            ${TENT_PLATFORM_INCLUDE_DIR}
        )
        target_link_libraries(tent_platform_cuda PUBLIC CUDA::cudart numa)
        set_target_properties(tent_platform_cuda PROPERTIES
            OUTPUT_NAME "tent_platform_cuda"
            LIBRARY_OUTPUT_DIRECTORY "${PLATFORM_PLUGIN_OUTPUT_DIR}"
        )
    endif()

    if(USE_HIP)
        message(STATUS "  Building platform plugin: HIP")
        add_library(tent_platform_hip SHARED ${BACKEND_SOURCES})
        target_compile_definitions(tent_platform_hip PRIVATE USE_HIP TENT_BUILD_PLUGIN)
        target_include_directories(tent_platform_hip PUBLIC
            ${TENT_PLATFORM_INCLUDE_DIR}
        )
        target_link_libraries(tent_platform_hip PUBLIC hip::host numa)
        set_target_properties(tent_platform_hip PROPERTIES
            OUTPUT_NAME "tent_platform_hip"
            LIBRARY_OUTPUT_DIRECTORY "${PLATFORM_PLUGIN_OUTPUT_DIR}"
        )
    endif()

    if(USE_ASCEND OR USE_ASCEND_DIRECT)
        message(STATUS "  Building platform plugin: Ascend")
        add_library(tent_platform_ascend SHARED ${BACKEND_SOURCES})
        target_compile_definitions(tent_platform_ascend PRIVATE USE_ASCEND TENT_BUILD_PLUGIN)
        target_include_directories(tent_platform_ascend PUBLIC
            ${TENT_PLATFORM_INCLUDE_DIR}
        )
        target_link_libraries(tent_platform_ascend PUBLIC ascendcl numa)
        set_target_properties(tent_platform_ascend PROPERTIES
            OUTPUT_NAME "tent_platform_ascend"
            LIBRARY_OUTPUT_DIRECTORY "${PLATFORM_PLUGIN_OUTPUT_DIR}"
        )
    endif()

    # CPU fallback (always available)
    message(STATUS "  Building platform plugin: CPU (fallback)")
    add_library(tent_platform_cpu SHARED ${BACKEND_SOURCES})
    target_compile_definitions(tent_platform_cpu PRIVATE TENT_BUILD_PLUGIN)
    target_include_directories(tent_platform_cpu PUBLIC ${TENT_PLATFORM_INCLUDE_DIR})
    target_link_libraries(tent_platform_cpu PUBLIC numa)
    set_target_properties(tent_platform_cpu PROPERTIES
        OUTPUT_NAME "tent_platform_cpu"
        LIBRARY_OUTPUT_DIRECTORY "${PLATFORM_PLUGIN_OUTPUT_DIR}"
    )
else()
    # Traditional static linking mode
    add_subdirectory(cuda)
    add_subdirectory(rocm)
    add_subdirectory(ascend)
    add_subdirectory(sunrise)

    file(GLOB PLATFORM_SOURCES "*.cpp")
    add_library(tent_platform_all STATIC ${PLATFORM_SOURCES})
    target_link_libraries(tent_platform_all PUBLIC tent_common)
    if(TARGET platform_cuda)
      target_link_libraries(tent_platform_all PUBLIC platform_cuda)
    endif()
    if(TARGET platform_rocm)
      target_link_libraries(tent_platform_all PUBLIC platform_rocm)
    endif()
    if(TARGET platform_ascend)
      target_link_libraries(tent_platform_all PUBLIC platform_ascend)
    endif()
    if(TARGET platform_sunrise)
      target_link_libraries(tent_platform_all PUBLIC platform_sunrise)
    endif()
endif()
