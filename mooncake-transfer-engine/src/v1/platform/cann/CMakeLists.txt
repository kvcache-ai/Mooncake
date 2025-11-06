if (USE_CANN)
    file(GLOB TENT_PLATFORM_CUDA_SOURCES "*.cpp")
    add_library(platform_cann_static STATIC ${TENT_PLATFORM_CUDA_SOURCES})
    target_link_libraries(platform_cann_static PUBLIC tent_common ascendcl)

    install(TARGETS platform_cann_static
            ARCHIVE DESTINATION lib/platform
            LIBRARY DESTINATION lib/platform
            RUNTIME DESTINATION bin)
endif()
