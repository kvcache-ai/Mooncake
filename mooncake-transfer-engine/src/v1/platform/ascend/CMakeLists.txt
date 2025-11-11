if (USE_ASCEND OR USE_ASCEND_DIRECT)
    file(GLOB TENT_PLATFORM_ASCEND_SOURCES "*.cpp")
    add_library(platform_ascend_static STATIC ${TENT_PLATFORM_ASCEND_SOURCES})
    target_link_libraries(platform_ascend_static PUBLIC tent_common ascendcl)

    install(TARGETS platform_ascend_static
            ARCHIVE DESTINATION lib/platform
            LIBRARY DESTINATION lib/platform
            RUNTIME DESTINATION bin)
endif()
