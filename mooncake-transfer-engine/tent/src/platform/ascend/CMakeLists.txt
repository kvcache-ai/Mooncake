if (USE_ASCEND OR USE_ASCEND_DIRECT)
    file(GLOB TENT_PLATFORM_ASCEND_SOURCES "*.cpp")
    add_library(platform_ascend STATIC ${TENT_PLATFORM_ASCEND_SOURCES})
    target_link_libraries(platform_ascend PUBLIC tent_common ascendcl)

    install(TARGETS platform_ascend
            ARCHIVE DESTINATION lib/platform
            LIBRARY DESTINATION lib/platform
            RUNTIME DESTINATION bin)
endif()
