add_subdirectory(cuda)

file(GLOB PLATFORM_SOURCES "*.cpp")
add_library(platform_all STATIC ${PLATFORM_SOURCES})
target_link_libraries(platform_all PUBLIC tent_common)
if (TARGET platform_cuda_static)
    target_link_libraries(platform_all PUBLIC platform_cuda_static)
endif()
