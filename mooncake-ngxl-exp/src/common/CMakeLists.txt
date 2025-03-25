file(GLOB COMMON_SOURCES "*.cpp")

add_subdirectory(base)
add_library(ngxl_common ${XPORT_SOURCES} $<TARGET_OBJECTS:ngxl_common_base>)
