file(GLOB COMMON_SOURCES "*.cpp")

add_subdirectory(base)
add_library(common ${XPORT_SOURCES} $<TARGET_OBJECTS:common_base>)
