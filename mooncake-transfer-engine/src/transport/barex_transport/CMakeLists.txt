file(GLOB BAREX_SOURCES "*.cpp")

add_library(barex_transport OBJECT ${BAREX_SOURCES})
target_link_libraries(barex_transport PRIVATE pthread accl_barex)
target_compile_definitions(barex_transport PRIVATE CMAKE_INCLUDE=1)