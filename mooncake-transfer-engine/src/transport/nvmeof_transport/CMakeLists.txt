file(GLOB NVMEOF_SOURCES "*.cpp")

add_library(nvmeof_transport OBJECT ${NVMEOF_SOURCES})
target_include_directories(nvmeof_transport PUBLIC ${CUDAToolkit_INCLUDE_DIRS})
# target_link_libraries(nvmeof_transport PUBLIC transport)