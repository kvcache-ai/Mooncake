file(GLOB NVMEOF_GENERIC_SOURCES "*.cpp")

add_library(nvmeof_generic_transport OBJECT ${NVMEOF_GENERIC_SOURCES})
target_include_directories(nvmeof_generic_transport PUBLIC)