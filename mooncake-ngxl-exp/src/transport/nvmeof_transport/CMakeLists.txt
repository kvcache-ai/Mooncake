file(GLOB NVMEOF_SOURCES "*.cpp")

add_library(nvmeof_ngxl_transport OBJECT ${NVMEOF_SOURCES})
target_include_directories(nvmeof_ngxl_transport PUBLIC "/usr/local/cuda/include")
# target_link_libraries(nvmeof_ngxl_transport PUBLIC ngxl_transport)