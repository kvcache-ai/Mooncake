file(GLOB GDS_SOURCES "*.cpp")

add_library(gds_transport OBJECT ${GDS_SOURCES})
target_include_directories(gds_transport PUBLIC "/usr/local/cuda/include")
