file(GLOB CXL_SOURCES "*.cpp")

add_library(tcp_transport OBJECT ${CXL_SOURCES})
