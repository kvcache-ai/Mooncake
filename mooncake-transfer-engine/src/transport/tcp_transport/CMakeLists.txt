file(GLOB TCP_SOURCES "*.cpp")

add_library(tcp_transport OBJECT ${TCP_SOURCES})
