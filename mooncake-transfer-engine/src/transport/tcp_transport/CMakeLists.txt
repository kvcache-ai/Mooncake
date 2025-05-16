file(GLOB TCP_SOURCES "*.cpp")

add_library(tcp_transport OBJECT ${TCP_SOURCES})
target_link_libraries(tcp_transport PRIVATE JsonCpp::JsonCpp yalantinglibs::yalantinglibs)
