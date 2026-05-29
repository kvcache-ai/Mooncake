file(GLOB FLAGCX_SOURCES "*.cpp")
add_library(flagcx_transport OBJECT ${FLAGCX_SOURCES})
target_link_libraries(flagcx_transport PRIVATE JsonCpp::JsonCpp yalantinglibs::yalantinglibs)
