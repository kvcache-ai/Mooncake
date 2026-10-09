file(GLOB FLAGCX_SOURCES "*.cpp")
add_library(flagcx_transport OBJECT ${FLAGCX_SOURCES})
target_include_directories(
  flagcx_transport PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../..
                           ${CMAKE_CURRENT_SOURCE_DIR}/../../../include)
target_link_libraries(flagcx_transport PRIVATE FlagCX::flagcx JsonCpp::JsonCpp
                                               yalantinglibs::yalantinglibs)
