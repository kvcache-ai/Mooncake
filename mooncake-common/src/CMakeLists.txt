set(MOONCAKE_COMMON_SOURCES default_config.cpp environ.cpp
                            rpc_client_io_context.cpp)

add_library(mooncake_common ${MOONCAKE_COMMON_SOURCES})

set_target_properties(mooncake_common PROPERTIES POSITION_INDEPENDENT_CODE ON)

target_include_directories(
  mooncake_common
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>
         $<INSTALL_INTERFACE:include>)

target_link_libraries(
  mooncake_common PUBLIC Mooncake::asio Mooncake::yaml_cpp Mooncake::jsoncpp
                         yalantinglibs::yalantinglibs)

if(BUILD_SHARED_LIBS)
  install(TARGETS mooncake_common DESTINATION lib)
endif()
