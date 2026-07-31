set(MOONCAKE_COMMON_SOURCES crc_checksum.cpp default_config.cpp environ.cpp
                            rpc_client_io_context.cpp)

add_library(mooncake_common ${MOONCAKE_COMMON_SOURCES})

target_include_directories(
  mooncake_common
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>
         $<INSTALL_INTERFACE:include>)

target_link_libraries(
  mooncake_common PUBLIC Mooncake::Asio Mooncake::YamlCpp Mooncake::JsonCpp
                         Mooncake::Yalantinglibs)

if(BUILD_SHARED_LIBS)
  install(TARGETS mooncake_common DESTINATION lib)
endif()
