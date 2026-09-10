find_package(yaml-cpp REQUIRED)
find_path(ASIO_INCLUDE_DIR asio.hpp
  HINTS ${yalantinglibs_SOURCE_DIR}/include/ylt/thirdparty
)
if(NOT ASIO_INCLUDE_DIR)
  message(FATAL_ERROR "standalone Asio headers not found")
endif()

set(MOONCAKE_COMMON_SOURCES crc_checksum.cpp default_config.cpp environ.cpp
                            rpc_client_io_context.cpp)

add_library(asio_shared SHARED asio_impl.cpp)

target_compile_definitions(asio_shared PUBLIC ASIO_SEPARATE_COMPILATION
                                              ASIO_DYN_LINK)
target_include_directories(asio_shared PUBLIC ${ASIO_INCLUDE_DIR})

set_target_properties(
  asio_shared
  PROPERTIES POSITION_INDEPENDENT_CODE ON
             INSTALL_RPATH "$ORIGIN"
             BUILD_WITH_INSTALL_RPATH TRUE
             OUTPUT_NAME "asio"
             LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/mooncake-common")

target_link_libraries(asio_shared PUBLIC pthread yalantinglibs::yalantinglibs)
install(TARGETS asio_shared DESTINATION lib)

# Static, PIC twin of asio_shared for consumers that absorb asio into their own
# shared object instead of taking a DT_NEEDED on libasio.so (see
# WITH_STORE_C_SHARED). Same TU and definitions as asio_shared so the symbols
# match what the rest of the tree was compiled against; only the linkage
# differs.
if(WITH_STORE_C_SHARED)
  add_library(asio_static STATIC asio_impl.cpp)
  target_compile_definitions(asio_static PUBLIC ASIO_SEPARATE_COMPILATION
                                                ASIO_DYN_LINK)
  target_include_directories(asio_static PUBLIC ${ASIO_INCLUDE_DIR})
  set_target_properties(asio_static PROPERTIES POSITION_INDEPENDENT_CODE ON)
  target_link_libraries(asio_static PUBLIC pthread)
endif()

add_library(mooncake_common ${MOONCAKE_COMMON_SOURCES})

target_include_directories(
  mooncake_common
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>
         $<INSTALL_INTERFACE:include>)

target_link_libraries(
  mooncake_common PUBLIC asio_shared yaml-cpp JsonCpp::JsonCpp
                         yalantinglibs::yalantinglibs)

if(BUILD_SHARED_LIBS)
  install(TARGETS mooncake_common DESTINATION lib)
endif()
