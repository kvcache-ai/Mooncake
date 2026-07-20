file(GLOB TCP_SOURCES "*.cpp")

if (USE_HIP)
  hipify_files(TCP_SOURCES)
endif()

add_library(tcp_transport OBJECT ${TCP_SOURCES})
target_link_libraries(tcp_transport PRIVATE JsonCpp::JsonCpp yalantinglibs::yalantinglibs)

if(BUILD_UNIT_TESTS)
  target_compile_definitions(tcp_transport
                             PRIVATE MOONCAKE_TCP_TRANSPORT_TEST_HOOKS)
endif()
