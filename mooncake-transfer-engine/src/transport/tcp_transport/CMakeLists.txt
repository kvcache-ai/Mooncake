file(GLOB TCP_SOURCES "*.cpp")
file(GLOB TCP_IMPL_HEADERS "*_impl.h")

if (USE_HIP)
  list(APPEND TCP_SOURCES ${TCP_IMPL_HEADERS})
  hipify_files(TCP_SOURCES)
endif()

add_library(tcp_transport OBJECT ${TCP_SOURCES})
target_link_libraries(tcp_transport PRIVATE JsonCpp::JsonCpp yalantinglibs::yalantinglibs)

if(BUILD_UNIT_TESTS)
  target_compile_definitions(tcp_transport
                             PRIVATE MOONCAKE_TCP_TRANSPORT_TEST_HOOKS)
endif()
