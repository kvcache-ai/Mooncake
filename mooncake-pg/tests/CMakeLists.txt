# Header-only host_ip resolve policy tests (no CUDA / mooncake_pg link).
add_executable(pg_host_ip_resolve_test pg_host_ip_resolve_test.cpp)
target_include_directories(
  pg_host_ip_resolve_test
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include
          ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-transfer-engine/include)
target_compile_features(pg_host_ip_resolve_test PRIVATE cxx_std_20)
target_link_libraries(pg_host_ip_resolve_test PUBLIC gtest gtest_main)
add_test(NAME pg_host_ip_resolve_test COMMAND pg_host_ip_resolve_test)
