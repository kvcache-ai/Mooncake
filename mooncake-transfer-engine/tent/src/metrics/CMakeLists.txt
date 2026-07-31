# TENT_METRICS_ENABLED option is declared in tent/CMakeLists.txt.
file(GLOB TENT_METRICS_SOURCES "*.cpp")
add_library(tent_metrics STATIC ${TENT_METRICS_SOURCES})
# Link the bundled yalantinglibs provider for metrics and HTTP server (ylt
# headers + transitive deps). ODR safety: yalantinglibs bundles ASIO headers but
# does NOT compile ASIO inline because ASIO_SEPARATE_COMPILATION is set
# globally. All ASIO symbols live exclusively in Mooncake::Asio, so there is no
# risk of duplicate symbols between yalantinglibs and the rest of TE. (See
# cmake/dependencies/Asio.cmake.)
target_link_libraries(
  tent_metrics PUBLIC tent_common tent_interface Mooncake::Yalantinglibs
                      Mooncake::Glog pthread)

# Pass compile definition based on option
if(TENT_METRICS_ENABLED)
  target_compile_definitions(tent_metrics PUBLIC TENT_METRICS_ENABLED=1)
  message(STATUS "TENT metrics: ENABLED")
else()
  target_compile_definitions(tent_metrics PUBLIC TENT_METRICS_ENABLED=0)
  message(STATUS "TENT metrics: DISABLED (zero overhead)")
endif()
