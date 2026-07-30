# TENT_METRICS_ENABLED option is declared in tent/CMakeLists.txt.
file(GLOB TENT_METRICS_SOURCES "*.cpp")
add_library(tent_metrics STATIC ${TENT_METRICS_SOURCES})
# Link yalantinglibs::yalantinglibs for metrics and HTTP server (ylt headers +
# transitive deps). Asio uses separate compilation through Mooncake::asio, so
# every final binary consumes the same bundled implementation archive.
target_link_libraries(
  tent_metrics PUBLIC tent_common tent_interface yalantinglibs::yalantinglibs
                      Mooncake::glog pthread)

# Pass compile definition based on option
if(TENT_METRICS_ENABLED)
  target_compile_definitions(tent_metrics PUBLIC TENT_METRICS_ENABLED=1)
  message(STATUS "TENT metrics: ENABLED")
else()
  target_compile_definitions(tent_metrics PUBLIC TENT_METRICS_ENABLED=0)
  message(STATUS "TENT metrics: DISABLED (zero overhead)")
endif()
