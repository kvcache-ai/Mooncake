# TENT_METRICS_ENABLED option is declared in tent/CMakeLists.txt.
file(GLOB TENT_METRICS_SOURCES "*.cpp")
# Build as a SHARED library so the TentMetrics singleton (the function-local
# `static TentMetrics instance` in TentMetrics::instance()) is not duplicated
# across DSOs. When statically linked, the metrics code is copied into every
# consumer .so (engine.so and store.so), giving each its own TentMetrics
# instance: a process that loads both modules ends up with two independent
# metrics singletons, two periodic report threads ("TENT Metrics:" logged
# twice per interval), and a metrics HTTP port clash (both default to 9100,
# so only one binds and the other silently degrades to log-only). As a shared
# library, both modules take a single DT_NEEDED on libtent_metrics.so and
# share the one process-wide singleton. PIC is already on for the whole tent
# tree (tent/CMakeLists.txt sets CMAKE_POSITION_INDEPENDENT_CODE ON), so the
# tent_common OBJECT dependency links cleanly into the shared object.
add_library(tent_metrics SHARED ${TENT_METRICS_SOURCES})
set_target_properties(
  tent_metrics
  PROPERTIES INSTALL_RPATH "$ORIGIN" BUILD_WITH_INSTALL_RPATH TRUE)
# Link yalantinglibs::yalantinglibs for metrics and HTTP server (ylt headers +
# transitive deps). ODR safety: yalantinglibs bundles ASIO headers but does NOT
# compile ASIO inline because ASIO_SEPARATE_COMPILATION is set globally. All
# ASIO symbols live exclusively in asio_shared.so, so there is no risk of
# duplicate symbols between yalantinglibs and the rest of TE. (See
# mooncake-common/src/CMakeLists.txt: "Build asio as a shared library to avoid
# ODR violations")
target_link_libraries(
  tent_metrics PUBLIC tent_common tent_interface yalantinglibs::yalantinglibs
                      glog::glog pthread)

# Pass compile definition based on option
if(TENT_METRICS_ENABLED)
  target_compile_definitions(tent_metrics PUBLIC TENT_METRICS_ENABLED=1)
  message(STATUS "TENT metrics: ENABLED")
else()
  target_compile_definitions(tent_metrics PUBLIC TENT_METRICS_ENABLED=0)
  message(STATUS "TENT metrics: DISABLED (zero overhead)")
endif()

# Install to lib/ for libtent_shared.so / system consumers. The python C
# extensions (engine.so, store.so) resolve it via $ORIGIN, so a second
# install lands libtent_metrics.so next to them — see
# mooncake-integration/CMakeLists.txt.
install(
  TARGETS tent_metrics
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib
  RUNTIME DESTINATION bin)
