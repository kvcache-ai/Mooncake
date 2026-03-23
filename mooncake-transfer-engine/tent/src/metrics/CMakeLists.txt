# Option to enable/disable TENT metrics at compile time
option(TENT_METRICS_ENABLED "Enable TENT metrics collection" OFF)

file(GLOB TENT_METRICS_SOURCES "*.cpp")
add_library(tent_metrics STATIC ${TENT_METRICS_SOURCES})
# Link yalantinglibs::yalantinglibs for metrics and HTTP server (ylt headers + transitive deps).
# ODR safety: yalantinglibs bundles ASIO headers but does NOT compile ASIO inline because
# ASIO_SEPARATE_COMPILATION is set globally. All ASIO symbols live exclusively in asio_shared.so,
# so there is no risk of duplicate symbols between yalantinglibs and the rest of TE.
# (See mooncake-asio/CMakeLists.txt: "Build asio as a shared library to avoid ODR violations")
target_link_libraries(tent_metrics PUBLIC tent_common tent_interface yalantinglibs::yalantinglibs glog pthread)

# Pass compile definition based on option
if(TENT_METRICS_ENABLED)
    target_compile_definitions(tent_metrics PUBLIC TENT_METRICS_ENABLED=1)
    message(STATUS "TENT metrics: ENABLED")
else()
    target_compile_definitions(tent_metrics PUBLIC TENT_METRICS_ENABLED=0)
    message(STATUS "TENT metrics: DISABLED (zero overhead)")
endif()