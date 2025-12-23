set(CMAKE_POSITION_INDEPENDENT_CODE ON)

add_library(tent_plugin INTERFACE)
target_include_directories(tent_plugin
    INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>
        $<INSTALL_INTERFACE:include>
)
target_compile_features(tent_plugin INTERFACE cxx_std_20)

add_subdirectory(cuda)
