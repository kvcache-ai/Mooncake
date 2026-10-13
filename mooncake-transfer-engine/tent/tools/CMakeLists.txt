# tent_plugin_inspect: the loader's verdict on transport plugin files. It uses
# TENT through libtent_shared.so, which also provides TENT's symbols to the
# plugins it loads.
add_executable(tent_plugin_inspect tent_plugin_inspect.cpp)
target_link_libraries(tent_plugin_inspect PRIVATE tent_shared)
target_include_directories(tent_plugin_inspect
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src/runtime)
set_target_properties(tent_plugin_inspect PROPERTIES INSTALL_RPATH
                                                     "$ORIGIN/../lib")
install(TARGETS tent_plugin_inspect RUNTIME DESTINATION bin)
