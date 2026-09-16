add_executable(tent_cli main.cpp)
set_target_properties(tent_cli PROPERTIES OUTPUT_NAME tent)
target_link_libraries(tent_cli PRIVATE tent_diagnostics)

install(TARGETS tent_cli RUNTIME DESTINATION bin)
