add_executable(tent_cli main.cpp)
set_target_properties(tent_cli PROPERTIES OUTPUT_NAME tent)
target_link_libraries(tent_cli PRIVATE tent_diagnostics)
target_include_directories(tent_cli SYSTEM
                           PRIVATE ${TENT_REPOSITORY_ROOT}/extern/cli11)

install(TARGETS tent_cli RUNTIME DESTINATION bin)
