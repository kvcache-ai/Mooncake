add_library(master_service_test_dsl STATIC scenario.cpp)
target_include_directories(master_service_test_dsl
                           PUBLIC ${MOONCAKE_STORE_TESTS_SOURCE_DIR})
target_link_libraries(master_service_test_dsl PUBLIC mooncake_store gtest)

add_store_test(master_scenario_test scenario_contract_test.cpp)
target_link_libraries(master_scenario_test PRIVATE master_service_test_dsl)
