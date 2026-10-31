# Component labels select portable PR tests, not their runtime dependencies.
# Unclassified tests always accompany a selective run; nightly remains full.
include_guard(GLOBAL)
include(${CMAKE_CURRENT_LIST_DIR}/StoreTestComponents.cmake)

foreach(
  component
  common
  te
  tent
  store-client
  store-master
  store-shared
  client-master-contract
  ep
  conductor
  unclassified)
  add_custom_target(mooncake-tests-${component})
endforeach()

function(mooncake_store_test_component name result)
  set(component unclassified)
  foreach(owner client master shared)
    string(TOUPPER ${owner} upper_owner)
    if(name IN_LIST MOONCAKE_STORE_${upper_owner}_TESTS)
      set(component store-${owner})
    endif()
  endforeach()
  if(name STREQUAL "client_master_contract_test")
    set(component client-master-contract)
  endif()
  set(${result}
      ${component}
      PARENT_SCOPE)
endfunction()

# Call in the directory that registers the tests: CMake 3.16 cannot set test
# properties from a parent directory. Append, rather than replace, hardware labels.
function(mooncake_label_tests component)
  get_property(
    tests
    DIRECTORY
    PROPERTY TESTS)
  foreach(test IN LISTS tests)
    set(owner ${component})
    if(component STREQUAL "store")
      mooncake_store_test_component(${test} owner)
    endif()
    set_property(
      TEST ${test}
      APPEND
      PROPERTY LABELS component-${owner})
    if(TARGET ${test})
      add_dependencies(mooncake-tests-${owner} ${test})
    endif()
  endforeach()
endfunction()
