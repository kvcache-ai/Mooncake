# Header-defined GE error registries must not interpose with the CANN SDK's
# copy. Keep the remaining Mooncake and SDK symbol boundaries unchanged.
set(_MOONCAKE_ASCEND_GE_MAP "${CMAKE_CURRENT_LIST_DIR}/ascend_ge.map")

function(mooncake_isolate_ascend_ge target)
  get_target_property(_target_type ${target} TYPE)
  if(USE_ASCEND_DIRECT
     AND CMAKE_SYSTEM_NAME STREQUAL "Linux"
     AND _target_type STREQUAL "SHARED_LIBRARY")
    set(_map "${_MOONCAKE_ASCEND_GE_MAP}")
    target_link_options(${target} PRIVATE "LINKER:--version-script=${_map}")
    set_property(
      TARGET ${target}
      APPEND
      PROPERTY LINK_DEPENDS "${_map}")
  endif()
endfunction()
