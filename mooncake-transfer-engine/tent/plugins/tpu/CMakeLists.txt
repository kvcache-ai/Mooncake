# Mooncake's built-in TPU adapter (OUTPUT_NAME is TpuPjrtShim's default
# MC_TPU_PJRT_LIB). It is dlopen()ed into processes that may not expose
# Mooncake's dependencies, so link only libdl and the C++ standard library.

add_library(tent_tpu_pjrt_adapter SHARED tpu_pjrt_adapter.cpp
                                         tpu_synthetic_address_registry.cpp)

target_link_libraries(tent_tpu_pjrt_adapter PRIVATE tent_plugin
                                                    ${CMAKE_DL_LIBS})

set_target_properties(
  tent_tpu_pjrt_adapter
  PROPERTIES OUTPUT_NAME "mooncake_tpu_pjrt"
             CXX_VISIBILITY_PRESET hidden
             VISIBILITY_INLINES_HIDDEN ON)

install(TARGETS tent_tpu_pjrt_adapter LIBRARY DESTINATION lib)
