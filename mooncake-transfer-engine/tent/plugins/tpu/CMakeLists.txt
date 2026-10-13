# Mooncake's built-in TPU adapter, loaded by TENT's TpuPjrtShim (it is the
# default MC_TPU_PJRT_LIB name, libmooncake_tpu_pjrt.so). Device copies are
# delegated at runtime to a TPU buffer provider (tpu_buffer_provider.h, e.g.
# tpu-sync), so there is no PJRT/XLA build dependency. The library depends only
# on the C++ standard library, POSIX and libdl, because it is dlopen()ed into
# processes that may not expose Mooncake's own dependencies.

add_library(tent_tpu_pjrt_adapter SHARED tpu_pjrt_adapter.cpp
                                         tpu_token_registry.cpp)

target_link_libraries(tent_tpu_pjrt_adapter PRIVATE tent_plugin
                                                    ${CMAKE_DL_LIBS})
target_include_directories(tent_tpu_pjrt_adapter
                           PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

set_target_properties(
  tent_tpu_pjrt_adapter
  PROPERTIES OUTPUT_NAME "mooncake_tpu_pjrt"
             CXX_VISIBILITY_PRESET hidden
             VISIBILITY_INLINES_HIDDEN ON)

install(TARGETS tent_tpu_pjrt_adapter LIBRARY DESTINATION lib)
