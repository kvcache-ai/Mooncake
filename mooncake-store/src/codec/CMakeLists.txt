add_library(mooncake_kv_codec STATIC kv_codec.cpp)
target_compile_features(mooncake_kv_codec PUBLIC cxx_std_20)
target_include_directories(mooncake_kv_codec
                           PUBLIC ${CMAKE_CURRENT_LIST_DIR}/../../include)
set_target_properties(mooncake_kv_codec PROPERTIES POSITION_INDEPENDENT_CODE ON)
