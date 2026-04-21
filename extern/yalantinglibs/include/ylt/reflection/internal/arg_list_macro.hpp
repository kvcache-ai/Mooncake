#pragma once
#include "common_macro.hpp"

#define YLT_MACRO_EXPAND(...) __VA_ARGS__

#define WRAP_ARGS0(w, o)
#define WRAP_ARGS1(w, o, _1) w(o, _1)
#include "generate/arg_list_macro_gen.hpp"

#define WRAP_ARGS_(w, object, ...)                  \
  YLT_CONCAT(WRAP_ARGS, YLT_ARG_COUNT(__VA_ARGS__)) \
  (w, object, ##__VA_ARGS__)
#define WRAP_ARGS(w, object, ...) WRAP_ARGS_(w, object, ##__VA_ARGS__)
