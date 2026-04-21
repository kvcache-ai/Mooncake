#include "ylt/struct_pack.hpp"
#include "ylt/struct_pack/reflection.hpp"
// set global default struct pack config
namespace struct_pack {
constexpr sp_config set_default(sp_config*) {
  return sp_config::DISABLE_ALL_META_INFO;
}
}  // namespace struct_pack
struct rect {
  int a, b, c, d;
};
struct rect2 {
  int a, b, c, d;
};
constexpr struct_pack::sp_config set_sp_config(rect2*) {
  return struct_pack::sp_config::DISABLE_TYPE_INFO;
}
static_assert(struct_pack::get_needed_size(rect{}).size() == 16);
static_assert(struct_pack::get_needed_size(rect2{}).size() == 20);
