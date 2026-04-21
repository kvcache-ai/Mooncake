#include <ylt/coro_io/ibverbs/ib_device.hpp>

#include "doctest.h"

using namespace coro_io;

TEST_CASE("test device list") {
  auto& inst = detail::ib_devices_t::instance();
  auto list = inst.get_devices();
  CHECK(!list.empty());
  CHECK(inst.size());
  auto dev = inst.at(list[0]->name);
  CHECK(dev != nullptr);

  CHECK(inst.at("") == dev);

  for (auto dev : list) {
    ELOG_INFO << dev->dev_name;
  }

  CHECK_THROWS_AS(inst[inst.size()], std::out_of_range);
  CHECK_THROWS_AS(inst.at("no such a device"), std::out_of_range);
}

TEST_CASE("test device") {
  auto dev = ib_device_t::create({});

  auto name = dev->name();
  CHECK(!name.empty());
  auto ctx = dev->context();
  CHECK(ctx);
  auto pd = dev->pd();
  CHECK(pd);
  CHECK(dev->port() == 1);
  CHECK(dev->gid_index() >= 0);
  auto& attr = dev->attr();
}

TEST_CASE("test device invalid") {
  CHECK_THROWS_AS(ib_device_t::create({"", 3}), std::system_error);
  CHECK_THROWS_AS(ib_device_t::create({"no such"}), std::out_of_range);
}
