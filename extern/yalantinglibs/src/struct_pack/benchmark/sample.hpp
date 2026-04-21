#pragma once

#include <stdint.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <iostream>
#include <numeric>

#include "config.hpp"

struct base_sample {
  virtual std::string name() const = 0;
  virtual void do_serialization() = 0;
  virtual void do_deserialization() = 0;
  virtual void create_samples() {}
  virtual ~base_sample() {}

  void print_buffer_size(LibType type) {
    auto lib_name = get_lib_name(type);
    for (auto iter : buf_size_map_) {
      std::string prefix = lib_name;
      prefix.append(" serialize ").append(get_sample_name(iter.first));

      auto space_str = get_space_str(prefix.size(), 36);
      std::cout << prefix << space_str << " buffer size = " << iter.second
                << "\n";
    }
  }

  auto &get_ser_time_elapsed_map() { return ser_time_elapsed_map_; }
  auto &get_deser_time_elapsed_map() { return deser_time_elapsed_map_; }

 protected:
  std::unordered_map<SampleType, size_t> buf_size_map_;
  std::unordered_map<SampleType, uint64_t> ser_time_elapsed_map_;
  std::unordered_map<SampleType, uint64_t> deser_time_elapsed_map_;
};
