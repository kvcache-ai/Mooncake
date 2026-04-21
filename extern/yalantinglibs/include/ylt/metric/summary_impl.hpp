#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <iterator>
#include <limits>
#include <memory>
#include <type_traits>
#include <vector>

#ifdef SUMMARY_DEBUG_STABLE_TEST
#include "ylt/easylog.hpp"
#endif

namespace ylt::metric::detail {

template <typename uint_type, std::size_t frac_bit = 6>
class summary_impl {
  static_assert(sizeof(uint_type) >= 4);
  static_assert(std::is_unsigned_v<uint_type>);
  constexpr static uint32_t decode_impl(uint16_t float16_value) {
    float16_value <<= (8 - frac_bit);
    uint32_t sign = float16_value >> 15;
    uint32_t exponent = (float16_value >> 8) & 0x7F;
    uint32_t fraction = (float16_value & 0xFF);
    uint32_t float32_value;
    if (exponent == 0) {
      /*discard Denormals, in encode they may not correct so we just decode it
       * as zero */
      float32_value = (sign << 31);
    }
    else if (exponent == 0x7F) {
      /* Inf or NaN */
      /* we just use return it as value 2^64 */
      float32_value = (sign << 31) | ((127 + (127 - 63)) << 23);
    }
    else {
      /* ordinary number */
      float32_value =
          (sign << 31) | ((exponent + (127 - 63)) << 23) | (fraction << 15);
    }
    return float32_value;
  }

  constexpr static auto generate_decode_table() {
    constexpr size_t bucket_size =
        1 << (frac_bit + 1 /*sign bit*/ + 7 /*exp bit*/);
    std::array<uint32_t, bucket_size> table{};
    for (uint16_t i = 0; i < bucket_size; ++i) {
      table[i] = decode_impl(i);
    }
    return table;
  };

  static auto& get_decode_table() {
    static constexpr auto table = generate_decode_table();
    return table;
  };

  /*my float16: | 1bit positive/negative flag | 6bit exp | 9bit frac |*/
  static_assert(frac_bit < 8);
  static constexpr float float16_max = (1ull << 63) * 2.0f;  // 2^64

  static uint16_t encode(float flt) {
    static_assert(sizeof(float) == 4);
    uint32_t& fltInt32 = *(uint32_t*)&flt;
    if (std::abs(flt) >= float16_max || std::isnan(flt)) {
      flt = (fltInt32 & 0x8000'0000) ? (-float16_max) : (float16_max);
    }
    unsigned short fltInt16;
    fltInt16 = (fltInt32 >> 31) << 7;             /*float32 flag: 1bit*/
    unsigned short tmp = (fltInt32 >> 23) & 0xff; /*float32 exp: 8bit*/

    tmp = (tmp - 0x40) & ((unsigned int)((int)(0x40 - tmp) >> 6) >> 25);
    fltInt16 = (fltInt16 | tmp) << 8;

    // this step cause error denormals for flt<2^-63, but we decode it as zero
    // later
    fltInt16 |= (fltInt32 >> 15) & 0xff;

    auto i = fltInt16 >> (8 - frac_bit);
    return i;
  }

  static float decode(uint16_t float16_value) {
    static_assert(frac_bit < 8);
    return *(float*)&(get_decode_table()[float16_value]);
  }

  static constexpr inline size_t bucket_size =
      1 << (frac_bit + 1 /*sign bit*/ + 7 /*exp bit*/);

  static constexpr size_t piece_cnt = 1 << 7;

  struct data_t {
    static constexpr size_t piece_size = bucket_size / piece_cnt;
    using piece_t = std::array<std::atomic<uint_type>, piece_size>;

    std::atomic<uint_type>& operator[](std::size_t index) {
      piece_t* piece = arr[index / piece_size];
      if (piece == nullptr) {
        auto ptr = new piece_t{};
        if (!arr[index / piece_size].compare_exchange_strong(piece, ptr)) {
          delete ptr;
        }
        return (*arr[index / piece_size].load())[index % piece_size];
      }
      else {
        return (*piece)[index % piece_size];
      }
    }
    void refresh() {
      for (auto& piece_ptr : arr) {
        if (piece_ptr) {
          for (auto& e : *piece_ptr) {
            e.store(0, std::memory_order::relaxed);
          }
        }
      }
    }
    static uint16_t get_ordered_index(int16_t raw_index) {
      return (raw_index >= bucket_size / 2) ? (bucket_size / 2 - 1 - raw_index)
                                            : (raw_index);
    }
    static uint16_t get_raw_index(int16_t ordered_index) {
      return (ordered_index < 0) ? (bucket_size / 2 - 1 - ordered_index)
                                 : (ordered_index);
    }
    template <bool inc_order>
    void stat_impl(uint64_t& count,
                   std::vector<std::pair<int16_t, uint_type>>& result, int i) {
      auto piece = arr[i].load(std::memory_order_acquire);
      if (piece) {
        if constexpr (inc_order) {
          for (int j = 0; j < piece->size(); ++j) {
            // tsan check data race here is expected. stat dont need to be very
            // strict. we allow old value.
            auto value = (*piece)[j].load(std::memory_order_relaxed);
            if (value) {
              result.emplace_back(get_ordered_index(i * piece_size + j), value);
              count += value;
            }
          }
        }
        else {
          for (int j = piece->size() - 1; j >= 0; --j) {
            auto value = (*piece)[j].load(std::memory_order_relaxed);
            if (value) {
              result.emplace_back(get_ordered_index(i * piece_size + j), value);
              count += value;
            }
          }
        }
      }
    }
    void stat(uint64_t& count,
              std::vector<std::pair<int16_t, uint_type>>& result) {
      for (int i = piece_cnt - 1; i >= piece_cnt / 2; --i) {
        stat_impl<false>(count, result, i);
      }
      for (int i = 0; i < piece_cnt / 2; ++i) {
        stat_impl<true>(count, result, i);
      }
    }

    ~data_t() {
      for (auto& e : arr) {
        delete e;
      }
    }

    std::array<std::atomic<piece_t*>, piece_cnt> arr;
    // fixed_thread_local_value<double,32> cnt;
  };

  data_t& get_data() {
    auto index = frontend_data_index_.load();
    data_t* data = data_[index];
    if (data == nullptr) [[unlikely]] {
      auto pointer = new data_t{};
      if (!data_[index].compare_exchange_strong(data, pointer)) {
        delete pointer;
      }
      return *data_[index];
    }
    else {
      return *data;
    }
  }

  static inline const unsigned long ms_count =
      std::chrono::steady_clock::duration{std::chrono::milliseconds{1}}.count();

  constexpr static uint32_t near_uint32_max = 4290000000U;

  void increase(data_t& arr, uint16_t pos) {
    auto res = arr[pos].fetch_add(1, std::memory_order::relaxed);
    if constexpr (std::is_same_v<uint_type, uint32_t>) {
      if (res > near_uint32_max) /*no overflow*/ [[likely]] {
        arr[pos].fetch_sub(1, std::memory_order::relaxed);
        int upper = (pos < bucket_size / 2) ? (bucket_size / 2) : (bucket_size);
        int lower = (pos < bucket_size / 2) ? (0) : (bucket_size / 2);
        for (int delta = 1, lim = (std::max)(upper - pos, pos - lower + 1);
             delta < lim; ++delta) {
          if (pos + delta < upper) {
            if (arr[pos + delta].fetch_add(1, std::memory_order::relaxed) <=
                near_uint32_max) {
              break;
            }
            arr[pos + delta].fetch_sub(1, std::memory_order::relaxed);
          }
          if (pos - delta >= lower) {
            if (arr[pos - delta].fetch_add(1, std::memory_order::relaxed) <=
                near_uint32_max) {
              break;
            }
            arr[pos - delta].fetch_sub(1, std::memory_order::relaxed);
          }
        }
      }
    }
  }

  struct data_copy_t {
    std::vector<std::pair<int16_t, uint_type>> arr[2];
    int index[2] = {}, smaller_one;
    void init() {
      if (arr[0][0] <= arr[1][0]) {
        smaller_one = 0;
      }
      else {
        smaller_one = 1;
      }
    }
    void inc() {
      index[smaller_one]++;
      if (arr[0][index[0]] <= arr[1][index[1]]) {
        smaller_one = 0;
      }
      else {
        smaller_one = 1;
      }
    }
    int16_t value() { return arr[smaller_one][index[smaller_one]].first; }
    uint_type count() { return arr[smaller_one][index[smaller_one]].second; }
  };

 public:
  void refresh() {
    if (refresh_time_.count() <= 0) {
      return;
    }
    uint64_t old_tp = tp_;
    auto new_tp = std::chrono::steady_clock::now().time_since_epoch().count();
    auto ms = (new_tp - old_tp) / ms_count;
    if (; ms >= refresh_time_.count()) [[unlikely]] {
      if (tp_.compare_exchange_strong(old_tp, new_tp)) {
        if (ms >= 2 * refresh_time_.count()) {
          for (auto& data : data_) {
            if (data != nullptr) {
              data.load()->refresh();
            }
          }
        }
        else {
          auto pos = frontend_data_index_ ^ 1;
          if (auto data = data_[pos].load(); data != nullptr) {
            data->refresh();
          }
          frontend_data_index_ = pos;
        }
      }
    }
  }
  void insert(float value) {
    refresh();
    auto& data = get_data();
    increase(data, encode(value));
    return;
  }

  std::vector<float> stat(double& sum, uint64_t& count) {
    refresh();
    count = 0;
    sum = 0;
    data_copy_t data_copy;
    {
      data_t* ar[2] = {data_[0], data_[1]};
      if (ar[0] == nullptr && ar[1] == nullptr) [[unlikely]] {
        return std::vector<float>(rate_.size(), 0.0f);
      }
      if (ar[0]) {
        ar[0]->stat(count, data_copy.arr[0]);
      }
      if (ar[1]) {
        ar[1]->stat(count, data_copy.arr[1]);
      }
    }
    if (count == 0) {
      return std::vector<float>(rate_.size(), 0);
    }
    uint64_t count_now = 0;
    data_copy.arr[0].emplace_back(bucket_size / 2, 0);
    data_copy.arr[1].emplace_back(bucket_size / 2, 0);
    data_copy.init();
    std::vector<float> result;
    result.reserve(rate_.size());
    float v = -float16_max;
    for (double e : rate_) {
      if (std::isnan(e) || e < 0) {
        result.push_back(v);
        continue;
      }
      else if (e > 1) [[unlikely]] {
        e = 1;
      }
      auto target_count = std::min<double>(e * count, count);
      if (e == 0) {
        target_count = std::min(uint64_t{1}, count);
      }
      while (true) {
        if (target_count <= count_now) [[unlikely]] {
          result.push_back(v);
          break;
        }
        auto tmp = data_copy.count();
        count_now += tmp;
        v = decode(data_t::get_raw_index(data_copy.value()));
        sum += v * tmp;
        data_copy.inc();
      }
    }
    while (data_copy.value() < bucket_size / 2) {
      sum +=
          decode(data_t::get_raw_index(data_copy.value())) * data_copy.count();
      data_copy.inc();
    }
    return result;
  };
#ifdef SUMMARY_DEBUG_STABLE_TEST
  static inline constexpr size_t ms_per_second = 1;
#else
  static inline constexpr size_t ms_per_second = 1000;
#endif

  summary_impl(std::vector<double>& rate,
               std::chrono::seconds refresh_time = std::chrono::seconds{0})
      : rate_(rate),
        refresh_time_(refresh_time.count() * ms_per_second / 2),
        tp_(std::chrono::steady_clock::now().time_since_epoch().count()) {
#ifdef SUMMARY_DEBUG_STABLE_TEST
    ELOG_WARN << "summary max_age is ms now! dont use it in production! It's "
                 "just for test";
#endif
  };

  ~summary_impl() {
    for (auto& data : data_) {
      delete data;
    }
  }

 private:
  const std::chrono::milliseconds refresh_time_;
  std::atomic<uint64_t> tp_;
  std::vector<double>& rate_;
  std::array<std::atomic<data_t*>, 2> data_;
  std::atomic<int> frontend_data_index_;
};
}  // namespace ylt::metric::detail
