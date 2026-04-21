#pragma once

#include <cassert>
#include <chrono>
#include <cstring>
#include <ctime>
#include <string_view>

#include "define.h"

namespace cinatra {
namespace time_util {
inline constexpr bool is_leap(int year) {
  return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
}

inline constexpr int get_day_index(std::string_view str) {
  return week_table[((str[0] & ~0x20) ^ (str[2] & ~0x20)) % week_table.size()];
}

inline constexpr int get_month_index(std::string_view str) {
  return month_table[((str[1] & ~0x20) + (str[2] & ~0x20)) %
                     month_table.size()];
}

inline constexpr int days_in(int m, int year) {
  constexpr int index = get_month_index("Feb");
  if (m == index && is_leap(year)) {
    return 29;
  }
  return int(days_before[m + 1] - days_before[m]);
}

inline constexpr int get_digit(std::string_view sv, int width) {
  int num = 0;
  for (int i = 0; i < width; i++) {
    if ('0' <= sv[i] && sv[i] <= '9') {
      num = num * 10 + (sv[i] - '0');
    }
    else {
      return -1;
    }
  }
  return num;
}

inline constexpr std::uint64_t days_since_epoch(int year) {
  auto y = std::uint64_t(std::int64_t(year) - absolute_zero_year);

  auto n = y / 400;
  y -= 400 * n;
  auto d = days_per_400_years * n;
  n = y / 100;
  y -= 100 * n;
  d += days_per_100_years * n;
  n = y / 4;
  y -= 4 * n;
  d += days_per_4_years * n;
  n = y;
  d += 365 * n;
  return d;
}

inline std::pair<bool, std::time_t> faster_mktime(int year, int month, int day,
                                                  int hour, int min, int sec,
                                                  int day_of_week) {
  auto d = days_since_epoch(year);
  d += std::uint64_t(days_before[month]);
  constexpr int index = get_month_index("Mar");
  if (is_leap(year) && month >= index) {
    d++;  // February 29
  }
  d += std::uint64_t(day - 1);
  auto abs = d * seconds_per_day;
  abs +=
      std::uint64_t(hour * seconds_per_hour + min * seconds_per_minute + sec);
  constexpr int day_index = get_day_index("Mon");
  if (day_of_week != -1) {
    std::int64_t wday = ((abs + std::uint64_t(day_index) * seconds_per_day) %
                         seconds_per_week) /
                        seconds_per_day;
    if (wday != day_of_week) {
      return {false, 0};
    }
  }
  return {true, std::int64_t(abs) + (absolute_to_internal + internal_to_unix)};
}

template <time_format Format>
inline constexpr std::array<component_of_time_format, 32> get_format() {
  if constexpr (Format == time_format::http_format) {
    return http_time_format;
  }
  else if constexpr (Format == time_format::utc_format) {
    return utc_time_format;
  }
  else {
    return utc_time_without_punctuation_format;
  }
}
}  // namespace time_util

template <time_format Format = time_format::http_format, typename String>
inline std::pair<bool, std::time_t> get_timestamp(const String &gmt_time_str) {
  using namespace time_util;
  std::string_view sv(gmt_time_str);
  int year, month, day, hour, min, sec, day_of_week;
  int len_of_gmt_time_str = (int)gmt_time_str.length();
  int len_of_processed_part = 0;
  int len_of_ignored_part = 0;  // second_decimal_part is ignored
  char c;
  constexpr std::array<component_of_time_format, 32> real_format =
      time_util::get_format<Format>();
  if constexpr (Format == time_format::utc_format) {
    day_of_week = -1;
  }
  else if (Format == time_format::utc_without_punctuation_format) {
    day_of_week = -1;
  }

  for (auto &comp : real_format) {
    switch (comp) {
      case component_of_time_format::ending:
        goto travel_done;
        break;
      case component_of_time_format::colon:
      case component_of_time_format::comma:
      case component_of_time_format::SP:
      case component_of_time_format::hyphen:
      case component_of_time_format::dot:
      case component_of_time_format::T:
      case component_of_time_format::Z:
        if (len_of_gmt_time_str - len_of_processed_part < 1) {
          return {false, 0};
        }
        c = sv[len_of_processed_part];
        if ((comp == component_of_time_format::Z && c != 'Z') ||
            (comp == component_of_time_format::T && c != 'T') ||
            (comp == component_of_time_format::dot && c != '.') ||
            (comp == component_of_time_format::hyphen && c != '-') ||
            (comp == component_of_time_format::SP && c != ' ') ||
            (comp == component_of_time_format::colon && c != ':') ||
            (comp == component_of_time_format::comma && c != ',')) {
          return {false, 0};
        }
        len_of_processed_part += 1;
        break;
      case component_of_time_format::year:
        if (len_of_gmt_time_str - len_of_processed_part < 4) {
          return {false, 0};
        }
        if ((year = get_digit(sv.substr(len_of_processed_part, 4), 4)) == -1) {
          return {false, 0};
        }
        len_of_processed_part += 4;
        break;
      case component_of_time_format::month_name:
        if (len_of_gmt_time_str - len_of_processed_part < 3) {
          return {false, 0};
        }
        if ((month = get_month_index(sv.substr(len_of_processed_part, 3))) ==
            -1) {
          return {false, 0};
        }
        len_of_processed_part += 3;
        break;
      case component_of_time_format::hour:
      case component_of_time_format::minute:
      case component_of_time_format::second:
      case component_of_time_format::month:
      case component_of_time_format::day:
        if (len_of_gmt_time_str - len_of_processed_part < 2) {
          return {false, 0};
        }
        int digit;
        if ((digit = get_digit(sv.substr(len_of_processed_part, 2), 2)) == -1) {
          return {false, 0};
        }
        if (comp == component_of_time_format::hour) {
          hour = digit;
          if (hour < 0 || hour >= 24) {
            return {false, 0};
          }
        }
        else if (comp == component_of_time_format::minute) {
          min = digit;
          if (min < 0 || min >= 60) {
            return {false, 0};
          }
        }
        else if (comp == component_of_time_format::month) {
          month = digit;
          if (month < 1 || month > 12) {
            return {false, 0};
          }
          month--;
        }
        else if (comp == component_of_time_format::second) {
          sec = digit;
          if (sec < 0 || sec >= 60) {
            return {false, 0};
          }
        }
        else {
          day = digit;
        }
        len_of_processed_part += 2;
        break;
      case component_of_time_format::day_name:
        if (len_of_gmt_time_str - len_of_processed_part < 3) {
          return {false, 0};
        }
        if ((day_of_week = get_day_index(sv.substr(len_of_processed_part, 3))) <
            0) {
          return {false, 0};
        }
        len_of_processed_part += 3;
        break;
      case component_of_time_format::GMT:
        if (len_of_gmt_time_str - len_of_processed_part < 3) {
          return {false, 0};
        }
        if (sv.substr(len_of_processed_part, 3) != "GMT") {
          return {false, 0};
        }
        len_of_processed_part += 3;
        break;
      case component_of_time_format::second_decimal_part:
        int cur = len_of_processed_part;
        while (cur < len_of_gmt_time_str &&
               (sv[cur] >= '0' && sv[cur] <= '9')) {
          len_of_ignored_part++;
          cur++;
        }
        if (cur == len_of_processed_part) {
          return {false, 0};
        }
        len_of_processed_part = cur;
        break;
    }
  }
travel_done:
  if ((len_of_processed_part != len_of_gmt_time_str) ||
      (len_of_processed_part != len_of_http_time_format &&
       (len_of_processed_part - len_of_ignored_part) !=
           len_of_utc_time_format) &&
          (len_of_processed_part - len_of_ignored_part) !=
              len_of_utc_time_without_punctuation_format) {
    return {false, 0};
  }
  if (day < 1 || day > days_in(month, year)) {
    return {false, 0};
  }
  return faster_mktime(year, month, day, hour, min, sec, day_of_week);
}

constexpr char digits[10] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};
constexpr std::string_view WDAY[7] = {"Sun", "Mon", "Tue", "Wed",
                                      "Thu", "Fri", "Sat"};
constexpr std::string_view YMON[12] = {"Jan", "Feb", "Mar", "Apr",
                                       "May", "Jun", "Jul", "Aug",
                                       "Sep", "Oct", "Nov", "Dec"};

template <size_t N>
inline void to_int(int num, char c, char *p) {
  for (int i = 0; i < N; i++) {
    p[N - 1 - i] = digits[num % 10];
    num = num / 10;
  }

  p[N] = c;
}

inline void to_year(char *buf, int year, char c) { to_int<4>(year, c, buf); }
inline void to_month(char *buf, int month, char c) { to_int<2>(month, c, buf); }
inline void to_day(char *buf, int day, char c) { to_int<2>(day, c, buf); }
inline void to_hour(char *buf, int day, char c) { to_int<2>(day, c, buf); }
inline void to_min(char *buf, int day, char c) { to_int<2>(day, c, buf); }
inline void to_sec(char *buf, int day, char c) { to_int<2>(day, c, buf); }

inline std::tm gmtime_safe(std::time_t &timer) {
  std::tm bt{};
#if defined(__unix__)
  gmtime_r(&timer, &bt);
#elif defined(_MSC_VER)
  gmtime_s(&bt, &timer);
#else
  static std::mutex mtx;
  std::lock_guard<std::mutex> lock(mtx);
  bt = *gmtime(&timer);
#endif
  return bt;
}

template <size_t Hour = 8, size_t N>
inline std::string_view get_local_time_str(char (&buf)[N], std::time_t t,
                                           std::string_view format) {
  static_assert(N >= 20, "wrong buf");
  std::tm tm = gmtime_safe(t);
  std::tm *loc_time = &tm;

  char *p = buf;

  for (int i = 0; i < format.size(); ++i) {
    if (format[i] == '%') {
      char c = i + 2 < format.size() ? format[i + 2] : '0';
      i++;
      if (format[i] == 'Y') {
        to_year(p, loc_time->tm_year + 1900, c);
        p += 5;
      }
      else if (format[i] == 'm') {
        to_month(p, loc_time->tm_mon + 1, c);
        p += 3;
      }
      else if (format[i] == 'd') {
        to_day(p, loc_time->tm_mday, c);
        p += 3;
      }
      else if (format[i] == 'H') {
        to_hour(p, loc_time->tm_hour + Hour, c);
        p += 3;
      }
      else if (format[i] == 'M') {
        to_min(p, loc_time->tm_min, c);
        p += 3;
      }
      else if (format[i] == 'S') {
        to_sec(p, loc_time->tm_sec, c);
        p += 3;
      }
      else if (format[i] == 'a') {
        memcpy(p, WDAY[loc_time->tm_wday].data(), 3);
        p += 3;
        *p++ = c;
        *p++ = ' ';
      }
      else if (format[i] == 'b') {
        memcpy(p, YMON[loc_time->tm_mon].data(), 3);
        p += 3;
        *p = c;
        p += 1;
      }
    }
  }

  size_t n = p - buf - 1;

  return {buf, n};
}

// template <size_t N>
// inline std::string_view get_local_time_str(char (&buf)[N], std::time_t t) {
//   struct tm *loc_time = localtime(&t);
//   size_t n = strftime(buf, N, "%Y-%m-%d %H:%M:%S", loc_time);
//   return {buf, n};
// }

inline std::string_view get_local_time_str(
    std::chrono::system_clock::time_point t) {
  static thread_local char buf[32];
  static thread_local std::chrono::seconds last_sec{};
  static thread_local size_t last_size{};

  std::chrono::system_clock::duration d = t.time_since_epoch();
  std::chrono::seconds s = std::chrono::duration_cast<std::chrono::seconds>(d);
  if (last_sec == s) {
    return {buf, last_size};
  }

  auto tm = std::chrono::system_clock::to_time_t(t);

  auto str = get_local_time_str(buf, tm, "%Y-%m-%d %H:%M:%S");
  last_size = str.size();
  last_sec = s;

  return str;
}

inline std::string_view get_local_time_str() {
  return get_local_time_str(std::chrono::system_clock::now());
}

// template <size_t N>
// inline std::string_view get_gmt_time_str2(char (&buf)[N], std::time_t t) {
//   static_assert(N >= 29, "wrong buf");
//   struct tm *gmt_time = gmtime(&t);
//   size_t n = strftime(buf, sizeof(buf), "%a, %d %b %Y %H:%M:%S GMT",
//   gmt_time); return {buf, n};
// }

template <size_t N>
inline std::string_view get_gmt_time_str(char (&buf)[N], std::time_t t) {
  static_assert(N >= 29, "wrong buf");
  auto s = get_local_time_str<0>(buf, t, "%a, %d %b %Y %H:%M:%S");
  size_t size = s.size();
  memcpy(buf + size, " GMT", 4);

  return {s.data(), size + 4};
}

inline std::string_view get_gmt_time_str(
    std::chrono::system_clock::time_point t) {
  static thread_local char buf[32];
  static thread_local std::chrono::seconds last_sec{};
  static thread_local size_t last_size{};

  std::chrono::system_clock::duration d = t.time_since_epoch();
  std::chrono::seconds s = std::chrono::duration_cast<std::chrono::seconds>(d);
  if (last_sec == s) {
    return {buf, last_size};
  }

  auto tm = std::chrono::system_clock::to_time_t(t);

  auto str = get_gmt_time_str(buf, tm);
  last_size = str.size();
  last_sec = s;

  return str;
}

inline std::string_view get_gmt_time_str() {
  return get_gmt_time_str(std::chrono::system_clock::now());
}

}  // namespace cinatra