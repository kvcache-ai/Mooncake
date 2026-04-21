#pragma once
#include <array>
#include <filesystem>
#include <string_view>
#include <unordered_map>
namespace fs = std::filesystem;
using namespace std::string_view_literals;

namespace cinatra {
enum class http_method {
  NIL = 0,
  GET,
  HEAD,
  POST,
  PUT,
  TRACE,
  PATCH,
  CONNECT,
  OPTIONS,
  DEL,
};
enum class content_encoding { gzip, deflate, br, none };
constexpr inline auto GET = http_method::GET;
constexpr inline auto POST = http_method::POST;
constexpr inline auto DEL = http_method::DEL;
constexpr inline auto HEAD = http_method::HEAD;
constexpr inline auto PUT = http_method::PUT;
constexpr inline auto CONNECT = http_method::CONNECT;
#ifdef TRACE
#undef TRACE
constexpr inline auto TRACE = http_method::TRACE;
#endif
constexpr inline auto OPTIONS = http_method::OPTIONS;

inline constexpr std::string_view method_name(http_method mthd) {
  switch (mthd) {
    case cinatra::http_method::DEL:
      return "DELETE"sv;
    case cinatra::http_method::GET:
      return "GET"sv;
    case cinatra::http_method::HEAD:
      return "HEAD"sv;
    case cinatra::http_method::POST:
      return "POST"sv;
    case cinatra::http_method::PUT:
      return "PUT"sv;
    case cinatra::http_method::PATCH:
      return "PATCH"sv;
    case cinatra::http_method::CONNECT:
      return "CONNECT"sv;
    case cinatra::http_method::OPTIONS:
      return "OPTIONS"sv;
    case cinatra::http_method::TRACE:
      return "TRACE"sv;
    default:
      return "NIL"sv;
  }
}

inline constexpr std::array<int, 20> method_table = {
    3, 1, 9, 0, 0, 0, 4, 5, 0, 0, 8, 0, 0, 0, 2, 0, 0, 0, 6, 7};

inline constexpr http_method method_type(std::string_view mthd) {
  int index = ((mthd[0] & ~0x20) ^ ((mthd[1] + 1) & ~0x20)) % 20;
  return (http_method)method_table[index];
}

enum class transfer_type { CHUNKED, ACCEPT_RANGES };

enum class content_type {
  string,
  multipart,
  urlencoded,
  chunked,
  octet_stream,
  websocket,
  unknown,
};

enum class req_content_type {
  html,
  json,
  text,
  string,
  multipart,
  ranges,
  form_url_encode,
  octet_stream,
  xml,
  none
};

constexpr inline auto HTML = req_content_type::html;
constexpr inline auto JSON = req_content_type::json;
constexpr inline auto TEXT = req_content_type::string;
constexpr inline auto RANGES = req_content_type::ranges;
constexpr inline auto NONE = req_content_type::none;

inline const std::string CSESSIONID = "CSESSIONID";

const static inline std::string CRCF = "\r\n";
const static inline std::string TWO_CRCF = "\r\n\r\n";
const static inline std::string BOUNDARY = "--CinatraBoundary2B8FAF4A80EDB307";
const static inline std::string MULTIPART_END =
    CRCF + "--" + BOUNDARY + "--" + CRCF;
constexpr std::string_view LAST_CHUNK = "0\r\n";
constexpr std::string_view CINATRA_HOST_SV = "Server: cinatra\r\n";
constexpr std::string_view TRANSFER_ENCODING_SV =
    "Transfer-Encoding: chunked\r\n";
constexpr std::string_view CONTENT_LENGTH_SV = "Content-Length: ";
constexpr std::string_view ZERO_LENGTH_SV = "Content-Length: 0\r\n";
constexpr std::string_view DATE_SV = "Date: ";
constexpr std::string_view CONN_KEEP_SV = "Connection: keep-alive\r\n";
constexpr std::string_view CONN_CLOSE_SV = "Connection: close\r\n";
constexpr std::string_view COLON_SV = ": ";

struct chunked_result {
  std::error_code ec;
  bool eof = false;
  std::string_view data;
};

struct part_head_t {
  std::error_code ec;
  std::string name;
  std::string filename;
};

enum resp_content_type {
  css,
  csv,
  htm,
  html,
  js,
  mjs,
  txt,
  vtt,
  apng,
  avif,
  bmp,
  gif,
  png,
  svg,
  webp,
  ico,
  tif,
  tiff,
  jpg,
  jpeg,
  mp4,
  mpeg,
  webm,
  mp3,
  mpga,
  weba,
  wav,
  otf,
  ttf,
  woff,
  woff2,
  x7z,
  atom,
  pdf,
  json,
  rss,
  tar,
  xht,
  xhtml,
  xslt,
  xml,
  gz,
  zip,
  wasm,
  unknown
};

constexpr std::array<std::string_view, 45> content_type_arr{
    "Content-Type: text/css\r\n",
    "Content-Type: text/csv\r\n",
    "Content-Type: text/html\r\n",
    "Content-Type: text/html\r\n",
    "Content-Type: text/javascript\r\n",
    "Content-Type: text/javascript\r\n",
    "Content-Type: text/plain\r\n",
    "Content-Type: text/vtt\r\n",
    "Content-Type: image/apng\r\n",
    "Content-Type: image/avif\r\n",
    "Content-Type: image/bmp\r\n",
    "Content-Type: image/gif\r\n",
    "Content-Type: image/png\r\n",
    "Content-Type: image/svg+xml\r\n",
    "Content-Type: image/webp\r\n",
    "Content-Type: image/x-icon\r\n",
    "Content-Type: image/tiff\r\n",
    "Content-Type: image/tiff\r\n",
    "Content-Type: image/jpeg\r\n",
    "Content-Type: image/jpeg\r\n",
    "Content-Type: video/mp4\r\n",
    "Content-Type: video/mpeg\r\n",
    "Content-Type: video/webm\r\n",
    "Content-Type: audio/mp3\r\n",
    "Content-Type: audio/mpeg\r\n",
    "Content-Type: audio/webm\r\n",
    "Content-Type: audio/wave\r\n",
    "Content-Type: font/otf\r\n",
    "Content-Type: font/ttf\r\n",
    "Content-Type: font/woff\r\n",
    "Content-Type: font/woff2\r\n",
    "Content-Type: application/x-7z-compressed\r\n",
    "Content-Type: application/atom+xml\r\n",
    "Content-Type: application/pdf\r\n",
    "Content-Type: application/json\r\n",
    "Content-Type: application/rss+xml\r\n",
    "Content-Type: application/x-tar\r\n",
    "Content-Type: application/xhtml+xml\r\n",
    "Content-Type: application/xhtml+xml\r\n",
    "Content-Type: application/xslt+xml\r\n",
    "Content-Type: application/xml\r\n",
    "Content-Type: application/gzip\r\n",
    "Content-Type: application/zip\r\n",
    "Content-Type: application/wasm\r\n",
    "Content-Type: unknown\r\n"};

template <size_t N>
inline constexpr std::string_view get_content_type() {
  if constexpr (N > 43) {
    return content_type_arr[44];
  }
  else {
    return content_type_arr[N];
  }
}

inline std::unordered_map<std::string, std::string> g_content_type_map = {
    {".css", "text/css"},
    {".csv", "text/csv"},
    {".htm", "text/html"},
    {".html", "text/html"},
    {".js", "text/javascript"},
    {".mjs", "text/javascript"},
    {".txt", "text/plain"},
    {".vtt", "text/vtt"},
    {".apng", "image/apng"},
    {".avif", "image/avif"},
    {".bmp", "image/bmp"},
    {".gif", "image/gif"},
    {".png", "image/png"},
    {".svg", "image/svg+xml"},
    {".webp", "image/webp"},
    {".ico", "image/x-icon"},
    {".tif", "image/tiff"},
    {".tiff", "image/tiff"},
    {".jpg", "image/jpeg"},
    {".jpeg", "image/jpeg"},
    {".mp4", "video/mp4"},
    {".mpeg", "video/mpeg"},
    {".webm", "video/webm"},
    {".mp3", "audio/mp3"},
    {".mpga", "audio/mpeg"},
    {".weba", "audio/webm"},
    {".wav", "audio/wave"},
    {".otf", "font/otf"},
    {".ttf", "font/ttf"},
    {".woff", "font/woff"},
    {".woff2", "font/woff2"},
    {".7z", "application/x-7z-compressed"},
    {".atom", "application/atom+xml"},
    {".pdf", "application/pdf"},
    {".json", "application/json"},
    {".rss", "application/rss+xml"},
    {".tar", "application/x-tar"},
    {"xht", "application/xhtml+xml"},
    {"xhtml", "application/xhtml+xml"},
    {"xslt", "application/xslt+xml"},
    {"xml", "application/xml"},
    {"gz", "application/gzip"},
    {"zip", "application/zip"},
    {"wasm", "application/wasm"}};
struct NonSSL {};
struct SSL {};

enum class time_format {
  http_format,
  utc_format,
  utc_without_punctuation_format
};
namespace time_util {
/*
  IMF-fixdate = day-name "," SP date1 SP time-of-day SP GMT
  day-name     = %s"Mon" / %s"Tue" / %s"Wed"
               / %s"Thu" / %s"Fri" / %s"Sat" / %s"Sun"

  date1        = day SP month SP year
               ; e.g., 02 Jun 1982

  day          = 2DIGIT
  month        = %s"Jan" / %s"Feb" / %s"Mar" / %s"Apr"
               / %s"May" / %s"Jun" / %s"Jul" / %s"Aug"
               / %s"Sep" / %s"Oct" / %s"Nov" / %s"Dec"
  year         = 4DIGIT

  GMT          = %s"GMT"

  time-of-day  = hour ":" minute ":" second
               ; 00:00:00 - 23:59:60 (leap second)

  hour         = 2DIGIT
  minute       = 2DIGIT
  second       = 2DIGIT
*/
enum component_of_time_format {
  day_name,
  day,
  month_name,
  month,
  year,
  hour,
  minute,
  second,
  second_decimal_part,
  SP,
  comma,
  colon,
  hyphen,
  dot,
  GMT,
  T,
  Z,
  ending
};

inline constexpr std::array<int, 17> month_table = {
    11, 4, -1, 7, -1, -1, -1, 0, 6, 3, 5, 2, 10, 8, -1, 9, 1};

inline constexpr std::array<int, 17> week_table = {
    2, 4, 3, 1, -1, -1, -1, 6, -1, -1, -1, -1, 0, -1, -1, 5, -1};

// Mon, 02 Jan 2006 15:04:05 GMT
inline constexpr std::array<component_of_time_format, 32> http_time_format{
    component_of_time_format::day_name, component_of_time_format::comma,
    component_of_time_format::SP,       component_of_time_format::day,
    component_of_time_format::SP,       component_of_time_format::month_name,
    component_of_time_format::SP,       component_of_time_format::year,
    component_of_time_format::SP,       component_of_time_format::hour,
    component_of_time_format::colon,    component_of_time_format::minute,
    component_of_time_format::colon,    component_of_time_format::second,
    component_of_time_format::SP,       component_of_time_format::GMT,
    component_of_time_format::ending};
// 2006-01-02T15:04:05.000Z
inline constexpr std::array<component_of_time_format, 32> utc_time_format{
    component_of_time_format::year,
    component_of_time_format::hyphen,
    component_of_time_format::month,
    component_of_time_format::hyphen,
    component_of_time_format::day,
    component_of_time_format::T,
    component_of_time_format::hour,
    component_of_time_format::colon,
    component_of_time_format::minute,
    component_of_time_format::colon,
    component_of_time_format::second,
    component_of_time_format::dot,
    component_of_time_format::second_decimal_part,
    component_of_time_format::Z,
    component_of_time_format::ending};
// 20060102T150405000Z
inline constexpr std::array<component_of_time_format, 32>
    utc_time_without_punctuation_format{
        component_of_time_format::year,
        component_of_time_format::month,
        component_of_time_format::day,
        component_of_time_format::T,
        component_of_time_format::hour,
        component_of_time_format::minute,
        component_of_time_format::second,
        component_of_time_format::second_decimal_part,
        component_of_time_format::Z,
        component_of_time_format::ending};
constexpr inline int len_of_http_time_format =
    3 + 1 + 1 + 2 + 1 + 3 + 1 + 4 + 1 + 2 + 1 + 2 + 1 + 2 + 1 + 3;
// ignore second_decimal_part
constexpr inline int len_of_utc_time_format =
    4 + 1 + 2 + 1 + 2 + 1 + 2 + 1 + 2 + 1 + 2 + 1 + 0 + 1;
// ignore second_decimal_part
constexpr inline int len_of_utc_time_without_punctuation_format =
    4 + 2 + 2 + 1 + 2 + 2 + 2 + 0 + 1;
constexpr inline std::int64_t absolute_zero_year = -292277022399;
constexpr inline std::int64_t days_per_100_years = 365 * 100 + 24;
constexpr inline std::int64_t days_per_400_years = 365 * 400 + 97;
constexpr inline std::int64_t days_per_4_years = 365 * 4 + 1;
constexpr inline std::int64_t seconds_per_minute = 60;
constexpr inline std::int64_t seconds_per_hour = 60 * seconds_per_minute;
constexpr inline std::int64_t seconds_per_day = 24 * seconds_per_hour;
constexpr inline std::int64_t seconds_per_week = 7 * seconds_per_day;
constexpr inline std::int64_t internal_year = 1;
constexpr inline std::int64_t absolute_to_internal =
    (absolute_zero_year - internal_year) *
    std::int64_t(365.2425 * seconds_per_day);
constexpr inline std::int64_t unix_to_internal =
    (1969ll * 365 + 1969 / 4 - 1969 / 100 + 1969 / 400) * seconds_per_day;
constexpr inline std::int64_t internal_to_unix = -unix_to_internal;
constexpr inline std::array<std::int32_t, 13> days_before = {
    0,
    31,
    31 + 28,
    31 + 28 + 31,
    31 + 28 + 31 + 30,
    31 + 28 + 31 + 30 + 31,
    31 + 28 + 31 + 30 + 31 + 30,
    31 + 28 + 31 + 30 + 31 + 30 + 31,
    31 + 28 + 31 + 30 + 31 + 30 + 31 + 31,
    31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30,
    31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31,
    31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30,
    31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30 + 31,
};
}  // namespace time_util
}  // namespace cinatra
