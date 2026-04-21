/*
 * Copyright (c) 2023, Alibaba Group Holding Limited;
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <ylt/util/time_util.h>

#include <filesystem>
#include <ylt/easylog.hpp>

#include "doctest.h"

using namespace easylog;

std::string get_last_line(const std::string& filename) {
  std::string last_line;
  std::string temp;
  std::ifstream file(filename);
  if (file) {
    while (std::getline(file, temp)) {
      last_line = std::move(temp);
    }
  }
  return last_line;
}

TEST_CASE("test severity") {
  CHECK(easylog::severity_str(easylog::Severity::WARN) ==
        easylog::severity_str(easylog::Severity::WARNING));
  CHECK(easylog::severity_str(easylog::Severity::CRITICAL) ==
        easylog::severity_str(easylog::Severity::FATAL));
}

struct dummy_t {
  int* data() const {
    static int val = 42;
    return &val;
  }

  friend std::ostream& operator<<(std::ostream& os, const dummy_t& t);
};

std::ostream& operator<<(std::ostream& os, const dummy_t& t) {
  os << t.data();
  return os;
}

TEST_CASE("test basic") {
  std::string filename = "easylog.txt";
  std::filesystem::remove(filename);
  easylog::init_log(Severity::DEBUG, filename, false, true, 5000, 1, true);

  std::chrono::seconds seconds(3);
  std::chrono::system_clock::time_point p(seconds);
  std::chrono::system_clock::now();

  std::time_t now = std::time(0);
  std::chrono::system_clock::time_point pt =
      std::chrono::system_clock::from_time_t(now);
  ELOG_INFO << pt;
  ELOG_INFO << std::chrono::system_clock::now();

  ELOG_INFO << "test begin";
  std::this_thread::sleep_for(std::chrono::milliseconds(15));
  ELOG_INFO << "test end";

  easylog::set_console(false);
  ELOG_INFO << "no console";
  easylog::set_console(true);

  easylog::set_min_severity(Severity::WARN);
  ELOG_INFO << "info log";
  easylog::set_min_severity(Severity::DEBUG);

  ELOG_INFO << dummy_t{};
  std::unique_ptr<int> ptr(new int(42));
  ELOG_INFO << ptr.get();
  ELOG_INFO << 42 << " " << 4.5 << 'a' << Severity::DEBUG;
  ELOG_INFO << false << ", " << true;
  char buf[5] = {"test"};
  std::string_view sv = "test";
  std::string str = "test";
  std::array<char, 5> arr{"test"};
  std::stringstream ss;
  ss << "test";
  ELOG_INFO << arr << ", " << ss;

  auto id = std::this_thread::get_id();
  ELOG_INFO << buf << ", " << str << ", " << sv << ", " << id;

#if __has_include(<fmt/format.h>) || (__has_include(<format>) && !defined(__APPLE__))
  ELOGFMT(INFO, "Hello");
  ELOGFMT(INFO, "{} {}", 20, 42);
  ELOGFMT(INFO, "it is a long string test {} {}", 42, "fmt");
#endif

  ELOGV(INFO, "test");
  ELOGV(INFO, "it is a long string test %d %s", 2, "ok");
  CHECK(get_last_line(filename).rfind("test 2 ok") != std::string::npos);

  int len = 42;
  ELOGV(INFO, "rpc header data_len: %d, buf sz: %lu", len, 20);
  CHECK(get_last_line(filename).rfind("rpc header data_len: 42, buf sz: 20") !=
        std::string::npos);

  ELOG(WARN) << "warn";
  CHECK(get_last_line(filename).find("WARNING") != std::string::npos);
  ELOG(WARNING) << "warning";
  CHECK(get_last_line(filename).find("WARNING") != std::string::npos);

  ELOG(INFO) << "test log";
  easylog::flush();
  CHECK(get_last_line(filename).rfind("test log") != std::string::npos);
  ELOG_INFO << "hello "
            << "easylog";
  CHECK(get_last_line(filename).rfind("hello easylog") != std::string::npos);
  ELOGI << "same";
  CHECK(get_last_line(filename).rfind("same") != std::string::npos);

  ELOG_DEBUG << "debug log";
  ELOGD << "debug log";
  CHECK(get_last_line(filename).rfind("debug log") != std::string::npos);

  // test multiple instance
  std::string other_filename = "other.txt";
  std::filesystem::remove(other_filename);
  constexpr size_t InstanceId = 2;
  easylog::init_log<InstanceId>(Severity::DEBUG, other_filename, false, false);
  ELOG(INFO, InstanceId) << "ok in other txt";
  easylog::flush<InstanceId>();
  CHECK(get_last_line(other_filename).rfind("ok in other txt") !=
        std::string::npos);
  MELOG_INFO(InstanceId) << "test in other";
  easylog::flush<InstanceId>();
  CHECK(get_last_line(other_filename).rfind("test in other") !=
        std::string::npos);

  MELOGV(INFO, InstanceId, "it is a test %d", 42);
  easylog::flush<InstanceId>();
  CHECK(get_last_line(other_filename).rfind("it is a test 42") !=
        std::string::npos);
}

TEST_CASE("test appenders") {
  easylog::set_console(false);
  easylog::add_appender([](record_t& record) {
    std::cout << record.get_file_str() << " " << record.get_message() << "\n";
  });
  ELOG_INFO << "test1";
  easylog::set_console(true);
}

TEST_CASE("test roll files and automatic create directories") {
  std::string filename = "a/b/c/test.txt";
  std::filesystem::remove(filename);
  CHECK(!std::filesystem::exists(filename));
  constexpr size_t InstanceId = 888;
  easylog::init_log<InstanceId>(Severity::DEBUG, filename, false, true, 20, 3,
                                true);
  CHECK(std::filesystem::exists(filename));
  MELOG_INFO(InstanceId) << 42 << " " << 4.5 << 'a' << " it is a test";
  MELOG_INFO(InstanceId) << 42 << " " << 4.5 << 'a' << " it is a test";
  MELOG_INFO(InstanceId) << 42 << " " << 4.5 << 'a' << " it is a test";

  CHECK(std::filesystem::exists(filename));
  CHECK(std::filesystem::exists("a/b/c/test.1.txt"));
  CHECK(std::filesystem::exists("a/b/c/test.2.txt"));

  std::error_code ec;
  std::filesystem::remove_all("a", ec);
  std::cout << ec.message() << "\n";
}

TEST_CASE("test_severity") {
  // test severity
  std::string severity_filename = "test_severity.txt";
  std::filesystem::remove(severity_filename);
  constexpr size_t SecId = 3;
  easylog::init_log<SecId>(Severity::WARN, severity_filename, false, true, 5000,
                           1, true);

  MELOGV(ERROR, SecId, "error log can put in file.");
  MELOGV(INFO, SecId, "info log can't put in file.");
  CHECK(get_last_line(severity_filename).rfind("info log can't put in file.") ==
        std::string::npos);
  CHECK(get_last_line(severity_filename).rfind("error log can put in file.") !=
        std::string::npos);

  ELOG(INFO, SecId) << "info log can't write to file.";
  CHECK(
      get_last_line(severity_filename).rfind("info log can't write to file.") ==
      std::string::npos);
  ELOG(WARN, SecId) << "warn log can write to file.";
  CHECK(get_last_line(severity_filename).rfind("warn log can write to file.") !=
        std::string::npos);
}

TEST_CASE("test_flush") {
  // test flush
  std::string flust_filename = "test_flush.txt";
  std::filesystem::remove(flust_filename);
  constexpr size_t ThirdId = 4;
  easylog::init_log<ThirdId>(Severity::DEBUG, flust_filename, false, true, 5000,
                             1, false);

  ELOG(INFO, ThirdId) << "flush log to file.";
  CHECK(get_last_line(flust_filename).rfind("flush log to file.") ==
        std::string::npos);
  easylog::flush<ThirdId>();
  CHECK(get_last_line(flust_filename).rfind("flush log to file.") !=
        std::string::npos);
}

bool file_exists(const std::string& path) {
  std::filesystem::path file(path.c_str());
  if (std::filesystem::exists(file)) {
    return true;
  }
  return false;
}

TEST_CASE("file_number") {
  std::string number_file = "test_number.txt";
  std::filesystem::remove(number_file);
  constexpr size_t FourthId = 5;
  easylog::init_log<FourthId>(Severity::DEBUG, number_file, false, true, 23, 4,
                              true);
  MELOGV(INFO, FourthId, "append data to file1.");
  MELOGV(INFO, FourthId, "append data to file2.");
  MELOGV(INFO, FourthId, "append data to file3.");

  CHECK(file_exists("test_number.1.txt") == true);
  CHECK(file_exists("test_number.2.txt") == true);
}

TEST_CASE("file_roll") {
  std::string roll_file = "roll_file.txt";
  std::filesystem::remove(roll_file);
  constexpr size_t FifthId = 6;
  easylog::init_log<FifthId>(Severity::DEBUG, roll_file, false, true, 10, 1,
                             true);
  MELOGV(INFO, FifthId, "should be covered string.");
  MELOGV(INFO, FifthId, "the string that should be saved in the file.");
  CHECK(get_last_line(roll_file).rfind(
            "he string that should be saved in the file.") !=
        std::string::npos);
}

TEST_CASE("async_file") {
  std::string async_file = "async_file.txt";
  std::filesystem::remove(async_file);
  constexpr size_t Id = 7;
  easylog::init_log<Id>(Severity::DEBUG, async_file, true, true, 10, 1, true);
  MELOGV(INFO, Id, "file 7 should be covered string.");
  MELOGV(INFO, Id, "the string that should be saved in the file 7.");
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  easylog::stop_async_log<Id>();
  auto s = get_last_line(async_file);
  CHECK(s.rfind("he string that should be saved in the file 7.") !=
        std::string::npos);
}
