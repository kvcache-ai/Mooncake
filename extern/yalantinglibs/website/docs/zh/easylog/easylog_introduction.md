# easylog 简介

easylog 是C++17 实现的高性能易用的 header only 日志库，它支持主流的3 中流输出模式：std::cout模式、sprintf模式和fmt::format/std::format 模式。主要特点:

- 支持3 种输出流
- 同步写日志
- 异步写日志
- 日志输出到控制台
- 日志输出到文件
- waring 以上级别的日志通过颜色区分
- 设置文件大小
- 设置文件数
- 手动flush
- 创建多个日志实例

# 基本用法

```cpp
// 流式输出
ELOG_INFO << "easylog " << 42;
ELOG(INFO) << "easylog " << 42;

// printf输出
ELOGV(INFO, "easylog %d", 42);

// fmt::format/std::format 输出
#if __has_include(<fmt/format.h>) || __has_include(<format>)
  ELOGFMT(INFO, "easylog {}", 42);
#endif
```

流式输出的ELOG_INFO 等价于ELOG(INFO)。 

easylog 定义了如下日志级别：
```cpp
enum class Severity {
  NONE,
  TRACE,
  DEBUG,
  INFO,
  WARN,
  WARNING = WARN,
  ERROR,
  CRITICAL,
  FATAL = CRITICAL,
};
```
用户可以根据需要输出这几种级别的日志，默认的日志级别是DEBUG，其中WARN 和 WARNING 是等价的，CRITICAL 和FATAL 是等价的，主要是为了适配其它日志库。

可以通过API ```easylog::set_min_severity(Severity severity)``` 来设置最小的日志级别，如把日志级别设置为WARN 之后，将会只输出WARNING 及以上级别的日志。

easylog 默认会将日志输出到控制台，如果不希望输出到控制台则通过API ```void set_console(bool enable)``` 将它禁用即可。

easylog 默认是同步输出日志，同步输出日志的性能没有异步日志的性能好，可以通过API ```easylog::void set_async(bool enable)```启用异步模式来输出日志以获得最好的性能。

easylog 默认不会每次flush 日志，可以通过调用API ```easylog::flush()```手动将日志内容flush 到日志文件。

# 输出到文件
easylog 默认会将日志输出到控制台，如果希望easylog 将日志输出到文件则需要调用easylog::init 接口做初始化。

```cpp
/// \param Id 日志实例的唯一id，默认为0
/// \param min_severity 最低的日志级别
/// \param filename 日志文件名称
/// \param async 是否为异步模式
/// \param enable_console 是否输出到控制台
/// \param max_file_size 日志文件最大size
/// \param max_files 最大日志文件数
/// \param flush_every_time 是否每次flush 日志
template <size_t Id = 0>
void init_log(Severity min_severity, const std::string &filename = "",
                     bool async = true, bool enable_console = true,
                     size_t max_file_size = 0, size_t max_files = 0,
                     bool flush_every_time = false);
```

如果日志文件大小达到了max_file_size 旧的日志文件将会被覆盖，如果max_files 设置为1，当大小达到了max_file_size 时，日志文件会被覆盖。

```cpp
easylog::init_log(Severity::DEBUG, filename, false, true, 5, 3);
ELOG_INFO << "long string test, long string test";
ELOG_INFO << "long string test, long string test";
ELOG_INFO << "long string test, long string test";
```
这个代码将产生3 个日志文件(easylog.txt, easylog.1.txt, easylog.2.txt)，当日志size达到设定的最大size 时，将会把easylog.txt 重命名为easylog.1.txt，接着创建新文件easylog.txt，日志内容会写到最新的easylog.txt 文件里。如果日志文件数达到了最大的max_files 3 时，最老的日志文件easylog.3.txt 会被删除，之前的easylog.txt 和 easylog.1.txt 会被重命名为easylog.1.txt 和 easylog.2.txt，然后创建最新的日志文件easylog.txt 用于写日志。

日志文件覆盖的原则是，当达到最大file size 时就要创建新的日志文件写日志，如果文件数量达到了最大size，则删掉最旧的日志文件，把之前的日志文件重命名重命名之后再创建最新的日志文件写日志，保持始终最多只有max_files 个日志文件。

# 创建多个日志实例

默认的日志实例只有一个，如果希望创建更多日志实例，则通过唯一的日志ID 来创建新的日志实例。

```cpp
constexpr size_t Id = 2;
easylog::init_log<Id>(Severity::DEBUG, "testlog.txt");
MELOG_INFO(Id) << "test";
MELOGV(INFO, Id, "it is a test %d", 42);

constexpr size_t Id3 = 3;
easylog::init_log<Id3>(Severity::DEBUG, "testlog3.txt");
MELOG_INFO(Id3) << "test";
MELOGV(INFO, Id3, "it is a test %d", 42);
```

# 线程模型
easylog 是线程安全的，无论是同步模式还是异步模式。同步模式和异步模式的区别在于是否使用了后台线程去写日志。

## 同步模式
同步模式是指日志格式化、输出控制台和写文件从头到尾都是在调用者线程中完成，要保证多线程写日志的安全性，写文件和写控制台的时候会有一个锁来保证安全性。

## 异步模式
异步模式是指日志格式化、输出控制台和写文件从头到尾都在后台线程中。异步模式下后台线程只有一个，它不停的从一个无锁队列中取出日志信息进行处理，
由于是单线程处理，所以异步模式下日志的生成和写文件是不需要加锁的。

异步模式无疑问比同步模式性能更好，因此一般情况下应该优先使用异步模式去写日志。