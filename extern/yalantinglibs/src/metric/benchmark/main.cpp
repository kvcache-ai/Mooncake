#include "bench.hpp"

int main() {
  std::cout << "start serialize bench" << std::endl;

  std::cout << "\ndynamic summary serialize:" << std::endl;
  bench_dynamic_summary_serialize(100000);

  std::cout << "\ndynamic summary serialize(json):" << std::endl;
  bench_dynamic_summary_serialize(100000, true);

  std::cout << "\ndynamic counter with many labels serialize:" << std::endl;
  bench_dynamic_counter_serialize(100000);

  std::cout << "\ndynamic counter with many labels serialize(json):"
            << std::endl;
  bench_dynamic_counter_serialize(100000, true);

  std::cout << "\nmulti dynamic counter serialize:" << std::endl;
  bench_many_metric_serialize(100000, 10);
  std::cout << "\nmulti dynamic counter serialize(json):" << std::endl;
  bench_many_metric_serialize(100000, 10, true);

  std::cout << "\nstart write bench" << std::endl;

  std::cout << "\nstatic summary performance test:" << std::endl;
  bench_static_summary_write(1, 5s);
  bench_static_summary_write(std::thread::hardware_concurrency(), 5s);

  std::cout << "\ndynamic summary performance test:" << std::endl;
  bench_dynamic_summary_write(1, 5s);
  bench_dynamic_summary_write(std::thread::hardware_concurrency(), 5s);

  std::cout << "\nstatic counter performance test:" << std::endl;
  bench_static_counter_write(1, 5s);
  bench_static_counter_write(std::thread::hardware_concurrency(), 5s);

  std::cout << "\ndynamic counter performance test:" << std::endl;
  bench_dynamic_counter_write(1, 5s);
  bench_dynamic_counter_write(std::thread::hardware_concurrency(), 5s);

  std::cout << "\nstart write/seriailize mixed bench" << std::endl;
  std::cout << "\nstatic summary mixed test:" << std::endl;
  bench_static_summary_mixed(1, 5s);
  bench_static_summary_mixed(std::thread::hardware_concurrency(), 5s);
  std::cout << "\nstatic counter mixed test:" << std::endl;
  bench_static_counter_mixed(1, 5s);
  bench_static_counter_mixed(std::thread::hardware_concurrency(), 5s);
  std::cout << "\ndynamic summary mixed test:" << std::endl;
  bench_dynamic_summary_mixed(1, 5s);
  bench_dynamic_summary_mixed(std::thread::hardware_concurrency(), 5s);
  std::cout << "\ndynamic counter mixed test:" << std::endl;
  bench_dynamic_counter_mixed(1, 5s);
  bench_dynamic_counter_mixed(std::thread::hardware_concurrency(), 5s);
}
