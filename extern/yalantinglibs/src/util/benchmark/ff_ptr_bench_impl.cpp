#include "ff_ptr_bench.h"
void test_ff_ptr_visit(ff_ptr<long> val) { *val.get() += 1; };
void test_normal_ptr_visit(long* val) { *val += 1; };
void test_ff_ptr_noop(ff_ptr<long> val) {}