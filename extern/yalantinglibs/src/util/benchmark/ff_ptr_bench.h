#pragma once
#include "ylt/util/ff_ptr.hpp"
void test_ff_ptr_visit(ff_ptr<long> val);
void test_normal_ptr_visit(long* val);
void test_ff_ptr_noop(ff_ptr<long> val);