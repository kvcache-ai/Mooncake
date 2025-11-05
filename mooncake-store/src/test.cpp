void test() {
    ylt::metric::counter_t counter;
    counter.inc();
    assert(counter.value() == 1);
}