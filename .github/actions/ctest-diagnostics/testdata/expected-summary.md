## CTest failure fixture

### Failed CTest targets
```text
1:google_test
2:native_assertion
3:tsan_test
```
### Failed test cases
```text
[  FAILED  ] FooTest.Bar
```
### Failure context
```text
3-1/3 Testing: google_test
4-[ RUN      ] FooTest.Bar
5:/workspace/foo_test.cpp:42: Failure
6-Expected equality of these values:
7-  1
8-  actual
9-    Which is: 2
10-[  FAILED  ] FooTest.Bar (5 ms)
11-[==========] 1 test from 1 test suite ran. (5 ms total)
12-[  PASSED  ] 0 tests.
13-[  FAILED  ] 1 test, listed below:
--
15-
16-2/3 Testing: native_assertion
17:native_assertion: /workspace/assert.cpp:17: int main(): Assertion 'ready' failed.
18-Subprocess aborted
19-
20-3/3 Testing: tsan_test
21:WARNING: ThreadSanitizer: data race (pid=123)
22-  Write of size 4 at 0x7b0400000800 by thread T1:
23-    #0 worker /workspace/worker.cpp:9
24-
25-SUMMARY: ThreadSanitizer: data race /workspace/worker.cpp:9 in worker
26-ThreadSanitizer: reported 1 warnings
27-
28-End testing
```

Download the CTest diagnostics artifact for the complete log and JUnit report.
