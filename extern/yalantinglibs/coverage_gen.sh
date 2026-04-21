# use llvm-cov
# you need install llvm-profdata & llvm-cov
echo "use llvm-cov"
rm -rf .coverage_llvm_cov
rm -rf build
mkdir build && cd build
export CC=clang
export CXX=clang++
cmake .. -DCOVERAGE_TEST=ON -DYLT_ENABLE_SSL=ON
make -j

# warning: test_ylt.profraw: malformed instrumentation profile data
# error: no profile can be merged
# add %m to fix the bug on ARM
# https://groups.google.com/g/llvm-dev/c/oaA58fbNMGg
# https://github.com/llvm/llvm-project/issues/50966
export LLVM_PROFILE_FILE="test_ylt-%m.profraw"
cd output/tests
./coro_io_test 
./coro_rpc_test 
./easylog_test 
./struct_pack_test 
./struct_pack_test_with_optimize 
./metric_test 
./struct_pb_test 
./reflection_test
llvm-profdata merge -sparse test_ylt-*.profraw -o test_ylt.profdata
llvm-cov show -object ./coro_io_test -object ./coro_rpc_test -object ./easylog_test -object ./struct_pack_test -object ./struct_pack_test_with_optimize -object ./metric_test -object ./struct_pb_test -object ./reflection_test -instr-profile=test_ylt.profdata -format=html -output-dir=../../.coverage_llvm_cov -ignore-filename-regex="thirdparty|standalone|src|template_switch" -show-instantiations=false
llvm-cov report -object ./coro_io_test -object ./coro_rpc_test -object ./easylog_test -object ./struct_pack_test -object ./struct_pack_test_with_optimize -object ./metric_test -object ./struct_pb_test -object ./reflection_test -instr-profile=test_ylt.profdata -ignore-filename-regex="thirdparty|standalone|src|template_switch" -show-region-summary=false
echo 'For more Detail, see:/build/.coverage_llvm_cov/index.html'

# OR, we can use gcc && lcov
# for coverage, we should install
# 1. gcc
# 2. lcov 1.16
echo "use gcc && lcov"
echo "ignore"
#rm -rf .coverage
#rm -rf build
#mkdir build && cd build
#cmake -DCOVERAGE_TEST:STRING=ENABLE -DCMAKE_BUILD_TYPE:STRING=Debug -DCMAKE_C_COMPILER:FILEPATH=gcc -DCMAKE_CXX_COMPILER:FILEPATH=g++ ..
#make -j
#cd ..
#./build/tests/test_rpc
#rm -f .coverage/lcov.info
#rm -rf .coverage/report
#mkdir ./.coverage_lcov
#lcov --rc lcov_branch_coverage=1 --capture --directory . --output-file ./.coverage_lcov/lcov.info
#genhtml --rc lcov_branch_coverage=1 ./.coverage_lcov/lcov.info --output-directory ./.coverage_lcov/report
