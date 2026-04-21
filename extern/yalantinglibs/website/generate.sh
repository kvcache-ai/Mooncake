#!/bin/bash
set -e

ROOT_DIR=$(pwd)
cd "$ROOT_DIR" || exit 1

# download doxygen-css warning
if [ ! -d "doxygen-awesome-css" ]; then
  echo "warning: doxygen-awesome-css does not exist here."
fi

# delete website's static dir generated before
rm -rf dist
mkdir dist

# Commands without errors : copy images from english to chinese dir
set +e # Disable -e option temporarily
cp -r docs/en/coro_rpc/images docs/zh/coro_rpc
cp -r docs/en/struct_pack/images docs/zh/struct_pack
cp -r docs/en/struct_pb/images docs/zh/struct_pb
set -e # Re-enable -e option

# generate
yarn docs:build
doxygen Doxyfile
doxygen Doxyfile_cn
echo 'Generate Done!'
