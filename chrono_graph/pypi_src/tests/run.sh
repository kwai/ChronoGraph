#!/bin/bash

test_dir=`dirname "$0"`
test_dir=`cd "$test_dir"; pwd`

pushd .  > /dev/null

# top working dir
cd "$test_dir"/../../../

pybind_lib=bazel-bin/chrono_graph/src/pybind/libgnn_pybind.so
# compile python library if need
if [ ! -f "$pybind_lib" ]; then
  echo "Compiling libgnn_pybind.so ..."
  bazel build chrono_graph/src/pybind:libgnn_pybind
fi

if [ ! -f "$pybind_lib" ]; then
  echo "Failed to build libgnn_pybind.so"
  exit -1
fi

# compile server if gnn_storage_service is a broken link
server_bin=chrono_graph/examples/storages/pytest/gnn_storage_service
if [ ! -e "$server_bin" ]; then
  echo "Compiling gnn_storage_service ..."
  bazel build chrono_graph/src/service:gnn_storage_service
fi

if [ ! -e "$server_bin" ]; then
  echo "Failed to build gnn_storage_service"
  exit -1
fi

# generate config
python3 chrono_graph/examples/storages/pytest/cluster_config.py
if [ $? -ne 0 ]; then
  echo "Failed to generate cluster config"
  exit -1
fi

# start gnn storage service
cd chrono_graph/examples/storages/pytest
./gnn_storage_service --gnn_storage_shm_dir=/dev/shm/gnn/pytest &
service_pid=$!

# sleep 3s to wait for the storage service
sleep 3

# run tests
cd $test_dir
pytest -s pybind_test.py

echo "Quitting gnn_storage_service (pid=$service_pid) ..."
kill $service_pid

popd  > /dev/null