#!/bin/bash

model_dir=`dirname "$0"`
model_dir=`cd "$model_dir"; pwd`

pushd .  > /dev/null
cd "$model_dir"/../../storages/gcn/

# generate config
python3 cluster_config.py
if [ $? -ne 0 ]; then
  echo "Failed to generate cluster config"
  exit -1
fi

server_bin=gnn_storage_service
if [ ! -e "$server_bin" ]; then
  # top working dir
  cd "$model_dir"/../../../../
  echo "Compiling gnn_storage_service ..."
  bazel build chrono_graph/src/service:gnn_storage_service
fi

cd "$model_dir"/../../storages/gcn/
if [ ! -e "$server_bin" ]; then
  echo "Failed to build gnn_storage_service"
  exit -1
fi

# start gnn storage service
./gnn_storage_service --gnn_storage_shm_dir=/dev/shm/gnn/gcn &
service_pid=$!

sleep 1
echo -e "\nSleeping 30s to wait for gnn storage service ready ...\n"
sleep 30

cd "$model_dir"
python3 "$model_dir"/train.py

echo "Quitting gnn_storage_service (pid=$service_pid) ..."
kill $service_pid

popd  > /dev/null