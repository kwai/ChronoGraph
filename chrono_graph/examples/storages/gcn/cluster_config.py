# coding=utf-8

# Copyright (c) 2025 Kuaishou Technology
# SPDX-License-Identifier: MIT

import copy
import json
import os

"""
Each relation corresponds to a kv table.
The problem to solve is: Given a host list and some kv tables, how to assign the kv tables to the hosts.

Run this script with python3, with host_shard.json as input, it will generate config.json for each shard.
"""

###############  BEGIN user config ###################

experiment_name = "gcn_demo"
shm_size = (1 << 30) * 32

dir_path = os.path.dirname(os.path.realpath(__file__))
part_configs = [
    {
        "shard_num": 1,
        "dbs": ["A2A"],
        "checkpoint": {
            "checkpoint_path":  dir_path + "/" + experiment_name + "_checkpoints",
            "reserve_days": 5
        },
    }
]

# key = relation name, value = config.
relations = {
    "A2A": {
        "total_memory": (1 << 30) * 32,
        "key_size": 1000000,
        "edge_max_num": 500,
        "oversize_replace_strategy": 2,
        "expire_interval": 0,
        "elst": "cpt",
    }
}

###############  END user config ###################

OUTPUT_JSON_CONFIG = dir_path + "/config.json"


def error_exit(msg):
  backup_debug_msg = {"error_msg": msg}
  with open(OUTPUT_JSON_CONFIG, "w", encoding="utf8") as f:
    json.dump(backup_debug_msg, f, ensure_ascii=False)
  exit()


# affect parallel writing
memkv_shard_num = 4


def translate_db_config(relation_configs):
  for k, v in relation_configs.items():
    v["relation_name"] = k
    v["kv_dict_size"] = int(v["key_size"] / memkv_shard_num)
    v["kv_size"] = int(v["total_memory"] / memkv_shard_num)
    v["kv_shard_num"] = memkv_shard_num
    v.pop("key_size")
    v.pop("total_memory")
  return relation_configs


def item_name(dbs, shard_id):
  prefix = "grpc_gnn_{}".format(experiment_name)
  return "-".join([prefix] + dbs + [str(shard_id)])


def rpc_service_name(dbs):
  prefix = "grpc_gnn_{}".format(experiment_name)
  return "-".join([prefix] + dbs)


def main():
  with open(dir_path + "/host_shard.json", "r") as f:
    host_shard = json.load(f)
  if not host_shard:
    error_exit("host shard load fail")

  if experiment_name is None or experiment_name == "":
    error_exit("experiment_name not set")

  total_shard_num = host_shard["shard_num"]
  total_part_shard_num = sum([item["shard_num"] if "shard_num" in item else 1 for item in part_configs])
  if total_shard_num != total_part_shard_num:
    error_exit(f"not matched shard nums: part shard = {total_part_shard_num}, total shard = {total_shard_num}")

  for part in part_configs:
    shard = part['shard_num']
    mem = sum([relations[db]["total_memory"] for db in part["dbs"]])
    if mem / shard > shm_size:
      error_exit(f"part with db: {part['dbs']} mem oversize: limit {shm_size}, config: {mem}")

  # key = shard_index, v = host_name
  shard_hosts = {}
  for item in host_shard["hosts"]:
    shard_id = item["shard"]
    if shard_id >= total_part_shard_num:
      error_exit(f"shard id exceeds limit = {shard_id}, total shard num = {total_part_shard_num}")
    if shard_id in shard_hosts:
      shard_hosts[shard_id].append(item["host"])
      continue
    shard_hosts[shard_id] = [item["host"]]

  host_service_config = {}
  final_config = {}
  final_config["db_list"] = translate_db_config(relations)
  final_config["service_config"] = {"default_rpc_thread_num": 64}

  global_shard_id = -1
  for item in part_configs:
    shard_num = item["shard_num"] if "shard_num" in item else 1
    for shard_id in range(shard_num):
      global_shard_id += 1
      if global_shard_id not in shard_hosts:
        continue
      for host in shard_hosts[global_shard_id]:
        host_service_config[host] = item_name(item["dbs"], shard_id)
        cp_item = copy.deepcopy(item)
        cp_item["service_name"] = rpc_service_name(item["dbs"])
        cp_item["exp_name"] = experiment_name
        cp_item["shard_id"] = shard_id
        cp_item["shard_num"] = shard_num
        final_config["service_config"].update({item_name(item["dbs"], shard_id): cp_item})

  final_config["host_service_config"] = host_service_config

  with open(OUTPUT_JSON_CONFIG, "w") as f:
    json.dump(final_config, f, indent=2)


if __name__ == "__main__":
  main()
