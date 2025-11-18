// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <memory>
#include <sstream>
#include <string>
#include <vector>
#include "base/common/gflags.h"
#include "base/jansson/json.h"
#include "chrono_graph/src/base/config_helper.h"
#include "chrono_graph/src/base/gnn_type.h"
#include "chrono_graph/src/proto/gnn_kv_service.pb.h"

namespace chrono_graph {

// db config for each relation.
// NOT manage lifetime for theses Json configs.
struct GraphStorageDBConfig {
  std::string relation_name;
  int kv_shard_num;
  // Maximum memory Bytes for each shard
  int64 shard_memory_size;
  // Maximum key num for each shard
  int64 kv_dict_capacity;
  // Node attr API.
  base::Json *attr_op_config;
  // Edge attr API.
  base::Json *edge_attr_op_config;
  // Maximum edge num for each src node.
  int edge_max_num;
  // Node expire time in seconds, 0 = never expire.
  int64 expire_interval = 0;
  // Edge expire time in seconds, 0 = never expire.
  int edge_expire_interval_s = 0;
  // Interval of checking all edges' expiration. Will traverse and update the whole DB.
  int edge_expire_check_interval_s = 86400;
  // Replace strategy when edge list is too long.
  // 0 = pop oldest, 1 = pop random, 2 = pop smallest weight, 3 = cancel insertion.
  int oversize_replace_strategy = 0;
  // Threshold memory use ratio.
  float max_occupancy_rate = 0.9;
  // Delete some nodes (and their edge list) when memory use ratio reaches threshold.
  float reduce_occupancy_rate = 0.05;
  // 0 = delete nodes with smallest degree, 1 = delete oldest node.
  int reduce_occupancy_strategy = 0;
  std::string elst = "cpt";
  // Weight and degree decay options.
  float weight_decay_ratio = 1;
  float degree_decay_ratio = 1;
  int64 decay_interval_s = 86400;
  float delete_threshold_weight = 1e-6;

  std::string ToString() const {
    std::ostringstream oss;
    oss << "relation = " << relation_name << ", kv_shard_num = " << kv_shard_num
        << ", shard_memory_size = " << shard_memory_size << ", kv_dict_capacity = " << kv_dict_capacity
        << ", attr_op_config = " << attr_op_config->ToString()
        << ", edge_attr_op_config = " << edge_attr_op_config->ToString()
        << ", edge_max_num = " << edge_max_num << ", expire_interval = " << expire_interval
        << ", edge_expire_interval_s = " << edge_expire_interval_s
        << ", edge_expire_check_interval_s = " << edge_expire_check_interval_s
        << ", oversize_replace_strategy = " << oversize_replace_strategy
        << ", max occupancy_rate = " << max_occupancy_rate
        << ", reduce_occupancy_rate = " << reduce_occupancy_rate
        << ", reduce_occupancy_strategy = " << reduce_occupancy_strategy << ", elst = " << elst
        << ", weight_decay_ratio = " << weight_decay_ratio << ", degree_decay_ratio = " << degree_decay_ratio
        << ", decay_interval_s = " << decay_interval_s
        << ", delete_threshold_weight = " << delete_threshold_weight;
    return oss.str();
  }

  bool need_time_decay() const {
    if (weight_decay_ratio == 1 && degree_decay_ratio == 1) { return false; }
    return true;
  }

  bool need_edge_expire() const {
    if (elst.compare("cdf") == 0 || elst.compare("simple") == 0) { return false; }
    if (edge_expire_interval_s == 0) return false;
    return true;
  }
};

// Config for the service, which is shared by all relations.
// NOT manage lifetime for theses Json configs.
struct GraphStorageConfig {
  std::string service_name;
  base::Json *init_config = nullptr;
  base::Json *checkpoint_config = nullptr;
  base::Json *exporter_config = nullptr;
  base::Json *sample_pool_config = nullptr;

  std::vector<GraphStorageDBConfig> db_configs;
};

// checkpoint's db_config, for checkpoint_loader.
struct PrevDBConfig {
  std::string elst;
  int edge_max_num;
  base::Json *attr_op_config;
  base::Json *edge_attr_op_config;
  int64 delta_expire_interval;
};

}  // namespace chrono_graph
