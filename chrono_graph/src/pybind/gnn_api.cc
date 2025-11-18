// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "chrono_graph/src/pybind/gnn_api.h"

#include <atomic>
#include <cstdint>
#include <vector>
#include "base/common/sleep.h"
#include "base/hash/city.h"
#include "chrono_graph/src/base/block_storage_api.h"
#include "chrono_graph/src/base/config_helper.h"
#include "chrono_graph/src/client/rgraph_client.h"
#include "chrono_graph/src/proto/gnn_kv_service.pb.h"

namespace chrono_graph {

GnnClientHelper GnnPyApi::helper_;

bool GnnClientHelper::AddClient(const std::string &client_name, const std::string &config_str) {
  CHECK(!client_name.empty());
  auto config = new base::Json(base::StringToJson(config_str));
  // config check
  std::string relation_name = config->GetString("relation_name", "");
  CHECK(!relation_name.empty());
  std::string service_name = config->GetString("gnn_service", "");
  CHECK(!service_name.empty());

  // set shard param for iterator.
  int iter_shard_num = config->GetInt("iter_shard_num", 1);
  int iter_shard_id = config->GetInt("iter_shard_id", 0);
  int iter_part_size = config->GetInt("iter_part_size", 1024);
  LOG(INFO) << "init client, client_name = " << client_name << ". relation_name = " << relation_name
            << ". traversal shard_num = " << iter_shard_num << ", shard_id = " << iter_shard_id
            << ", part_size = " << iter_part_size;

  ClientInfo client_info;
  auto rgraph = RGraphClient::NewInstance("remote");
  CHECK(rgraph && rgraph->Init(config)) << "failed to init rgraph client";
  client_info.graph_iter.reset(rgraph->ShardedTraversal(iter_shard_num, iter_shard_id, iter_part_size));
  client_info.rgraph_client = std::move(rgraph);
  client_map_[client_name] = client_info;
  delete config;
  return true;
}

std::vector<uint64_t> GnnClientHelper::SampleNodes(const std::string &client_name,
                                                   const int count,
                                                   const std::string &pool_name) const {
  CHECK(!client_name.empty());
  auto info = client_map_.find(client_name);
  CHECK(info != client_map_.end()) << "Client not found with name: " << client_name;

  std::vector<uint64_t> node_ids;
  node_ids.reserve(count);
  if (!count) { return node_ids; }

  auto proc_fn = [&](ProtoList<uint64_t> ids, ProtoPtrList<NodeAttr> nodes) {
    for (size_t i = 0; i < ids.size(); ++i) {
      auto &id = ids.Get(i);
      node_ids.emplace_back(id);
    }
  };

  auto rgraph_client = info->second.rgraph_client.get();
  CHECK(rgraph_client) << "no client for " << client_name;
  bool success = rgraph_client->SampleNodes({count, false, 5000, pool_name}, proc_fn);
  if (!success) {
    LOG(ERROR) << "failed to sample source nodes, sample count:" << count;
    node_ids.resize(count, 0);
  }
  return node_ids;
}

GnnClientHelper::EdgeVec GnnClientHelper::SampleNeighbors(const std::string &client_name,
                                                          const std::vector<uint64> &source_nodes,
                                                          int fanout,
                                                          int sample_type,
                                                          int padding_type,
                                                          bool sampling_without_replacement,
                                                          uint32 max_timestamp_s,
                                                          uint32 min_timestamp_s,
                                                          float min_weight_required) const {
  auto info = client_map_.find(client_name);
  CHECK(info != client_map_.end()) << client_name;

  GnnClientHelper::EdgeVec result;
  if (source_nodes.empty()) { return result; }

  RGraphClient::SampleNeighborParam param;
  param.fanout = fanout;
  param.padding_strategy = static_cast<RGraphClient::SampleNeighborParam::PaddingStrategy>(padding_type);
  param.sample_type = static_cast<SamplingParam_SamplingStrategy>(sample_type);
  param.sampling_without_replacement = sampling_without_replacement;
  param.max_timestamp_s = max_timestamp_s;
  param.min_timestamp_s = min_timestamp_s;
  param.min_weight_required = min_weight_required;
  param.timeout_ms = 5000;

  result.ids.reserve(source_nodes.size() * param.fanout);
  result.timestamps.reserve(source_nodes.size() * param.fanout);
  result.weights.reserve(source_nodes.size() * param.fanout);

  auto proc_fn = [&](const EdgeList &edge_list) {
    if (edge_list.node_id_size() == edge_list.timestamp_s_size() &&
        edge_list.node_id_size() == edge_list.edge_weight_size()) {
      for (int idx = 0; idx < edge_list.node_id_size(); ++idx) {
        result.ids.push_back(edge_list.node_id(idx));
        result.timestamps.push_back(edge_list.timestamp_s(idx));
        result.weights.push_back(edge_list.edge_weight(idx));
      }
    } else {
      for (int idx = 0; idx < edge_list.node_id_size(); ++idx) {
        result.ids.push_back(edge_list.node_id(idx));
        result.timestamps.push_back(0);
        result.weights.push_back(0.0f);
      }
    }
    CHECK_EQ(result.ids.size(), source_nodes.size() * param.fanout)
        << "sample neighbors size not match, except: " << source_nodes.size() * param.fanout << ", actual "
        << result.ids.size();
  };

  auto rgraph_client = info->second.rgraph_client.get();
  CHECK(rgraph_client) << "no client for " << client_name;
  bool success = rgraph_client->SampleNeighbors(source_nodes, param, proc_fn);
  if (!success) {
    LOG(ERROR) << "failed to sample neighbors " << client_name;
    result.ids.clear();
    result.timestamps.clear();
    result.weights.clear();
  }
  return result;
}

GnnClientHelper::BatchNodeAttr GnnClientHelper::NodesAttr(const std::string &client_name,
                                                          const std::vector<uint64_t> ids,
                                                          int int64_attr_len,
                                                          int float_attr_len) const {
  auto info = client_map_.find(client_name);
  CHECK(info != client_map_.end()) << "client_name not exist when get node attrs: " << client_name;

  base::Json attr_config(base::StringToJson("{}"));
  attr_config.set("type_name", "SimpleAttrBlock");
  attr_config.set("int_max_size", int64_attr_len);
  attr_config.set("float_max_size", float_attr_len);
  auto attr_op = std::make_unique<SimpleAttrBlock>(&attr_config);

  GnnClientHelper::BatchNodeAttr result;
  result.exist.resize(ids.size(), false);
  result.degrees.resize(ids.size(), 0);
  result.int_attrs.resize(ids.size() * int64_attr_len, 0);
  result.float_attrs.resize(ids.size() * float_attr_len, 0);

  auto proc_fn = [&](const ProtoPtrList<NodeAttr> &nodes) {
    if (nodes.size() != ids.size()) {
      LOG_EVERY_N_SEC(WARNING, 2) << "node size not match:" << nodes.size() << " vs " << ids.size();
      return false;
    }
    for (size_t i = 0; i < nodes.size(); ++i) {
      auto &node = nodes.Get(i);
      if (node.id() != 0) {
        CHECK_EQ(node.id(), ids[i]) << "node attr id not match: expect " << ids[i] << ", given " << node.id()
                                    << ", offset = " << i;
      }
      result.exist[i] = (node.id() != 0);
      result.degrees[i] = node.degree();
      for (int j = 0; j < int64_attr_len; ++j) {
        result.int_attrs[i * int64_attr_len + j] = attr_op->GetIntX(node.attr_info().data(), j);
      }
      for (int j = 0; j < float_attr_len; ++j) {
        result.float_attrs[i * float_attr_len + j] = attr_op->GetFloatX(node.attr_info().data(), j);
      }
    }
    return true;
  };

  auto rgraph_client = info->second.rgraph_client.get();
  CHECK(rgraph_client) << "no client for " << client_name;

  bool success = rgraph_client->LookupNodes(ids, proc_fn, 5000);
  if (!success) {
    LOG_EVERY_N_SEC(ERROR, 1) << "failed to get " << client_name << " node attr, size: " << ids.size();
  }
  return result;
}

bool GnnClientHelper::BatchInsert(const std::string &client_name,
                                  const BatchInsertInfo &insert_info,
                                  bool is_delete) const {
  auto info = client_map_.find(client_name);
  CHECK(info != client_map_.end()) << "batch insert given unexisted client_name: " << client_name;

  auto rgraph_client = info->second.rgraph_client.get();
  CHECK(rgraph_client) << "no client for " << client_name;

  if (is_delete) { return rgraph_client->BatchDelete(insert_info); }
  return rgraph_client->BatchInsert(insert_info);
}

std::vector<uint64_t> GnnClientHelper::IterateNodes(const std::string &client_name) {
  auto info = client_map_.find(client_name);
  CHECK(info != client_map_.end()) << "invalid client for client_name: " << client_name;
  auto iter = info->second.graph_iter.get();
  CHECK(iter) << "invalid iterator for client_name: " << client_name;
  static std::atomic_int counter{0};
  if (!iter->HasNext()) {
    LOG_EVERY_N_SEC(INFO, 60) << "one traversal loop has finished.";
    counter = 0;
    iter->Reset();
    return {};
  }
  std::vector<uint64_t> result;
  iter->Next(
      [&](ProtoList<uint64> node_list) { result.insert(result.end(), node_list.begin(), node_list.end()); });
  counter += result.size();
  return result;
}

/* Begin GnnPyApi */

bool GnnPyApi::BatchInsert(const std::string &client_name,
                           bool is_delete,
                           const std::vector<uint64_t> &src_ids,
                           int int64_attr_len,
                           int float_attr_len,
                           const std::vector<int64_t> &src_int_attrs,
                           const std::vector<float> &src_float_attrs,
                           const std::vector<uint64_t> &dst_ids,
                           const std::vector<float> &dst_weights,
                           const std::vector<int64_t> &dst_timestamps) {
  BatchInsertInfo info;
  base::Json attr_config(base::StringToJson("{}"));
  attr_config.set("type_name", "SimpleAttrBlock");
  attr_config.set("int_max_size", int64_attr_len);
  attr_config.set("float_max_size", float_attr_len);
  auto attr_op = std::make_unique<SimpleAttrBlock>(&attr_config);
  CHECK(attr_op) << "attr op init fail, config = " << attr_config.ToString();
  info.iewa = InsertEdgeWeightAct::REPLACE;
  info.src_ids.reserve(src_ids.size());
  info.src_ids.insert(info.src_ids.end(), src_ids.begin(), src_ids.end());
  info.dest_ids.reserve(dst_ids.size());
  info.dest_ids.insert(info.dest_ids.end(), dst_ids.begin(), dst_ids.end());
  info.dest_weights.reserve(dst_weights.size());
  info.dest_weights.insert(info.dest_weights.end(), dst_weights.begin(), dst_weights.end());
  info.dst_ts.reserve(dst_timestamps.size());
  info.dst_ts.insert(info.dst_ts.end(), dst_timestamps.begin(), dst_timestamps.end());
  info.src_attrs.resize(src_ids.size(), "");
  CHECK_EQ(src_int_attrs.size(), src_ids.size() * int64_attr_len)
      << "int attr size not match, expect: " << src_ids.size() * int64_attr_len << ", actual "
      << src_int_attrs.size();
  CHECK_EQ(src_float_attrs.size(), src_ids.size() * float_attr_len)
      << "float attr size not match, expect: " << src_ids.size() * float_attr_len << ", actual "
      << src_float_attrs.size();
  for (size_t i = 0; i < info.src_ids.size(); ++i) {
    std::string attr;
    attr.resize(attr_op->MaxSize());
    attr_op->InitialBlock(&attr[0], attr_op->MaxSize());
    for (int x = 0; x < int64_attr_len; ++x) {
      attr_op->SetIntX(&attr[0], x, src_int_attrs[i * int64_attr_len + x]);
    }
    for (int x = 0; x < float_attr_len; ++x) {
      attr_op->SetFloatX(&attr[0], x, src_float_attrs[i * float_attr_len + x]);
    }
    info.src_attrs[i] = std::move(attr);
  }
  return helper_.BatchInsert(client_name, info, is_delete);
}

int GnnPyApi::SampleTypeByName(const std::string &sample_type) {
  if (sample_type == "random") { return static_cast<int>(SamplingParam_SamplingStrategy_RANDOM); }
  if (sample_type == "weight") { return static_cast<int>(SamplingParam_SamplingStrategy_EDGE_WEIGHT); }
  if (sample_type == "most_recent") { return static_cast<int>(SamplingParam_SamplingStrategy_MOST_RECENT); }
  if (sample_type == "least_recent") { return static_cast<int>(SamplingParam_SamplingStrategy_LEAST_RECENT); }
  if (sample_type == "topn_weight") { return static_cast<int>(SamplingParam_SamplingStrategy_TOPN_WEIGHT); }
  NOT_REACHED() << "Invalid given sample_type: " << sample_type;
  return -1;
}

int GnnPyApi::PaddingTypeByName(const std::string &padding_type) {
  if (padding_type == "zero") { return static_cast<int>(RGraphClient::SampleNeighborParam::ZERO_FILLING); }
  if (padding_type == "self") { return static_cast<int>(RGraphClient::SampleNeighborParam::SELF_ON_EMPTY); }
  if (padding_type == "neigh_loop") {
    return static_cast<int>(RGraphClient::SampleNeighborParam::LOOP_NEIGHBOR);
  }
  if (padding_type == "no_padding") {
    return static_cast<int>(RGraphClient::SampleNeighborParam::NO_PADDING);
  }
  NOT_REACHED() << "Invalid given padding type: " << padding_type;
  return -1;
}

std::vector<uint64_t> GnnPyApi::StringKeysToIds(const std::vector<std::string> &keys) {
  std::vector<uint64_t> ids;
  for (size_t i = 0; i < keys.size(); ++i) {
    ids.emplace_back(base::CityHash64WithSeed(keys[i].data(), keys[i].size(), 0));
  }
  return ids;
}

}  // namespace chrono_graph
