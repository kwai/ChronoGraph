// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "chrono_graph/src/client/sharding_rgraph_client.h"

#include "base/common/timer.h"
#include "chrono_graph/src/base/config_helper.h"

namespace chrono_graph {

bool RemoteShardingRGraphClient::Init(const base::Json *config) {
  RGraphClient::Init(config);
  service_name_ = GetStringConfigSafe(config, "gnn_service");
  CHECK(!service_name_.empty()) << config->ToString();
  response_id_batch_size_ = config->GetInt("response_id_batch_size", 10240);
  int shard_num = config->GetInt("shard_num", 0);
  if (shard_num == 0) { LOG(ERROR) << "shard_num should be set explicitly in config!"; }
  CHECK_GT(shard_num, 0) << "shard_num should > 0, config = " << config->ToString();
  shard_clients_.resize(shard_num);
  for (size_t i = 0; i < shard_num; ++i) {
    std::string shard_id = "s" + std::to_string(i);
    base::Json shard_config(base::StringToJson(config->ToString()));
    shard_config.set("shard_id", shard_id);
    shard_clients_[i] = std::make_unique<RemoteRGraphClient>();
    CHECK(shard_clients_[i]->Init(&shard_config)) << "shard " << i << " client init fail";
  }
  return true;
}

bool RemoteShardingRGraphClient::BatchInsert(const BatchInsertInfo &batch_insert_info) {
  if (sharding_num() == 1) { return shard_clients_[0]->BatchInsert(batch_insert_info); }
  base::Timer timer;
  thread_local std::vector<BatchInsertInfo> sharding_infos;
  sharding_infos.resize(sharding_num());
  for (size_t i = 0; i < sharding_num(); ++i) {
    sharding_infos[i].Clear();
    sharding_infos[i].iewa = batch_insert_info.iewa;
    sharding_infos[i].timeout_ms = batch_insert_info.timeout_ms;
  }
  bool has_attr = !batch_insert_info.src_attrs.empty();
  bool has_edge = !batch_insert_info.dest_ids.empty();
  bool has_weight = !batch_insert_info.dest_weights.empty();
  bool has_ts = !batch_insert_info.dst_ts.empty();
  bool has_dest_attr = !batch_insert_info.dest_attrs.empty();
  bool has_expire_ts_update_flags = !batch_insert_info.expire_ts_update_flags.empty();
  bool has_use_item_ts_for_expires = !batch_insert_info.use_item_ts_for_expires.empty();
  for (size_t i = 0; i < batch_insert_info.src_ids.size(); ++i) {
    auto src_id = batch_insert_info.src_ids[i];
    sharding_infos[sharding_pos(src_id)].src_ids.push_back(src_id);
    if (has_attr) {
      sharding_infos[sharding_pos(src_id)].src_attrs.push_back(batch_insert_info.src_attrs[i]);
    }
    if (has_edge) { sharding_infos[sharding_pos(src_id)].dest_ids.push_back(batch_insert_info.dest_ids[i]); }
    if (has_weight) {
      sharding_infos[sharding_pos(src_id)].dest_weights.push_back(batch_insert_info.dest_weights[i]);
    }
    if (has_ts) { sharding_infos[sharding_pos(src_id)].dst_ts.push_back(batch_insert_info.dst_ts[i]); }
    if (has_dest_attr) {
      sharding_infos[sharding_pos(src_id)].dest_attrs.push_back(batch_insert_info.dest_attrs[i]);
    }
    if (has_expire_ts_update_flags) {
      sharding_infos[sharding_pos(src_id)].expire_ts_update_flags.push_back(
          batch_insert_info.expire_ts_update_flags[i]);
    }
    if (has_use_item_ts_for_expires) {
      sharding_infos[sharding_pos(src_id)].use_item_ts_for_expires.push_back(
          batch_insert_info.use_item_ts_for_expires[i]);
    }
  }
  timer.AppendCostMs("sharding insert build request");
  for (size_t i = 0; i < sharding_num(); ++i) {
    bool result = shard_clients_[i]->BatchInsert(sharding_infos[i]);
    if (!result) { LOG_EVERY_N_SEC(ERROR, 1) << "BatchInsert failed for shard " << i; }
  }
  timer.AppendCostMs("sharding insert request");
  LOG_EVERY_N_SEC(INFO, 10) << "service " << service_name_ << ", cost:" << timer.display();
  return true;
}

bool RemoteShardingRGraphClient::BatchDelete(const BatchInsertInfo &batch_insert_info) {
  if (sharding_num() == 1) { return shard_clients_[0]->BatchDelete(batch_insert_info); }
  base::Timer timer;
  thread_local std::vector<BatchInsertInfo> sharding_infos;
  sharding_infos.resize(sharding_num());
  for (size_t i = 0; i < sharding_num(); ++i) {
    sharding_infos[i].Clear();
    sharding_infos[i].timeout_ms = batch_insert_info.timeout_ms;
  }
  bool has_edge = !batch_insert_info.dest_ids.empty();
  for (size_t i = 0; i < batch_insert_info.src_ids.size(); ++i) {
    auto src_id = batch_insert_info.src_ids[i];
    sharding_infos[sharding_pos(src_id)].src_ids.push_back(src_id);
    if (has_edge) { sharding_infos[sharding_pos(src_id)].dest_ids.push_back(batch_insert_info.dest_ids[i]); }
  }
  timer.AppendCostMs("sharding delete build request");
  for (size_t i = 0; i < sharding_num(); ++i) {
    bool result = shard_clients_[i]->BatchDelete(sharding_infos[i]);
    if (!result) { LOG_EVERY_N_SEC(ERROR, 1) << "BatchDelete failed for shard " << i; }
  }
  timer.AppendCostMs("sharding delete request");
  LOG_EVERY_N_SEC(INFO, 10) << "service " << service_name_ << ", cost:" << timer.display();
  return true;
}

bool RemoteShardingRGraphClient::GetEdgesInfo(const IdVec &src_ids,
                                              const IdVec &dest_ids,
                                              EdgesProcFn proc_fn,
                                              const int64 timeout_ms) {
  NOT_REACHED() << "not implemented yet";
  return true;
}

bool RemoteShardingRGraphClient::SampleNeighbors(const IdVec &source_nodes,
                                                 SampleNeighborParam param,
                                                 NodesProcFn proc_fn) {
  if (source_nodes.empty()) {
    proc_fn(chrono_graph::SampleNeighborResponse().edge_list());
    return true;
  }

  base::Timer timer;

  EdgeList results;
  ResizeEdgeList(source_nodes, param, &results);

  // ids in one shard may be splitted to serveral requests, according to response_id_batch_size_
  // So suppose there are N shards, each shard may has M requests, each request has at most
  // response_id_batch_size_ ids, which result in this triple-nested vector here.
  thread_local std::vector<std::vector<IdVec>> sharding_ids;
  thread_local std::vector<std::vector<std::vector<size_t>>> sharding_offsets;
  thread_local std::vector<size_t> sharding_id_sizes;
  sharding_ids.resize(sharding_num());
  sharding_offsets.resize(sharding_num());
  sharding_id_sizes.resize(sharding_num());
  for (size_t i = 0; i < sharding_num(); ++i) {
    sharding_ids[i].clear();
    sharding_offsets[i].clear();
    sharding_id_sizes[i] = 0;
  }

  for (int offset = 0; offset < source_nodes.size(); ++offset) {
    auto id = source_nodes[offset];

    int shard = sharding_pos(id);
    int src_id_batch_size = response_id_batch_size_ / param.fanout;
    if (src_id_batch_size < 1) src_id_batch_size = 1;
    if (sharding_id_sizes[shard] % src_id_batch_size == 0) {
      sharding_ids[shard].emplace_back(IdVec());
      sharding_offsets[shard].emplace_back(std::vector<size_t>());
    }
    int sub_id = sharding_id_sizes[shard] / src_id_batch_size;
    sharding_ids[shard][sub_id].push_back(id);
    sharding_offsets[shard][sub_id].push_back(offset * param.fanout);
    sharding_id_sizes[shard]++;
  }

  timer.AppendCostMs("build request");

  bool success = true;
  for (int sharding_id = 0; sharding_id < sharding_num(); ++sharding_id) {
    for (int sub_id = 0; sub_id < sharding_ids[sharding_id].size(); ++sub_id) {
      const IdVec *const ids_ptr = &(sharding_ids[sharding_id][sub_id]);
      const std::vector<size_t> *const offsets_ptr = &(sharding_offsets[sharding_id][sub_id]);
      // Put result to the right position.
      auto shard_extractor = [=, &results](const EdgeList &edge_list) {
        CHECK_EQ(edge_list.node_id_size(), ids_ptr->size() * param.fanout)
            << "sample neighbors shard " << sharding_id << " size not match, expect "
            << ids_ptr->size() * param.fanout << ", actual " << edge_list.node_id_size();
        for (size_t i = 0; i < ids_ptr->size(); ++i) {
          for (int j = 0; j < param.fanout; ++j) {
            results.set_node_id(offsets_ptr->at(i) + j, edge_list.node_id(i * param.fanout + j));
            results.set_timestamp_s(offsets_ptr->at(i) + j, edge_list.timestamp_s(i * param.fanout + j));
            results.set_edge_weight(offsets_ptr->at(i) + j, edge_list.edge_weight(i * param.fanout + j));
            if (edge_list.edge_attrs_size() > 0) {
              results.set_edge_attrs(offsets_ptr->at(i) + j, edge_list.edge_attrs(i * param.fanout + j));
            }
          }
          if (edge_list.node_attrs_size() > 0) {
            auto src_id_offset = offsets_ptr->at(i) / param.fanout;
            if (results.node_attrs_size() <= src_id_offset) {
              LOG_EVERY_N_SEC(ERROR, 10)
                  << "Unexpected control flow. reserved size: " << results.node_attrs_size()
                  << ", to write offset: " << src_id_offset;
            } else {
              *(results.mutable_node_attrs(src_id_offset)) = edge_list.node_attrs(i);
            }
          } else {  // no node attrs in response
            results.clear_node_attrs();
          }
        }
      };
      auto client = shard_clients_[sharding_id].get();
      bool succ = client->SampleNeighbors(*ids_ptr, param, shard_extractor);
      if (!succ) {
        LOG_EVERY_N_SEC(WARNING, 3) << "sample neighbors failed, service: " << service_name_
                                    << ", source nodes size: " << source_nodes.size()
                                    << ", sample type: " << param.sample_type << ", shard: " << sharding_id
                                    << ", sub_id: " << sub_id;
      }
      success &= succ;
    }
  }

  timer.AppendCostMs("get response");
  if (!success) {
    LOG_EVERY_N_SEC(WARNING, 3) << "sample has some failure, service: " << service_name_
                                << ", source nodes size: " << source_nodes.size()
                                << ", fanout:" << param.fanout << ", sample type: " << param.sample_type
                                << ", cost:" << timer.display();
    // No proc_fn if any sub shard fails.
    return false;
  }

  proc_fn(results);
  timer.AppendCostMs("proc_fn");
  LOG_EVERY_N_SEC(INFO, 10) << "sample succeed, service: " << service_name_
                            << ", source nodes size: " << source_nodes.size() << ", fanout:" << param.fanout
                            << ", sample type: " << param.sample_type << ", cost:" << timer.display();

  return success;
}

bool RemoteShardingRGraphClient::LookupNodes(const IdVec &ids, NodesAttrProcFn func, const int64 timeout_ms) {
  base::Timer timer;
  if (sharding_num() == 1) { return shard_clients_[0]->LookupNodes(ids, func, timeout_ms); }

  thread_local std::vector<IdVec> sharding_ids;
  thread_local std::vector<size_t> sharding_offsets;
  sharding_ids.resize(sharding_num());
  sharding_offsets.resize(sharding_num(), 0);
  for (size_t i = 0; i < sharding_num(); ++i) {
    sharding_ids[i].clear();
    sharding_offsets[i] = 0;
  }
  for (auto id : ids) { sharding_ids[sharding_pos(id)].push_back(id); }
  timer.AppendCostMs("sharding sample neighbors build request");
  bool success = true;
  for (int sharding_id = 0; sharding_id < sharding_num(); ++sharding_id) {
    success &= shard_clients_[sharding_id]->LookupNodes(sharding_ids[sharding_id], func, timeout_ms);
  }
  timer.AppendCostMs("sharding sample neighbors request");
  LOG_EVERY_N_SEC(INFO, 10) << "service " << service_name_ << ", cost:" << timer.display();
  return success;
}

}  // namespace chrono_graph