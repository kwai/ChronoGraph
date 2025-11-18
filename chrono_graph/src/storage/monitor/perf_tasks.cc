// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "chrono_graph/src/storage/monitor/perf_tasks.h"

ABSL_FLAG(int32, monitor_divide_part_num, 100, "Divide each shard to parts, analyze only one part");
ABSL_FLAG(std::string, PerfQuantile_POS, "10,20,30,40,50,60,70,80,90,100", "");

namespace chrono_graph {

bool PerfGraphLength::RunOnce() {
  for (const auto &pair : kv_->Dbs()) {
    auto relation_name = pair.first;
    auto db = pair.second.get();
    const auto &updater = kv_->Updaters().at(pair.first).get();
    std::vector<uint64> keys;
    uint64 key_count = 0;
    uint64 length_sum = 0;
    for (int shard_id = 0; shard_id < db->PartNum(); ++shard_id) {
      db->GetPartKeys(shard_id, absl::GetFlag(FLAGS_monitor_divide_part_num), 0, &keys);
      std::for_each(keys.begin(), keys.end(), [&](uint64 id) {
        auto item = db->Get(id);
        if (!item.data) { return; }
        key_count++;
        length_sum += updater->Adaptor()->GetSize(item.data);
      });
    }
    if (key_count == 0) { continue; }
    PERF_AVG((length_sum * 10) / key_count, relation_name, P_EL, "avg_length");
  }
  return true;
}

bool PerfQuantile::Initialize(GraphKV *kv) {
  if (!MonitorTask::Initialize(kv)) { return false; }
  std::vector<std::string> tmp = absl::StrSplit(absl::GetFlag(FLAGS_PerfQuantile_POS), ",");
  for (auto &pos : tmp) {
    int pos_v = std::stoi(pos);
    if (pos_v < 0 || pos_v > 100) {
      LOG(ERROR) << "quantile init fail when parse pos: " << absl::GetFlag(FLAGS_PerfQuantile_POS);
      return false;
    }
    quantile_pos_.emplace_back(pos_v);
  }
  std::sort(quantile_pos_.begin(), quantile_pos_.end());
  if (quantile_pos_.empty()) {
    LOG(ERROR) << "quantile init fail, pos = 0: " << absl::GetFlag(FLAGS_PerfQuantile_POS);
    return false;
  }
  return true;
}

bool PerfQuantile::RunOnce() {
  for (const auto &pair : kv_->Dbs()) {
    auto relation_name = pair.first;
    auto db = pair.second.get();
    const auto &updater = kv_->Updaters().at(pair.first).get();
    std::map<int, int64> counter;  // [edge_list_length, num]
    std::vector<uint64> keys;
    for (int shard_id = 0; shard_id < db->PartNum(); ++shard_id) {
      // Estimate key num for each part.
      keys.reserve(db->ValidKeyNum() / db->PartNum() / absl::GetFlag(FLAGS_monitor_divide_part_num));
      db->GetPartKeys(shard_id, absl::GetFlag(FLAGS_monitor_divide_part_num), 0, &keys);
      std::for_each(keys.begin(), keys.end(), [&](uint64 id) {
        auto item = db->Get(id);
        if (!item.data) { return; }
        auto size = updater->Adaptor()->GetSize(item.data);
        auto iter = counter.find(size);
        if (iter == counter.end()) {
          counter.insert({size, 1});
        } else {
          iter->second += 1;
        }
      });
    }

    size_t key_quantile_pos = 0;
    // Each key has an edge list.
    int64 total_key_size = 0, iter_key_size = 0;
    for (auto &cnt_pair : counter) { total_key_size += cnt_pair.second; }
    for (auto &cnt_pair : counter) {
      if (key_quantile_pos >= quantile_pos_.size()) { break; }
      iter_key_size += cnt_pair.second;
      int now_quantile_pos = iter_key_size * 100 / total_key_size;
      while (now_quantile_pos >= quantile_pos_[key_quantile_pos]) {
        PERF_AVG(cnt_pair.first,
                 relation_name,
                 P_EL,
                 "quantile." + std::to_string(quantile_pos_[key_quantile_pos]));
        key_quantile_pos += 1;
        if (key_quantile_pos >= quantile_pos_.size()) { break; }
      }
    }
  }
  return true;
}

bool PerfSampleResultCountQuantile::Initialize(GraphKV *kv) {
  if (!MonitorTask::Initialize(kv)) { return false; }
  std::vector<std::string> tmp = absl::StrSplit(absl::GetFlag(FLAGS_PerfQuantile_POS), ",");
  for (auto &pos : tmp) {
    int pos_v = std::stoi(pos);
    if (pos_v < 0 || pos_v > 100) {
      LOG(ERROR) << "quantile init fail when parse pos: " << absl::GetFlag(FLAGS_PerfQuantile_POS);
      return false;
    }
    quantile_pos_.emplace_back(pos_v);
  }
  std::sort(quantile_pos_.begin(), quantile_pos_.end());
  if (quantile_pos_.empty()) {
    LOG(ERROR) << "quantile init fail, pos = 0: " << absl::GetFlag(FLAGS_PerfQuantile_POS);
    return false;
  }
  return true;
}
void PerfSampleResultCountQuantile::AddOne(int size,
                                           const std::string &relation_name,
                                           const std::string &caller_service_name) {
  PERF_AVG(size, relation_name, P_RQ, "sample.returned_degree", caller_service_name);
  auto relation_iter = count_map_.find(relation_name);
  if (relation_iter == count_map_.end()) {
    {
      absl::MutexLock lock(&map_mutex_);
      // double check
      relation_iter = count_map_.find(relation_name);
      if (relation_iter == count_map_.end()) {
        std::map<std::string, std::vector<int>> temp_map;
        count_map_.emplace(std::make_pair(relation_name, std::move(temp_map)));
      }
    }
    relation_iter = count_map_.find(relation_name);
  }
  auto &caller_map = relation_iter->second;
  auto caller_iter = caller_map.find(caller_service_name);
  if (caller_iter == caller_map.end()) {
    {
      absl::MutexLock lock(&map_mutex_);
      // double check
      caller_iter = caller_map.find(caller_service_name);
      if (caller_iter == caller_map.end()) {
        std::vector<int> temp_vec;
        temp_vec.resize(kVecMaxSize, 0);
        caller_map.emplace(std::make_pair(caller_service_name, std::move(temp_vec)));
      }
    }
    caller_iter = caller_map.find(caller_service_name);
  }
  auto &counter = caller_iter->second;
  if (size > kVecMaxSize) size = kVecMaxSize;
  counter[size]++;
}

bool PerfSampleResultCountQuantile::RunOnce() {
  absl::MutexLock lock(&map_mutex_);
  for (auto &relation_pair : count_map_) {            // [relation, caller_pair]
    for (auto &caller_pair : relation_pair.second) {  // [caller, counter_vec]
      auto &counter = caller_pair.second;             // counter_vec[degree] = num
      size_t node_quantile_pos = 0;
      int64 node_type_total_node_size = 0, node_type_iter_node_size = 0;
      for (auto &value : counter) { node_type_total_node_size += value; }
      if (node_type_total_node_size == 0) continue;
      for (int index = 0; index < counter.size(); ++index) {
        if (node_quantile_pos >= quantile_pos_.size()) { break; }
        node_type_iter_node_size += counter[index];
        int now_quantile_pos = node_type_iter_node_size * 100 / node_type_total_node_size;
        while (now_quantile_pos >= quantile_pos_[node_quantile_pos]) {
          PERF_AVG(index,
                   relation_pair.first,
                   P_RQ,
                   "sample.returned_degree_quantile." + std::to_string(quantile_pos_[node_quantile_pos]),
                   caller_pair.first);
          node_quantile_pos += 1;
          if (node_quantile_pos >= quantile_pos_.size()) { break; }
        }
      }
      counter.assign(counter.size(), 0);
    }
  }
  return true;
}

}  // namespace chrono_graph