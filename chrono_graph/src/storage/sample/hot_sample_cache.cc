// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "chrono_graph/src/storage/sample/hot_sample_cache.h"

#include <math.h>
#include <memory>
#include "chrono_graph/src/base/perf_util.h"
#include "chrono_graph/src/storage/graph_kv.h"

ABSL_FLAG(int32,
          double_buffer_switch_interval_s,
          10,
          "Double buffer's switch interval. For huge graph this may set to 60.");

namespace chrono_graph {

DoubleBufferSamplePool::DoubleBufferSamplePool() {
  std::vector<float> dummy_weight{1};
  sampler_[0] = std::make_shared<Sampler>(dummy_weight);
  sampler_[1] = std::make_shared<Sampler>(dummy_weight);
}

bool DoubleBufferSamplePool::ClearBackupBuffer() {
  int prepare_buffer = 1 - serving_buffer_;
  sampler_[prepare_buffer]->ids.clear();
  return true;
}

bool DoubleBufferSamplePool::InsertBackupBuffer(uint64 key) {
  int prepare_buffer = 1 - serving_buffer_;
  sampler_[prepare_buffer]->ids.push_back(key);
  return true;
}

bool DoubleBufferSamplePool::CommitAndSwitch(const CalcWeightFunc &func, const std::string &relation_name) {
  int prepare_buffer = 1 - serving_buffer_;
  std::vector<float> weights;
  weights.reserve(sampler_[prepare_buffer]->ids.size());
  int64 valid_key_num = 0;
  for (auto key : sampler_[prepare_buffer]->ids) {
    auto calced_weight = func({key, relation_name});
    weights.push_back(calced_weight);
    if (calced_weight > 0) valid_key_num++;
  }
  bool succ = sampler_[prepare_buffer]->alias_method.Reset(weights);
  if (!succ) {
    LOG(INFO) << "double buffer update waiting for data.";
    ready_ = false;
    return false;
  }
  LOG(INFO) << "double buffer update finish, total key = " << weights.size()
            << ", key(weight > 0) = " << valid_key_num << ", relation = " << relation_name;
  serving_buffer_ = 1 - serving_buffer_;
  // begin serving after initialized.
  ready_ = true;
  PERF_AVG(valid_key_num, relation_name, "pool_size-" + relation_name);
  return true;
}

ErrorCode DoubleBufferSamplePool::Sample(int sample_count, uint64 *result) const {
  if (!ready_.load()) {
    LOG_EVERY_N_SEC(ERROR, 1) << "DoubleBufferSamplePool not ready yet!";
    return ErrorCode::RELATION_NAME_NOT_AVAILABLE;
  }
  auto sampler = sampler_[serving_buffer_.load()];
  bool succ = sampler.get()->alias_method.SampleAndSet<uint64>(
      sample_count, result, [&](size_t i) -> uint64 { return sampler.get()->ids[i]; });
  if (!succ) { return ErrorCode::RELATION_NAME_NOT_AVAILABLE; }
  return ErrorCode::OK;
}

ErrorCode WeightSampleCache::Sample(std::string relation_name, int sample_count, uint64 *ret) const {
  auto it = caches_.find(relation_name);
  if (it == caches_.end()) {
    LOG_EVERY_N_SEC(ERROR, 1) << "relation_name: " << relation_name << " not ready yet.";
    return ErrorCode::RELATION_NAME_NOT_AVAILABLE;
  }
  return it->second->Sample(sample_count, ret);
}

void WeightSampleCache::UpdateDoubleBuffers() {
  // To avoid sleeping too long when the process exit, set a short sleep interval.
  // The security of double buffer rely on this interval, so it cannot be too short, because
  // the prepare buffer will be updated after this interval, and all access to it must have finished.
  int sleep_interval_seconds = 10;
  int sleep_time_seconds = 0;
  // MemKV initialize is slow, so we sleep for a while, waiting for GraphKV init finished and do
  // InsertRelation. If not sleep here, DoubleBuffer will be switched with no data. Then, some data are
  // inserted to backup buffer, and we need to wait for next round of switch, which is 60s on default.
  base::SleepForSeconds(2);
  while (!stop_) {
    for (const auto &pair : caches_) { pair.second->ClearBackupBuffer(); }

    std::vector<uint64> tmp(1000000, 0);
    for (const auto &pair : db_->Dbs()) {
      auto relation_name = pair.first;
      for (int shard_id = 0; shard_id < pair.second->PartNum(); ++shard_id) {
        pair.second->GetPartKeys(shard_id, 1, 0, &tmp);
        for (const auto &key : tmp) {
          if (caches_.find(relation_name) == caches_.end()) {
            caches_[relation_name] = std::make_unique<DoubleBufferSamplePool>();
          }
          caches_[relation_name]->InsertBackupBuffer(key);
        }
      }
    }

    auto weight_calc_func = [=](GraphNode info) -> float { return this->CalcItemWeight(info); };
    for (const auto &pair : caches_) {
      pair.second->CommitAndSwitch(weight_calc_func, pair.first);
      LOG(INFO) << "Double Sample Hot Cache " + pair.first + ", update finish.";
    }
    ready_ = (caches_.size() == db_->Dbs().size());
    for (const auto &pair : caches_) { ready_ = ready_ && pair.second->IsReady(); }
    while (!stop_ && sleep_time_seconds < absl::GetFlag(FLAGS_double_buffer_switch_interval_s)) {
      base::SleepForSeconds(sleep_interval_seconds);
      sleep_time_seconds += sleep_interval_seconds;
    }
    sleep_time_seconds = 0;
  }
}

WeightSampleCache::WeightSampleCache(GraphKV *db) : db_(db), updater_(1) {}

float FlattenWeightSampleCache::CalcItemWeight(GraphNode item_info) {
  if (db_->Dbs().find(item_info.relation_name) == db_->Dbs().end()) {
    LOG(ERROR) << "Sample from unexist relation: " << item_info.relation_name;
    return 1.0;
  }
  auto expire_time = db_->GetNodeExpireTime(item_info);
  // exclude items that are about to expire.
  auto min_expire_time = base::GetTimestamp() / base::Time::kMicrosecondsPerSecond +
                         absl::GetFlag(FLAGS_double_buffer_switch_interval_s);
  return expire_time > min_expire_time ? 1 : 0;
}

float DegreeBaseSampleCache::CalcItemWeight(GraphNode item_info) { return db_->GetNodeDegree(item_info); }

float CustomSampleCache::GetNodeAttrValue(GraphNode item_info, int int_index, int float_index, GraphKV *db) {
  NodeAttr node_attr;
  bool succ = db->QueryNeighbors(item_info.node_id, {}, item_info.relation_name, &node_attr, nullptr);
  if (!succ) {
    LOG(WARNING) << "Failed to get NodeAttr of id = " << item_info.node_id
                 << ", relation = " << item_info.relation_name << ". Regard weight as 0 instead.";
    return 0;
  }
  std::vector<int64> int_vec;
  std::vector<float> float_vec;
  float result;
  db->get_attr_op(item_info.relation_name)->GetAll(node_attr.attr_info().data(), &int_vec, &float_vec);
  if (int_index > 0) {
    CHECK(int_vec.size() > int_index)
        << "int_index = " << int_index << " is out of range: int attr size = " << int_vec.size();
    result = static_cast<float>(int_vec[int_index]);
    if (result < 0) {
      LOG(WARNING) << "Got attr < 0 as weight: " << result << ". Regard weight as 0 instead.";
      result = 0;
    }
    return result;
  }
  if (float_index > 0) {
    CHECK(float_vec.size() > float_index)
        << "float_index = " << float_index << " is out of range: float attr size = " << float_vec.size();
    result = float_vec[float_index];
    if (result < 0) {
      LOG(WARNING) << "Got attr < 0 as weight: " << result << ". Regard weight as 0 instead.";
      result = 0;
    }
    return result;
  }
  NOT_REACHED() << "Unexpected control flow";
  return 0.0;
}

float CustomSampleCache::CalcItemWeight(GraphNode item_info) {
  // filter
  if (has_degree_filter_) {
    auto degree = db_->GetNodeDegree(item_info);
    if (degree < degree_lower_bound_ || degree > degree_upper_bound_) return 0;
  }
  if (has_size_filter_) {
    auto size = db_->GetNodeEdgeLength(item_info);
    if (size < size_lower_bound_ || size > size_upper_bound_) return 0;
  }
  if (expire_tolerate_second_ > 0) {
    auto item_data = db_->Dbs().at(item_info.relation_name)->Get(item_info.node_id);
    auto cur_ts = base::GetTimestamp() / base::Time::kMicrosecondsPerSecond +
                  absl::GetFlag(FLAGS_double_buffer_switch_interval_s);
    if (item_data.expire_timet != 0 && item_data.expire_timet - cur_ts <= expire_tolerate_second_) {
      return 0;
    }
  }

  // get weight
  float weight = 1;  // UNIFORM on default
  switch (weight_type_) {
    case WeightType::SIZE:
      weight = db_->GetNodeEdgeLength(item_info);
      break;
    case WeightType::DEGREE:
      weight = db_->GetNodeDegree(item_info);
      break;
    case WeightType::ATTR:
      weight = GetNodeAttrValue(item_info, int_index_, float_index_, db_);
      break;
    case WeightType::UNIFORM:
      weight = 1;
      break;
  }

  // transfer weight
  switch (trans_func_) {
    case TransFunc::LOG:
      weight = log(weight);
      break;
    case TransFunc::LOG1P:
      weight = log1p(weight);
      break;
    case TransFunc::POWER:
      weight = powf(weight, pow_exp_);
      break;
    case TransFunc::DEFAULT:
      break;
  }
  return weight;
}

}  // namespace chrono_graph
