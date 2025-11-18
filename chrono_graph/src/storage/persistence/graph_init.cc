// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "chrono_graph/src/storage/persistence/graph_init.h"

#include <algorithm>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "base/common/string.h"
#include "chrono_graph/src/base/env.h"
#include "chrono_graph/src/base/util.h"
#include "chrono_graph/src/storage/graph_kv.h"
#include "chrono_graph/src/storage/persistence/init_format.h"

namespace chrono_graph {

std::unique_ptr<GnnBaseIniter> GnnBaseIniter::NewInstance(base::Json *config, GraphKV *kv) {
  // Add types here if needed.
  return std::make_unique<GnnFileIniter>(config, kv);
}

bool GnnBaseIniter::Initialize(base::Json *config) {
  root_path_ = GetStringConfigSafe(config, "path");
  CHECK_GT(root_path_.size(), 0) << "root_path init fail, get empty string!";
  LOG(INFO) << "root_path = " << root_path_;

  relation_name_ = GetStringConfigSafe(config, "relation_name");
  CHECK(!relation_name_.empty()) << "relation name get fail, get empty string!";

  iewa_ = static_cast<InsertEdgeWeightAct>(config->GetInt("iewa", (int)InsertEdgeWeightAct::ADD));

  use_item_ts_for_expire_ = config->GetBoolean("use_item_ts_for_expire", false);
  LOG(INFO) << "use_item_ts_for_expire = " << use_item_ts_for_expire_;

  if (!CollectFiles()) { LOG(ERROR) << "CollectFiles fail! Check your config."; }
  return true;
}

bool GnnBaseIniter::ParseLineData(const std::string &data, GraphOneLineData *record) {
  GraphOneLineStr one_line(data);
  if (!one_line.GetSrcID(&(record->src_id))) { return false; }

  if (!HitLocalGraphKV(record->src_id)) {
    LOG_EVERY_N_SEC(INFO, 2) << "id not hit shard, ignore it " << record->src_id
                             << ", global shard id = " << global_shard_id << ", num = " << global_shard_num;
    return false;
  }

  auto block_op = kv_->get_attr_op(relation_name_);
  record->attr_info.resize(block_op->MaxSize());
  char *attr_content = const_cast<char *>(record->attr_info.data());
  block_op->InitialBlock(attr_content, block_op->MaxSize());
  auto simple_block_op = reinterpret_cast<SimpleAttrBlock *>(block_op);

  std::vector<int64> int_attrs;
  if (!one_line.GetIntAttrs(&int_attrs)) { return false; }
  for (size_t i = 0; i < int_attrs.size(); ++i) {
    // might coredump here if config is wrong
    simple_block_op->SetIntX(attr_content, i, int_attrs[i]);
  }

  std::vector<float> float_attrs;
  if (!one_line.GetFloatAttrs(&float_attrs)) { return false; }
  for (size_t i = 0; i < float_attrs.size(); ++i) {
    // might coredump here if config is wrong
    simple_block_op->SetFloatX(attr_content, i, float_attrs[i]);
  }

  if (!one_line.HasDestId()) { return true; }

  std::vector<std::string> dests_tmp;
  one_line.GetEdgesInfo(&dests_tmp);

  // each dest represents an edge
  for (const auto &dest : dests_tmp) {
    GraphOneEdgeStr edge_str(dest);
    if (!edge_str.IsValid()) { return false; }

    uint64 idtmp;
    if (!edge_str.GetID(&idtmp)) { return false; }
    record->dest_ids.emplace_back(idtmp);

    double weight_tmp;
    if (!edge_str.GetWeight(&weight_tmp)) { return false; }
    record->dest_weights.emplace_back(weight_tmp);

    std::vector<int64> edge_int_attrs;
    std::vector<float> edge_float_attrs;
    if (!edge_str.GetEdgeAttrs(&edge_int_attrs, &edge_float_attrs)) { return false; }

    auto edge_block_op = kv_->get_edge_attr_op(relation_name_);
    std::string edge_attr;
    edge_attr.resize(edge_block_op->MaxSize());
    char *edge_attr_content = const_cast<char *>(edge_attr.data());
    edge_block_op->InitialBlock(edge_attr_content, edge_block_op->MaxSize());
    auto edge_simple_block_op = reinterpret_cast<SimpleAttrBlock *>(edge_block_op);

    for (size_t i = 0; i < edge_int_attrs.size(); ++i) {
      // might coredump here if config is wrong
      edge_simple_block_op->SetIntX(edge_attr_content, i, edge_int_attrs[i]);
    }
    for (size_t i = 0; i < edge_float_attrs.size(); ++i) {
      // might coredump here if config is wrong
      edge_simple_block_op->SetFloatX(edge_attr_content, i, edge_float_attrs[i]);
    }
    record->edge_attr_infos.emplace_back(edge_attr);
  }

  return true;
}

void GnnBaseIniter::UpdateThread(int thread_id) {
  int dealing_cnt = 0;
  int dealing_line_cnt = 0;
  std::string current_path;

  while (!stop_ && path_queue_.Take(&current_path)) {
    CHECK(!current_path.empty()) << "UpdateThread get an unexpect empty path!";
    uint32 timestamp_s = GetFileTimestamp(current_path);

    std::string file_content;
    auto result = fs_.GetData(current_path, &file_content);
    if (!result) {
      LOG(WARNING) << "HDFS Updater get HDFS file fail, error code = " << result
                   << ", hdfs file = " << current_path;
      continue;
    }
    std::vector<std::string> lines = absl::StrSplit(file_content, "\n");

    GraphOneLineData record;
    for (const auto &line : lines) {
      if (stop_) { break; }
      record.Clear();
      LOG_EVERY_N(INFO, 100000) << "[debug] line = " << line;
      if (ParseLineData(line, &record)) {
        kv_->InsertRelations(record.src_id,
                             record.dest_ids,
                             relation_name_,
                             std::move(record.attr_info),
                             std::move(record.dest_weights),
                             std::vector<uint32_t>(record.dest_ids.size(), timestamp_s),
                             std::move(record.edge_attr_infos),
                             {},  // expire_ts_update_flags
                             iewa_,
                             false,
                             use_item_ts_for_expire_);
      } else {
        LOG(ERROR) << "Invalid line: " << line;
      }
      dealing_line_cnt += 1;
    }
    dealing_cnt += 1;
    total_records_.fetch_add(lines.size(), std::memory_order_release);
  }

  LOG(INFO) << "Gnn init thread " << thread_id << " update file no." << dealing_cnt
            << ", line count = " << dealing_line_cnt;
}

bool GnnFileIniter::Initialize(base::Json *config) {
  if (!GnnBaseIniter::Initialize(config)) { return false; }
  int64 relation_node_count = 0;
  kv_->GetRelationNodeCount(relation_name_, &relation_node_count);
  if (relation_node_count > 0) {
    LOG(INFO) << "relation: " << relation_name_ << " has been initialized, so skip it.";
    return true;
  }

  UpdateThread(0);
  CHECK_GT(total_records_.load(), 0);
  LOG(INFO) << "total_records:" << total_records_.load();
  return true;
}

bool GnnFileIniter::CollectFiles() {
  // root_path_ is the only file to read
  path_queue_.Push(root_path_);
  return true;
}

bool GnnDirIniter::Initialize(base::Json *config) {
  if (!GnnBaseIniter::Initialize(config)) { return false; }
  int64 relation_node_count = 0;
  kv_->GetRelationNodeCount(relation_name_, &relation_node_count);
  if (relation_node_count > 0) {
    LOG(INFO) << "relation: " << relation_name_ << " has been initialized, so skip it.";
    return true;
  }

  parallel_num_ = config->GetInt("parallel_num", 4);
  LOG(INFO) << "Init parallel num = " << parallel_num_;

  pool_ = std::make_unique<thread::ThreadPool>(parallel_num_);

  for (int thread_id = 0; thread_id < parallel_num_; ++thread_id) {
    pool_->AddTask([this, thread_id]() { this->UpdateThread(thread_id); });
  }
  pool_->JoinAll();

  CHECK_GT(total_records_.load(), 0);
  LOG(INFO) << "total_records:" << total_records_.load();
  return true;
}

uint32 GnnDirIniter::GetFileTimestamp(const std::string &path) { return GetTimestampFromDTPath(path); }

bool GnnDirIniter::CollectFiles() {
  std::vector<std::string> filenames;
  if (!fs_.ListDirectory(root_path_, &filenames)) {
    LOG(ERROR) << "List directory fail, ignore, path = " << root_path_;
    return false;
  }
  if (filenames.empty()) {
    LOG(ERROR) << "Got an empty folder: " << root_path_;
    return false;
  }
  LOG(INFO) << "Total file num = " << filenames.size() << ", example = " << filenames[0];

  std::for_each(
      filenames.begin(), filenames.end(), [this](std::string &file) { this->path_queue_.Push(file); });
  LOG(INFO) << "Added all files in dir: " << root_path_;
  return true;
}

}  // namespace chrono_graph
