// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "chrono_graph/src/storage/persistence/checkpoint.h"

#include <algorithm>
#include <atomic>
#include <string>
#include <utility>
#include <vector>

#include "base/common/file_util.h"
#include "base/common/logging.h"
#include "base/common/string.h"
#include "base/common/time.h"
#include "base/util/fs_wrapper.h"
#include "base/util/future_pool.h"
#include "chrono_graph/src/base/config_helper.h"
#include "chrono_graph/src/base/env.h"
#include "chrono_graph/src/base/perf_util.h"
#include "chrono_graph/src/storage/graph_kv.h"
#include "chrono_graph/src/storage/persistence/graph_init.h"

namespace chrono_graph {

static const char kSuccess[] = "_SUCCESS";
thread_local base::LocalFSWrapper fs_wrapper;

GraphCheckpoint::GraphCheckpoint(const GraphCheckpoint::CheckpointConfig &config) {
  kv_ = config.kv;

  save_interval_s_ = config.config->GetInt("save_interval_s", 21600);
  if (save_interval_s_ < 3600) {
    LOG(WARNING) << "Checkpoint only support save interval larger than 3600(s), got " << save_interval_s_
                 << " from config, which is out of range. Use 21600 instead.";
    save_interval_s_ = 21600;
  }

  auto checkpoint_root = GetStringConfigSafe(config.config, "checkpoint_path");
  auto load_checkpoint_root = config.config->GetString("load_checkpoint_path", "");
  if (load_checkpoint_root == "") { load_checkpoint_root = checkpoint_root; }
  load_checkpoint_path_ = absl::StrFormat("%s/checkpoints", load_checkpoint_root.c_str());
  load_done_file_path_ = absl::StrFormat("%s/done_file", load_checkpoint_root.c_str());
  checkpoint_path_ = absl::StrFormat("%s/checkpoints", checkpoint_root.c_str());
  done_file_path_ = absl::StrFormat("%s/done_file", checkpoint_root.c_str());

  prev_db_config_json_ = config.config->Get("prev_db_config");
}

bool GraphCheckpoint::Init(bool need_load) {
  if (need_load) {
    auto temp1 = checkpoint_path_;
    auto temp2 = done_file_path_;
    checkpoint_path_ = load_checkpoint_path_;
    done_file_path_ = load_done_file_path_;
    TryInitLoad();
    checkpoint_path_ = temp1;
    done_file_path_ = temp2;
  }
  if (!fs_wrapper.ExistOrCreateDirectory(checkpoint_path_)) {
    LOG(ERROR) << "fail to create dict: " << checkpoint_path_;
  }
  thread_pool_.AddTask([this]() { this->ScheduledSaveThread(); });
  return true;
}

bool GraphCheckpoint::TryInitLoad() {
  auto load_path = GetLoadCheckpointPath();
  if (load_path.empty()) { return false; }
  std::vector<std::string> relation_data_paths;
  if (!fs_wrapper.ListDirectoryItems(load_path, &relation_data_paths)) {
    LOG(INFO) << "Try init load fail, load latest checkpoint path fail, path = " << load_path
              << ", error = " << std::strerror(errno);
    return false;
  }
  for (auto &relation_data_path : relation_data_paths) {
    if (fs_wrapper.IsDirectory(relation_data_path)) { LoadRelation(relation_data_path); }
  }
  return true;
}

std::string GraphCheckpoint::ParseDoneFile() {
  std::string done_content;
  if (!fs_wrapper.FileExists(done_file_path_)) {
    LOG(INFO) << "Parse done file context fail, file not exist: " << done_file_path_;
    return "";
  }
  if (!fs_wrapper.GetData(done_file_path_, &done_content)) {
    LOG(INFO) << "failed to get old done conent:" << done_file_path_ << ", error = " << std::strerror(errno);
    return "";
  }
  std::vector<std::string> lines = absl::StrSplit(done_content, "\n");
  LOG(INFO) << "done_content size: " << lines.size() << ", data content = " << done_content;
  done_content = "";
  if (lines.empty()) { return ""; }
  for (const auto &line : lines) {
    auto final_success = true;
    // Each relation should have a SUCCESS file
    for (const auto &pair : kv_->Dbs()) {
      auto relation_name = pair.first;
      final_success &= fs_wrapper.FileExists(line + "/" + kSuccess + "_" + relation_name);
      relations_shard_num_[relation_name] = 0;
      for (size_t i = 0;; ++i) {
        if (fs_wrapper.FileExists(line + "/s" + std::to_string(i) + kSuccess + "_" + relation_name)) {
          relations_shard_num_[relation_name]++;
        } else {
          break;
        }
      }
    }
    if (final_success) {
      done_content += line + "\n";
    } else {
      LOG(INFO) << "parse done file, ignoring path because lack of SUCCESS : " << line
                << ", shard_num = " << global_shard_num;
    }
  }
  LOG(INFO) << "[debug], done_content = " << done_content;
  return done_content;
}

std::string GraphCheckpoint::GetLoadCheckpointPath() {
  std::string done_paths = ParseDoneFile();
  if (done_paths.empty()) {
    LOG(INFO) << "done file context is empty, path = " << done_file_path_;
    return "";
  }
  std::vector<std::string> lines = absl::StrSplit(done_paths, "\n");
  LOG(INFO) << "done file parse line: " << lines.size() << ", paths = " << done_paths;
  if (lines.empty()) { return ""; }
  return lines.back();
}

int GraphCheckpoint::CalcGCD(int a, int b) {
  if (a <= 0 || b <= 0) { NOT_REACHED() << "Invalid parameter for CalcGCD, a = " << a << ", b = " << b; }
  while (a != 0) {
    if (a < b) std::swap(a, b);
    a = a % b;
  }
  return b;
}

bool GraphCheckpoint::ShouldReadFile(const std::string &file_name, int ckpt_shard_num) {
  int file_shard_id = 0;
  try {
    file_shard_id = std::stoi(file_name.substr(1, 3));
  } catch (std::invalid_argument &) {
    LOG(ERROR) << "Unexpected file_name (no shard_id):" << file_name;
    return true;
  }
  CHECK_GT(global_shard_num, 0) << "Unexpected global_shard_num: " << global_shard_num;
  int gcd = CalcGCD(global_shard_num, ckpt_shard_num);
  if (file_shard_id % gcd == global_shard_id % gcd) return true;
  return false;
}

void GraphCheckpoint::LoadRelation(const std::string &relation_path) {
  auto relation_name = fs::path(relation_path).filename().string();
  auto db_kernel = kv_->Dbs().at(relation_name).get();
  if (!db_kernel) {
    LOG(INFO) << "relation: " << relation_name << " not support on this instance, skip.";
    return;
  }
  // LoadRelation cannot parallel execute
  auto db_config = kv_->RelationConfig(relation_name);
  if (prev_db_config_json_) {
    checkpoint_loader_.reset(new CheckpointLoader(db_config, prev_db_config_json_));
  } else {
    checkpoint_loader_.reset(nullptr);
  }

  LOG(INFO) << "Load relation path: " << relation_path;
  std::vector<std::string> files;
  if (!fs_wrapper.ListDirectory(relation_path, &files)) {
    PERF_SUM(1, relation_name, P_EXCEPTION, "checkpoint.read_failed");
    LOG(INFO) << "load relation get file list fail: " << relation_path;
    return;
  }
  std::random_device rd;
  std::mt19937 g(rd());
  // Prevent files for the same shard are processed concurrently
  std::shuffle(files.begin(), files.end(), g);

  base::AsyncFuturePool executor_pool(32);
  std::vector<std::future<bool>> futures;
  for (auto &file : files) {
    auto file_name = fs::path(file).filename().string();

    if (!ShouldReadFile(file_name, relations_shard_num_[relation_name])) continue;

    auto executor = [=, &file, &db_kernel]() -> bool {
      LOG(INFO) << "load checkpoint file: " << file;
      std::string block;
      if (!fs_wrapper.GetData(file, &block)) {
        LOG(INFO) << "read file data fail, skip: " << file;
        return false;
      }
      std::vector<std::shared_ptr<std::string>> result;  // checkpoint loader will put parsed result in it
      std::vector<base::KVSyncData> parsed_data;
      ParseBatch(block, &parsed_data, &result);
      return db_kernel->MultiSync(parsed_data);
    };
    futures.emplace_back(executor_pool.Async<bool>(executor));
  }
  int success_cnt = 0;
  for (auto &future : futures) { success_cnt += future.get(); }
  LOG(INFO) << "load relation: " << relation_path << ", files = " << files.size()
            << ", success = " << success_cnt;
}

void GraphCheckpoint::UpdateDoneFile(const std::string &new_line) {
  std::string done_content = ParseDoneFile();
  std::vector<std::string> lines = absl::StrSplit(done_content, "\n");
  if (!lines.empty() && new_line == lines.back()) return;

  auto temp_path = done_file_path_ + "_tmp";
  auto old_path = done_file_path_ + "_old";
  done_content += new_line + "\n";

  if (!fs_wrapper.PutData(done_content.data(), done_content.size(), temp_path)) {
    LOG(ERROR) << "failed to write done_file content to:" << temp_path;
    return;
  }

  if (!fs_wrapper.Rmr(old_path)) {
    LOG(ERROR) << "failed to write done file when rm old path: " << old_path;
    return;
  }
  if (fs_wrapper.FileExists(done_file_path_) && !fs_wrapper.Move(done_file_path_, old_path)) {
    LOG(ERROR) << "failed to write done file when move old done file(" << done_file_path_
               << ") to old path: " << old_path;
    return;
  }
  if (!fs_wrapper.Move(temp_path, done_file_path_)) {
    LOG(ERROR) << "failed to write done file, when move temp file(" << temp_path << ") to done file path"
               << done_file_path_;
    return;
  }
}

void GraphCheckpoint::FormatOneNode(uint64 key, base::KVReadData data, std::string *result) {
  uint32_t length = 0;
  length += sizeof(key);
  length += sizeof(data.expire_timet);
  length += data.size;
  auto old_size = result->size();
  result->resize(result->size() + sizeof(length) + length);
  char *data_ptr = const_cast<char *>(result->data() + old_size);

  uint32_t offset = 0;
  *reinterpret_cast<uint32_t *>(data_ptr + offset) = length;
  offset += sizeof(uint32_t);
  *reinterpret_cast<uint64 *>(data_ptr + offset) = key;
  offset += sizeof(uint64);
  *reinterpret_cast<uint32 *>(data_ptr + offset) = data.expire_timet;
  offset += sizeof(uint32);

  memcpy((data_ptr + offset), data.data, data.size);
}

void GraphCheckpoint::ReadOneNode(const std::string &block,
                                  uint64 *key,
                                  const char **data,
                                  int *size,
                                  uint32 *expire_timet,
                                  std::vector<std::shared_ptr<std::string>> *result) {
  uint32_t offset = 0;
  *key = *reinterpret_cast<const uint64 *>(block.data() + offset);
  offset += sizeof(uint64);
  *expire_timet = *reinterpret_cast<const uint32 *>(block.data() + offset);
  offset += sizeof(uint32);
  *size = block.size() - offset;
  CHECK_GT(*size, 0) << "checkpoint parse block error, data content size < 0, total = " << block.size();
  *data = reinterpret_cast<const char *>(block.data() + offset);
  if (checkpoint_loader_) {
    result->emplace_back(std::make_shared<std::string>());
    *data = checkpoint_loader_->ParseAppend(
        reinterpret_cast<const char *>(block.data() + offset), key, size, expire_timet, result->back().get());
  }
}

void GraphCheckpoint::ParseBatch(const std::string &block,
                                 std::vector<base::KVSyncData> *parsed_data,
                                 std::vector<std::shared_ptr<std::string>> *result) {
  size_t offset = 0;
  while (offset < block.size()) {
    if (block.size() < offset + sizeof(uint32_t)) {
      LOG(WARNING) << "ParseBatch read a error offset: " << offset << ", block size = " << block.size()
                   << ", until now success = " << parsed_data->size();
      return;
    }
    uint32_t item_size = *reinterpret_cast<const uint32_t *>(block.data() + offset);
    offset += sizeof(item_size);
    if (block.size() < offset + item_size) {
      LOG(WARNING) << "ParseBatch read a error offset: " << offset << ", block size = " << block.size()
                   << ", item size = " << item_size << ", until now success = " << parsed_data->size();
      return;
    }
    uint64 key = 0;
    uint32 expire_timet = 0;
    const char *data = nullptr;
    int size = 0;
    ReadOneNode(std::string(block.data() + offset, item_size), &key, &data, &size, &expire_timet, result);
    offset += item_size;
    if (!HitLocalGraphKV(key)) { continue; }
    parsed_data->emplace_back(key, data, size, expire_timet);
  }
}

int64 GraphCheckpoint::GetNextSaveTimestamp() {
  int64 now = base::GetTimestamp() / 1000000;
  return (now / save_interval_s_ + 1) * save_interval_s_;
}

void GraphCheckpoint::ScheduledSaveThread() {
  int64 next_dump_timestamp = GetNextSaveTimestamp();
  while (!stop_) {
    if (base::GetTimestamp() / 1000000 < next_dump_timestamp) {
      base::SleepForSeconds(1);
      continue;
    }
    uint64 version = next_dump_timestamp;
    next_dump_timestamp = GetNextSaveTimestamp();
    if (!Save(version)) {
      LOG(ERROR) << "dump version: " << version << " fail.";
      continue;
    }

    // After each shard finish writing, write a success_part file, so that other shards know
    // how many shards are there.
    // When all shards finish writing, the last one to see all shards finished will update the done_file.
    auto ckpt_path = absl::StrFormat("%s/%lu", checkpoint_path_.c_str(), version);
    std::vector<std::string> success_files;
    fs_wrapper.ListDirectory(ckpt_path, &success_files);
    for (const auto &pair : kv_->Dbs()) {
      auto relation_name = pair.first;
      std::string postfix = std::string(kSuccess) + "_" + relation_name;
      int relation_success_count = 0;
      // Check sN_SUCCESS_{relation_name}
      for (const auto &path : success_files) {
        if (path.find(postfix) != std::string::npos) { relation_success_count++; }
      }
      if (relation_success_count == global_shard_num) {
        // The last one writes _SUCCESS_{relation_name}, indicates this relation has finished successfully.
        fs_wrapper.PutData(
            "", 0, absl::StrFormat("%s/%lu", checkpoint_path_.c_str(), version) + "/" + postfix);
        UpdateDoneFile(ckpt_path);
      }
    }
    LOG(INFO) << "Finish one loop checkpoint dump, next dump timestamp = " << next_dump_timestamp;
  }
}

bool GraphCheckpoint::Save(uint64 version) {
  auto version_path = absl::StrFormat("%s/%lu", checkpoint_path_.c_str(), version);
  LOG(INFO) << "begin to dump, root path = " << version_path;
  if (!fs_wrapper.ExistOrCreateDirectory(version_path)) {
    PERF_SUM(1, P_CKPT, P_EXCEPTION, "checkpoint.save_failed");
    LOG(ERROR) << "dump fail, fail to create path: " << version_path;
    return false;
  }
  for (const auto &pair : kv_->Dbs()) {
    auto relation_name = pair.first;
    if (!SaveRelation(version_path, relation_name)) {
      PERF_SUM(1, relation_name, P_EXCEPTION, "checkpoint.save_failed");
      LOG(ERROR) << "dump fail, fail to save relation: " << relation_name;
      return false;
    }
  }
  return true;
}

bool GraphCheckpoint::SaveRelation(const std::string &version_path, std::string relation_name) {
  auto relation_path = absl::StrFormat("%s/%s", version_path.c_str(), relation_name.data());
  if (!fs_wrapper.ExistOrCreateDirectory(relation_path)) {
    PERF_SUM(1, relation_name, P_EXCEPTION, "checkpoint.save_failed");
    LOG(ERROR) << "dump fail, fail to create hdfs path: " << relation_path;
    return false;
  }

  auto db_kernel = kv_->Dbs().at(relation_name).get();

  std::atomic_int file_id{0};
  std::atomic_ullong key_counter{0};
  std::atomic_ullong size_counter{0};
  std::atomic_ullong file_counter{0};
  base::AsyncFuturePool executor_pool(8);
  std::vector<std::future<bool>> results;
  for (int sub_shard_id = 0; sub_shard_id < db_kernel->PartNum(); ++sub_shard_id) {
    auto executor = [&, sub_shard_id]() -> bool {
      base::LocalFSWrapper fs_op;
      std::string tmp_file_content;
      std::vector<uint64> keys;
      tmp_file_content.reserve(kBytesEachFile);
      // Estimate part num
      int part_num = db_kernel->MemUse() / db_kernel->PartNum() / kBytesEachFile + 1;
      // Each part writes a file.
      for (int part_id = 0; part_id < part_num; ++part_id) {
        db_kernel->GetPartKeys(sub_shard_id, part_num, part_id, &keys);
        if (stop_) { return false; }
        tmp_file_content.clear();
        for (auto key : keys) {
          absl::MutexLock lock(db_kernel->GetModifyLockPtr(key));
          auto value = db_kernel->Get(key);
          if (value.data == nullptr || value.expired()) { continue; }
          FormatOneNode(key, value, &tmp_file_content);
        }
        key_counter.fetch_add(keys.size());
        size_counter.fetch_add(tmp_file_content.size());
        file_counter.fetch_add(1);
        auto file_path =
            absl::StrFormat("%s/s%03d-%05d", relation_path.data(), global_shard_id, file_id.fetch_add(1));
        if (!fs_op.PutData(tmp_file_content.data(), tmp_file_content.size(), file_path)) {
          LOG(ERROR) << "fail to write file: " << file_path << ", data size = " << tmp_file_content.size()
                     << ", sub_shard = " << sub_shard_id << ", part = " << part_id
                     << ", part_num = " << part_num << ", error = " << std::strerror(errno);
          return false;
        }
      }
      return true;
    };
    results.emplace_back(executor_pool.Async<bool>(executor));
  }
  for (auto &result : results) {
    if (!result.get()) { return false; }
  }

  PERF_SUM(key_counter, relation_name, P_CKPT, "key.count");
  PERF_SUM(size_counter, relation_name, P_CKPT, "size.count");
  PERF_SUM(file_counter, relation_name, P_CKPT, "file.count");
  PERF_AVG(size_counter / file_counter, relation_name, P_CKPT, "file.avg_size");

  if (!fs_wrapper.Touch(version_path + "/s" + std::to_string(global_shard_id) + kSuccess + "_" +
                        relation_name)) {
    LOG(ERROR) << "fail to write success file for shard " << global_shard_id;
    return false;
  }
  return true;
}

}  // namespace chrono_graph
