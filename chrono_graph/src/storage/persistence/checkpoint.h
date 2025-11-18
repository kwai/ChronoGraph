// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <atomic>
#include <memory>
#include <set>
#include <string>
#include <tuple>
#include <vector>
#include "base/jansson/json.h"
#include "base/thread/thread_pool.h"
#include "memtable/mem_kv.h"
#include "chrono_graph/src/base/env.h"
#include "chrono_graph/src/proto/gnn_kv_service.pb.h"
#include "chrono_graph/src/storage/persistence/checkpoint_loader.h"

namespace chrono_graph {
class GraphKV;
/**
 * Dump key-value pairs and some other metadata, from memory to files.
 * Checkpoint structure:
 * root_path/version/relation_name/0000-9999
 * In which each file contains many key-value pairs.
 */
class GraphCheckpoint {
 public:
  struct CheckpointConfig {
    GraphKV *kv = nullptr;
    base::Json *config = nullptr;
  };

  GraphCheckpoint() = delete;
  explicit GraphCheckpoint(const CheckpointConfig &config);
  ~GraphCheckpoint() {
    stop_ = true;
    thread_pool_.JoinAll();
  }

  bool Init(bool need_load = false);

 private:
  // Try loading graph from checkpoint.
  bool TryInitLoad();
  /**
   * \return checkpoint path to load.
   */
  std::string GetLoadCheckpointPath();
  // Parse shard_id from file name, and decide whether to read file content
  // according to the shard_id and ckpt_shard_num.
  // Example: current shard_id = 1, shard_num = 60. ckpt_shard_num = 80.
  // GCD(60, 80) = 20, so files should be read are s1, s21, s41, s61
  bool ShouldReadFile(const std::string &file_name, int ckpt_shard_num);
  int CalcGCD(int a, int b);

  void LoadRelation(const std::string &relation_path);

  void ScheduledSaveThread();
  // Save a checkpoint, timestamp as version.
  // The whole checkpoint fails if any error happens.
  bool Save(uint64 version);
  // Save a relation within a checkpoint.
  bool SaveRelation(const std::string &version_path, std::string relation_name);

  void UpdateDoneFile(const std::string &new_line);
  /**
   * \return all valid checkpoint paths in done_file.
   */
  std::string ParseDoneFile();

  /**
   * Append a key-value pair to the end of `result`.
   */
  void FormatOneNode(uint64 key, base::KVReadData data, std::string *result);

  /**
   * \brief Parse a block
   * \param result If checkpoint loader is used, extra space is needed to store the result.
   */
  void ReadOneNode(const std::string &block,
                   uint64 *key,
                   const char **data,
                   int *size,
                   uint32 *expire_timet,
                   std::vector<std::shared_ptr<std::string>> *result);

  void ParseBatch(const std::string &block,
                  std::vector<base::KVSyncData> *parsed_data,
                  std::vector<std::shared_ptr<std::string>> *result);

  int64 GetNextSaveTimestamp();

  GraphKV *kv_;
  int save_interval_s_;
  std::string checkpoint_path_;       // current config
  std::string done_file_path_;        // done_file contains all valid checkpoint paths.
  std::string load_checkpoint_path_;  // checkpoint to load
  std::string load_done_file_path_;
  thread::ThreadPool thread_pool_{1};
  std::atomic_bool stop_{false};
  std::unordered_map<std::string, int> relations_shard_num_;
  static const uint64 kBytesEachFile = 2ll << 30;

  base::Json *prev_db_config_json_;
  std::unique_ptr<CheckpointLoader> checkpoint_loader_;
  DISALLOW_COPY_AND_ASSIGN(GraphCheckpoint);
};

}  // namespace chrono_graph
