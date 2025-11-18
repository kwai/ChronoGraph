// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "base/common/file_util.h"
#include "base/common/logging.h"
#include "base/common/string.h"
#include "base/thread/thread_pool.h"
#include "base/thread/thread_safe_queue.h"
#include "base/util/fs_wrapper.h"
#include "chrono_graph/src/base/config_helper.h"
#include "chrono_graph/src/base/env.h"
#include "chrono_graph/src/proto/gnn_kv_service.pb.h"
#include "chrono_graph/src/storage/persistence/init_format.h"

namespace chrono_graph {
class GraphKV;

// Load graph from files that describes edges.
class GnnBaseIniter {
 public:
  explicit GnnBaseIniter(base::Json *config, GraphKV *kv) : kv_(kv) { Initialize(config); }
  virtual ~GnnBaseIniter() { stop_ = true; }

  static std::unique_ptr<GnnBaseIniter> NewInstance(base::Json *config, GraphKV *kv);

 protected:
  virtual bool Initialize(base::Json *config);

  struct GraphOneLineData {
    uint64 src_id;
    std::vector<uint64> dest_ids;
    std::vector<float> dest_weights;
    std::vector<std::string> edge_attr_infos;
    std::string attr_info;
    void Clear() {
      dest_ids.clear();
      dest_weights.clear();
      edge_attr_infos.clear();
      attr_info.clear();
    }
  };

  /**
   * \brief Parse a line of data to the class for insertion.
   * \param data a line to parse.
   * \param record parsed data.
   */
  bool ParseLineData(const std::string &data, GraphOneLineData *record);
  // Take file from `path_queue_` and update its content to GNN.
  void UpdateThread(int thread_id);

  // Sometimes file path may contains time info. By default just use physical time.
  virtual uint32 GetFileTimestamp(const std::string &path) { return absl::ToUnixSeconds(absl::Now()); }

  // Add all files to read into `path_queue_`.
  virtual bool CollectFiles() = 0;

  GraphKV *const kv_;

  // From config.
  std::string root_path_;
  std::string relation_name_;
  InsertEdgeWeightAct iewa_;
  bool use_item_ts_for_expire_ = false;  // Use time in directory instead of physical time

  // Internal variable.
  base::LocalFSWrapper fs_;
  thread::ThreadSafeQueue<std::string> path_queue_;
  std::atomic<int64> total_records_{0l};
  std::atomic_bool stop_{false};
};

// Load graph from a file.
class GnnFileIniter : public GnnBaseIniter {
 public:
  explicit GnnFileIniter(base::Json *config, GraphKV *kv) : GnnBaseIniter(config, kv) {
    LOG(INFO) << "Going to read file: " << root_path_;
  }
  ~GnnFileIniter() { LOG(INFO) << "GnnFileIniter is exiting"; }

 protected:
  bool Initialize(base::Json *config) override;
  bool CollectFiles() override;
};

// Load graph from all files under a directory.
class GnnDirIniter : public GnnBaseIniter {
 public:
  explicit GnnDirIniter(base::Json *config, GraphKV *kv) : GnnBaseIniter(config, kv) {
    LOG(INFO) << "Going to read all files in: " << root_path_;
  }
  ~GnnDirIniter() { LOG(INFO) << "GnnDirIniter is exiting"; }

 protected:
  bool Initialize(base::Json *config) override;
  bool CollectFiles() override;
  uint32 GetFileTimestamp(const std::string &path) override;

 private:
  int parallel_num_;
  std::unique_ptr<thread::ThreadPool> pool_;
};

}  // namespace chrono_graph
