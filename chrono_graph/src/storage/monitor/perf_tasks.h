// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include <algorithm>
#include <unordered_map>
#include <vector>
#include "base/common/gflags.h"
#include "base/common/logging.h"
#include "base/common/string.h"
#include "base/thread/thread_pool.h"
#include "chrono_graph/src/storage/graph_kv.h"
#include "chrono_graph/src/storage/monitor/graph_monitor.h"

namespace chrono_graph {

/**
 * Monitor average edge list length.
 */
class PerfGraphLength final : public MonitorTask {
 public:
  bool Initialize(GraphKV *kv) override { return MonitorTask::Initialize(kv); }

  bool RunOnce() override;
};

/**
 * Monitor edge list length quantile.
 */
class PerfQuantile : public MonitorTask {
 public:
  bool Initialize(GraphKV *kv) override;

  bool RunOnce() override;

 private:
  std::vector<int> quantile_pos_;
};

class PerfMemUsage : public MonitorTask {
 public:
  bool Initialize(GraphKV *kv) override { return MonitorTask::Initialize(kv); }

  bool RunOnce() override {
    for (const auto &pair : kv_->Dbs()) {
      auto &relation_name = pair.first;
      auto db = pair.second.get();
      float mem_usage_rate = db->MemUse() / (db->MemLimit() + 0.0);
      float key_usage_rate = db->ValidKeyNum() / (db->Capacity() + 0.0);
      PERF_AVG(mem_usage_rate * 10000, relation_name, P_STORAGE, "mem.block.usage");
      PERF_AVG(key_usage_rate * 10000, relation_name, P_STORAGE, "key.usage");
    }
    return true;
  }
};

/**
 * Monitor VALID sample result length (padding not included).
 * If no padding occurred, valid sample result length == sample num.
 */
class PerfSampleResultCountQuantile : public MonitorTask {
 public:
  bool Initialize(GraphKV *kv) override;

  void AddOne(int size, const std::string &relation_name, const std::string &caller_service_name) override;

  bool RunOnce() override;

 private:
  std::vector<int> quantile_pos_;
  // count_map_[relation_name][caller_name] = std::vector<int> counter
  // counter[degree] = num
  std::unordered_map<std::string, std::map<std::string, std::vector<int>>> count_map_;
  mutable absl::Mutex map_mutex_;
  // Degree larger than this is truncated.
  static constexpr const int kVecMaxSize = 60000;
};

}  // namespace chrono_graph