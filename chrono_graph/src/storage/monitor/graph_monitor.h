// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include "base/common/gflags.h"
#include "base/common/logging.h"
#include "base/common/sleep.h"
#include "base/thread/thread_pool.h"
#include "base/util/future_pool.h"
#include "chrono_graph/src/base/perf_util.h"

ABSL_DECLARE_FLAG(int32, monitor_duration);

namespace chrono_graph {
class GraphKV;

class MonitorTask {
 public:
  MonitorTask() {}

  virtual ~MonitorTask() {}

  virtual bool Initialize(GraphKV *kv);

  virtual bool RunOnce() = 0;

  virtual void AddOne(int size, const std::string &caller_service_name, const std::string &relation_name) {}

 protected:
  GraphKV *kv_;
};

class MonitorManager {
 public:
  MonitorManager() = delete;

  explicit MonitorManager(GraphKV *kv);

  void StopMonitor() {
    stop_ = true;
    while (!stopped_) {
      for (size_t i = 0; i < absl::GetFlag(FLAGS_monitor_duration); ++i) {
        if (stopped_) { break; }
        base::SleepForSeconds(1);
      }
    }
  }

  void AddSampleResultCount(int size,
                            const std::string &relation_name,
                            const std::string &caller_service_name) {
    auto it = tasks_.find("PerfSampleResultCountQuantile");
    if (it != tasks_.end()) { it->second->AddOne(size, relation_name, caller_service_name); }
  }

 private:
  void AddTasks(GraphKV *kv);

  void ScheduleTasks() {
    while (!stop_) {
      std::vector<std::future<bool>> futures;
      for (const auto &pair : tasks_) {
        auto &task = pair.second;
        futures.emplace_back(future_pool_->Async<bool>([&task]() { return task->RunOnce(); }));
      }
      for (auto &future : futures) { future.get(); }
      for (size_t i = 0; i < absl::GetFlag(FLAGS_monitor_duration); ++i) {
        if (stop_) { break; }
        base::SleepForSeconds(1);
      }
    }
    stopped_ = true;
    LOG(INFO) << "graph monitor exit.";
  }

  std::unique_ptr<base::AsyncFuturePool> future_pool_;  // Execute MonitorTasks
  std::unordered_map<std::string, std::unique_ptr<MonitorTask>> tasks_;
  std::string service_name_ = "";
  bool stop_ = false;
  bool stopped_ = false;
};

}  // namespace chrono_graph
