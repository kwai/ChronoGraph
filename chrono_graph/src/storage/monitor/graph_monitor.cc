// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "chrono_graph/src/storage/monitor/graph_monitor.h"

#include <string>
#include "base/thread/thread_pool.h"
#include "base/util/future_pool.h"
#include "chrono_graph/src/storage/monitor/perf_tasks.h"

ABSL_FLAG(int32, monitor_thread_num, 2, "");
ABSL_FLAG(int32, monitor_duration, 10, "Monitor interval in seconds");

namespace chrono_graph {

class GraphKV;

MonitorManager::MonitorManager(GraphKV *kv) {
  int async_monitor_thread_num = absl::GetFlag(FLAGS_monitor_thread_num);
  future_pool_ = std::make_unique<base::AsyncFuturePool>(async_monitor_thread_num);
  AddTasks(kv);
  LOG(INFO) << "Monitor Add Task: " << tasks_.size();
  future_pool_->Async<void>([this]() { this->ScheduleTasks(); });
}

bool MonitorTask::Initialize(GraphKV *kv) {
  CHECK(kv) << "MonitorTask init fail, kv = null";
  kv_ = kv;
  return true;
}

void MonitorManager::AddTasks(GraphKV *kv) {
#define ADD_TASK(name, Type)                                  \
  do {                                                        \
    std::unique_ptr<MonitorTask> a(std::make_unique<Type>()); \
    if (!a->Initialize(kv)) {                                 \
      LOG(ERROR) << "Task: " << #Type << " init fail!";       \
      break;                                                  \
    }                                                         \
    tasks_[name] = std::move(a);                              \
    LOG(INFO) << "Add Task: " << #Type;                       \
  } while (0)
  ADD_TASK("PerfGraphLength", PerfGraphLength);
  ADD_TASK("PerfQuantile", PerfQuantile);
  ADD_TASK("PerfSampleResultCountQuantile", PerfSampleResultCountQuantile);
  ADD_TASK("PerfMemUsage", PerfMemUsage);
}

}  // namespace chrono_graph
