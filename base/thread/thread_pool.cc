// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "base/thread/thread_pool.h"

#include <memory>
#include "base/common/logging.h"

namespace thread {

ThreadPool::ThreadPool(int thread_num) : stopped_(false) {
  threads_.reserve(thread_num);
  for (size_t i = 0; i < thread_num; ++i) { threads_.push_back(std::thread(&ThreadPool::Loop, this)); }
}

ThreadPool::~ThreadPool() {
  LOG_IF(FATAL, !stopped_) << "You need to call JoinAll() or StopAll() before ThreadPool is destroyed.";
}

void ThreadPool::AddTask(absl::AnyInvocable<void()> func) {
  LOG_IF(FATAL, stopped_) << "You can't call AddTask() after JoinAll() or StopAll() was called.";
  CHECK(func != nullptr);
  absl::MutexLock l(&mu_);
  task_queue_.push(std::move(func));
}

void ThreadPool::JoinAll() {
  LOG_IF(FATAL, stopped_) << "You can't call JoinAll() again after JoinAll() or StopAll() was called.";

  // Post empty task to each thread
  {
    absl::MutexLock l(&mu_);
    for (size_t i = 0; i < (int)threads_.size(); ++i) { task_queue_.push(nullptr); }
  }
  // Join all threads
  for (auto &t : threads_) { t.join(); }
  stopped_ = true;
}

void ThreadPool::StopAll() {
  LOG_IF(FATAL, stopped_) << "You can't call StopAll() again after JoinAll() or StopAll() was called.";

  // clear pending tasks
  std::queue<absl::AnyInvocable<void()>> empty;
  {
    absl::MutexLock l(&mu_);
    task_queue_.swap(empty);
    for (size_t i = 0; i < (int)threads_.size(); ++i) { task_queue_.push(nullptr); }
  }
  int num = empty.size();
  LOG_IF(WARNING, num > 0) << num << " pending tasks deleted without running.";
  for (auto &t : threads_) { t.join(); }
  stopped_ = true;
}

// The thread function, each thread in pool runs this loop, waiting for tasks
// from the task queue, keep invoking the task, until gets a NULL task.
void ThreadPool::Loop() {
  for (;;) {
    absl::AnyInvocable<void()> func;
    {
      absl::MutexLock l(&mu_);
      mu_.Await(absl::Condition(this, &ThreadPool::WorkAvailable));
      func = std::move(task_queue_.front());
      task_queue_.pop();
    }
    if (func == nullptr) {  // Shutdown signal.
      LOG(INFO) << "ThreadPool::Loop() is exiting.";
      break;
    }
    func();
  }
}

}  // namespace thread
