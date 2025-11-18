// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <queue>
#include <thread>
#include <vector>

#include "absl/functional/any_invocable.h"
#include "absl/synchronization/mutex.h"
#include "base/common/basic_types.h"

namespace thread {

// |ThreadPool| provides a light-weight way to run multiple tasks in a pool of threads.
class ThreadPool {
 public:
  explicit ThreadPool(int thread_num);
  ~ThreadPool();

  int ThreadNum() const { return threads_.size(); }

  /** \brief Puts |task| into this pool, which will run immediately after there is an idle thread.
   * This method returns immediately, won't block the caller.
   *
   * NOTE:
   * 1. this method is NOT thread-safe
   * 2. Can't call AddTask() after JoinAll() or StopAll() called.
   */
  void AddTask(absl::AnyInvocable<void()> func);

  /** \brief Joins all threads. If there are pending tasks, this function will wait until all tasks get run,
   * and then join all threads.
   *
   * NOTE:
   * 1. this method is NOT thread-safe
   * 2. Can't continue using the thread pool after this function returns.
   */
  void JoinAll();

  /** \brief Stops all threads. If there are pending tasks, this function will delete them, and then join all
   * threads.
   *
   * NOTE:
   * 1. this method is NOT thread-safe
   * 2. Can't continue using the thread pool after this function returns.
   */
  void StopAll();

  /** \brief Get number of tasks that are added but not started yet.
   *
   * NOTE: this method IS thread-safe
   */
  int PendingTaskNum() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) { return task_queue_.size(); }

 private:
  bool WorkAvailable() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) { return !task_queue_.empty() || stopped_; }
  // the internal thread loop
  void Loop();

  bool stopped_;
  absl::Mutex mu_;
  std::queue<absl::AnyInvocable<void()>> task_queue_ ABSL_GUARDED_BY(mu_);
  std::vector<std::thread> threads_;

  DISALLOW_COPY_AND_ASSIGN(ThreadPool);
};

}  // namespace thread
