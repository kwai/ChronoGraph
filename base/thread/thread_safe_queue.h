// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <atomic>
#include <queue>

#include "absl/synchronization/mutex.h"
#include "absl/synchronization/notification.h"

namespace thread {

template <typename T>
class ThreadSafeQueue {
 public:
  void Push(const T &value) {
    absl::MutexLock lock(&mutex_);
    queue_.push(value);
  }

  bool TryPop(T &value) {
    absl::MutexLock lock(&mutex_);
    if (queue_.empty()) { return false; }
    value = queue_.front();
    queue_.pop();
    return true;
  }

  bool Take(T *element) {
    absl::MutexLock lock(&mutex_);
    mutex_.Await(absl::Condition(this, &ThreadSafeQueue::NotEmpty));
    if (closed_) { return false; }
    *element = queue_.front();
    queue_.pop();
    return true;
  }

  bool Empty() const {
    absl::MutexLock lock(&mutex_);
    return queue_.empty();
  }

  void Close() {
    absl::MutexLock lock(&mutex_);
    if (closed_) { return; }
    closed_ = true;
  }

 private:
  bool NotEmpty() const { return !queue_.empty() || closed_; }

  mutable absl::Mutex mutex_;
  std::queue<T> queue_;
  std::atomic<bool> closed_ = false;
};

}  // namespace thread
