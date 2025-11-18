// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <future>
#include <memory>
#include <utility>

#include "base/thread/thread_pool.h"

namespace base {

class AsyncFuturePool {
 public:
  AsyncFuturePool() { thread_pool_ = std::make_unique<thread::ThreadPool>(1); }
  explicit AsyncFuturePool(int thread_num) {
    thread_pool_ = std::make_unique<thread::ThreadPool>(thread_num);
  }
  ~AsyncFuturePool() {
    if (thread_pool_) { thread_pool_->JoinAll(); }
  }
  template <typename R>
  std::future<R> Async(std::function<R()> func) {
    Closure<R> *closure = new Closure<R>(func);
    std::future<R> future = closure->task.get_future();
    thread_pool_->AddTask([closure]() { closure->Run(); });
    return future;
  }

 private:
  template <typename R>
  struct Closure {
    explicit Closure(std::function<R()> func) : task(func) {}
    std::packaged_task<R()> task;
    void Run() {
      task();
      delete this;
    }
  };
  std::unique_ptr<thread::ThreadPool> thread_pool_;
};

}  // namespace base
