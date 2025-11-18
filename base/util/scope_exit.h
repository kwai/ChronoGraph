// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <functional>

namespace base {

class ScopeExit {
 public:
  explicit ScopeExit(std::function<void()> f): func_(f) {}
  ~ScopeExit() { func_(); }

 private:
  std::function<void()> func_;
};

}  // namespace base

