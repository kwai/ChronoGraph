// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <string>

#include "base/common/basic_types.h"
#include "base/common/string.h"
#include "base/common/time.h"

namespace base {
class Timer {
 public:
  Timer() { Start(); }

  ~Timer() {}

  void Start() {
    start_time_ = base::GetTimestamp();
    last_stop_ = start_time_;
    display_.clear();
  }

  int64 Stop() {
    int64 stop_time = base::GetTimestamp();
    last_stop_ = stop_time;
    return (stop_time - start_time_);
  }

  int64 start_ts() const { return start_time_; }
  int64 last_stop_ts() const { return last_stop_; }

  int64 Interval() {
    int64 stop_time = base::GetTimestamp();
    int64 delta_time = stop_time - last_stop_;
    last_stop_ = stop_time;
    return delta_time;
  }

  void AppendCostMs(const std::string &key) {
    float interval_ms = Interval() / 1000.f;
    display_.append(absl::StrFormat(" %s:%fms", key.c_str(), interval_ms));
  }

  void AppendCostMs(const std::string &key, int64 interval_ms) {
    display_.append(absl::StrFormat(" %s:%ldms", key.c_str(), interval_ms));
  }

  int64 IntervalWithLog(const std::string &key) {
    int64 interval_us = Interval();
    display_.append(absl::StrFormat(",%s:%fms", key.c_str(), interval_us / 1000.f));
    return interval_us;
  }

  const std::string display() const { return display_; }

 private:
  int64 start_time_;
  int64 last_stop_;
  std::string display_;
};
}  // namespace base