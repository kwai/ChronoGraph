// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <string>

#include "base/common/basic_types.h"
#include "base/common/time.h"

namespace base {

// NOT thread safe.
class Counter {
 public:
  struct IntervalCount {
    int64_t timestamp;
    int64_t start_count;
    int64_t end_count;
    void Update(int64_t now, int64_t current_count, int64_t interval) {
      if (now - timestamp > interval) {
        if (now - timestamp > 2 * interval) {
          timestamp += ((now - timestamp) / interval) * interval;
        } else {
          timestamp += interval;
        }
        start_count = end_count;
        end_count = current_count;
      }
    }
    int64_t GetCount() const { return end_count - start_count; }
    int64_t GetCount(int64_t interval) const {
      if (base::GetTimestamp() - timestamp < interval) { return end_count - start_count; }
      return 0;
    }
    IntervalCount() : timestamp(0), start_count(0), end_count(0) {}
  };
  Counter() : count_(0) {}
  explicit Counter(const std::string &name) : name_(name), count_(0) {}
  explicit Counter(const char *name) : name_(name), count_(0) {}
  void Inc(int64_t count);
  void SetName(const std::string &name) { name_ = name; }
  int64_t GetCount() const { return count_; }
  const std::string &GetName() const { return name_; }
  std::string Display() const;
  int64_t GetQPS() const { return second_count_.GetCount(1000000L); }
  int64_t GetMinuteCount() const { return minute_count_.GetCount(1000000L * 60); }

 private:
  std::string name_;
  int64_t count_;
  IntervalCount second_count_;
  IntervalCount minute_count_;
  IntervalCount hour_count_;
  IntervalCount day_count_;

  DISALLOW_COPY_AND_ASSIGN(Counter);
};

}  // namespace base
