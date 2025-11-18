// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "memtable/util/counter.h"

#include <cstdio>
#include <map>

#include "base/common/basic_types.h"
#include "base/common/string.h"
#include "base/common/time.h"

namespace base {

static std::map<std::string, Counter *> counters;

void Counter::Inc(int64_t count) {
  int64_t now = base::GetTimestamp();
  second_count_.Update(now, count_, 1000000L);
  minute_count_.Update(now, count_, 1000000L * 60);
  hour_count_.Update(now, count_, 1000000L * 3600);
  day_count_.Update(now, count_, 1000000L * 3600L * 24L);
  count_ += count;
}

std::string Counter::Display() const {
  return absl::StrFormat("[%s] qps: %ld, minute: %ld, hour: %ld, day: %ld, total: %ld",
                         name_.c_str(),
                         second_count_.GetCount(1000000L),
                         minute_count_.GetCount(1000000L * 60),
                         hour_count_.GetCount(1000000L * 3600),
                         day_count_.GetCount(1000000L * 3600L * 24L),
                         count_);
}

}  // namespace base
