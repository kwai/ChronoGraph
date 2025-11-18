// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "base/common/time.h"

namespace base {

int64_t GetTimestamp() { return absl::ToUnixMicros(absl::Now()); }

std::string TimestampToString(int64 timestamp_s, const std::string &format) {
  absl::Time timestamp = absl::FromUnixSeconds(timestamp_s);
  return absl::FormatTime(format, timestamp, absl::LocalTimeZone());
}

}  // namespace base