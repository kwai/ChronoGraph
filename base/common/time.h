// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "base/common/basic_types.h"

namespace base {

// Get microseconds sine Epoch (1970-01-01 00:00:00 000000)
// NOTE: it's micro-seconds (0.000001 second), not milli-seconds (0.001 second).
int64 GetTimestamp();

std::string TimestampToString(int64 timestamp_s, const std::string &format = "[%Y-%m-%d %H:%M:%S]");

class Time {
 public:
  static const int64 kSecondsPerMinute = 60;
  static const int64 kSecondsPerHour = kSecondsPerMinute * 60;
  static const int64 kSecondsPerDay = kSecondsPerHour * 24;
  static const int64 kSecondsPerWeek = kSecondsPerDay * 7;

  static const int64 kMillisecondsPerSecond = 1000;
  static const int64 kMillisecondsPerMinute = kMillisecondsPerSecond * 60;
  static const int64 kMillisecondsPerHour = kMillisecondsPerMinute * 60;
  static const int64 kMillisecondsPerDay = kMillisecondsPerHour * 24;
  static const int64 kMillisecondsPerWeek = kMillisecondsPerDay * 7;

  static const int64 kMicrosecondsPerMillisecond = 1000;
  static const int64 kMicrosecondsPerSecond = kMicrosecondsPerMillisecond * kMillisecondsPerSecond;
  static const int64 kMicrosecondsPerMinute = kMicrosecondsPerSecond * 60;
  static const int64 kMicrosecondsPerHour = kMicrosecondsPerMinute * 60;
  static const int64 kMicrosecondsPerDay = kMicrosecondsPerHour * 24;
  static const int64 kMicrosecondsPerWeek = kMicrosecondsPerDay * 7;
  static const int64 kNanosecondsPerMicrosecond = 1000;
  static const int64 kNanosecondsPerSecond = kNanosecondsPerMicrosecond * kMicrosecondsPerSecond;
};

}  // namespace base