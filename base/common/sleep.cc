// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include <time.h>
#include <errno.h>

#include "base/common/logging.h"
#include "base/common/sleep.h"

namespace base {

void SleepForMilliseconds(int milliseconds) {
  // Sleep for a few milliseconds
  struct timespec sleep_time;
  sleep_time.tv_sec = milliseconds / 1000;
  sleep_time.tv_nsec = (milliseconds % 1000) * 1000000;
  int ret;
  while ((ret = nanosleep(&sleep_time, &sleep_time)) != 0 && errno == EINTR) {
    // Ignore signals and wait for the full interval to elapse.
  }
  PLOG_IF(FATAL, ret == -1);
}

void SleepForSeconds(int seconds) {
  SleepForMilliseconds(seconds * 1000);
}

}  // namespace base

