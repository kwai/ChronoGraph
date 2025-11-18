// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "base/common/logging.h"

#include <filesystem>
#include <string>
#include "absl/log/initialize.h"
#include "absl/log/log_sink_registry.h"
#include "base/common/gflags.h"

ABSL_FLAG(std::string, log_dir, "log", "lod directory");

void InitLogging(char *file_prefix) {
  std::string prefix;
  if (file_prefix) { prefix = std::filesystem::path(file_prefix).filename(); }
  static FileLogSink log_sink(prefix);
  absl::AddLogSink(&log_sink);
  absl::InitializeLog();
}