// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <fstream>
#include <iostream>

#include "absl/log/check.h"
#include "absl/log/log.h"
#include "base/common/file_util.h"
#include "base/common/gflags.h"
#include "base/common/time.h"

#define NOT_REACHED() CHECK(false)

ABSL_DECLARE_FLAG(std::string, log_dir);

class FileLogSink : public absl::LogSink {
 public:
  FileLogSink() { FileLogSink(""); }
  explicit FileLogSink(const std::string &filename) {
    if (!filename.empty()) {
      std::string fullname =
          filename + "." + absl::FormatTime("%Y%m%d-%H%M%S", absl::Now(), absl::LocalTimeZone()) + ".log";
      if (!base::ExistOrCreateDirectory(absl::GetFlag(FLAGS_log_dir))) {
        std::cout << "fail to create log dir: " << absl::GetFlag(FLAGS_log_dir);
      }
      file_.open(absl::GetFlag(FLAGS_log_dir) + "/" + fullname, std::ios::out | std::ios::app);
      std::cout << "Log writing to " << fullname << std::endl;
    }
  }

  ~FileLogSink() override {
    if (file_.is_open()) { file_.close(); }
  }

  void Send(const absl::LogEntry &entry) override {
    if (file_.is_open()) {
      file_ << entry.text_message_with_prefix_and_newline();
    } else {
      std::cout << entry.text_message_with_prefix_and_newline();
    }
  }

 private:
  std::ofstream file_;
};

void InitLogging(char *file_prefix = nullptr);