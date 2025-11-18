// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "base/common/file_util.h"

#include <fstream>
#include "base/common/logging.h"

namespace base {

bool GetFileSize(const std::string &file_name, uint64_t *size) {
  std::error_code ec;
  if (*size = fs::file_size(file_name, ec); ec) {
    LOG(ERROR) << "GetFileSize Error: " << file_name << " , " << ec.message();
    return false;
  }
  return true;
}

bool ReadFileToString(const fs::path &filePath, std::string *content) {
  if (!fs::exists(filePath)) {
    LOG(WARNING) << "File not exist: " + filePath.string();
    return false;
  }
  if (!fs::is_regular_file(filePath)) {
    LOG(WARNING) << "Not a regular file: " + filePath.string();
    return false;
  }

  std::ifstream file(filePath, std::ios::binary);
  if (!file.is_open()) { throw std::runtime_error("Cannot open file: " + filePath.string()); }

  *content = std::move(std::string((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>()));
  return true;
}

bool WriteFile(const std::string &file_path, const char *buf, int64_t buf_len) {
  try {
    std::ofstream file(file_path);
    if (!file.is_open()) {
      LOG(ERROR) << "Error opening file" << std::endl;
      return false;
    }

    std::string content(buf, buf_len);
    file << content;
    return file.good();
  } catch (const std::exception &e) {
    LOG(ERROR) << "Error: " << e.what();
    return false;
  }
}

bool ExistOrCreateDirectory(const std::string &path) {
  if (fs::is_directory(path)) { return true; }
  if (fs::is_regular_file(path)) { fs::remove_all(path); }
  return fs::create_directories(path);
}

};  // namespace base
