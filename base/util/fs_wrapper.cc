// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "base/util/fs_wrapper.h"

#include <filesystem>
#include <fstream>
#include "base/common/file_util.h"

namespace base {
bool LocalFSWrapper::PutData(const char *data, int64 size, const std::string &filename) {
  return base::WriteFile(filename, data, size);
}

bool LocalFSWrapper::GetData(const std::string &filename, std::string *file_data) {
  return base::ReadFileToString(filename, file_data);
}

bool LocalFSWrapper::ListDirectory(const std::string &dir_path, std::vector<std::string> *filenames) {
  try {
    if (!fs::exists(dir_path)) return false;

    for (const auto &entry : fs::directory_iterator(dir_path)) {
      if (entry.is_regular_file()) { filenames->push_back(entry.path().filename().string()); }
    }
    return true;
  } catch (...) { return false; }
}

bool LocalFSWrapper::ListDirectoryItems(const std::string &dir_path, std::vector<std::string> *itemnames) {
  try {
    if (!fs::exists(dir_path)) { return false; }

    for (const auto &entry : fs::directory_iterator(dir_path)) {
      itemnames->push_back(entry.path().filename().string());
    }
    return true;
  } catch (...) { return false; }
}

bool LocalFSWrapper::IsDirectory(const std::string &path) { return fs::is_directory(path); }

bool LocalFSWrapper::FileExists(const std::string &path) { return fs::is_regular_file(path); }

bool LocalFSWrapper::ExistOrCreateDirectory(const std::string &path) {
  if (IsDirectory(path)) { return true; }
  if (FileExists(path)) { Rmr(path); }
  return fs::create_directories(path);
}

bool LocalFSWrapper::Rmr(const std::string &path) { return fs::remove_all(path); }

bool LocalFSWrapper::Move(const std::string &src, const std::string &dest) {
  try {
    if (fs::exists(dest)) { Rmr(dest); }
    fs::rename(src, dest);
    return true;
  } catch (...) { return false; }
}

bool LocalFSWrapper::Touch(const std::string &path) { return PutData("", 0, path); }

}  // namespace base