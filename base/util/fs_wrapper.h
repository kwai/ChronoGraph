// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once
#include <string>
#include <vector>
#include "base/common/basic_types.h"

namespace base {

// Dummy filesystem wrapper.
class FSWrapper {
 public:
  FSWrapper() {}
  ~FSWrapper() {}

  // All functions should return true on success

  // Put `data` to file, if file exists, clear and overwrite it; else create the file.
  // In HDFS, if size is larger than INT32_MAX, the write request should be split to several smaller parts.
  virtual bool PutData(const char *data, int64 size, const std::string &filename) = 0;

  // Load file content to `file_data`.
  virtual bool GetData(const std::string &filename, std::string *file_data) = 0;

  // List all files under directory (directories not listed), result written to `filenames`.
  virtual bool ListDirectory(const std::string &dir_path, std::vector<std::string> *filenames) = 0;

  // List all files and directories under directory, result written to `itemnames`.
  virtual bool ListDirectoryItems(const std::string &dir_path, std::vector<std::string> *itemnames) = 0;

  // return true only when `path` is a directory.
  virtual bool IsDirectory(const std::string &path) = 0;

  // Check if file exists. If `path` is a directory, return false.
  virtual bool FileExists(const std::string &path) = 0;

  // If `path` is directory, return true.
  // Otherwise, remove `path` if it's a file, and create a new directory.
  virtual bool ExistOrCreateDirectory(const std::string &path) = 0;

  // remove `path` recursively, return true on success.
  virtual bool Rmr(const std::string &path) = 0;

  // Move file at `src` to `dest`. If `dest` exists, remove it first.
  virtual bool Move(const std::string &src, const std::string &dest) = 0;

  // Create an empty file at `path`.
  virtual bool Touch(const std::string &path) = 0;

 private:
  DISALLOW_COPY_AND_ASSIGN(FSWrapper);
};

// Dummy filesystem wrapper.
class DummyFSWrapper : public FSWrapper {
 public:
  DummyFSWrapper() {}
  ~DummyFSWrapper() {}

  bool PutData(const char *data, int64 size, const std::string &filename) override { return false; }
  bool GetData(const std::string &filename, std::string *file_data) override { return false; }
  bool ListDirectory(const std::string &dir_path, std::vector<std::string> *filenames) override {
    return false;
  }
  bool ListDirectoryItems(const std::string &dir_path, std::vector<std::string> *itemnames) override {
    return false;
  }
  bool IsDirectory(const std::string &path) override { return false; }
  bool FileExists(const std::string &path) override { return false; }
  bool ExistOrCreateDirectory(const std::string &path) override { return false; }
  bool Rmr(const std::string &path) override { return false; }
  bool Move(const std::string &src, const std::string &dest) override { return false; }
  bool Touch(const std::string &path) override { return false; }

 private:
  DISALLOW_COPY_AND_ASSIGN(DummyFSWrapper);
};

// Local filesystem wrapper.
class LocalFSWrapper : public FSWrapper {
 public:
  LocalFSWrapper() {}
  ~LocalFSWrapper() {}

  bool PutData(const char *data, int64 size, const std::string &filename) override;
  bool GetData(const std::string &filename, std::string *file_data) override;
  bool ListDirectory(const std::string &dir_path, std::vector<std::string> *filenames) override;
  bool ListDirectoryItems(const std::string &dir_path, std::vector<std::string> *itemnames) override;
  bool IsDirectory(const std::string &path) override;
  bool FileExists(const std::string &path) override;
  bool ExistOrCreateDirectory(const std::string &path) override;
  bool Rmr(const std::string &path) override;
  bool Move(const std::string &src, const std::string &dest) override;
  bool Touch(const std::string &path) override;

 private:
  DISALLOW_COPY_AND_ASSIGN(LocalFSWrapper);
};

}  // namespace base