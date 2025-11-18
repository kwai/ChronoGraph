// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <filesystem>

namespace fs = std::filesystem;

namespace base {

bool GetFileSize(const std::string &file_name, uint64_t *size);

bool ReadFileToString(const fs::path &filePath, std::string *content);

bool WriteFile(const std::string &file_path, const char *buf, int64_t buf_len);

bool ExistOrCreateDirectory(const std::string &path);

};  // namespace base
