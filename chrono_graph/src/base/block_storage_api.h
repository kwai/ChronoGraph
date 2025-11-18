// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <functional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "base/common/basic_types.h"
#include "base/common/gflags.h"
#include "base/common/logging.h"
#include "base/jansson/json.h"

ABSL_DECLARE_FLAG(int32, int_block_max_size);
ABSL_DECLARE_FLAG(int32, float_block_max_size);

namespace chrono_graph {

/**
 * Manipulate a memory block, which contains several int64 and float.
 */
class BlockStorageApi {
 public:
  BlockStorageApi() {}
  virtual ~BlockStorageApi() {}
  virtual size_t MaxSize() const = 0;

  virtual float UsedAreaRatio(const char *block) const = 0;

  /**
   * \brief Update the given block.
   * \param block
   * \param update_info update content ptr
   * \param info_size update content size
   * \return success.
   */
  virtual bool UpdateBlock(char *block, const char *update_info, size_t info_size) = 0;

  virtual std::string SerializeBlock(const char *block) const = 0;

  virtual bool InitialBlock(char *block, size_t available_size) = 0;

  /**
   * int and float in block -> vector.
   */
  virtual void GetAll(const char *block,
                      std::vector<int64> *int_vec,
                      std::vector<float> *float_vec) const = 0;

  /**
   * int and float in vector -> block, truncate if block is not large enough.
   */
  virtual void SetAll(char *block, std::vector<int64> *int_vec, std::vector<float> *float_vec) = 0;

  virtual std::string DebugInfo(const char *block) { return "Debug Func Not Implement!"; }

  static std::unique_ptr<BlockStorageApi> NewInstance(base::Json *config);
};

// Default block api, do nothing.
class EmptyBlock final : public BlockStorageApi {
 public:
  EmptyBlock() {}
  explicit EmptyBlock(base::Json *config) {}

  size_t MaxSize() const override { return 0; }

  float UsedAreaRatio(const char *block) const override { return BlockStorageApi::UsedAreaRatio(block); }

  bool UpdateBlock(char *block, const char *update_log, size_t log_size) override { return true; }

  std::string SerializeBlock(const char *block) const override { return ""; }

  bool InitialBlock(char *block, size_t available_size) override {
    return BlockStorageApi::InitialBlock(block, available_size);
  }

  void GetAll(const char *block, std::vector<int64> *int_vec, std::vector<float> *float_vec) const override {}
  void SetAll(char *block, std::vector<int64> *int_vec, std::vector<float> *float_vec) override {}
  std::string DebugInfo(const char *block) override { return "EmptyBlock"; }
};

/**
 * Contains several int64 and float.
 */
class SimpleAttrBlock : public BlockStorageApi {
 public:
  SimpleAttrBlock() {
    int_max_size_ = absl::GetFlag(FLAGS_int_block_max_size);
    float_max_size_ = absl::GetFlag(FLAGS_float_block_max_size);
    LOG(WARNING) << "Initialize SimpleAttrBlock without config. Use flag config: int_size = " << int_max_size_
                 << ", float_size = " << float_max_size_;
  }
  explicit SimpleAttrBlock(base::Json *config) {
    int_max_size_ = config->GetInt("int_max_size", absl::GetFlag(FLAGS_int_block_max_size));
    float_max_size_ = config->GetInt("float_max_size", absl::GetFlag(FLAGS_float_block_max_size));
  }

  size_t MaxSize() const override;

  float UsedAreaRatio(const char *block) const override;

  bool UpdateBlock(char *block, const char *update_info, size_t info_size) override;

  std::string SerializeBlock(const char *block) const override;

  bool InitialBlock(char *block, size_t available_size) override;

  void GetAll(const char *block, std::vector<int64> *int_vec, std::vector<float> *float_vec) const override {
    if (int_vec) {
      for (size_t i = 0; i < int_max_size_; ++i) { int_vec->emplace_back(GetIntX(block, i)); }
    }
    if (float_vec) {
      for (size_t i = 0; i < float_max_size_; ++i) { float_vec->emplace_back(GetFloatX(block, i)); }
    }
  }

  void SetAll(char *block, std::vector<int64> *int_vec, std::vector<float> *float_vec) override {
    if (int_vec) {
      for (size_t i = 0; i < int_max_size_ && i < int_vec->size(); ++i) { SetIntX(block, i, (*int_vec)[i]); }
    }
    if (float_vec) {
      for (size_t i = 0; i < float_max_size_ && i < float_vec->size(); ++i) {
        SetFloatX(block, i, (*float_vec)[i]);
      }
    }
  }

  int64 GetIntX(const char *block, size_t offset) const {
    CHECK_LT(offset, int_max_size_) << "SimpleAttrBlock get error: offset = " << offset
                                    << ", max size = " << int_max_size_ << ", memory size = " << MaxSize();
    return *reinterpret_cast<const int64 *>(UnsafeGetIntXAddr(const_cast<char *>(block), offset));
  }

  float GetFloatX(const char *block, size_t offset) const {
    CHECK_LT(offset, float_max_size_)
        << "SimpleAttrBlock get error: offset = " << offset << ", max size = " << float_max_size_
        << ", memory size = " << MaxSize();
    return *reinterpret_cast<const float *>(UnsafeGetFloatXAddr(const_cast<char *>(block), offset));
  }

  void SetIntX(char *block, size_t offset, int64 val) const {
    CHECK_LT(offset, int_max_size_) << "SimpleAttrBlock get error: offset = " << offset
                                    << ", max size = " << int_max_size_ << ", memory size = " << MaxSize();
    *reinterpret_cast<int64 *>(UnsafeGetIntXAddr(block, offset)) = val;
  }

  void SetFloatX(char *block, size_t offset, float val) const {
    CHECK_LT(offset, float_max_size_)
        << "SimpleAttrBlock get error: offset = " << offset << ", max size = " << float_max_size_
        << ", memory size = " << MaxSize();
    *reinterpret_cast<float *>(UnsafeGetFloatXAddr(block, offset)) = val;
  }

  std::string DebugInfo(const char *block) override {
    std::ostringstream stream;
    stream << "int attr len = " << int_max_size_ << ", content=(";
    for (size_t i = 0; i < int_max_size_; ++i) { stream << GetIntX(block, i) << ","; }
    stream << ");";
    stream << "float attr len = " << float_max_size_ << ", content=(";
    for (size_t i = 0; i < float_max_size_; ++i) { stream << GetFloatX(block, i) << ","; }
    stream << ")";
    return stream.str();
  }

 protected:
  char *UnsafeGetIntXAddr(char *block, size_t offset) const { return block + offset * sizeof(int64); }
  char *UnsafeGetFloatXAddr(char *block, size_t offset) const {
    return block + int_max_size_ * sizeof(int64) + offset * sizeof(float);
  }
  int int_max_size_ = 0;
  int float_max_size_ = 0;
};

}  // namespace chrono_graph
