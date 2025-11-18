// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "chrono_graph/src/base/block_storage_api.h"

#include "base/common/logging.h"
#include "base/util/math.h"
#include "chrono_graph/src/base/config_helper.h"

ABSL_FLAG(int32, int_block_max_size, 4, "number of int attr in simple attr block");
ABSL_FLAG(int32, float_block_max_size, 4, "number of float attr in simple attr block");
ABSL_FLAG(int64, simple_block_int_init_value, 0, "initial value for int attr");
ABSL_FLAG(double, simple_block_float_init_value, 0.0f, "initial value for float attr");

namespace chrono_graph {

std::unique_ptr<BlockStorageApi> BlockStorageApi::NewInstance(base::Json *config) {
  auto type = GetStringConfigSafe(config, "type_name");
  if (type == "EmptyBlock") { return std::make_unique<EmptyBlock>(config); }
  if (type == "SimpleAttrBlock") { return std::make_unique<SimpleAttrBlock>(config); }
  NOT_REACHED() << "BlockStorageApi type not support: " << type;
  return std::unique_ptr<BlockStorageApi>();
}

bool BlockStorageApi::InitialBlock(char *block, size_t available_size) { return available_size >= MaxSize(); }

float BlockStorageApi::UsedAreaRatio(const char *block) const { return 1.0f; }

size_t SimpleAttrBlock::MaxSize() const {
  return int_max_size_ * sizeof(int64) + float_max_size_ * sizeof(float);
}

float SimpleAttrBlock::UsedAreaRatio(const char *block) const {
  float total = int_max_size_ + float_max_size_;
  int used = 0;
  for (size_t i = 0; i < int_max_size_; ++i) {
    used += (GetIntX(block, i) != absl::GetFlag(FLAGS_simple_block_int_init_value));
  }
  for (size_t i = 0; i < float_max_size_; ++i) {
    used += (!base::IsEqual(absl::GetFlag(FLAGS_simple_block_float_init_value), GetFloatX(block, i)));
  }
  return used / total;
}

bool SimpleAttrBlock::UpdateBlock(char *block, const char *update_info, size_t info_size) {
  if (info_size != MaxSize()) {
    LOG_EVERY_N(WARNING, 10000) << "SimpleAttrBlock update info size not match, "
                                << "max size = " << MaxSize() << ", given info block size = " << info_size;
    return false;
  }
  memcpy(block, update_info, info_size);
  return true;
}

std::string SimpleAttrBlock::SerializeBlock(const char *block) const { return std::string(block, MaxSize()); }

bool SimpleAttrBlock::InitialBlock(char *block, size_t available_size) {
  if (available_size != MaxSize()) {
    LOG_EVERY_N(WARNING, 10000) << "SimpleAttrBlock Init Error, available size = " << available_size
                                << ", expect size = " << MaxSize();
    return false;
  }
  for (size_t i = 0; i < int_max_size_; ++i) {
    SetIntX(block, i, absl::GetFlag(FLAGS_simple_block_int_init_value));
  }
  for (size_t i = 0; i < float_max_size_; ++i) {
    SetFloatX(block, i, absl::GetFlag(FLAGS_simple_block_float_init_value));
  }
  return true;
}

}  // namespace chrono_graph
