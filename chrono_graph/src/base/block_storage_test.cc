// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include <memory>
#include <sstream>
#include <string>

#include "base/common/gflags.h"
#include "base/jansson/json.h"
#include "base/testing/gtest.h"
#include "base/util/scope_exit.h"
#include "chrono_graph/src/base/block_storage_api.h"

ABSL_DECLARE_FLAG(int32, int_block_max_size);
ABSL_DECLARE_FLAG(int32, float_block_max_size);
ABSL_DECLARE_FLAG(int64, simple_block_int_init_value);
ABSL_DECLARE_FLAG(double, simple_block_float_init_value);

namespace chrono_graph {

TEST(BlockStorageApiTest, SimpleAttrBlockTest) {
  base::Json init_config(base::StringToJson("{}"));
  init_config.set("type_name", "SimpleAttrBlock");

  absl::SetFlag(&FLAGS_int_block_max_size, 16);
  absl::SetFlag(&FLAGS_float_block_max_size, 16);
  auto block_handler = BlockStorageApi::NewInstance(&init_config);
  auto block_handler_act = reinterpret_cast<SimpleAttrBlock *>(block_handler.get());

  ASSERT_EQ(block_handler->MaxSize(),
            absl::GetFlag(FLAGS_int_block_max_size) * sizeof(int64) +
                absl::GetFlag(FLAGS_float_block_max_size) * sizeof(float));
  char *content = static_cast<char *>(malloc(block_handler->MaxSize()));
  char *update_info = static_cast<char *>(malloc(block_handler->MaxSize()));
  base::ScopeExit a([update_info, content]() {
    free(static_cast<void *>(update_info));
    free(static_cast<void *>(content));
  });
  ASSERT_TRUE(block_handler->InitialBlock(content, block_handler->MaxSize()));
  ASSERT_TRUE(block_handler->InitialBlock(update_info, block_handler->MaxSize()));
  for (size_t i = 0; i < 8; ++i) {
    block_handler_act->SetIntX(update_info, i, 1 + i);
    block_handler_act->SetFloatX(update_info, i, 10.0f + i);
  }
  ASSERT_FLOAT_EQ(block_handler->UsedAreaRatio(content), 0);
  ASSERT_FLOAT_EQ(block_handler->UsedAreaRatio(update_info), 0.5);
  ASSERT_EQ(absl::GetFlag(FLAGS_simple_block_int_init_value), block_handler_act->GetIntX(update_info, 9));
  ASSERT_FLOAT_EQ(absl::GetFlag(FLAGS_simple_block_float_init_value),
                  block_handler_act->GetFloatX(update_info, 9));

  ASSERT_EQ(block_handler->SerializeBlock(update_info).size(), block_handler->MaxSize());

  ASSERT_TRUE(block_handler->UpdateBlock(content, update_info, block_handler->MaxSize()));
  ASSERT_FLOAT_EQ(block_handler->UsedAreaRatio(content), 0.5);
}

}  // namespace chrono_graph
