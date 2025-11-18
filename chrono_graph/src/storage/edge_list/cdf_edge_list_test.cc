// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "chrono_graph/src/storage/edge_list/cdf_edge_list.h"
#include <unordered_map>
#include <vector>
#include "base/common/gflags.h"
#include "base/jansson/json.h"
#include "base/testing/gtest.h"
#include "chrono_graph/src/storage/edge_list/el_interface.h"
#include "chrono_graph/src/storage/edge_list/mem_allocator.h"

ABSL_DECLARE_FLAG(bool, cdf_allow_weighted_sample);
ABSL_DECLARE_FLAG(int64, simple_block_int_init_value);
ABSL_DECLARE_FLAG(double, simple_block_float_init_value);

namespace chrono_graph {

#define ASSERT_ARRAY_EQ(A1, A2)                                              \
  do {                                                                       \
    ASSERT_EQ(A1.size(), A2.size());                                         \
    for (size_t i = 0; i < A1.size(); ++i) { ASSERT_EQ(A1[i], A2[i]) << i; } \
  } while (0)

class MockMallocApi : public MemAllocator {
 public:
  virtual ~MockMallocApi() {}
  base::KVData *New(int memory_size, uint32 *new_addr = nullptr) override {
    return static_cast<base::KVData *>(malloc(memory_size + sizeof(base::KVData)));
  }
  bool Free(base::KVData *memory_data) override {
    NOT_REACHED() << "SHOULD NOT CALL THIS!";
    return false;
  }
  bool CustomFree(void *memory_data) {
    free(static_cast<char *>(memory_data) - sizeof(base::KVData));
    return true;
  }
  std::string GetInfo() const { return "mock api"; }
};

TEST(CDFTest, TestAdd1) {
  auto attr_op = std::make_unique<EmptyBlock>();
  auto edge_attr_op = std::make_unique<EmptyBlock>();
  int capacity = 10;
  auto malloc_size = CDFEdgeList::MemorySize(attr_op.get(), edge_attr_op.get(), capacity);
  ASSERT_EQ(sizeof(CDFEdgeList), 33);
  ASSERT_EQ(malloc_size, sizeof(CDFEdgeList) + sizeof(CDFEdgeListItem) * capacity);

  auto edge_list = reinterpret_cast<CDFEdgeList *>(malloc(malloc_size));
  edge_list->Initialize(attr_op.get(), edge_attr_op.get(), 10);
  ASSERT_EQ(capacity, edge_list->capacity);
  uint64 big = 1ll << 50;
  std::vector<uint64> ids = {4 + big, 3, 2 + big, 1};
  std::vector<float> weights = {4, 3, 2, 1};
  edge_list->Add(ids, weights, LRU);
  ASSERT_EQ(4, edge_list->size);
  ASSERT_EQ(4, edge_list->out_degree);

  std::vector<uint64> actual_ids;
  std::vector<float> actual_weights;
  std::vector<uint64> expect_ids{1, 2 + big, 3, 4 + big};
  std::vector<float> expect_weights{1, 2, 3, 4};
  edge_list->Get(&actual_ids, &actual_weights);
  ASSERT_ARRAY_EQ(actual_ids, expect_ids);
  ASSERT_ARRAY_EQ(actual_weights, expect_weights);
}

// Test when the number of inserted edges is larger than the capacity.
TEST(CDFTest, TestAddMany) {
  auto attr_op = std::make_unique<EmptyBlock>();
  auto edge_attr_op = std::make_unique<EmptyBlock>();
  int capacity = 10;
  auto malloc_size = CDFEdgeList::MemorySize(attr_op.get(), edge_attr_op.get(), capacity);
  auto edge_list = reinterpret_cast<CDFEdgeList *>(malloc(malloc_size));
  edge_list->Initialize(attr_op.get(), edge_attr_op.get(), 10);
  ASSERT_EQ(capacity, edge_list->capacity);
  uint64 big = 1ll << 50;

  std::vector<uint64> ids = {1 + big, 2, 3, 4};
  std::vector<float> weights = {1, 2, 3, 4};
  ASSERT_EQ(true, edge_list->Add(ids, weights, LRU));
  ASSERT_EQ(4, edge_list->size);
  ASSERT_EQ(4, edge_list->out_degree);

  std::vector<uint64> actual_ids;
  std::vector<float> actual_weights;
  std::vector<uint64> expect_ids{4, 3, 2, 1 + big};
  std::vector<float> expect_weights{4, 3, 2, 1};
  edge_list->Get(&actual_ids, &actual_weights);
  ASSERT_ARRAY_EQ(actual_ids, expect_ids);
  ASSERT_ARRAY_EQ(actual_weights, expect_weights);

  ids = {5, 6 + big, 7, 8, 9};
  weights = {5, 6, 7, 8, 9};
  ASSERT_EQ(true, edge_list->Add(ids, weights, LRU));
  ASSERT_EQ(9, edge_list->size);
  ASSERT_EQ(9, edge_list->out_degree);
  actual_ids.clear();
  actual_weights.clear();
  expect_ids = {9, 8, 7, 6 + big, 5, 4, 3, 2, 1 + big};
  expect_weights = {9, 8, 7, 6, 5, 4, 3, 2, 1};
  edge_list->Get(&actual_ids, &actual_weights);
  ASSERT_ARRAY_EQ(actual_ids, expect_ids);
  ASSERT_ARRAY_EQ(actual_weights, expect_weights);

  // Exceed length limit
  ids = {10, 11, 12 + big, 13};
  weights = {10, 11, 12, 13};
  ASSERT_EQ(true, edge_list->Add(ids, weights, LRU));
  ASSERT_EQ(10, edge_list->size);
  ASSERT_EQ(13, edge_list->out_degree);
  actual_ids.clear();
  actual_weights.clear();
  expect_ids = {13, 12 + big, 11, 10, 9, 8, 7, 6 + big, 5, 4};
  expect_weights = {13, 12, 11, 10, 9, 8, 7, 6, 5, 4};
  edge_list->Get(&actual_ids, &actual_weights);
  ASSERT_ARRAY_EQ(actual_ids, expect_ids);
  ASSERT_ARRAY_EQ(actual_weights, expect_weights);
}

TEST(CDFTest, TestReformatCDF) {
  auto attr_op = std::make_unique<EmptyBlock>();
  auto edge_attr_op = std::make_unique<EmptyBlock>();
  int capacity = 10;
  auto malloc_size = CDFEdgeList::MemorySize(attr_op.get(), edge_attr_op.get(), capacity);
  auto edge_list = reinterpret_cast<CDFEdgeList *>(malloc(malloc_size));
  edge_list->Initialize(attr_op.get(), edge_attr_op.get(), 10);
  ASSERT_EQ(capacity, edge_list->capacity);

  std::vector<uint64> ids;
  std::vector<float> weights;
  for (size_t i = 0; i < 100000; ++i) {
    ids.emplace_back(i % 99 + 1);
    weights.emplace_back(i % 99 + 1);
  }
  ASSERT_EQ(true, edge_list->Add(ids, weights, LRU));
  ASSERT_EQ(10, edge_list->size);
  ASSERT_EQ(100000, edge_list->out_degree);
}

// CDF Adaptor Fill Test
TEST(CDFTest, CDFAdaptorFill) {
  std::unique_ptr<BlockStorageApi> attr_op = std::make_unique<EmptyBlock>();
  std::unique_ptr<BlockStorageApi> edge_attr_op = std::make_unique<EmptyBlock>();
  EdgeListConfig config;

  config.edge_capacity_max_num = 10000;

  MockMallocApi malloc_api;
  CDFAdaptor adaptor(attr_op.get(), edge_attr_op.get(), config);
  CommonUpdateItems items;
  items.ids = {1, 2};
  items.id_weights = {1, 2};

  uint32 new_addr;
  auto kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), nullptr);
  auto edge_list_ptr = kv_data_ptr->data();
  auto ptr = reinterpret_cast<CDFEdgeList *>(edge_list_ptr);
  int capacity = ptr->capacity;
  adaptor.config_.oversize_replace_strategy = LRU;
  adaptor.Fill(edge_list_ptr, nullptr, items);
  // capacity is set in InitBlock and won't be changed later
  ASSERT_EQ(capacity, ptr->capacity);
  ASSERT_EQ(ptr->size, 2);

  items.ids = {3, 4, 5, 6, 0, 100};
  items.id_weights = {3, 4, 5, 6, 100, 0};
  kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), edge_list_ptr);
  auto old_edge_list_ptr = edge_list_ptr;
  edge_list_ptr = kv_data_ptr->data();
  adaptor.config_.oversize_replace_strategy = LRU;
  adaptor.Fill(edge_list_ptr, old_edge_list_ptr, items);
  ASSERT_NE(edge_list_ptr, old_edge_list_ptr);
  ptr = reinterpret_cast<CDFEdgeList *>(edge_list_ptr);
  malloc_api.CustomFree(old_edge_list_ptr);
  ASSERT_EQ(ptr->size, 6);
  ASSERT_EQ(ptr->out_degree, 6);

  ASSERT_EQ(ptr->capacity, 8);
  std::vector<uint64> actual_ids;
  std::vector<float> actual_weights;
  std::vector<uint64> expect_ids{6, 5, 4, 3, 2, 1};
  std::vector<float> expect_weights{6, 5, 4, 3, 2, 1};
  ptr->Get(&actual_ids, &actual_weights);
  ASSERT_ARRAY_EQ(actual_ids, expect_ids);
  ASSERT_ARRAY_EQ(actual_weights, expect_weights);
  malloc_api.CustomFree(edge_list_ptr);
}

TEST(CDFTest, CDFAdaptorFill2) {
  std::unique_ptr<BlockStorageApi> attr_op = std::make_unique<EmptyBlock>();
  std::unique_ptr<BlockStorageApi> edge_attr_op = std::make_unique<EmptyBlock>();
  EdgeListConfig config;

  config.edge_capacity_max_num = 10000;

  MockMallocApi malloc_api;
  CDFAdaptor adaptor(attr_op.get(), edge_attr_op.get(), config);
  CommonUpdateItems items;
  items.ids = {1, 2};
  items.id_weights = {1, 2};
  uint32 new_addr;
  auto kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), nullptr);
  auto edge_list_ptr = kv_data_ptr->data();
  auto ptr = reinterpret_cast<CDFEdgeList *>(edge_list_ptr);
  adaptor.config_.oversize_replace_strategy = LRU;
  adaptor.Fill(edge_list_ptr, nullptr, items);

  // Exceed limit.
  items.ids = std::vector<uint64>(config.edge_capacity_max_num + 5, 1);
  items.id_weights = std::vector<float>(config.edge_capacity_max_num + 5, 1);
  items.ids[config.edge_capacity_max_num] = 50;
  items.ids[config.edge_capacity_max_num + 1] = 51;
  items.ids[config.edge_capacity_max_num + 2] = 52;
  items.ids[config.edge_capacity_max_num + 3] = 53;
  items.ids[config.edge_capacity_max_num + 4] = 54;
  items.id_weights[config.edge_capacity_max_num] = 50;
  items.id_weights[config.edge_capacity_max_num + 1] = 51;
  items.id_weights[config.edge_capacity_max_num + 2] = 52;
  items.id_weights[config.edge_capacity_max_num + 3] = 53;
  items.id_weights[config.edge_capacity_max_num + 4] = 54;

  kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), edge_list_ptr);
  auto old_edge_list_ptr = edge_list_ptr;
  edge_list_ptr = kv_data_ptr->data();
  adaptor.config_.oversize_replace_strategy = LRU;
  adaptor.Fill(edge_list_ptr, old_edge_list_ptr, items);
  ASSERT_NE(edge_list_ptr, old_edge_list_ptr);
  ptr = reinterpret_cast<CDFEdgeList *>(edge_list_ptr);
  malloc_api.CustomFree(old_edge_list_ptr);
  ASSERT_EQ(ptr->size, config.edge_capacity_max_num);
  ASSERT_EQ(ptr->out_degree, config.edge_capacity_max_num + 7);

  ASSERT_EQ(ptr->capacity, config.edge_capacity_max_num);
  std::vector<uint64> actual_ids;
  std::vector<float> actual_weights;
  std::vector<uint64> expect_ids(config.edge_capacity_max_num, 1);
  std::vector<float> expect_weights(config.edge_capacity_max_num, 1);
  expect_ids[0] = 54;
  expect_ids[1] = 53;
  expect_ids[2] = 52;
  expect_ids[3] = 51;
  expect_ids[4] = 50;
  expect_weights[0] = 54;
  expect_weights[1] = 53;
  expect_weights[2] = 52;
  expect_weights[3] = 51;
  expect_weights[4] = 50;
  ptr->Get(&actual_ids, &actual_weights);
  ASSERT_ARRAY_EQ(expect_ids, actual_ids);
  ASSERT_ARRAY_EQ(expect_weights, actual_weights);
  malloc_api.CustomFree(edge_list_ptr);
}

// Weight as single value.
TEST(CDFTest, CDFAdaptorFill3) {
  absl::SetFlag(&FLAGS_cdf_allow_weighted_sample, false);
  std::unique_ptr<BlockStorageApi> attr_op = std::make_unique<EmptyBlock>();
  std::unique_ptr<BlockStorageApi> edge_attr_op = std::make_unique<EmptyBlock>();
  EdgeListConfig config;

  config.edge_capacity_max_num = 10000;

  MockMallocApi malloc_api;
  CDFAdaptor adaptor(attr_op.get(), edge_attr_op.get(), config);
  CommonUpdateItems items;
  items.ids = {1, 2};
  items.id_weights = {1, 2};
  uint32 new_addr;
  auto kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), nullptr);
  auto edge_list_ptr = kv_data_ptr->data();
  auto ptr = reinterpret_cast<CDFEdgeList *>(edge_list_ptr);
  adaptor.config_.oversize_replace_strategy = LRU;
  adaptor.Fill(edge_list_ptr, nullptr, items);

  items.ids = std::vector<uint64>(config.edge_capacity_max_num + 5, 1);
  items.id_weights = std::vector<float>(config.edge_capacity_max_num + 5, 1);
  items.ids[config.edge_capacity_max_num] = 50;
  items.ids[config.edge_capacity_max_num + 1] = 51;
  items.ids[config.edge_capacity_max_num + 2] = 52;
  items.ids[config.edge_capacity_max_num + 3] = 53;
  items.ids[config.edge_capacity_max_num + 4] = 54;
  items.id_weights[config.edge_capacity_max_num] = 50;
  items.id_weights[config.edge_capacity_max_num + 1] = 51;
  items.id_weights[config.edge_capacity_max_num + 2] = 52;
  items.id_weights[config.edge_capacity_max_num + 3] = 53;
  items.id_weights[config.edge_capacity_max_num + 4] = 54;

  kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), edge_list_ptr);
  auto old_edge_list_ptr = edge_list_ptr;
  edge_list_ptr = kv_data_ptr->data();
  adaptor.config_.oversize_replace_strategy = LRU;
  adaptor.Fill(edge_list_ptr, old_edge_list_ptr, items);
  ASSERT_NE(edge_list_ptr, old_edge_list_ptr);
  ptr = reinterpret_cast<CDFEdgeList *>(edge_list_ptr);
  malloc_api.CustomFree(old_edge_list_ptr);
  ASSERT_EQ(ptr->size, config.edge_capacity_max_num);
  ASSERT_EQ(ptr->out_degree, config.edge_capacity_max_num + 7);

  ASSERT_EQ(ptr->capacity, config.edge_capacity_max_num);
  std::vector<uint64> actual_ids;
  std::vector<float> actual_weights;
  std::vector<uint64> expect_ids(config.edge_capacity_max_num, 1);
  std::vector<float> expect_weights(config.edge_capacity_max_num, 1);
  expect_ids[0] = 54;
  expect_ids[1] = 53;
  expect_ids[2] = 52;
  expect_ids[3] = 51;
  expect_ids[4] = 50;
  expect_weights[0] = 54;
  expect_weights[1] = 53;
  expect_weights[2] = 52;
  expect_weights[3] = 51;
  expect_weights[4] = 50;
  ptr->Get(&actual_ids, &actual_weights);
  ASSERT_ARRAY_EQ(expect_ids, actual_ids);
  ASSERT_ARRAY_EQ(expect_weights, actual_weights);
  malloc_api.CustomFree(edge_list_ptr);
  absl::SetFlag(&FLAGS_cdf_allow_weighted_sample, true);
}

TEST(CDFTest, CDFAdaptorFillAttr) {
  base::Json attr_config(base::StringToJson("{}"));
  attr_config.set("type_name", "SimpleAttrBlock");
  std::unique_ptr<BlockStorageApi> block_op_unique = BlockStorageApi::NewInstance(&attr_config);
  SimpleAttrBlock *block_op = reinterpret_cast<SimpleAttrBlock *>(block_op_unique.get());
  std::unique_ptr<BlockStorageApi> edge_attr_op = std::make_unique<EmptyBlock>();
  EdgeListConfig config;

  config.edge_capacity_max_num = 100;
  MockMallocApi malloc_api;
  CDFAdaptor adaptor(block_op, edge_attr_op.get(), config);
  CommonUpdateItems items;
  items.ids = {1, 2};
  items.id_weights = {1, 2};

  uint32 new_addr;
  auto kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), nullptr);
  auto edge_list_ptr = kv_data_ptr->data();
  auto ptr = reinterpret_cast<CDFEdgeList *>(edge_list_ptr);

  char *update_info = static_cast<char *>(std::malloc(block_op->MaxSize()));
  base::ScopeExit defer([update_info]() { free(static_cast<void *>(update_info)); });
  block_op->InitialBlock(update_info, block_op->MaxSize());
  block_op->SetIntX(update_info, 1, 1);
  block_op->SetIntX(update_info, 2, 2);
  block_op->SetFloatX(update_info, 1, 10.0f);
  block_op->SetFloatX(update_info, 2, 10.0f);
  items.attr_update_info = std::string(update_info, block_op->MaxSize());
  adaptor.config_.oversize_replace_strategy = LRU;
  adaptor.Fill(edge_list_ptr, nullptr, items);

  ASSERT_EQ(ptr->capacity, 2);
  ASSERT_EQ(ptr->out_degree, 2);

  ASSERT_EQ(ptr->size, 2);
  std::vector<uint64> expect = {2, 1};
  std::vector<uint64> actual;
  ptr->Get(&actual);
  ASSERT_ARRAY_EQ(expect, actual);
  ASSERT_EQ(block_op->GetIntX(ptr->attr_block(), 0), absl::GetFlag(FLAGS_simple_block_int_init_value));
  ASSERT_EQ(block_op->GetIntX(ptr->attr_block(), 1), 1);
  ASSERT_EQ(block_op->GetIntX(ptr->attr_block(), 2), 2);
  ASSERT_EQ(block_op->GetIntX(ptr->attr_block(), 3), absl::GetFlag(FLAGS_simple_block_int_init_value));
  ASSERT_FLOAT_EQ(block_op->GetFloatX(ptr->attr_block(), 0),
                  absl::GetFlag(FLAGS_simple_block_float_init_value));
  ASSERT_FLOAT_EQ(block_op->GetFloatX(ptr->attr_block(), 1), 10.0f);
  ASSERT_FLOAT_EQ(block_op->GetFloatX(ptr->attr_block(), 2), 10.0f);
  ASSERT_FLOAT_EQ(block_op->GetFloatX(ptr->attr_block(), 3),
                  absl::GetFlag(FLAGS_simple_block_float_init_value));
}

TEST(CDFTest, AdaptorFillEdgeAttr) {
  base::Json attr_config(base::StringToJson("{}"));
  attr_config.set("type_name", "SimpleAttrBlock");
  std::unique_ptr<BlockStorageApi> block_op_unique = BlockStorageApi::NewInstance(&attr_config);
  SimpleAttrBlock *block_op = reinterpret_cast<SimpleAttrBlock *>(block_op_unique.get());
  std::unique_ptr<BlockStorageApi> empty_op = std::make_unique<EmptyBlock>();
  EdgeListConfig config;

  config.edge_capacity_max_num = 100;
  MockMallocApi malloc_api;
  CDFAdaptor adaptor(empty_op.get(), block_op, config);
  CommonUpdateItems items;
  items.ids = {1, 2};
  items.id_weights = {1, 2};

  uint32 new_addr;
  auto kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), nullptr);
  auto edge_list_ptr = kv_data_ptr->data();
  auto ptr = reinterpret_cast<CDFEdgeList *>(edge_list_ptr);

  char *update_info = static_cast<char *>(std::malloc(block_op->MaxSize()));
  base::ScopeExit defer([update_info]() { free(static_cast<void *>(update_info)); });
  block_op->InitialBlock(update_info, block_op->MaxSize());
  block_op->SetIntX(update_info, 1, 1);
  block_op->SetIntX(update_info, 2, 2);
  block_op->SetFloatX(update_info, 1, 10.0f);
  block_op->SetFloatX(update_info, 2, 10.0f);
  items.id_attr_update_infos.push_back(std::string(update_info, block_op->MaxSize()));
  block_op->SetIntX(update_info, 0, 4);
  block_op->SetIntX(update_info, 1, 3);
  block_op->SetFloatX(update_info, 2, 20.0f);
  block_op->SetFloatX(update_info, 3, 30.0f);
  items.id_attr_update_infos.push_back(std::string(update_info, block_op->MaxSize()));

  adaptor.config_.oversize_replace_strategy = LRU;
  adaptor.Fill(edge_list_ptr, nullptr, items);

  ASSERT_EQ(ptr->capacity, 2);
  ASSERT_EQ(ptr->out_degree, 2);

  ASSERT_EQ(ptr->size, 2);
  std::vector<uint64> expect = {2, 1};
  std::vector<uint64> actual;
  ptr->Get(&actual);
  ASSERT_ARRAY_EQ(expect, actual);
  // Batch read result has the inverse order of insertion.
  ASSERT_EQ(block_op->GetIntX(ptr->IthEdgeAttrBlock(1), 0), absl::GetFlag(FLAGS_simple_block_int_init_value));
  ASSERT_EQ(block_op->GetIntX(ptr->IthEdgeAttrBlock(1), 1), 1);
  ASSERT_EQ(block_op->GetIntX(ptr->IthEdgeAttrBlock(1), 2), 2);
  ASSERT_EQ(block_op->GetIntX(ptr->IthEdgeAttrBlock(1), 3), absl::GetFlag(FLAGS_simple_block_int_init_value));
  ASSERT_FLOAT_EQ(block_op->GetFloatX(ptr->IthEdgeAttrBlock(1), 0),
                  absl::GetFlag(FLAGS_simple_block_float_init_value));
  ASSERT_FLOAT_EQ(block_op->GetFloatX(ptr->IthEdgeAttrBlock(1), 1), 10.0f);
  ASSERT_FLOAT_EQ(block_op->GetFloatX(ptr->IthEdgeAttrBlock(1), 2), 10.0f);
  ASSERT_FLOAT_EQ(block_op->GetFloatX(ptr->IthEdgeAttrBlock(1), 3),
                  absl::GetFlag(FLAGS_simple_block_float_init_value));
  ASSERT_EQ(block_op->GetIntX(ptr->IthEdgeAttrBlock(0), 0), 4);
  ASSERT_EQ(block_op->GetIntX(ptr->IthEdgeAttrBlock(0), 1), 3);
  ASSERT_EQ(block_op->GetIntX(ptr->IthEdgeAttrBlock(0), 2), 2);
  ASSERT_EQ(block_op->GetIntX(ptr->IthEdgeAttrBlock(0), 3), absl::GetFlag(FLAGS_simple_block_int_init_value));
  ASSERT_FLOAT_EQ(block_op->GetFloatX(ptr->IthEdgeAttrBlock(0), 0),
                  absl::GetFlag(FLAGS_simple_block_float_init_value));
  ASSERT_FLOAT_EQ(block_op->GetFloatX(ptr->IthEdgeAttrBlock(0), 1), 10.0f);
  ASSERT_FLOAT_EQ(block_op->GetFloatX(ptr->IthEdgeAttrBlock(0), 2), 20.0f);
  ASSERT_FLOAT_EQ(block_op->GetFloatX(ptr->IthEdgeAttrBlock(0), 3), 30.0f);
}

TEST(CDFTest, CDFAdaptorFillMostRecent) {
  SimpleAttrBlock block_op;
  EmptyBlock edge_op;
  EdgeListConfig config;

  config.edge_capacity_max_num = 100;
  MockMallocApi malloc_api;
  CDFAdaptor adaptor(&block_op, &edge_op, config);

  CommonUpdateItems items;
  items.ids = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  items.id_weights = {2, 8, 1, 4, 7, 9, 3, 8, 9, 3};
  items.id_ts = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

  uint32 new_addr;
  auto kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), nullptr);
  auto edge_list_ptr = kv_data_ptr->data();
  adaptor.config_.oversize_replace_strategy = LRU;
  adaptor.Fill(edge_list_ptr, nullptr, items);

  items.ids = {101, 102, 103};
  items.id_weights = {0.2, 0.8, 0.1};
  items.id_ts = {10, 10, 10};

  kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), edge_list_ptr);
  auto old_edge_list_ptr = edge_list_ptr;
  edge_list_ptr = kv_data_ptr->data();
  adaptor.config_.oversize_replace_strategy = LRU;
  adaptor.Fill(edge_list_ptr, old_edge_list_ptr, items);
  ASSERT_NE(edge_list_ptr, old_edge_list_ptr);

  SamplingParam param;
  param.set_strategy(SamplingParam_SamplingStrategy_MOST_RECENT);
  param.set_sampling_num(15);
  // Without param max_timestamp.
  EdgeList l;
  EdgeListWriter writer(&l, 15);
  auto slice = writer.Slice(0);
  adaptor.Sample(edge_list_ptr, param, &slice);
  std::vector<uint64_t> results = {103, 102, 101, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 0};
  for (size_t i = 0; i < 15; ++i) {
    ASSERT_EQ(l.node_id(i), results[i]) << "i error: " << l.node_id(i) << " vs " << results[i];
  }

  EdgeList l2;
  EdgeListWriter writer2(&l2, 15);
  param.set_max_timestamp_s(9);
  slice = writer2.Slice(0);
  adaptor.Sample(edge_list_ptr, param, &slice);
  std::vector<uint64_t> results2 = {10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 0, 0, 0, 0};
  for (size_t i = 0; i < 15; ++i) {
    ASSERT_EQ(l2.node_id(i), results2[i]) << "i error: " << l2.node_id(i) << " vs " << results2[i];
  }

  EdgeList l3;
  EdgeListWriter writer3(&l3, 15);
  param.set_max_timestamp_s(0);
  param.set_min_timestamp_s(1);
  slice = writer3.Slice(0);
  adaptor.Sample(edge_list_ptr, param, &slice);
  std::vector<uint64_t> results3 = {103, 102, 101, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
  for (size_t i = 0; i < 15; ++i) {
    ASSERT_EQ(l3.node_id(i), results3[i]) << "i error: " << l3.node_id(i) << " vs " << results3[i];
  }
}

TEST(CDFTest, CDFAdaptorFillMaxTimestampAndWeightAndTimeDecay) {
  SimpleAttrBlock block_op;
  EmptyBlock edge_op;
  EdgeListConfig config;

  config.edge_capacity_max_num = 100;
  MockMallocApi malloc_api;
  CDFAdaptor adaptor(&block_op, &edge_op, config);

  CommonUpdateItems items;
  items.ids = {1, 2, 3, 4};
  items.id_weights = {2, 4, 6, 8};  // 1, 2, 3, 4 after 50% decay

  uint32 new_addr;
  auto kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), nullptr);
  auto edge_list_ptr = kv_data_ptr->data();
  adaptor.config_.oversize_replace_strategy = LRU;
  adaptor.Fill(edge_list_ptr, nullptr, items);

  auto delete_count = adaptor.TimeDecay(edge_list_ptr, nullptr, 1, 0.5, 99);
  CHECK_EQ(delete_count, 0);  // cdf not support delete
  CommonUpdateItems items2;
  items2.ids = {5, 6, 7};
  items2.id_weights = {10, 10, 10};
  items2.id_ts = {10, 10, 10};

  kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items2.ids.size(), edge_list_ptr);
  auto old_edge_list_ptr = edge_list_ptr;
  edge_list_ptr = kv_data_ptr->data();
  adaptor.config_.oversize_replace_strategy = LRU;
  adaptor.Fill(edge_list_ptr, old_edge_list_ptr, items2);
  ASSERT_NE(edge_list_ptr, old_edge_list_ptr);

  int sample_time = 100000, sample_num = 3;
  EdgeList list;
  EdgeListWriter writer(&list, sample_num * sample_time);
  SamplingParam param;
  std::unordered_map<int, int> counter;
  for (int i = 1; i < 8; ++i) { counter.insert({i, 0}); }

  param.set_strategy(SamplingParam_SamplingStrategy_EDGE_WEIGHT);
  param.set_sampling_num(sample_num);
  param.set_max_timestamp_s(9);
  for (size_t i = 0; i < sample_time; ++i) {
    auto slice = writer.Slice(i * sample_num);
    adaptor.Sample(edge_list_ptr, param, &slice);
    ASSERT_EQ(slice.offset, i * sample_num + sample_num);
  }

  auto reader = writer.Slice(0);
  for (int offset = 0; offset < sample_time * sample_num; ++offset) {
    int node_id = list.node_id(offset);
    ASSERT_GE(node_id, 1) << "node_id: " << node_id;
    ASSERT_LE(node_id, 7) << "node_id: " << node_id;
    auto iter = counter.find(node_id);
    iter->second += 1;
  }

  for (auto item : counter) {
    std::cout << "key = " << item.first << ", occur time = " << item.second
              << ", ratio = " << ((item.second + 0.0) / (sample_time * sample_num)) << std::endl;
  }

  float delta = 0.01;
  ASSERT_LE(std::fabs(((counter.find(1)->second + 0.0) / (sample_num * sample_time)) - 0.1), delta);
  ASSERT_LE(std::fabs(((counter.find(2)->second + 0.0) / (sample_num * sample_time)) - 0.2), delta);
  ASSERT_LE(std::fabs(((counter.find(3)->second + 0.0) / (sample_num * sample_time)) - 0.3), delta);
  ASSERT_LE(std::fabs(((counter.find(4)->second + 0.0) / (sample_num * sample_time)) - 0.4), delta);
  ASSERT_EQ(counter.find(5)->second, 0);
  ASSERT_EQ(counter.find(6)->second, 0);
  ASSERT_EQ(counter.find(7)->second, 0);
  std::cout << "--------------------------------" << std::endl;

  // Without timestamp.
  counter.clear();
  for (int i = 1; i < 8; ++i) { counter.insert({i, 0}); }
  EdgeList list2;
  EdgeListWriter writer2(&list2, sample_num * sample_time);
  param.set_max_timestamp_s(0);
  for (size_t i = 0; i < sample_time; ++i) {
    auto slice = writer2.Slice(i * sample_num);
    adaptor.Sample(edge_list_ptr, param, &slice);
    ASSERT_EQ(slice.offset, i * sample_num + sample_num);
  }

  reader = writer2.Slice(0);
  for (int offset = 0; offset < sample_time * sample_num; ++offset) {
    int node_id = list2.node_id(offset);
    ASSERT_GE(node_id, 1) << "node_id: " << node_id;
    ASSERT_LE(node_id, 7) << "node_id: " << node_id;
    auto iter = counter.find(node_id);
    iter->second += 1;
  }

  for (auto item : counter) {
    std::cout << "key = " << item.first << ", occur time = " << item.second
              << ", ratio = " << ((item.second + 0.0) / (sample_time * sample_num)) << std::endl;
  }

  delta = 0.01;
  ASSERT_LE(std::fabs(((counter.find(1)->second + 0.0) / (sample_num * sample_time)) - 0.025), delta);
  ASSERT_LE(std::fabs(((counter.find(2)->second + 0.0) / (sample_num * sample_time)) - 0.05), delta);
  ASSERT_LE(std::fabs(((counter.find(3)->second + 0.0) / (sample_num * sample_time)) - 0.075), delta);
  ASSERT_LE(std::fabs(((counter.find(4)->second + 0.0) / (sample_num * sample_time)) - 0.1), delta);
  ASSERT_LE(std::fabs(((counter.find(5)->second + 0.0) / (sample_num * sample_time)) - 0.25), delta);
  ASSERT_LE(std::fabs(((counter.find(6)->second + 0.0) / (sample_num * sample_time)) - 0.25), delta);
  ASSERT_LE(std::fabs(((counter.find(7)->second + 0.0) / (sample_num * sample_time)) - 0.25), delta);
}

// Test weight valid after many insertion.
TEST(CDFTest, ManyInsertion) {
  absl::SetFlag(&FLAGS_cdf_allow_weighted_sample, false);
  std::unique_ptr<BlockStorageApi> attr_op = std::make_unique<EmptyBlock>();
  std::unique_ptr<BlockStorageApi> edge_attr_op = std::make_unique<EmptyBlock>();
  EdgeListConfig config;

  config.edge_capacity_max_num = 1000;

  MockMallocApi malloc_api;
  CDFAdaptor adaptor(attr_op.get(), edge_attr_op.get(), config);
  CommonUpdateItems items;
  uint32 new_addr;

  base::PseudoRandom pr(base::RandomSeed());
  for (size_t i = 0; i < 100000; ++i) {
    items.ids.push_back(i);
    items.id_weights.push_back(pr.GetInt(0, 100));
    items.id_ts.push_back(pr.GetInt(0, 100));
  }

  auto kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size());
  auto edge_list_ptr = kv_data_ptr->data();
  adaptor.config_.oversize_replace_strategy = LRU;
  adaptor.Fill(edge_list_ptr, nullptr, items);
  auto ptr = reinterpret_cast<CDFEdgeList *>(edge_list_ptr);
  ASSERT_EQ(ptr->size, config.edge_capacity_max_num);

  ASSERT_EQ(ptr->capacity, config.edge_capacity_max_num);
  std::vector<uint64> actual_ids;
  std::vector<float> actual_weights;
  ptr->Get(&actual_ids, &actual_weights);
  for (size_t i = 0; i < config.edge_capacity_max_num; ++i) {
    CHECK(actual_weights[i] > 0) << i << ", " << ptr->Get(i)->weight << ", " << ptr->Get(i + 1)->weight
                                 << ", " << ptr->Get(i - 1)->weight;
    CHECK(actual_weights[i] <= 100) << i << ", " << ptr->Get(i)->weight << ", " << ptr->Get(i + 1)->weight
                                    << ", " << ptr->Get(i - 1)->weight;
  }
  malloc_api.CustomFree(edge_list_ptr);
  absl::SetFlag(&FLAGS_cdf_allow_weighted_sample, true);
}

}  // namespace chrono_graph
