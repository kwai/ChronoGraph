// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "chrono_graph/src/storage/edge_list/cpt_edge_list.h"
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "base/common/basic_types.h"
#include "base/common/gflags.h"
#include "base/common/timer.h"
#include "base/jansson/json.h"
#include "base/testing/gtest.h"
#include "chrono_graph/src/base/proto_helper.h"

ABSL_DECLARE_FLAG(int64, simple_block_int_init_value);
ABSL_DECLARE_FLAG(double, simple_block_float_init_value);
ABSL_DECLARE_FLAG(int32, int_block_max_size);
ABSL_DECLARE_FLAG(int32, float_block_max_size);

namespace chrono_graph {

#define ASSERT_ARRAY_EQ(A1, A2)                                              \
  do {                                                                       \
    ASSERT_EQ(A1.size(), A2.size());                                         \
    for (size_t i = 0; i < A1.size(); ++i) { ASSERT_EQ(A1[i], A2[i]) << i; } \
  } while (0)

template <class ItemType>
class CPTStandardTest : public ::testing::Test {
 protected:
  using EdgeListType = CPTreapEdgeList<ItemType>;
  using AdaptorType = CPTreapAdaptor<ItemType>;

  CPTStandardTest() { invalid_offset_ = ItemType::invalid_offset; }
  EdgeListType *ResetEdgeList(BlockStorageApi *attr_op, BlockStorageApi *edge_attr_op, int capacity) {
    el_ = static_cast<EdgeListType *>(malloc(EdgeListType::MemorySize(attr_op, edge_attr_op, capacity)));
    return el_;
  }
  EdgeListType *ReinterpretPtr(void *ptr) { return reinterpret_cast<EdgeListType *>(ptr); }

  ItemType item_;
  EdgeListType *el_;
  int64 invalid_offset_;
};

typedef ::testing::Types<CPTreapItemStandard, CPTreapItemPreciseWeightIndexed> TestingTypes;

TYPED_TEST_SUITE(CPTStandardTest, TestingTypes);

/*** Begin Struct Test ***/

TYPED_TEST(CPTStandardTest, ItemTest) {
  auto item = this->item_;
  uint64 big = 1ll << 52;
  item.set_id(10 + big);
  item.set_father_offset(5);
  item.cum_weight = 10;
  item.left = 10;
  item.set_prev_offset(this->invalid_offset_);
  item.set_next_offset(this->invalid_offset_);
  std::cout << item.info() << std::endl;
  CHECK_EQ(item.id(), 10 + big);
  CHECK_EQ(item.father_offset(), 5);
  CHECK_EQ(item.prev_offset(), this->invalid_offset_);
  CHECK_EQ(item.next_offset(), this->invalid_offset_);
  CHECK(item.has_one_son());
  CHECK(item.has_father());
}

// Write with rotate
TYPED_TEST(CPTStandardTest, EL_Add1) {
  int capacity = 10;
  uint64 big = 1ll << 52;
  EmptyBlock attr_op;
  auto *el = this->ResetEdgeList(&attr_op, &attr_op, capacity);

  std::vector<uint64> ids = {1, 3, 4 + big, 5, 2};
  std::vector<float> weights = {1, 3, 4, 5, 2};
  el->Initialize(&attr_op, &attr_op, capacity);
  el->Add(ids, weights, EdgeListReplaceStrategy::WEIGHT);
  CHECK_EQ(el->meta.size, 5);
  CHECK_EQ(el->out_degree, 5);
  CHECK_EQ(el->items()[0].id(), 1);
  CHECK_EQ(el->items()[1].id(), 3);
  CHECK_EQ(el->items()[2].id(), 4 + big);
  CHECK_EQ(el->items()[3].id(), 5);
  CHECK_EQ(el->items()[4].id(), 2);
  CHECK_EQ(el->meta.root, 0);
}

// Write with root rotate
TYPED_TEST(CPTStandardTest, EL_Add2) {
  int capacity = 10;
  EmptyBlock attr_op;
  auto *el = this->ResetEdgeList(&attr_op, &attr_op, capacity);

  std::vector<uint64> ids = {3, 4, 5, 2, 1};
  std::vector<float> weights = {3, 4, 5, 2, 1};
  el->Initialize(&attr_op, &attr_op, capacity);
  el->Add(ids, weights, EdgeListReplaceStrategy::WEIGHT);
  CHECK_EQ(el->meta.size, 5);
  CHECK_EQ(el->out_degree, 5);
  CHECK_EQ(el->items()[0].id(), 3);
  CHECK_EQ(el->items()[1].id(), 4);
  CHECK_EQ(el->items()[2].id(), 5);
  CHECK_EQ(el->items()[3].id(), 2);
  CHECK_EQ(el->items()[4].id(), 1);
  CHECK_EQ(el->meta.root, 4);
}

// Add edge with replace weight.
TYPED_TEST(CPTStandardTest, EL_AddReplace) {
  int capacity = 10;
  EmptyBlock attr_op;
  auto *el = this->ResetEdgeList(&attr_op, &attr_op, capacity);

  std::vector<uint64> ids = {3, 4, 5, 2, 1, 4, 3, 1};
  std::vector<float> weights = {3, 4, 5, 2, 1, 3, 4, 10};
  el->Initialize(&attr_op, &attr_op, capacity);
  el->Add(ids, weights, EdgeListReplaceStrategy::WEIGHT, {}, InsertEdgeWeightAct::REPLACE);
  CHECK_EQ(el->meta.size, 5);
  CHECK_EQ(el->out_degree, 8);
  CHECK_EQ(el->items()[0].id(), 3);
  CHECK_EQ(el->weight(el->items()), 4);
  CHECK_EQ(el->items()[1].id(), 4);
  CHECK_EQ(el->weight(el->items() + 1), 3);
  CHECK_EQ(el->items()[2].id(), 5);
  CHECK_EQ(el->weight(el->items() + 2), 5);
  CHECK_EQ(el->items()[3].id(), 2);
  CHECK_EQ(el->weight(el->items() + 3), 2);
  CHECK_EQ(el->items()[4].id(), 1);
  CHECK_EQ(el->weight(el->items() + 4), 10);
  CHECK_EQ(el->meta.root, 3);
}

TYPED_TEST(CPTStandardTest, EL_Delete) {
  int capacity = 10;
  EmptyBlock attr_op;
  auto *el = this->ResetEdgeList(&attr_op, &attr_op, capacity);
  el->Initialize(&attr_op, &attr_op, capacity);
  CHECK(el->Add(100, 99, WEIGHT));
  el->DeleteById(100);
  CHECK_EQ(el->meta.size, 0);
  CHECK_EQ(el->out_degree, 1);
  CHECK(el->Add(100, 99, WEIGHT));
  el->PopHeap();
  CHECK_EQ(el->meta.size, 0);
  CHECK_EQ(el->out_degree, 2);
  el->PopHeap();
  el->PopRandom();

  std::vector<uint64> ids = {3, 4, 5, 2, 1, 6};
  std::vector<float> weights = {5, 2, 1, 3, 4, 2};
  CHECK(el->Add(ids, weights, EdgeListReplaceStrategy::WEIGHT));
  CHECK_EQ(el->meta.size, 6);
  CHECK_EQ(el->out_degree, 8);
  CHECK_EQ(el->items()[0].id(), 3);
  CHECK_EQ(el->items()[1].id(), 4);
  CHECK_EQ(el->items()[2].id(), 5);
  CHECK_EQ(el->items()[3].id(), 2);
  CHECK_EQ(el->items()[4].id(), 1);
  CHECK_EQ(el->items()[5].id(), 6);
  CHECK_EQ(el->meta.root, 2);
  el->DeleteById(10);
  CHECK_EQ(el->meta.size, 6);
  CHECK_EQ(el->out_degree, 8);
  CHECK_EQ(el->items()[0].id(), 3);
  CHECK_EQ(el->items()[1].id(), 4);
  CHECK_EQ(el->items()[2].id(), 5);
  CHECK_EQ(el->items()[3].id(), 2);
  CHECK_EQ(el->items()[4].id(), 1);
  CHECK_EQ(el->items()[5].id(), 6);
  CHECK_EQ(el->meta.root, 2);
  el->DeleteById(5);
  CHECK_EQ(el->meta.size, 5);
  CHECK_EQ(el->out_degree, 8);
  CHECK_EQ(el->items()[0].id(), 3);
  CHECK_EQ(el->items()[1].id(), 4);
  CHECK_EQ(el->items()[2].id(), 6);
  CHECK_EQ(el->items()[3].id(), 2);
  CHECK_EQ(el->items()[4].id(), 1);
  CHECK_EQ(el->meta.root, 2);
  CHECK_EQ(el->items()[el->meta.root].cum_weight, 16);
}

TYPED_TEST(CPTStandardTest, EL_Expire) {
  int capacity = 10;
  EmptyBlock attr_op;
  auto *el = this->ResetEdgeList(&attr_op, &attr_op, capacity);

  std::vector<uint64> ids = {3, 4, 5, 2, 1, 9, 6, 10, 8, 7, 12, 11};
  std::vector<float> weights = {2, 1, 9, 6, 10, 8, 7, 12, 11, 3, 4, 5};
  el->Initialize(&attr_op, &attr_op, capacity);
  el->Add(ids, weights, EdgeListReplaceStrategy::WEIGHT);
  CHECK_EQ(el->meta.size, 10);
  CHECK_EQ(el->out_degree, 12);
  CHECK_EQ(el->items()[0].id(), 12);
  CHECK_EQ(el->items()[1].id(), 7);
  CHECK_EQ(el->items()[2].id(), 5);
  CHECK_EQ(el->items()[3].id(), 2);
  CHECK_EQ(el->items()[4].id(), 1);
  CHECK_EQ(el->items()[5].id(), 9);
  CHECK_EQ(el->items()[6].id(), 6);
  CHECK_EQ(el->items()[7].id(), 10);
  CHECK_EQ(el->items()[8].id(), 8);
  CHECK_EQ(el->items()[9].id(), 11);
  CHECK_EQ(el->meta.root, 1);
  CHECK_EQ(el->items()[el->meta.root].cum_weight, 75);
}

TYPED_TEST(CPTStandardTest, EL_TimeExpire) {
  int capacity = 5;
  EmptyBlock attr_op;
  auto *el = this->ResetEdgeList(&attr_op, &attr_op, capacity);

  std::vector<uint64> ids = {4, 4, 5, 2, 1, 9, 6, 10, 8, 7, 12, 11};
  std::vector<float> weights = {2, 1, 9, 6, 10, 8, 7, 12, 11, 3, 4, 5};
  el->Initialize(&attr_op, &attr_op, capacity);
  el->Add(ids, weights, EdgeListReplaceStrategy::LRU);
  ProtoPtrList<EdgeInfo> edge_info;
  el->GetAllByTime(&edge_info);
  CHECK_EQ(el->meta.size, 5);
  CHECK_EQ(el->out_degree, 12);
  CHECK_EQ(edge_info.Get(0).id(), 10);
  CHECK_EQ(edge_info.Get(1).id(), 8);
  CHECK_EQ(edge_info.Get(2).id(), 7);
  CHECK_EQ(edge_info.Get(3).id(), 12);
  CHECK_EQ(edge_info.Get(4).id(), 11);
}

TYPED_TEST(CPTStandardTest, EL_Get) {
  int capacity = 10;
  EmptyBlock attr_op;
  auto *el = this->ResetEdgeList(&attr_op, &attr_op, capacity);

  std::vector<uint64> ids = {3, 4, 5, 2, 1, 9, 6, 10, 8, 7, 12, 11};
  std::vector<float> weights = {2, 1, 9, 6, 10, 8, 7, 12, 11, 3, 4, 5};
  el->Initialize(&attr_op, &attr_op, capacity);
  el->Add(ids, weights, EdgeListReplaceStrategy::WEIGHT);
  CHECK_EQ(el->meta.size, 10);
  CHECK_EQ(el->out_degree, 12);
  CHECK_EQ(el->meta.root, 1);
  CHECK_EQ(el->items()[el->meta.root].cum_weight, 75);
  auto item = this->item_;
  CHECK_EQ(el->GetById(100, nullptr), this->invalid_offset_);
  CHECK_NE(el->GetById(10, &item), this->invalid_offset_);
  CHECK_EQ(item.id(), 10);
  CHECK(item.has_father());
  CHECK_EQ(item.cum_weight, 12);
}

TYPED_TEST(CPTStandardTest, EL_DuplicateInsert) {
  int capacity = 10;
  EmptyBlock attr_op;
  auto *el = this->ResetEdgeList(&attr_op, &attr_op, capacity);

  std::vector<uint64> ids = {3, 4, 5, 2, 1, 9, 6, 10, 8, 7, 12, 7};
  std::vector<float> weights = {2, 1, 9, 6, 10, 8, 7, 12, 11, 3, 4, 10};
  el->Initialize(&attr_op, &attr_op, capacity);
  el->Add(ids, weights, EdgeListReplaceStrategy::WEIGHT);
  CHECK_EQ(el->meta.size, 9);
  CHECK_EQ(el->out_degree, 12);
  CHECK_EQ(el->items()[0].id(), 12);
  CHECK_EQ(el->items()[1].id(), 7);
  CHECK_EQ(el->items()[2].id(), 5);
  CHECK_EQ(el->items()[3].id(), 2);
  CHECK_EQ(el->items()[4].id(), 1);
  CHECK_EQ(el->items()[5].id(), 9);
  CHECK_EQ(el->items()[6].id(), 6);
  CHECK_EQ(el->items()[7].id(), 10);
  CHECK_EQ(el->items()[8].id(), 8);
  CHECK_EQ(el->meta.root, 0);
  CHECK_EQ(el->items()[el->meta.root].cum_weight, 80);
}

TYPED_TEST(CPTStandardTest, EL_Sample) {
  int capacity = 10;
  EmptyBlock attr_op;
  auto *el = this->ResetEdgeList(&attr_op, &attr_op, capacity);

  std::vector<uint64> ids = {1, 3, 4, 5, 2};
  std::vector<float> weights = {4, 5, 2, 1, 3};
  el->Initialize(&attr_op, &attr_op, capacity);
  el->Add(ids, weights, EdgeListReplaceStrategy::WEIGHT);
  CHECK_EQ(el->meta.size, 5);
  CHECK_EQ(el->out_degree, 5);
  CHECK_EQ(el->items()[0].id(), 1);
  CHECK_EQ(el->items()[1].id(), 3);
  CHECK_EQ(el->items()[2].id(), 4);
  CHECK_EQ(el->items()[3].id(), 5);
  CHECK_EQ(el->items()[4].id(), 2);
  CHECK_EQ(el->meta.root, 3);

  int sample_time = 100000;
  std::unordered_map<uint64, int> counter;
  for (uint64 i = 1; i <= 5; ++i) { counter.insert({i, 0}); }
  for (size_t i = 0; i < sample_time; ++i) {
    auto id = el->SampleOnce();
    auto iter = counter.find(id);
    iter->second += 1;
  }
  for (auto item : counter) {
    std::cout << "key = " << item.first << ", occur time = " << item.second
              << ", ratio = " << (item.second + 0.0) / sample_time << std::endl;
  }

  float delta = 0.01;
  CHECK_LE(std::fabs(((counter.find(1)->second + 0.0) / sample_time) - (4.0 / 15)), delta);
  CHECK_LE(std::fabs(((counter.find(2)->second + 0.0) / sample_time) - (3.0 / 15)), delta);
  CHECK_LE(std::fabs(((counter.find(3)->second + 0.0) / sample_time) - (5.0 / 15)), delta);
  CHECK_LE(std::fabs(((counter.find(4)->second + 0.0) / sample_time) - (2.0 / 15)), delta);
  CHECK_LE(std::fabs(((counter.find(5)->second + 0.0) / sample_time) - (1.0 / 15)), delta);
}

TYPED_TEST(CPTStandardTest, EL_TimeLinkedList) {
  int capacity = 10;
  EmptyBlock attr_op;
  auto *el = this->ResetEdgeList(&attr_op, &attr_op, capacity);
  std::vector<uint64> ids = {1, 3, 4, 5, 2, 5};
  std::vector<float> weights = {4, 5, 2, 1, 3, 10};
  std::vector<uint32_t> t = {3, 5, 2, 5, 1, 0};
  el->Initialize(&attr_op, &attr_op, capacity);
  for (size_t i = 0; i < 5; ++i) { el->Add(ids[i], weights[i], EdgeListReplaceStrategy::WEIGHT, t[i]); }
  CHECK_EQ(el->meta.time_head, 4);
  CHECK_EQ(el->meta.time_tail, 3);
  CHECK_EQ(el->items()[0].timestamp_s_, 3);
  CHECK_EQ(el->items()[0].next_id_, 1);
  CHECK_EQ(el->items()[0].prev_id_, 2);
  CHECK_EQ(el->items()[1].timestamp_s_, 5);
  CHECK_EQ(el->items()[1].next_id_, 3);
  CHECK_EQ(el->items()[1].prev_id_, 0);
  CHECK_EQ(el->items()[2].timestamp_s_, 2);
  CHECK_EQ(el->items()[2].next_id_, 0);
  CHECK_EQ(el->items()[2].prev_id_, 4);
  CHECK_EQ(el->items()[3].timestamp_s_, 5);
  CHECK_EQ(el->items()[3].next_id_, this->invalid_offset_);
  CHECK_EQ(el->items()[3].prev_id_, 1);
  CHECK_EQ(el->items()[4].timestamp_s_, 1);
  CHECK_EQ(el->items()[4].next_id_, 2);
  CHECK_EQ(el->items()[4].prev_id_, this->invalid_offset_);
}

TYPED_TEST(CPTStandardTest, EL_TimeLinkedList2) {
  int capacity = 10;
  EmptyBlock attr_op;
  auto *el = this->ResetEdgeList(&attr_op, &attr_op, capacity);
  std::vector<uint64> ids = {16, 233, 90, 19, 12};
  std::vector<float> weights = {16, 16, 16, 48, 16};
  std::vector<uint32_t> t = {1627815915, 1626947375, 1627883773, 1627739203, 1627688296};
  el->Initialize(&attr_op, &attr_op, capacity);
  for (size_t i = 0; i < 5; ++i) { el->Add(ids[i], weights[i], EdgeListReplaceStrategy::WEIGHT, t[i]); }
  CHECK_EQ(el->meta.time_head, 1);
  CHECK_EQ(el->meta.time_tail, 2);
  CHECK_EQ(el->items()[0].timestamp_s_, 1627815915);
  CHECK_EQ(el->items()[0].next_id_, 2);
  CHECK_EQ(el->items()[0].prev_id_, 3);
  CHECK_EQ(el->items()[1].timestamp_s_, 1626947375);
  CHECK_EQ(el->items()[1].next_id_, 4);
  CHECK_EQ(el->items()[1].prev_id_, this->invalid_offset_);
  CHECK_EQ(el->items()[2].timestamp_s_, 1627883773);
  CHECK_EQ(el->items()[2].next_id_, this->invalid_offset_);
  CHECK_EQ(el->items()[2].prev_id_, 0);
  CHECK_EQ(el->items()[3].timestamp_s_, 1627739203);
  CHECK_EQ(el->items()[3].next_id_, 0);
  CHECK_EQ(el->items()[3].prev_id_, 4);
  CHECK_EQ(el->items()[4].timestamp_s_, 1627688296);
  CHECK_EQ(el->items()[4].next_id_, 3);
  CHECK_EQ(el->items()[4].prev_id_, 1);
}

TYPED_TEST(CPTStandardTest, EL_TimeLinkedList3) {
  int capacity = 10;
  EmptyBlock attr_op;
  auto *el = this->ResetEdgeList(&attr_op, &attr_op, capacity);
  std::vector<uint64_t> ids{97, 53, 27, 14, 45, 84, 65, 77, 45, 88};
  std::vector<float> weights{0.75, 0.92, 0.82, 0.95, 0.37, 0.59, 0.74, 0.33, 0.73, 0.43};
  el->Initialize(&attr_op, &attr_op, capacity);
  for (size_t i = 0; i < capacity; ++i) {
    el->Add(ids[i], weights[i], LRU, i);
    CHECK(el->IsValid()) << el->ToString();
  }
  auto req_t = capacity / 2;
  auto expect = 5;
  auto &item = el->items()[el->meta.time_tail];
  while (item.timestamp_s() > req_t) {
    el->DeleteById(item.id());
    item = el->items()[item.prev_offset()];
  }
  CHECK_EQ(expect, el->meta.size);
}

// Check tree is valid after many random insertion.
TYPED_TEST(CPTStandardTest, ManyInsert) {
  base::PseudoRandom pr(base::RandomSeed());
  int capacity = 3000;
  EmptyBlock attr_op;
  auto *el = this->ResetEdgeList(&attr_op, &attr_op, capacity);
  el->Initialize(&attr_op, &attr_op, capacity);
  for (size_t i = 0; i < 10000; ++i) {
    el->Add(pr.GetInt(10000000, 100000000), pr.GetInt(1, 10000), LRU, 0);
    CHECK(el->IsValid()) << i;
  }

  // make sure some ids are updated
  for (size_t i = 0; i < 10000; ++i) {
    el->Add(pr.GetInt(10000000, 10005000), pr.GetInt(1, 10000), LRU, pr.GetInt(1649332631, 1650332631));
    CHECK(el->IsValid()) << i;
  }

  // make sure some ids are negative updated
  for (size_t i = 0; i < 10000; ++i) {
    el->Add(pr.GetInt(10000000, 10005000), -pr.GetInt(1, 10000), LRU, pr.GetInt(1649332631, 1650332631));
    CHECK(el->IsValid()) << i;
  }

  // make sure some ids are deleted
  for (size_t i = 0; i < 10000; ++i) {
    el->DeleteById(pr.GetInt(10000000, 10005000));
    CHECK(el->IsValid()) << i;
  }
}

TYPED_TEST(CPTStandardTest, VeryLongList) {
  base::PseudoRandom pr(base::RandomSeed());
  int capacity = 60000;
  EmptyBlock attr_op;
  auto *el = this->ResetEdgeList(&attr_op, &attr_op, capacity);
  int t = 0;
  el->Initialize(&attr_op, &attr_op, capacity);
  for (size_t i = 0; i < capacity; ++i) {
    auto id = pr.GetInt(10000000, 100000000);
    auto weight = pr.GetDouble() * 0.9 + 0.1;
    el->Add(id, weight, LRU, t++);
  }
  auto req_t = capacity / 2;
  base::Timer timer;
  auto &item = el->items()[el->meta.time_tail];
  while (item.timestamp_s() > req_t) {
    el->DeleteById(item.id());
    item = el->items()[item.prev_offset()];
  }
  timer.AppendCostMs("delete long list");
  LOG(INFO) << el->IsValid() << "||" << timer.display() << "|| size = " << el->meta.size;
}

TYPED_TEST(CPTStandardTest, EL_Attr) {
  int capacity = 10;
  absl::SetFlag(&FLAGS_int_block_max_size, 4);
  absl::SetFlag(&FLAGS_float_block_max_size, 4);
  base::Json attr_config(base::StringToJson("{}"));
  attr_config.set("type_name", "SimpleAttrBlock");
  std::unique_ptr<BlockStorageApi> attr_op_unique = BlockStorageApi::NewInstance(&attr_config);
  SimpleAttrBlock *attr_op = dynamic_cast<SimpleAttrBlock *>(attr_op_unique.get());
  EmptyBlock empty_op;
  auto *el = this->ResetEdgeList(attr_op, &empty_op, capacity);

  std::vector<uint64> ids = {1, 3, 4, 5, 2};
  std::vector<float> weights = {4, 5, 2, 1, 3};
  el->Initialize(attr_op, &empty_op, capacity);
  el->Add(ids, weights, EdgeListReplaceStrategy::WEIGHT);
  CHECK_EQ(el->meta.size, 5);
  CHECK_EQ(el->out_degree, 5);
  CHECK_EQ(el->items()[0].id(), 1);
  CHECK_EQ(el->items()[1].id(), 3);
  CHECK_EQ(el->items()[2].id(), 4);
  CHECK_EQ(el->items()[3].id(), 5);
  CHECK_EQ(el->items()[4].id(), 2);
  CHECK_EQ(el->meta.root, 3);

  int sample_time = 100000;
  std::unordered_map<uint64, int> counter;
  for (uint64 i = 1; i <= 5; ++i) { counter.insert({i, 0}); }
  for (size_t i = 0; i < sample_time; ++i) {
    auto id = el->SampleOnce();
    auto iter = counter.find(id);
    iter->second += 1;
  }
  for (auto item : counter) {
    std::cout << "key = " << item.first << ", occur time = " << item.second
              << ", ratio = " << (item.second + 0.0) / sample_time << std::endl;
  }

  float delta = 0.01;
  CHECK_LE(std::fabs(((counter.find(1)->second + 0.0) / sample_time) - (4.0 / 15)), delta);
  CHECK_LE(std::fabs(((counter.find(2)->second + 0.0) / sample_time) - (3.0 / 15)), delta);
  CHECK_LE(std::fabs(((counter.find(3)->second + 0.0) / sample_time) - (5.0 / 15)), delta);
  CHECK_LE(std::fabs(((counter.find(4)->second + 0.0) / sample_time) - (2.0 / 15)), delta);
  CHECK_LE(std::fabs(((counter.find(5)->second + 0.0) / sample_time) - (1.0 / 15)), delta);
  CHECK_EQ(attr_op->GetIntX(el->attr_block(), 0), absl::GetFlag(FLAGS_simple_block_int_init_value));
  CHECK_EQ(attr_op->GetFloatX(el->attr_block(), 0), absl::GetFlag(FLAGS_simple_block_float_init_value));
}

/*** Begin Adaptor Test ***/

class MockMallocApi : public MemAllocator {
 public:
  virtual ~MockMallocApi() {
    for (auto ptr : ptrs) { free(ptr); }
  }
  base::KVData *New(int memory_size, uint32 *new_addr = nullptr) override {
    auto ptr = malloc(memory_size + sizeof(base::KVData));
    ptrs.push_back(ptr);
    return static_cast<base::KVData *>(ptr);
  }
  bool Free(base::KVData *memory_data) override {
    NOT_REACHED() << "SHOULD NOT CALL THIS!";
    return false;
  }
  std::string GetInfo() const { return "mock api"; }
  std::vector<void *> ptrs;
};

TYPED_TEST(CPTStandardTest, AdaptorFill) {
  EmptyBlock attr_op;
  EdgeListConfig config;
  config.edge_capacity_max_num = 10000;

  MockMallocApi malloc_api;
  typename TestFixture::AdaptorType adaptor(&attr_op, &attr_op, config);

  CommonUpdateItems items;
  items.ids = {1, 2};
  items.id_weights = {1, 2};

  uint32 new_addr;
  auto kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), nullptr);
  auto edge_list_ptr = kv_data_ptr->data();
  auto ptr = this->ReinterpretPtr(edge_list_ptr);
  int capacity = ptr->capacity;
  adaptor.config_.oversize_replace_strategy = WEIGHT;
  adaptor.Fill(edge_list_ptr, nullptr, items);
  // capacity is set in InitBlock and won't be changed later
  ASSERT_EQ(capacity, ptr->capacity);
  ASSERT_EQ(ptr->meta.size, 2);

  items.ids = {3, 4, 5, 6, 10, 99};
  items.id_weights = {3, 4, 5, 6, 99, 10};

  kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), edge_list_ptr);
  auto old_edge_list_ptr = edge_list_ptr;
  edge_list_ptr = kv_data_ptr->data();
  adaptor.config_.oversize_replace_strategy = WEIGHT;
  adaptor.Fill(edge_list_ptr, old_edge_list_ptr, items);
  ASSERT_NE(edge_list_ptr, old_edge_list_ptr);
  ptr = this->ReinterpretPtr(edge_list_ptr);
  ASSERT_EQ(ptr->meta.size, 8);
  ASSERT_EQ(ptr->out_degree, 8);
  ASSERT_EQ(ptr->capacity, 8);
  std::vector<uint64> actual_ids;
  std::vector<float> actual_weights;
  std::vector<uint64> expect_ids{1, 2, 3, 4, 5, 6, 10, 99};
  std::vector<float> expect_weights{1, 2, 3, 4, 5, 6, 99, 10};
  ptr->Get(&actual_ids, &actual_weights);
}

// Test dynamic growth and exceeding the limit.
TYPED_TEST(CPTStandardTest, AdaptorFill2) {
  EmptyBlock attr_op;
  EdgeListConfig config;
  config.edge_capacity_max_num = 10000;

  MockMallocApi malloc_api;
  typename TestFixture::AdaptorType adaptor(&attr_op, &attr_op, config);

  CommonUpdateItems items;
  items.ids = {1, 2};
  items.id_weights = {1, 2};

  uint32 new_addr;
  auto kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), nullptr);
  auto edge_list_ptr = kv_data_ptr->data();
  auto ptr = this->ReinterpretPtr(edge_list_ptr);
  int capacity = ptr->capacity;
  adaptor.config_.oversize_replace_strategy = WEIGHT;
  adaptor.Fill(edge_list_ptr, nullptr, items);
  // capacity is set in InitBlock and won't be changed later
  ASSERT_EQ(capacity, ptr->capacity);
  ASSERT_EQ(ptr->meta.size, 2);

  // Exceed the capacity.
  items.ids = {};
  items.id_weights = {};
  thread_local base::PseudoRandom pr(base::RandomSeed());
  float random = pr.GetDouble() + 0.001;
  for (int i = 1; i <= config.edge_capacity_max_num + 5; ++i) {
    items.ids.push_back(i);
    items.id_weights.push_back(random);
  }

  items.ids[config.edge_capacity_max_num] = 50000;
  items.ids[config.edge_capacity_max_num + 1] = 51000;
  items.ids[config.edge_capacity_max_num + 2] = 52000;
  items.ids[config.edge_capacity_max_num + 3] = 53000;
  items.ids[config.edge_capacity_max_num + 4] = 54000;
  items.id_weights[config.edge_capacity_max_num] = 50;
  items.id_weights[config.edge_capacity_max_num + 1] = 51;
  items.id_weights[config.edge_capacity_max_num + 2] = 52;
  items.id_weights[config.edge_capacity_max_num + 3] = 53;
  items.id_weights[config.edge_capacity_max_num + 4] = 54;

  kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), edge_list_ptr);
  auto old_edge_list_ptr = edge_list_ptr;
  edge_list_ptr = kv_data_ptr->data();
  adaptor.config_.oversize_replace_strategy = WEIGHT;
  adaptor.Fill(edge_list_ptr, old_edge_list_ptr, items);
  ASSERT_NE(edge_list_ptr, old_edge_list_ptr);

  ptr = this->ReinterpretPtr(edge_list_ptr);
  ASSERT_EQ(ptr->meta.size, config.edge_capacity_max_num);
  ASSERT_EQ(ptr->out_degree, config.edge_capacity_max_num + 7);
  ASSERT_EQ(ptr->capacity, config.edge_capacity_max_num);
  std::vector<uint64> actual_ids;
  std::vector<float> actual_weights;
  ptr->Get(&actual_ids, &actual_weights);
  ASSERT_EQ(actual_ids.size(), 10000);
  std::unordered_set<uint64> actual_ids_s{actual_ids.begin(), actual_ids.end()};
  bool find50 = false;
  bool find51 = false;
  bool find52 = false;
  bool find53 = false;
  bool find54 = false;
  for (size_t i = 0; i < actual_weights.size(); ++i) {
    if (std::fabs(actual_weights[i] - 50) < 0.00001) { find50 = true; }
    if (std::fabs(actual_weights[i] - 51) < 0.00001) { find51 = true; }
    if (std::fabs(actual_weights[i] - 52) < 0.00001) { find52 = true; }
    if (std::fabs(actual_weights[i] - 53) < 0.00001) { find53 = true; }
    if (std::fabs(actual_weights[i] - 54) < 0.00001) { find54 = true; }
  }

  ASSERT_NE(actual_ids_s.count(50000), 0);
  ASSERT_NE(actual_ids_s.count(51000), 0);
  ASSERT_NE(actual_ids_s.count(52000), 0);
  ASSERT_NE(actual_ids_s.count(53000), 0);
  ASSERT_NE(actual_ids_s.count(54000), 0);
  CHECK(find50);
  CHECK(find51);
  CHECK(find52);
  CHECK(find53);
  CHECK(find54);
}

TYPED_TEST(CPTStandardTest, BatchMergeFill) {
  absl::SetFlag(&FLAGS_cpt_edge_buffer_capacity, 10);
  EmptyBlock attr_op;
  EdgeListConfig config;
  config.edge_capacity_max_num = 10000;

  MockMallocApi malloc_api;
  typename TestFixture::AdaptorType adaptor(&attr_op, &attr_op, config);

  CommonUpdateItems items;
  items.ids = {1, 2};
  items.id_weights = {1, 2};

  uint32 new_addr;
  auto kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), nullptr);
  auto edge_list_ptr = kv_data_ptr->data();
  auto ptr = this->ReinterpretPtr(edge_list_ptr);
  int capacity = ptr->capacity;
  adaptor.config_.oversize_replace_strategy = WEIGHT;
  adaptor.Fill(edge_list_ptr, nullptr, items);
  ASSERT_EQ(capacity, ptr->capacity);
  ASSERT_EQ(ptr->meta.size, 2);

  items.ids.clear();
  items.id_weights.clear();
  thread_local base::PseudoRandom pr(base::RandomSeed());
  int offset = 1;
  for (size_t i = 0; i < config.edge_capacity_max_num / 2; ++i) {
    items.ids.push_back(offset++);
    items.id_weights.push_back(pr.GetDouble() + 0.001);
  }

  kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), edge_list_ptr);
  auto old_edge_list_ptr = edge_list_ptr;
  edge_list_ptr = kv_data_ptr->data();
  ptr = this->ReinterpretPtr(edge_list_ptr);
  adaptor.config_.oversize_replace_strategy = WEIGHT;
  adaptor.Fill(edge_list_ptr, old_edge_list_ptr, items);

  ASSERT_EQ(ptr->meta.size, config.edge_capacity_max_num / 2);
  ASSERT_EQ(ptr->BatchMergeEnabled(), false);

  items.ids.clear();
  items.id_weights.clear();
  for (size_t i = 0; i < config.edge_capacity_max_num / 2; ++i) {
    items.ids.push_back(offset++);
    items.id_weights.push_back(pr.GetDouble() + 0.001);
  }

  kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), edge_list_ptr);
  old_edge_list_ptr = edge_list_ptr;
  edge_list_ptr = kv_data_ptr->data();
  ptr = this->ReinterpretPtr(edge_list_ptr);
  adaptor.config_.oversize_replace_strategy = WEIGHT;
  adaptor.Fill(edge_list_ptr, old_edge_list_ptr, items);
  // capacity is full
  ASSERT_EQ(ptr->meta.size, config.edge_capacity_max_num);
  ASSERT_EQ(ptr->BatchMergeEnabled(), false);

  // any new item will trigger batch merge
  CHECK(!adaptor.CanReuse(edge_list_ptr, 1));

  items.ids.clear();
  items.id_weights.clear();
  items.ids.push_back(offset++);
  float weight = 50.0;
  items.id_weights.push_back(weight);
  kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), edge_list_ptr);
  old_edge_list_ptr = edge_list_ptr;
  edge_list_ptr = kv_data_ptr->data();
  ptr = this->ReinterpretPtr(edge_list_ptr);
  adaptor.Fill(edge_list_ptr, old_edge_list_ptr, items);
  ASSERT_EQ(config.edge_capacity_max_num, ptr->capacity);
  ASSERT_EQ(ptr->meta.size, config.edge_capacity_max_num);
  ASSERT_EQ(ptr->meta.buffer_size, 0);
  CHECK(ptr->BatchMergeEnabled());

  std::vector<uint64> actual_ids;
  std::vector<float> actual_weights;
  ptr->Get(&actual_ids, &actual_weights);
  ASSERT_EQ(actual_ids.size(), config.edge_capacity_max_num);
  std::unordered_set<uint64> actual_ids_s{actual_ids.begin(), actual_ids.end()};
  bool find = false;
  for (size_t i = 0; i < actual_weights.size(); ++i) {
    if (std::fabs(actual_weights[i] - weight) < 0.00001) { find = true; }
  }
  CHECK(find);

  items.ids.clear();
  items.id_weights.clear();
  for (int i = 1; i <= absl::GetFlag(FLAGS_cpt_edge_buffer_capacity); ++i) {
    items.ids.push_back(offset++);
    items.id_weights.push_back(weight + i);
  }
  // can reuse, since buffer size can hold new items exactly
  CHECK(adaptor.CanReuse(edge_list_ptr, items.ids.size()));
  adaptor.Fill(edge_list_ptr, nullptr, items);
  ASSERT_EQ(ptr->meta.size, config.edge_capacity_max_num);
  // all buffered
  ASSERT_EQ(ptr->meta.buffer_size, items.ids.size());

  std::vector<bool> finds(absl::GetFlag(FLAGS_cpt_edge_buffer_capacity), false);
  for (size_t i = 0; i < actual_weights.size(); ++i) {
    for (int j = 1; j <= absl::GetFlag(FLAGS_cpt_edge_buffer_capacity); ++j) {
      if (std::fabs(actual_weights[i] - weight - j) < 0.00001) { finds[j - 1] = true; }
    }
  }
  // all buffered, can't find
  for (size_t i = 0; i < absl::GetFlag(FLAGS_cpt_edge_buffer_capacity); ++i) { CHECK(!finds[i]); }

  // any new item will trigger a merge
  items.ids.clear();
  items.id_weights.clear();
  items.ids.push_back(offset++);
  items.id_weights.push_back(weight + absl::GetFlag(FLAGS_cpt_edge_buffer_capacity) + 1);
  CHECK(!adaptor.CanReuse(edge_list_ptr, 1));

  kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), edge_list_ptr);
  old_edge_list_ptr = edge_list_ptr;
  edge_list_ptr = kv_data_ptr->data();
  ptr = this->ReinterpretPtr(edge_list_ptr);
  adaptor.Fill(edge_list_ptr, old_edge_list_ptr, items);
  ASSERT_EQ(ptr->meta.size, config.edge_capacity_max_num);
  ASSERT_EQ(ptr->meta.buffer_size, 0);

  ptr->Get(&actual_ids, &actual_weights);
  ASSERT_EQ(actual_ids.size(), config.edge_capacity_max_num);
  actual_ids_s.clear();
  actual_ids_s.insert(actual_ids.begin(), actual_ids.end());
  finds.resize(absl::GetFlag(FLAGS_cpt_edge_buffer_capacity) + 1, false);
  for (size_t i = 0; i < actual_weights.size(); ++i) {
    for (int j = 1; j <= absl::GetFlag(FLAGS_cpt_edge_buffer_capacity) + 1; ++j) {
      if (std::fabs(actual_weights[i] - weight - j) < 0.00001) { finds[j - 1] = true; }
    }
  }
  // merged, all found
  for (size_t i = 0; i < absl::GetFlag(FLAGS_cpt_edge_buffer_capacity) + 1; ++i) { CHECK(finds[i]); }

  // add half capacity items, buffered
  items.ids.clear();
  items.id_weights.clear();
  for (size_t i = 0; i < absl::GetFlag(FLAGS_cpt_edge_buffer_capacity) / 2; ++i) {
    items.ids.push_back(offset++);
    items.id_weights.push_back(pr.GetDouble() + 0.001);
  }
  CHECK(adaptor.CanReuse(edge_list_ptr, items.ids.size()));
  adaptor.Fill(edge_list_ptr, nullptr, items);
  CHECK_EQ(config.edge_capacity_max_num, ptr->capacity);
  CHECK_EQ(config.edge_capacity_max_num, ptr->meta.size);
  CHECK_EQ(ptr->meta.buffer_size, absl::GetFlag(FLAGS_cpt_edge_buffer_capacity) / 2);

  absl::SetFlag(&FLAGS_cpt_edge_buffer_capacity, 0);
}

TYPED_TEST(CPTStandardTest, AdaptorFillAttr) {
  base::Json attr_config(base::StringToJson("{}"));
  attr_config.set("type_name", "SimpleAttrBlock");
  std::unique_ptr<BlockStorageApi> block_op_unique = BlockStorageApi::NewInstance(&attr_config);
  SimpleAttrBlock *block_op = reinterpret_cast<SimpleAttrBlock *>(block_op_unique.get());
  EmptyBlock empty_op;
  EdgeListConfig config;
  config.edge_capacity_max_num = 100;
  MockMallocApi malloc_api;
  typename TestFixture::AdaptorType adaptor(block_op, &empty_op, config);

  CommonUpdateItems items;
  items.ids = {1, 2};
  items.id_weights = {1, 2};

  uint32 new_addr;
  auto kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), nullptr);
  auto edge_list_ptr = kv_data_ptr->data();
  auto ptr = this->ReinterpretPtr(edge_list_ptr);

  char *update_info = static_cast<char *>(std::malloc(block_op->MaxSize()));
  base::ScopeExit defer([update_info]() { free(static_cast<void *>(update_info)); });
  block_op->InitialBlock(update_info, block_op->MaxSize());
  block_op->SetIntX(update_info, 1, 1);
  block_op->SetIntX(update_info, 2, 2);
  block_op->SetFloatX(update_info, 1, 10.0f);
  block_op->SetFloatX(update_info, 2, 10.0f);
  items.attr_update_info = std::string(update_info, block_op->MaxSize());

  int capacity = ptr->capacity;
  adaptor.config_.oversize_replace_strategy = WEIGHT;
  adaptor.Fill(edge_list_ptr, nullptr, items);

  // capacity is set in InitBlock and won't be changed later
  ASSERT_EQ(capacity, ptr->capacity);
  ASSERT_EQ(ptr->capacity, 2);
  ASSERT_EQ(ptr->out_degree, 2);
  ASSERT_EQ(ptr->meta.size, 2);
  std::vector<uint64> expect = {1, 2};
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

TYPED_TEST(CPTStandardTest, AdaptorFillEdgeAttr) {
  base::Json attr_config(base::StringToJson("{}"));
  attr_config.set("type_name", "SimpleAttrBlock");
  std::unique_ptr<BlockStorageApi> block_op_unique = BlockStorageApi::NewInstance(&attr_config);
  SimpleAttrBlock *block_op = reinterpret_cast<SimpleAttrBlock *>(block_op_unique.get());
  EmptyBlock empty_op;
  EdgeListConfig config;
  config.edge_capacity_max_num = 100;
  MockMallocApi malloc_api;
  typename TestFixture::AdaptorType adaptor(&empty_op, block_op, config);

  CommonUpdateItems items;
  items.ids = {1, 2};
  items.id_weights = {1, 2};

  uint32 new_addr;
  auto kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), nullptr);
  auto edge_list_ptr = kv_data_ptr->data();
  auto ptr = this->ReinterpretPtr(edge_list_ptr);

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

  int capacity = ptr->capacity;
  adaptor.config_.oversize_replace_strategy = WEIGHT;
  adaptor.Fill(edge_list_ptr, nullptr, items);

  // capacity is set in InitBlock and won't be changed later
  ASSERT_EQ(capacity, ptr->capacity);
  ASSERT_EQ(ptr->capacity, 2);
  ASSERT_EQ(ptr->out_degree, 2);
  ASSERT_EQ(ptr->meta.size, 2);
  std::vector<uint64> expect = {1, 2};
  std::vector<uint64> actual;
  ptr->Get(&actual);
  ASSERT_ARRAY_EQ(expect, actual);
  ASSERT_EQ(block_op->GetIntX(ptr->IthEdgeAttrBlock(0), 0), absl::GetFlag(FLAGS_simple_block_int_init_value));
  ASSERT_EQ(block_op->GetIntX(ptr->IthEdgeAttrBlock(0), 1), 1);
  ASSERT_EQ(block_op->GetIntX(ptr->IthEdgeAttrBlock(0), 2), 2);
  ASSERT_EQ(block_op->GetIntX(ptr->IthEdgeAttrBlock(0), 3), absl::GetFlag(FLAGS_simple_block_int_init_value));
  ASSERT_FLOAT_EQ(block_op->GetFloatX(ptr->IthEdgeAttrBlock(0), 0),
                  absl::GetFlag(FLAGS_simple_block_float_init_value));
  ASSERT_FLOAT_EQ(block_op->GetFloatX(ptr->IthEdgeAttrBlock(0), 1), 10.0f);
  ASSERT_FLOAT_EQ(block_op->GetFloatX(ptr->IthEdgeAttrBlock(0), 2), 10.0f);
  ASSERT_FLOAT_EQ(block_op->GetFloatX(ptr->IthEdgeAttrBlock(0), 3),
                  absl::GetFlag(FLAGS_simple_block_float_init_value));
  ASSERT_EQ(block_op->GetIntX(ptr->IthEdgeAttrBlock(1), 0), 4);
  ASSERT_EQ(block_op->GetIntX(ptr->IthEdgeAttrBlock(1), 1), 3);
  ASSERT_EQ(block_op->GetIntX(ptr->IthEdgeAttrBlock(1), 2), 2);
  ASSERT_EQ(block_op->GetIntX(ptr->IthEdgeAttrBlock(1), 3), absl::GetFlag(FLAGS_simple_block_int_init_value));
  ASSERT_FLOAT_EQ(block_op->GetFloatX(ptr->IthEdgeAttrBlock(1), 0),
                  absl::GetFlag(FLAGS_simple_block_float_init_value));
  ASSERT_FLOAT_EQ(block_op->GetFloatX(ptr->IthEdgeAttrBlock(1), 1), 10.0f);
  ASSERT_FLOAT_EQ(block_op->GetFloatX(ptr->IthEdgeAttrBlock(1), 2), 20.0f);
  ASSERT_FLOAT_EQ(block_op->GetFloatX(ptr->IthEdgeAttrBlock(1), 3), 30.0f);
}

TYPED_TEST(CPTStandardTest, AdaptorFillMostRecent) {
  SimpleAttrBlock block_op;
  EmptyBlock empty_op;
  EdgeListConfig config;

  config.edge_capacity_max_num = 100;
  MockMallocApi malloc_api;
  typename TestFixture::AdaptorType adaptor(&block_op, &empty_op, config);

  CommonUpdateItems items;
  items.ids = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  items.id_weights = {2, 8, 58, 4, 7, 9, 34, 8, 9, 3};

  uint32 new_addr;
  auto kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), nullptr);
  auto edge_list_ptr = kv_data_ptr->data();
  adaptor.config_.oversize_replace_strategy = LRU;
  adaptor.Fill(edge_list_ptr, nullptr, items);

  // Storage without timestamp, return on insertion order.
  EdgeList l;
  EdgeListWriter writer(&l, 15);
  SamplingParam param;
  param.set_strategy(SamplingParam_SamplingStrategy_MOST_RECENT);
  param.set_sampling_num(15);
  auto slice = writer.Slice(0);
  adaptor.Sample(edge_list_ptr, param, &slice);
  std::vector<uint64_t> results = {10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 0, 0, 0, 0};
  for (size_t i = 0; i < 15; ++i) {
    ASSERT_EQ(l.node_id(i), results[i])
        << "index " << i << " error: " << l.node_id(i) << " vs " << results[i];
  }

  // Storage with timestamp, result sorted by timestamp.
  adaptor.Clean(edge_list_ptr);
  std::vector<uint32_t> timestamps = {1, 2, 4, 10, 8, 7, 3, 6, 9, 5};
  for (size_t i = 0; i < 10; ++i) {
    CommonUpdateItems t;
    t.ids = {items.ids[i]};
    t.id_weights = {items.id_weights[i]};
    t.id_ts = {timestamps[i]};
    adaptor.config_.oversize_replace_strategy = LRU;
    adaptor.Fill(edge_list_ptr, nullptr, t);
  }
  writer.Resize(15);
  slice = writer.Slice(0);
  param.set_max_timestamp_s(0);
  adaptor.Sample(edge_list_ptr, param, &slice);
  results = {4, 9, 5, 6, 8, 10, 3, 7, 2, 1, 0, 0, 0, 0, 0};
  for (size_t i = 0; i < 15; ++i) {
    ASSERT_EQ(l.node_id(i), results[i])
        << "index " << i << " error: " << l.node_id(i) << " vs " << results[i];
  }

  // Storage with timestamp, and filter by request timestamp.
  adaptor.Clean(edge_list_ptr);
  for (size_t i = 0; i < 10; ++i) {
    CommonUpdateItems t;
    t.ids = {items.ids[i]};
    t.id_weights = {items.id_weights[i]};
    t.id_ts = {timestamps[i]};
    adaptor.config_.oversize_replace_strategy = LRU;
    adaptor.Fill(edge_list_ptr, nullptr, t);
  }
  writer.Resize(15);
  slice = writer.Slice(0);
  param.set_max_timestamp_s(6);
  adaptor.Sample(edge_list_ptr, param, &slice);
  results = {8, 10, 3, 7, 2, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0};
  for (size_t i = 0; i < 15; ++i) {
    ASSERT_EQ(l.node_id(i), results[i])
        << "index " << i << " error: " << l.node_id(i) << " vs " << results[i];
  }
}

TYPED_TEST(CPTStandardTest, AdaptorFillRandom) {
  EmptyBlock empty_op;
  EdgeListConfig config;

  config.edge_capacity_max_num = 100;
  MockMallocApi malloc_api;
  typename TestFixture::AdaptorType adaptor(&empty_op, &empty_op, config);

  CommonUpdateItems items;
  items.ids = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  items.id_weights = {2, 8, 58, 4, 7, 9, 34, 8, 9, 3};

  uint32 new_addr;
  auto kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), nullptr);
  auto edge_list_ptr = kv_data_ptr->data();
  adaptor.config_.oversize_replace_strategy = LRU;
  adaptor.Fill(edge_list_ptr, nullptr, items);

  std::unordered_map<uint64, int> results = {
      {1, 0}, {2, 0}, {3, 0}, {4, 0}, {5, 0}, {6, 0}, {7, 0}, {8, 0}, {9, 0}, {10, 0}};

  EdgeList l;
  EdgeListWriter writer(&l, 15);
  SamplingParam param;
  param.set_strategy(SamplingParam_SamplingStrategy_RANDOM);
  param.set_sampling_num(15);
  // When sample enough times, each number should appear at least once.
  // The main purpose is to test whether some ids are deleted incorrectly.
  // In theory it does have a little chance to fail.
  for (int t = 0; t < 1000; ++t) {
    writer.Resize(15);
    auto slice = writer.Slice(0);
    adaptor.Sample(edge_list_ptr, param, &slice);
    for (size_t i = 0; i < 15; ++i) {
      ASSERT_GT(results.count(l.node_id(i)), 0) << "index = " << i << ", id = " << l.node_id(i);
      results[l.node_id(i)]++;
    }
  }
  for (auto &v : results) {
    ASSERT_GT(v.second, 0) << v.first;
    v.second = 0;
  }

  // Filter by request timestamp.
  param.set_max_timestamp_s(6);
  adaptor.config_.oversize_replace_strategy = LRU;
  for (int t = 0; t < 1000; t++) {
    for (uint32_t i = 0; i < 10; ++i) {
      CommonUpdateItems t;
      t.ids = {items.ids[i]};
      t.id_weights = {items.id_weights[i]};
      t.id_ts = {i};
      adaptor.Fill(edge_list_ptr, nullptr, t);
    }
    writer.Resize(15);
    auto slice = writer.Slice(0);
    adaptor.Sample(edge_list_ptr, param, &slice);
    for (size_t i = 0; i < 15; ++i) {
      ASSERT_GT(results.count(l.node_id(i)), 0) << "index = " << i << ", id = " << l.node_id(i);
      results[l.node_id(i)]++;
    }
  }
  for (auto &v : results) {
    if (v.first > 7) {
      ASSERT_EQ(v.second, 0) << v.first;
    } else {
      ASSERT_GT(v.second, 0) << v.first;
    }
    v.second = 0;
  }

  // Test sampling_without_replacement
  param.set_max_timestamp_s(6);
  param.set_sampling_without_replacement(true);
  adaptor.Clean(edge_list_ptr);
  adaptor.config_.oversize_replace_strategy = LRU;
  for (uint32_t i = 0; i < 10; ++i) {
    CommonUpdateItems t;
    t.ids = {items.ids[i]};
    t.id_weights = {items.id_weights[i]};
    t.id_ts = {i};
    adaptor.Fill(edge_list_ptr, nullptr, t);
  }
  writer.Resize(15);
  auto slice = writer.Slice(0);
  adaptor.Sample(edge_list_ptr, param, &slice);
  for (size_t i = 0; i < 15; ++i) { results[l.node_id(i)]++; }

  for (auto &v : results) {
    if (v.first == 0) { continue; }
    if (v.first > 7) {
      ASSERT_EQ(v.second, 0);
    } else {
      ASSERT_EQ(v.second, 1) << v.first << ", " << v.second;
    }
    v.second = 0;
  }
}

TYPED_TEST(CPTStandardTest, AdaptorFillTopN) {
  SimpleAttrBlock block_op;
  EmptyBlock empty_op;
  EdgeListConfig config;

  config.edge_capacity_max_num = 100;
  MockMallocApi malloc_api;
  typename TestFixture::AdaptorType adaptor(&block_op, &empty_op, config);

  CommonUpdateItems items;
  items.ids = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 4};
  items.id_weights = {2, 7, 58, 4, 7, 9, 34, 8, 9, 3, 1};

  uint32 new_addr;
  auto kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), nullptr);
  auto edge_list_ptr = kv_data_ptr->data();
  adaptor.config_.oversize_replace_strategy = LRU;
  adaptor.Fill(edge_list_ptr, nullptr, items);

  EdgeList l;
  EdgeListWriter writer(&l, 15);
  SamplingParam param;
  // step1: Test topn weight truncate.
  param.set_strategy(SamplingParam_SamplingStrategy_TOPN_WEIGHT);
  param.set_sampling_num(5);
  auto slice = writer.Slice(0);
  adaptor.Sample(edge_list_ptr, param, &slice);
  std::vector<uint64_t> results = {9, 6, 3, 8, 7};
  std::vector<float> w = {9, 9, 58, 8, 34};
  if constexpr (std::is_same_v<TypeParam, CPTreapItemPreciseWeightIndexed>) {
    // CPTreapItemPreciseWeightIndexed maintains an extra linked list for weight, so the returned TOPN_WEIGHT
    // sample result is sorted by weight.
    results = {3, 7, 9, 6, 8};
    w = {58, 34, 9, 9, 8};
  }
  for (size_t i = 0; i < 5; ++i) {
    ASSERT_EQ(l.node_id(i), results[i])
        << "index " << i << " error: " << l.node_id(i) << " vs " << results[i];
    ASSERT_EQ(l.edge_weight(i), w[i]) << "weight " << i << " error: " << l.node_id(i) << " vs " << w[i];
  }
  // step2: topn > size.
  slice = writer.Slice(0);
  param.set_sampling_num(15);
  adaptor.Sample(edge_list_ptr, param, &slice);
  results = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 0, 0, 0, 0};
  w = {2, 7, 58, 5, 7, 9, 34, 8, 9, 3, 0, 0, 0, 0, 0};
  if constexpr (std::is_same_v<TypeParam, CPTreapItemPreciseWeightIndexed>) {
    results = {3, 7, 9, 6, 8, 5, 2, 4, 10, 1, 0, 0, 0, 0, 0};
    w = {58, 34, 9, 9, 8, 7, 7, 5, 3, 2, 0, 0, 0, 0, 0};
  }
  for (size_t i = 0; i < 15; ++i) {
    ASSERT_EQ(l.node_id(i), results[i])
        << "index " << i << " error: " << l.node_id(i) << " vs " << results[i];
    ASSERT_EQ(l.edge_weight(i), w[i]) << "weight " << i << " error: " << l.node_id(i) << " vs " << w[i];
  }
}

TYPED_TEST(CPTStandardTest, AdaptorFillSampleWithoutReplacement) {
  SimpleAttrBlock block_op;
  EmptyBlock empty_op;
  EdgeListConfig config;

  config.edge_capacity_max_num = 100;
  MockMallocApi malloc_api;
  typename TestFixture::AdaptorType adaptor(&block_op, &empty_op, config);

  CommonUpdateItems items;
  items.ids = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  items.id_weights = {2, 8, 58, 4, 7, 9, 34, 8, 9, 3};

  uint32 new_addr;
  auto kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), nullptr);
  auto edge_list_ptr = kv_data_ptr->data();
  adaptor.config_.oversize_replace_strategy = LRU;
  adaptor.Fill(edge_list_ptr, nullptr, items);

  for (size_t i = 0; i < 100; ++i) {
    EdgeList l;
    EdgeListWriter writer(&l, 5);
    SamplingParam param;
    param.set_strategy(SamplingParam_SamplingStrategy_EDGE_WEIGHT);
    param.set_sampling_without_replacement(true);
    param.set_sampling_num(5);
    auto slice = writer.Slice(0);
    adaptor.Sample(edge_list_ptr, param, &slice);
    std::set<uint64> results(l.node_id().begin(), l.node_id().end());
    // Check deduplicated.
    ASSERT_EQ(results.size(), 5) << i;
    ASSERT_EQ(adaptor.GetSize(edge_list_ptr), 10);
  }
  // Padding by 0.
  EdgeList l;
  EdgeListWriter writer(&l, 15);
  SamplingParam param;
  param.set_strategy(SamplingParam_SamplingStrategy_EDGE_WEIGHT);
  param.set_sampling_without_replacement(true);
  param.set_sampling_num(15);
  auto slice = writer.Slice(0);
  adaptor.Sample(edge_list_ptr, param, &slice);
  for (int i = 10; i > 15; ++i) { ASSERT_EQ(l.node_id(i), 0); }
}

TYPED_TEST(CPTStandardTest, AdaptorFillMaxTimestamp) {
  SimpleAttrBlock block_op;
  EmptyBlock empty_op;
  EdgeListConfig config;

  config.edge_capacity_max_num = 100;
  MockMallocApi malloc_api;
  typename TestFixture::AdaptorType adaptor(&block_op, &empty_op, config);

  CommonUpdateItems items;
  items.ids = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  items.id_weights = {2, 8, 58, 4, 7, 9, 34, 8, 9, 3};
  items.id_ts = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

  uint32 new_addr;
  auto kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), nullptr);
  auto edge_list_ptr = kv_data_ptr->data();
  adaptor.config_.oversize_replace_strategy = LRU;
  adaptor.Fill(edge_list_ptr, nullptr, items);

  items.ids = {101, 102, 103};
  items.id_weights = {2, 8, 14};
  // Timestamp in seconds.
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
  param.set_max_timestamp_s(10);
  EdgeList l;
  EdgeListWriter writer(&l, 15);
  auto slice = writer.Slice(0);
  adaptor.Sample(edge_list_ptr, param, &slice);
  std::vector<uint64_t> results = {103, 102, 101, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 0};
  for (size_t i = 0; i < 15; ++i) {
    ASSERT_EQ(l.node_id(i), results[i])
        << "index " << i << " error: " << l.node_id(i) << " vs " << results[i];
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
}

TYPED_TEST(CPTStandardTest, AdaptorFillMaxTimestampAndWeightAndTimeDecay) {
  SimpleAttrBlock block_op;
  EmptyBlock empty_op;
  EdgeListConfig config;

  config.edge_capacity_max_num = 100;
  MockMallocApi malloc_api;
  typename TestFixture::AdaptorType adaptor(&block_op, &empty_op, config);

  CommonUpdateItems items;
  items.ids = {1, 2, 3, 4, 8};
  items.id_weights = {2, 4, 6, 8, 1.5};  // After 50% decay, they are 1, 2, 3, 4, the last one removed.

  uint32 new_addr;
  auto kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), nullptr);
  auto edge_list_ptr = kv_data_ptr->data();
  adaptor.config_.oversize_replace_strategy = LRU;
  adaptor.Fill(edge_list_ptr, nullptr, items);

  int delete_count = adaptor.TimeDecay(edge_list_ptr, nullptr, 1, 0.5, 1);
  CHECK_EQ(delete_count, 1);
  items.ids = {5, 6, 7};
  items.id_weights = {10, 10, 10};
  items.id_ts = {10, 10, 10};

  kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), edge_list_ptr);
  auto old_edge_list_ptr = edge_list_ptr;
  edge_list_ptr = kv_data_ptr->data();
  adaptor.config_.oversize_replace_strategy = LRU;
  adaptor.Fill(edge_list_ptr, old_edge_list_ptr, items);
  ASSERT_NE(edge_list_ptr, old_edge_list_ptr);

  int sample_time = 100000, sample_num = 3;
  EdgeList list;
  EdgeListWriter writer(&list, sample_num * sample_time);
  SamplingParam param;
  std::unordered_map<int, int> counter;
  for (int i = 1; i < 8; ++i) { counter.insert({i, 0}); }

  // Test without timestamp
  param.set_strategy(SamplingParam_SamplingStrategy_EDGE_WEIGHT);
  param.set_sampling_num(sample_num);
  for (size_t i = 0; i < sample_time; ++i) {
    auto slice = writer.Slice(i * sample_num);
    adaptor.Sample(edge_list_ptr, param, &slice);
    ASSERT_EQ(slice.offset, i * sample_num + sample_num);
  }

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
  ASSERT_LE(std::fabs(((counter.find(1)->second + 0.0) / (sample_num * sample_time)) - 0.025), delta);
  ASSERT_LE(std::fabs(((counter.find(2)->second + 0.0) / (sample_num * sample_time)) - 0.05), delta);
  ASSERT_LE(std::fabs(((counter.find(3)->second + 0.0) / (sample_num * sample_time)) - 0.075), delta);
  ASSERT_LE(std::fabs(((counter.find(4)->second + 0.0) / (sample_num * sample_time)) - 0.1), delta);
  ASSERT_LE(std::fabs(((counter.find(5)->second + 0.0) / (sample_num * sample_time)) - 0.25), delta);
  ASSERT_LE(std::fabs(((counter.find(6)->second + 0.0) / (sample_num * sample_time)) - 0.25), delta);
  ASSERT_LE(std::fabs(((counter.find(7)->second + 0.0) / (sample_num * sample_time)) - 0.25), delta);
  std::cout << "--------------------------------" << std::endl;

  // Test with timestamp
  counter.clear();
  for (int i = 1; i < 8; ++i) { counter.insert({i, 0}); }
  EdgeList list2;
  EdgeListWriter writer2(&list2, sample_num * sample_time);
  param.set_max_timestamp_s(9);
  for (size_t i = 0; i < sample_time; ++i) {
    auto slice = writer2.Slice(i * sample_num);
    adaptor.Sample(edge_list_ptr, param, &slice);
    ASSERT_EQ(slice.offset, i * sample_num + sample_num);
  }

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

  ASSERT_LE(std::fabs(((counter.find(1)->second + 0.0) / (sample_num * sample_time)) - 0.1), delta);
  ASSERT_LE(std::fabs(((counter.find(2)->second + 0.0) / (sample_num * sample_time)) - 0.2), delta);
  ASSERT_LE(std::fabs(((counter.find(3)->second + 0.0) / (sample_num * sample_time)) - 0.3), delta);
  ASSERT_LE(std::fabs(((counter.find(4)->second + 0.0) / (sample_num * sample_time)) - 0.4), delta);
  ASSERT_EQ(counter.find(5)->second, 0);
  ASSERT_EQ(counter.find(6)->second, 0);
  ASSERT_EQ(counter.find(7)->second, 0);
}

TYPED_TEST(CPTStandardTest, AdaptorFillTimeDecay) {
  SimpleAttrBlock block_op;
  EmptyBlock empty_op;
  EdgeListConfig config;

  config.edge_capacity_max_num = 100;
  MockMallocApi malloc_api;
  typename TestFixture::AdaptorType adaptor(&block_op, &empty_op, config);

  CommonUpdateItems items;
  items.ids = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  items.id_weights = {2, 3, 4, 5, 6, 7, 8, 12, 16, 32};

  uint32 new_addr;
  auto kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), nullptr);
  auto edge_list_ptr = kv_data_ptr->data();
  adaptor.config_.oversize_replace_strategy = LRU;
  adaptor.Fill(edge_list_ptr, nullptr, items);

  int delete_count = adaptor.TimeDecay(edge_list_ptr, nullptr, 0.9, 0.5, 1.01);
  CHECK_EQ(delete_count, 1);  // Remove id = 1
  SamplingParam param;
  param.set_strategy(SamplingParam_SamplingStrategy_MOST_RECENT);
  param.set_sampling_num(15);
  EdgeList l;
  EdgeListWriter writer(&l, 15);
  auto slice = writer.Slice(0);
  adaptor.Sample(edge_list_ptr, param, &slice);
  std::vector<uint64_t> results = {10, 9, 8, 7, 6, 5, 4, 3, 2, 0, 0, 0, 0, 0, 0};
  for (size_t i = 0; i < 15; ++i) {
    ASSERT_EQ(l.node_id(i), results[i])
        << "index " << i << " error: " << l.node_id(i) << " vs " << results[i];
  }

  delete_count = adaptor.TimeDecay(edge_list_ptr, nullptr, 0.9, 0.5, 1.01);
  CHECK_EQ(delete_count, 2);  // Remove id = 2 & 3
  EdgeList l2;
  EdgeListWriter writer2(&l2, 15);
  slice = writer2.Slice(0);
  adaptor.Sample(edge_list_ptr, param, &slice);
  std::vector<uint64_t> results2 = {10, 9, 8, 7, 6, 5, 4, 0, 0, 0, 0, 0, 0, 0, 0};
  for (size_t i = 0; i < 15; ++i) {
    ASSERT_EQ(l2.node_id(i), results2[i])
        << "index " << i << " error: " << l.node_id(i) << " vs " << results2[i];
  }
}

TYPED_TEST(CPTStandardTest, BatchMergeTimeDecay) {
  absl::SetFlag(&FLAGS_cpt_edge_buffer_capacity, 100);
  SimpleAttrBlock block_op;
  EmptyBlock empty_op;
  EdgeListConfig config;

  config.edge_capacity_max_num = 100;
  MockMallocApi malloc_api;
  typename TestFixture::AdaptorType adaptor(&block_op, &empty_op, config);

  CommonUpdateItems items;
  int offset = 1;
  for (int i = 1; i <= config.edge_capacity_max_num; ++i) {
    items.ids.push_back(offset);
    items.id_weights.push_back(offset++ / 10.0);
  }

  uint32 new_addr;
  auto kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), nullptr);
  auto edge_list_ptr = kv_data_ptr->data();
  auto ptr = this->ReinterpretPtr(edge_list_ptr);
  adaptor.config_.oversize_replace_strategy = LRU;
  adaptor.Fill(edge_list_ptr, nullptr, items);

  CHECK_EQ(config.edge_capacity_max_num, ptr->capacity);
  CHECK_EQ(config.edge_capacity_max_num, ptr->meta.size);
  CHECK(!ptr->BatchMergeEnabled());

  // add 10 items will trigger batch merge, since FLAGS_cpt_edge_buffer_capacity > 0
  items.ids.clear();
  items.id_weights.clear();
  for (int i = 1; i <= 10; ++i) {
    items.ids.push_back(offset);
    items.id_weights.push_back(offset++ / 10.0);
  }

  CHECK(!adaptor.CanReuse(edge_list_ptr, items.ids.size()));
  kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), edge_list_ptr);
  auto old_edge_list_ptr = edge_list_ptr;
  edge_list_ptr = kv_data_ptr->data();
  ptr = this->ReinterpretPtr(edge_list_ptr);
  CHECK_EQ(config.edge_capacity_max_num, ptr->capacity);
  CHECK_EQ(0, ptr->meta.size);
  CHECK_EQ(0, ptr->meta.buffer_size);
  CHECK(ptr->BatchMergeEnabled());

  adaptor.Fill(edge_list_ptr, old_edge_list_ptr, items);
  CHECK_EQ(config.edge_capacity_max_num, ptr->capacity);
  CHECK_EQ(config.edge_capacity_max_num, ptr->meta.size);
  CHECK_EQ(0, ptr->meta.buffer_size);

  items.ids.clear();
  items.id_weights.clear();
  for (int i = 1; i <= 10; ++i) {
    items.ids.push_back(offset);
    items.id_weights.push_back(offset++ / 10.0);
  }
  // 10 item is buffered
  adaptor.Fill(edge_list_ptr, nullptr, items);
  CHECK_EQ(config.edge_capacity_max_num, ptr->capacity);
  CHECK_EQ(config.edge_capacity_max_num, ptr->meta.size);
  CHECK_EQ(10, ptr->meta.buffer_size);

  CHECK(!adaptor.CanReuse(edge_list_ptr, -1));
  kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, -1, edge_list_ptr);
  old_edge_list_ptr = edge_list_ptr;
  edge_list_ptr = kv_data_ptr->data();
  ptr = this->ReinterpretPtr(edge_list_ptr);

  // TimeDecay will merge buffered edge first.
  // the first 20 edges are popped, so the left edge weight should be 2.1, 2.2, 2.3, ...
  int delete_count = adaptor.TimeDecay(edge_list_ptr, old_edge_list_ptr, 1, 0.9, 3);
  CHECK_EQ(delete_count, 13);  // delete id 21,22...,30,31,32,33
  CHECK_EQ(config.edge_capacity_max_num, ptr->capacity);
  CHECK_EQ(config.edge_capacity_max_num - delete_count, ptr->meta.size);
  CHECK_EQ(0, ptr->meta.buffer_size);

  SamplingParam param;
  param.set_strategy(SamplingParam_SamplingStrategy_MOST_RECENT);
  param.set_sampling_num(5);
  EdgeList l;
  EdgeListWriter writer(&l, 5);
  auto slice = writer.Slice(0);
  adaptor.Sample(edge_list_ptr, param, &slice);
  std::vector<uint64_t> results = {120, 119, 118, 117, 116};
  for (size_t i = 0; i < 5; ++i) {
    ASSERT_EQ(l.node_id(i), results[i])
        << "index " << i << " error: " << l.node_id(i) << " vs " << results[i];
  }
  absl::SetFlag(&FLAGS_cpt_edge_buffer_capacity, 0);
}

TYPED_TEST(CPTStandardTest, BatchMergeDelete) {
  absl::SetFlag(&FLAGS_cpt_edge_buffer_capacity, 100);
  SimpleAttrBlock block_op;
  EmptyBlock empty_op;
  EdgeListConfig config;

  config.edge_capacity_max_num = 100;
  MockMallocApi malloc_api;
  typename TestFixture::AdaptorType adaptor(&block_op, &empty_op, config);

  CommonUpdateItems items;
  int offset = 1;
  for (int i = 1; i <= config.edge_capacity_max_num; ++i) {
    items.ids.push_back(offset);
    items.id_weights.push_back(offset++ / 10.0);
  }

  uint32 new_addr;
  auto kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), nullptr);
  auto edge_list_ptr = kv_data_ptr->data();
  auto ptr = this->ReinterpretPtr(edge_list_ptr);
  adaptor.config_.oversize_replace_strategy = LRU;
  adaptor.Fill(edge_list_ptr, nullptr, items);

  CHECK_EQ(config.edge_capacity_max_num, ptr->capacity);
  CHECK_EQ(config.edge_capacity_max_num, ptr->meta.size);
  CHECK(!ptr->BatchMergeEnabled());

  // add 10 items will trigger batch merge, since FLAGS_cpt_edge_buffer_capacity > 0
  items.ids.clear();
  items.id_weights.clear();
  for (int i = 1; i <= 10; ++i) {
    items.ids.push_back(offset);
    items.id_weights.push_back(offset++ / 10.0);
  }

  CHECK(!adaptor.CanReuse(edge_list_ptr, items.ids.size()));
  kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), edge_list_ptr);
  auto old_edge_list_ptr = edge_list_ptr;
  edge_list_ptr = kv_data_ptr->data();
  ptr = this->ReinterpretPtr(edge_list_ptr);
  CHECK_EQ(ptr->meta.size, 0);
  CHECK_EQ(ptr->meta.buffer_size, 0);
  CHECK(ptr->BatchMergeEnabled());
  // ids are 11,12,...
  adaptor.Fill(edge_list_ptr, old_edge_list_ptr, items);
  CHECK_EQ(config.edge_capacity_max_num, ptr->capacity);
  CHECK_EQ(config.edge_capacity_max_num, ptr->meta.size);
  CHECK_EQ(ptr->meta.buffer_size, 0);

  // 10 item is buffered
  items.ids.clear();
  items.id_weights.clear();
  for (int i = 1; i <= 10; ++i) {
    items.ids.push_back(offset);
    items.id_weights.push_back(offset++ / 10.0);
  }
  adaptor.Fill(edge_list_ptr, nullptr, items);
  CHECK_EQ(config.edge_capacity_max_num, ptr->capacity);
  CHECK_EQ(config.edge_capacity_max_num, ptr->meta.size);
  CHECK_EQ(ptr->meta.buffer_size, 10);

  // delete required replace update
  CHECK(!adaptor.CanReuse(edge_list_ptr, -1));
  kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, -1, edge_list_ptr);
  old_edge_list_ptr = edge_list_ptr;
  edge_list_ptr = kv_data_ptr->data();
  ptr = this->ReinterpretPtr(edge_list_ptr);
  std::vector<uint64> ids = {21, 50, 115};
  adaptor.DeleteItems(edge_list_ptr, old_edge_list_ptr, ids);
  CHECK_EQ(config.edge_capacity_max_num - 3, ptr->meta.size);
  // delete will perform merge first
  CHECK_EQ(ptr->meta.buffer_size, 0);

  absl::SetFlag(&FLAGS_cpt_edge_buffer_capacity, 0);
}

TYPED_TEST(CPTStandardTest, MinWeightRequired) {
  SimpleAttrBlock block_op;
  EmptyBlock empty_op;
  EdgeListConfig config;

  config.edge_capacity_max_num = 100;
  MockMallocApi malloc_api;
  typename TestFixture::AdaptorType adaptor(&block_op, &empty_op, config);

  CommonUpdateItems items;
  items.ids = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  items.id_weights = {2, 8, 45, 4, 7, 21, 34, 18, 9, 3};
  items.id_ts = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

  uint32 new_addr;
  auto kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), nullptr);
  auto edge_list_ptr = kv_data_ptr->data();
  adaptor.config_.oversize_replace_strategy = LRU;
  adaptor.Fill(edge_list_ptr, nullptr, items);

  items.ids = {101, 102, 103};
  items.id_weights = {2.5, 28, 14};
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
  param.set_max_timestamp_s(10);
  param.set_min_weight_required(5);
  EdgeList l;
  EdgeListWriter writer(&l, 15);
  auto slice = writer.Slice(0);
  adaptor.Sample(edge_list_ptr, param, &slice);
  std::vector<uint64_t> results = {103, 102, 9, 8, 7, 6, 5, 3, 2, 0, 0, 0, 0, 0, 0};
  for (size_t i = 0; i < 15; ++i) {
    ASSERT_EQ(l.node_id(i), results[i])
        << "index " << i << " error: " << l.node_id(i) << " vs " << results[i];
  }

  param.set_strategy(SamplingParam_SamplingStrategy_LEAST_RECENT);
  param.set_sampling_num(15);
  param.set_max_timestamp_s(0);
  param.set_min_weight_required(10);
  EdgeList l0;
  EdgeListWriter writer0(&l0, 15);
  slice = writer0.Slice(0);
  adaptor.Sample(edge_list_ptr, param, &slice);
  std::vector<uint64_t> results0 = {3, 6, 7, 8, 102, 103, 0, 0, 0, 0, 0, 0, 0, 0, 0};
  for (size_t i = 0; i < 15; ++i) {
    ASSERT_EQ(l0.node_id(i), results0[i])
        << "index " << i << " error: " << l0.node_id(i) << " vs " << results0[i];
  }

  param.set_strategy(SamplingParam_SamplingStrategy_TOPN_WEIGHT);
  param.set_max_timestamp_s(9);
  param.set_min_weight_required(5);
  EdgeList l1;
  EdgeListWriter writer1(&l1, 15);
  slice = writer1.Slice(0);
  adaptor.Sample(edge_list_ptr, param, &slice);
  std::vector<uint64_t> results1 = {9, 2, 3, 8, 5, 6, 7, 0, 0, 0, 0, 0, 0, 0, 0};
  if constexpr (std::is_same_v<TypeParam, CPTreapItemPreciseWeightIndexed>) {
    results1 = {3, 7, 6, 8, 9, 2, 5, 0, 0, 0, 0, 0, 0, 0, 0};
  }
  for (size_t i = 0; i < 15; ++i) {
    ASSERT_EQ(l1.node_id(i), results1[i])
        << "index " << i << " error: " << l1.node_id(i) << " vs " << results1[i];
  }

  std::unordered_map<int, int> counter;
  int sample_time = 100000, sample_num = 3;
  for (int i = 1; i < 11; ++i) { counter.insert({i, 0}); }
  EdgeList l2;
  EdgeListWriter writer2(&l2, sample_num * sample_time);
  param.set_strategy(SamplingParam_SamplingStrategy_EDGE_WEIGHT);
  param.set_max_timestamp_s(9);
  param.set_min_weight_required(21);
  param.set_sampling_num(sample_num);
  for (size_t i = 0; i < sample_time; ++i) {
    auto slice2 = writer2.Slice(i * sample_num);
    adaptor.Sample(edge_list_ptr, param, &slice2);
    ASSERT_EQ(slice2.offset, i * sample_num + sample_num);
  }

  for (int offset = 0; offset < sample_time * sample_num; ++offset) {
    int node_id = l2.node_id(offset);
    ASSERT_GE(node_id, 1) << "node_id: " << node_id;
    ASSERT_LE(node_id, 10) << "node_id: " << node_id;
    auto iter = counter.find(node_id);
    iter->second += 1;
  }

  for (auto item : counter) {
    std::cout << "key = " << item.first << ", occur time = " << item.second
              << ", ratio = " << ((item.second + 0.0) / (sample_time * sample_num)) << std::endl;
  }

  float delta = 0.01;
  ASSERT_EQ(counter.find(1)->second, 0);
  ASSERT_EQ(counter.find(2)->second, 0);
  ASSERT_LE(std::fabs(((counter.find(3)->second + 0.0) / (sample_num * sample_time)) - 0.45), delta);
  ASSERT_EQ(counter.find(4)->second, 0);
  ASSERT_EQ(counter.find(5)->second, 0);
  ASSERT_LE(std::fabs(((counter.find(6)->second + 0.0) / (sample_num * sample_time)) - 0.21), delta);
  ASSERT_LE(std::fabs(((counter.find(7)->second + 0.0) / (sample_num * sample_time)) - 0.34), delta);
  ASSERT_EQ(counter.find(8)->second, 0);
  ASSERT_EQ(counter.find(9)->second, 0);
  ASSERT_EQ(counter.find(10)->second, 0);
  std::cout << "--------------------------------" << std::endl;

  counter.clear();
  for (int i = 1; i < 11; ++i) { counter.insert({i, 0}); }
  EdgeList l3;
  EdgeListWriter writer3(&l3, sample_num * sample_time);
  param.set_strategy(SamplingParam_SamplingStrategy_RANDOM);
  param.set_max_timestamp_s(9);
  param.set_min_weight_required(18);
  param.set_sampling_num(sample_num);
  for (size_t i = 0; i < sample_time; ++i) {
    auto slice3 = writer3.Slice(i * sample_num);
    adaptor.Sample(edge_list_ptr, param, &slice3);
    ASSERT_EQ(slice3.offset, i * sample_num + sample_num);
  }

  for (int offset = 0; offset < sample_time * sample_num; ++offset) {
    int node_id = l3.node_id(offset);
    ASSERT_GE(node_id, 1) << "node_id: " << node_id;
    ASSERT_LE(node_id, 10) << "node_id: " << node_id;
    auto iter = counter.find(node_id);
    iter->second += 1;
  }

  for (auto item : counter) {
    std::cout << "key = " << item.first << ", occur time = " << item.second
              << ", ratio = " << ((item.second + 0.0) / (sample_time * sample_num)) << std::endl;
  }

  ASSERT_EQ(counter.find(1)->second, 0);
  ASSERT_EQ(counter.find(2)->second, 0);
  ASSERT_LE(std::fabs(((counter.find(3)->second + 0.0) / (sample_num * sample_time)) - 0.25), delta);
  ASSERT_EQ(counter.find(4)->second, 0);
  ASSERT_EQ(counter.find(5)->second, 0);
  ASSERT_LE(std::fabs(((counter.find(6)->second + 0.0) / (sample_num * sample_time)) - 0.25), delta);
  ASSERT_LE(std::fabs(((counter.find(7)->second + 0.0) / (sample_num * sample_time)) - 0.25), delta);
  ASSERT_LE(std::fabs(((counter.find(8)->second + 0.0) / (sample_num * sample_time)) - 0.25), delta);
  ASSERT_EQ(counter.find(9)->second, 0);
  ASSERT_EQ(counter.find(10)->second, 0);
}

TYPED_TEST(CPTStandardTest, MinWeightFilterTimeTest) {
  SimpleAttrBlock block_op;
  EmptyBlock empty_op;
  EdgeListConfig config;

  config.edge_capacity_max_num = 10000;
  MockMallocApi malloc_api;
  typename TestFixture::AdaptorType adaptor(&block_op, &empty_op, config);

  CommonUpdateItems items;
  for (size_t i = 0; i < 10000; ++i) {
    items.ids.push_back(i);
    items.id_weights.push_back(i);
    items.id_ts.push_back(i);
  }
  auto rng = std::default_random_engine{};
  std::shuffle(items.id_weights.begin(), items.id_weights.end(), rng);

  uint32 new_addr;
  auto kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), nullptr);
  auto edge_list_ptr = kv_data_ptr->data();
  adaptor.config_.oversize_replace_strategy = LRU;
  adaptor.Fill(edge_list_ptr, nullptr, items);

  base::Timer timer;
  SamplingParam param;
  param.set_strategy(SamplingParam_SamplingStrategy_EDGE_WEIGHT);
  param.set_sampling_num(15);
  param.set_max_timestamp_s(10001);
  for (size_t i = 0; i < 1000; i++) {
    EdgeList l;
    EdgeListWriter writer(&l, 15);
    auto slice = writer.Slice(0);
    adaptor.Sample(edge_list_ptr, param, &slice);
  }
  timer.AppendCostMs("[min-filter:0, time-filter:0]");

  param.set_max_timestamp_s(8000);
  for (size_t i = 0; i < 1000; i++) {
    EdgeList l;
    EdgeListWriter writer(&l, 15);
    auto slice = writer.Slice(0);
    adaptor.Sample(edge_list_ptr, param, &slice);
  }
  timer.AppendCostMs("[min-filter:0, time-filter:2000]");

  param.set_max_timestamp_s(10001);
  param.set_min_weight_required(8000);
  for (size_t i = 0; i < 1000; i++) {
    EdgeList l;
    EdgeListWriter writer(&l, 15);
    auto slice = writer.Slice(0);
    adaptor.Sample(edge_list_ptr, param, &slice);
  }
  timer.AppendCostMs("[min-filter:8000, time-filter:0]");
  LOG(INFO) << "cost: " << timer.display();
}

TYPED_TEST(CPTStandardTest, PendingEdgeTest) {
  absl::SetFlag(&FLAGS_cpt_edge_buffer_capacity, 2);
  base::Json attr_config(base::StringToJson("{}"));
  attr_config.set("type_name", "SimpleAttrBlock");
  std::unique_ptr<BlockStorageApi> block_op_unique = BlockStorageApi::NewInstance(&attr_config);
  SimpleAttrBlock *block_op = reinterpret_cast<SimpleAttrBlock *>(block_op_unique.get());
  EmptyBlock empty_op;
  EdgeListConfig config;

  config.edge_capacity_max_num = 2;
  config.oversize_replace_strategy = LRU;
  MockMallocApi malloc_api;
  typename TestFixture::AdaptorType adaptor(&empty_op, block_op, config);

  CommonUpdateItems items;
  items.ids = {1, 2};
  items.id_weights = {1, 2};

  // Generate checksum mode edge_list, and fullfill it.
  uint32 old_addr;
  auto old_kv_data_ptr = adaptor.InitBlock(&malloc_api, &old_addr, items.ids.size(), nullptr);
  auto old_edge_list_ptr = old_kv_data_ptr->data();

  char *update_info = static_cast<char *>(std::malloc(block_op->MaxSize()));
  base::ScopeExit defer([update_info]() { free(static_cast<void *>(update_info)); });
  block_op->InitialBlock(update_info, block_op->MaxSize());
  block_op->SetIntX(update_info, 1, 1);
  block_op->SetIntX(update_info, 2, 2);
  block_op->SetFloatX(update_info, 1, 10.0f);
  block_op->SetFloatX(update_info, 2, 10.0f);
  items.id_attr_update_infos.push_back(std::string(update_info, block_op->MaxSize()));
  items.id_attr_update_infos.push_back(std::string(update_info, block_op->MaxSize()));

  adaptor.Fill(old_edge_list_ptr, nullptr, items);

  // Generate batch merge mode edge_list by the edge_list above.
  uint32 new_addr;
  auto kv_data_ptr = adaptor.InitBlock(&malloc_api, &new_addr, items.ids.size(), old_edge_list_ptr);
  auto edge_list_ptr = kv_data_ptr->data();
  auto ptr = this->ReinterpretPtr(edge_list_ptr);
  // No pending
  adaptor.Fill(edge_list_ptr, old_edge_list_ptr, items);

  // Trigger pending now.
  items.ids = {3, 4};
  items.id_weights = {3, 4};
  block_op->SetIntX(update_info, 0, 4);
  block_op->SetIntX(update_info, 1, 3);
  block_op->SetFloatX(update_info, 2, 20.0f);
  block_op->SetFloatX(update_info, 3, 30.0f);
  items.id_attr_update_infos.clear();
  items.id_attr_update_infos.push_back(std::string(update_info, block_op->MaxSize()));
  items.id_attr_update_infos.push_back(std::string(update_info, block_op->MaxSize()));
  adaptor.Fill(edge_list_ptr, edge_list_ptr, items);

  // Buffer is full after inserted 2 new edges.
  CHECK(!adaptor.CanReuse(edge_list_ptr, 1));
  ASSERT_EQ(ptr->capacity, 2);
  ASSERT_EQ(ptr->out_degree, 4);
  ASSERT_EQ(ptr->meta.size, 2);
  ASSERT_EQ(ptr->meta.buffer_size, 2);
  std::vector<uint64> expect = {1, 2};
  std::vector<uint64> actual;
  ptr->Get(&actual);
  ASSERT_ARRAY_EQ(expect, actual);
  ASSERT_EQ(block_op->GetIntX(ptr->IthEdgeAttrBlock(0), 0), absl::GetFlag(FLAGS_simple_block_int_init_value));
  ASSERT_EQ(block_op->GetIntX(ptr->IthEdgeAttrBlock(0), 1), 1);
  ASSERT_EQ(block_op->GetIntX(ptr->IthEdgeAttrBlock(0), 2), 2);
  ASSERT_EQ(block_op->GetIntX(ptr->IthEdgeAttrBlock(0), 3), absl::GetFlag(FLAGS_simple_block_int_init_value));
  ASSERT_FLOAT_EQ(block_op->GetFloatX(ptr->IthEdgeAttrBlock(0), 0),
                  absl::GetFlag(FLAGS_simple_block_float_init_value));
  ASSERT_FLOAT_EQ(block_op->GetFloatX(ptr->IthEdgeAttrBlock(0), 1), 10.0f);
  ASSERT_FLOAT_EQ(block_op->GetFloatX(ptr->IthEdgeAttrBlock(0), 2), 10.0f);
  ASSERT_FLOAT_EQ(block_op->GetFloatX(ptr->IthEdgeAttrBlock(0), 3),
                  absl::GetFlag(FLAGS_simple_block_float_init_value));
  char *pending_attr = ptr->attr_of_pending_edge(ptr->edges(1));
  ASSERT_EQ(block_op->GetIntX(pending_attr, 0), 4);
  ASSERT_EQ(block_op->GetIntX(pending_attr, 1), 3);
  ASSERT_EQ(block_op->GetIntX(pending_attr, 2), 2);
  ASSERT_EQ(block_op->GetIntX(pending_attr, 3), absl::GetFlag(FLAGS_simple_block_int_init_value));
  ASSERT_FLOAT_EQ(block_op->GetFloatX(pending_attr, 0), absl::GetFlag(FLAGS_simple_block_float_init_value));
  ASSERT_FLOAT_EQ(block_op->GetFloatX(pending_attr, 1), 10.0f);
  ASSERT_FLOAT_EQ(block_op->GetFloatX(pending_attr, 2), 20.0f);
  ASSERT_FLOAT_EQ(block_op->GetFloatX(pending_attr, 3), 30.0f);
}

}  // namespace chrono_graph
