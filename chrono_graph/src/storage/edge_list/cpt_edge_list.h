// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <functional>
#include <queue>
#include <random>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>
#include "base/common/basic_types.h"
#include "base/common/file_util.h"
#include "base/common/gflags.h"
#include "base/common/time.h"
#include "base/common/timer.h"
#include "base/random/pseudo_random.h"
#include "base/util/scope_exit.h"
#include "chrono_graph/src/base/bitmap.h"
#include "chrono_graph/src/base/env.h"
#include "chrono_graph/src/base/gnn_type.h"
#include "chrono_graph/src/base/perf_util.h"
#include "chrono_graph/src/base/proto_helper.h"
#include "chrono_graph/src/storage/edge_list/el_interface.h"
#include "chrono_graph/src/storage/edge_list/mem_allocator.h"

ABSL_DECLARE_FLAG(int32, cpt_dynamic_growth_base);
ABSL_DECLARE_FLAG(double, cpt_dynamic_growth_factor);
ABSL_DECLARE_FLAG(int32, cpt_edge_buffer_capacity);
ABSL_DECLARE_FLAG(double, cpt_weight_max);
ABSL_DECLARE_FLAG(double, cpt_weight_min);

namespace chrono_graph {

typedef uint16_t offsetT;

#pragma pack(push, 1)

template <typename offsetT>
struct CPTreapEdgeListMetaBase {
  offsetT size;
  offsetT root;
  // Maintain a linked list of nodes sorted by timestamp.
  offsetT time_head;
  offsetT time_tail;
  offsetT buffer_size;  // Pending edge list size, -1 means no buffer memory allocated.
                        // Never use buffer_size < 0 to discriminate since buffer_size is unsigned

  offsetT weight_head() const {
    NOT_REACHED();
    return 0;
  }
  void set_weight_head(offsetT val) { NOT_REACHED(); }
  offsetT weight_tail() const {
    NOT_REACHED();
    return 0;
  }
  void set_weight_tail(offsetT val) { NOT_REACHED(); }
};

template <typename offsetT>
struct CPTreapEdgeListMetaWithWeight : public CPTreapEdgeListMetaBase<offsetT> {
  // Maintain a linked list of nodes sorted by weight.
  offsetT weight_head_;
  offsetT weight_tail_;

  offsetT weight_head() const { return weight_head_; }
  void set_weight_head(offsetT val) { weight_head_ = val; }
  offsetT weight_tail() const { return weight_tail_; }
  void set_weight_tail(offsetT val) { weight_tail_ = val; }
};

/** \brief An element in edge list, which is an edge of a src_id, identified by dst_id.
 * Some notes:
 * 1. offsetT is defined as uint16_t, which restricts the max edge list length to 65534 (invalid_offset takes
 * 65535). Even if it can be defined as uint32, larger edge list often suffers from slow insertion.
 * 2. Extra linked list is maintained for timestamp, for fast LRU search.
 * 3. Cumulative weight is stored in float, which may not be precise enough when its possible value range is
 * large or the edgelist's length is more than 10000.
 */
struct CPTreapItemStandard {
  using MetaType = typename chrono_graph::CPTreapEdgeListMetaBase<offsetT>;

 private:
  uint64_t id_ = 0;
  offsetT father_offset_ = invalid_offset;

 public:
  offsetT left = invalid_offset;
  offsetT right = invalid_offset;
  float cum_weight = 0;
  // Since we maintain a linked list of nodes sorted by timestamp, each node needs to store the offset of the
  // previous and next node in the linked list.
  offsetT prev_id_ = invalid_offset;
  offsetT next_id_ = invalid_offset;
  uint32 timestamp_s_ = 0;

  static constexpr offsetT invalid_offset = 0xFFFF;

  uint64_t id() const { return id_; }
  void set_id(uint64_t id) { id_ = id; }
  float get_weight() const {
    NOT_REACHED() << "no precise weight in CPTreapItemStandard";
    return 0.0;
  }
  void set_weight(float w) { return; }
  void decay_weight(float decay_ratio) { cum_weight *= decay_ratio; }
  // fallback to replace
  float update_weight_avg(float w) { return w; }
  offsetT father_offset() const { return father_offset_; }
  void set_father_offset(offsetT father_offset) { father_offset_ = father_offset; }
  offsetT prev_offset() const { return prev_id_; }
  void set_prev_offset(offsetT offset) { prev_id_ = offset; }
  offsetT next_offset() const { return next_id_; }
  void set_next_offset(offsetT offset) { next_id_ = offset; }
  offsetT weight_prev_offset() const { return invalid_offset; }
  void set_weight_prev_offset(offsetT offset) { return; }
  offsetT weight_next_offset() const { return invalid_offset; }
  void set_weight_next_offset(offsetT offset) { return; }
  bool is_leaf() const { return !(has_left_son() || has_right_son()); }
  bool has_left_son() const { return left != invalid_offset; }
  bool has_right_son() const { return right != invalid_offset; }
  bool has_one_son() const { return has_left_son() ^ has_right_son(); }
  bool has_father() const { return father_offset() != invalid_offset; }
  bool is_root() const { return !has_father(); }
  uint32 timestamp_s() const { return timestamp_s_; }
  void set_timestamp_s(uint32 timestamp_s) { timestamp_s_ = timestamp_s; }
  static bool is_time_indexed() { return true; }
  static bool is_weight_indexed() { return false; }
  static bool is_precise_weight() { return false; }
  std::string info() const {
    std::ostringstream oss;
    oss << "id = " << id() << ", father_offset = " << father_offset() << ", left = " << left
        << ", right = " << right << ", cumulative weight = " << cum_weight
        << ", timestamp = " << timestamp_s() << ", next_t_ = " << next_id_ << ", prev_t_ = " << prev_id_;
    return oss.str();
  }
};

/*! \brief CPTreapItemPreciseWeightIndexed A single element in edge list.

  There are something on top of CPTreapItemStandard:
  1. Extra linked list is maintained for weight, for fast topN_weight sample. If
  no such sample type is used, remove them can reduce the size of each edge.
  2. For each node, not only cumulative weight, but also precise weight is maintained.
 */
struct CPTreapItemPreciseWeightIndexed {
  using MetaType = typename chrono_graph::CPTreapEdgeListMetaWithWeight<offsetT>;

 private:
  uint64_t id_ = 0;
  offsetT father_offset_ = invalid_offset;

 public:
  offsetT left = invalid_offset;
  offsetT right = invalid_offset;
  float cum_weight = 0;
  float weight = 0;
  // Since we maintain a linked list of nodes sorted by timestamp, each node needs to store the offset of the
  // previous and next node in the linked list.
  offsetT prev_id_ = invalid_offset;
  offsetT next_id_ = invalid_offset;
  // Also maintain a linked list of nodes sorted by weight.
  offsetT weight_prev_id_ = invalid_offset;
  offsetT weight_next_id_ = invalid_offset;
  uint32 timestamp_s_ = 0;

  static constexpr offsetT invalid_offset = 0xFFFF;
  static constexpr uint64_t father_offset_mask = 0xFFFF000000000000;
  static constexpr uint64_t id_mask = 0x0000FFFFFFFFFFFF;

  uint64_t id() const { return id_; }
  void set_id(uint64_t id) { id_ = id; }
  float get_weight() const { return weight; }
  void set_weight(float w) { weight = w; }
  void decay_weight(float decay_ratio) {
    cum_weight *= decay_ratio;
    weight *= decay_ratio;
  }
  // fallback to middle point of update value and current weight
  float update_weight_avg(float w) {
    weight = (weight + w) / 2;
    return weight;
  }

  offsetT father_offset() const { return father_offset_; }
  void set_father_offset(offsetT father_offset) { father_offset_ = father_offset; }
  offsetT prev_offset() const { return prev_id_; }
  void set_prev_offset(offsetT offset) { prev_id_ = offset; }
  offsetT next_offset() const { return next_id_; }
  void set_next_offset(offsetT offset) { next_id_ = offset; }
  offsetT weight_prev_offset() const { return weight_prev_id_; }
  void set_weight_prev_offset(offsetT offset) { weight_prev_id_ = offset; }
  offsetT weight_next_offset() const { return weight_next_id_; }
  void set_weight_next_offset(offsetT offset) { weight_next_id_ = offset; }
  bool is_leaf() const { return !(has_left_son() || has_right_son()); }
  bool has_left_son() const { return left != invalid_offset; }
  bool has_right_son() const { return right != invalid_offset; }
  bool has_one_son() const { return has_left_son() ^ has_right_son(); }
  bool has_father() const { return father_offset() != invalid_offset; }
  bool is_root() const { return !has_father(); }
  uint32 timestamp_s() const { return timestamp_s_; }
  void set_timestamp_s(uint32 timestamp_s) { timestamp_s_ = timestamp_s; }
  static bool is_time_indexed() { return true; }
  static bool is_weight_indexed() { return true; }
  static bool is_precise_weight() { return true; }
  std::string info() const {
    std::ostringstream oss;
    oss << "id = " << id() << ", father_offset = " << father_offset() << ", left = " << left
        << ", right = " << right << ", cumulative weight = " << cum_weight << ", weight = " << weight
        << ", timestamp = " << timestamp_s() << ", next_t_ = " << next_id_ << ", prev_t_ = " << prev_id_;
    return oss.str();
  }
};

// struct to hold edges which are gonna be merged
struct PendingEdge {
  uint64 id;
  float weight;
  uint32 timestamp;
  InsertEdgeWeightAct iewa;
};

/*!
  CPT = Cumulative Probability Treap.
  Binary search tree for id, min-heap for weight.

  Main purpose: support id query and deduplicate, support weighted sampling.
  Suitable for dynamic graph, and requires weighted sampling, expire edges by weight or timestamp.

  Structure:
  Similar to CDF, each item stores id + weight + timestamp, where weight is the cumulative probability rather
  than the original probability. After that, it is followed by edge_attr, whose length is determined by the
  configuration, and is logically bound to the item, but not stored in the item's data structure.

  NOTE:
  A feature: we do not query whether the inserted node exists before inserting. If the edge list is full, we
  always delete an edge first before insertion.

  Insert time complexity: O(logn)
  Sample time complexity: O(logn)

  \ref https://en.wikipedia.org/wiki/Treap
 */
template <typename CPTreapItem = CPTreapItemStandard>
struct CPTreapEdgeList : public EdgeListMeta {
  typename CPTreapItem::MetaType meta;

  bool IsTimeIndexed() const { return CPTreapItem::is_time_indexed(); }

  bool IsWeightIndexed() const { return CPTreapItem::is_weight_indexed(); }

  bool IsPreciseWeight() const { return CPTreapItem::is_precise_weight(); }

  // Remove node from the time indexed linked list.
  void TimeRemoveNode(CPTreapItem *node, offsetT offset);

  // Add node to the time indexed linked list.
  void TimeAddNode(CPTreapItem *node, uint32_t timestamp_s, offsetT node_offset, offsetT insert_offset);

  // Find offset of the node with given timestamp.
  void FindTsOffset(uint32 timestamp_s, offsetT cur_node_offset, offsetT *cursor);

  void WeightRemoveNode(CPTreapItem *node, offsetT offset);

  void WeightAddNode(CPTreapItem *node, float weight, offsetT node_offset);

  // Find offset of the node with given weight.
  void FindWeightOffset(float weight, offsetT cur_node_offset, offsetT *cursor);

  /*! \brief count node number in the time range
      \param both start_ts and end_ts are inclusive
  */
  int TimeRangeCountNode(uint32 start_ts, uint32 end_ts, offsetT *cursor);

  static int MemorySize(BlockStorageApi *attr_op,
                        BlockStorageApi *edge_attr_op,
                        int capacity,
                        bool buffered = false) {
    CHECK(attr_op && edge_attr_op) << "attr_op and edge_attr_op must be given!";
    int res = sizeof(CPTreapEdgeList) + attr_op->MaxSize() +
              capacity * (sizeof(CPTreapItem) + edge_attr_op->MaxSize());
    if (buffered) {
      CHECK_GT(absl::GetFlag(FLAGS_cpt_edge_buffer_capacity), 0);
      res += absl::GetFlag(FLAGS_cpt_edge_buffer_capacity) * (sizeof(PendingEdge) + edge_attr_op->MaxSize());
    }
    return res;
  }

  bool BatchMergeEnabled() const { return meta.buffer_size != (offsetT)-1; }

  // return allocated offset, with initialize
  offsetT AllocateNode() {
    auto offset = meta.size;
    meta.size++;
    auto node = items(offset);
    FormatLeafNode(node);
    return offset;
  }

  // format block space as empty leaf node to add into tree.
  void FormatLeafNode(CPTreapItem *item) {
    item->set_id(0);
    item->left = item->right = CPTreapItem::invalid_offset;
    item->set_father_offset(CPTreapItem::invalid_offset);
    item->cum_weight = 0;
    item->set_weight(0);
    if (IsTimeIndexed()) {
      item->set_prev_offset(CPTreapItem::invalid_offset);
      item->set_next_offset(CPTreapItem::invalid_offset);
    }
    if (IsWeightIndexed()) {
      item->set_weight_prev_offset(CPTreapItem::invalid_offset);
      item->set_weight_next_offset(CPTreapItem::invalid_offset);
    }
    item->set_timestamp_s(0);
    memset(attr_of_item(item), 0, edge_attr_size);
  }

  void Initialize(BlockStorageApi *attr_op,
                  BlockStorageApi *edge_attr_op,
                  uint32_t cap,
                  bool buffered = false) {
    CHECK_GT(cap, 0) << "LoopList init fail, cap less than 1, cap = " << cap;
    EdgeListMeta::Initialize(attr_op, edge_attr_op, cap);
    meta.size = 0;
    meta.root = CPTreapItem::invalid_offset;
    // -1 indicates no buffer
    meta.buffer_size = -1;
    attr_op->InitialBlock(attr_block(), attr_op->MaxSize());
    memset(reinterpret_cast<void *>(items()), 0, capacity * item_size());
    if (buffered) {
      CHECK_GT(absl::GetFlag(FLAGS_cpt_edge_buffer_capacity), 0);
      // actually, memset is not really required
      memset(reinterpret_cast<void *>(edges()),
             0,
             absl::GetFlag(FLAGS_cpt_edge_buffer_capacity) * pending_edge_size());
      meta.buffer_size = 0;
    }
  }

  bool Add(const std::vector<uint64> &ids,
           const std::vector<float> &weights,
           EdgeListReplaceStrategy replace_type,
           const std::vector<uint32_t> &timestamp_s = {},
           InsertEdgeWeightAct iewa = ADD,
           const std::vector<std::string> &id_attrs = {},
           BlockStorageApi *edge_attr_op = nullptr);

  /*! \brief Add a node recursively */
  bool Add(const uint64 id,
           const float weight,
           EdgeListReplaceStrategy replace_type,
           const uint32 timestamp_s = 0,
           InsertEdgeWeightAct iewa = ADD,
           const std::string &attr = "",
           BlockStorageApi *edge_attr_op = nullptr);

  /*! \brief Add node in pseudo recursive mode
   */
  void PseudoRecursiveAdd(uint64_t id,
                          float weight,
                          uint32 timestamp_s,
                          InsertEdgeWeightAct iewa,
                          const std::string &attr,
                          BlockStorageApi *edge_attr_op);

  /*!
    \brief Root node should has smallest weight.
  */
  void PopHeap() {
    if (meta.size > 0) { DeleteById(items(meta.root)->id()); }
  }

  void PopRandom() {
    if (meta.size == 0) { return; }
    thread_local base::PseudoRandom pr(base::RandomSeed());
    offsetT offset = pr.GetInt(0, meta.size - 1);
    DeleteById(items(offset)->id());
  }

  void PopOldest() {
    CHECK(IsTimeIndexed());
    if (meta.size == 0) { return; }
    offsetT old_time_head = meta.time_head;
    if (meta.time_head >= meta.size) {
      // Should never happens.
      LOG_EVERY_N_SEC(ERROR, 5) << "Invalid time_head: " << meta.time_head << ", size: " << meta.size
                                << ". data has been corrupted. Clear data.";
      meta.size = 0;
      meta.root = CPTreapItem::invalid_offset;
    }
    DeleteById(items(old_time_head)->id());
  }

  /*!
    \brief Find the node with the given id, and delete it.
    Then do `Rotate` to keep the heap property.
  */
  void DeleteById(uint64_t id);

  /*!
    \brief Rotate to satisfy heap property.
  */
  bool Rotate(CPTreapItem *item, offsetT item_offset);

  bool RotateLeft(CPTreapItem *item, offsetT item_offset);

  bool RotateRight(CPTreapItem *item, offsetT item_offset);

  /*!
    Swap the last node with the deleted node, so all remaining nodes are consecutive in memory.
    This function does not handle the weight change caused by the recycling of the node,
    assuming that the caller has done that.
    This function assumes that the node has been detached from the tree, and a single detached node is assumed
    to be handled here.
  */
  void RecycleNode(offsetT offset);

  bool CanTake(int appending_size) const { return (capacity - meta.size) >= appending_size; }

  /*!
    \brief Batch get ids. Order not guaranteed in result.
    \param ids result container.
  */
  void Get(std::vector<uint64> *ids) const {
    ids->clear();
    for (uint32 i = 0; i < meta.size; ++i) { ids->emplace_back(items(i)->id()); }
  }

  /*!
    \brief Batch get ids and weights. Order not guaranteed in result.
    \param ids result container.
    \param weights result container.
  */
  void Get(std::vector<uint64> *ids, std::vector<float> *weights) const {
    Get(ids);
    weights->clear();
    for (uint32 i = 0; i < meta.size; ++i) { weights->emplace_back(weight(items(i))); }
  }

  /*!
    \brief Query by id, O(logn)
    \param result Query result container.
    \return invalid_offset if node not found.
  */
  offsetT GetById(uint64_t id, CPTreapItem *result) const;

  /*!
    \brief Decay all weights and delete edges whose weight is less than delete_threshold_weight.
    \return Number of deleted edges.
  */
  int DecayAllWeights(float decay_ratio, float delete_threshold_weight) {
    for (uint32 i = 0; i < meta.size; ++i) { items(i)->decay_weight(decay_ratio); }
    int delete_count = 0;
    for (uint32 i = 0; i < meta.size; ++i) {
      float new_node_weight = this->weight(items(i));
      if (new_node_weight < delete_threshold_weight) {
        DeleteById(items(i)->id());
        --i;
        ++delete_count;
      }
    }
    return delete_count;
  }

  /*!
    \brief Check all timestamps, delete expired edges.
    \param expire_interval Expire time in seconds.
    \return Number of deleted edges.
  */
  int CheckAllTimestamps(int expire_interval) {
    base::Timer timer;
    auto before_size = meta.size;
    base::ScopeExit t([&] {
      LOG_EVERY_N_SEC(INFO, 2) << "cpt check all timestamps time: " << timer.display()
                               << ", before_size = " << before_size << ", after_size = " << meta.size;
    });
    int delete_count = 0;
    int cur_ts = base::GetTimestamp() / 1000 / 1000;
    for (uint32 i = 0; i < meta.size; ++i) {
      // 0 is never expire
      if (items(i)->timestamp_s() > 0 && expire_interval + items(i)->timestamp_s() < cur_ts) {
        DeleteById(items(i)->id());
        --i;
        ++delete_count;
      }
    }
    timer.AppendCostMs("check all");
    return delete_count;
  }

  /*!
    \brief log(N) time complexity.
    \return sample result id.
  */
  uint64 SampleOnce() const {
    if (meta.size == 0) { return 0; }
    auto node = SampleEdge();
    return node->id();
  }

  const CPTreapItem *SampleEdge() const;

  /*!
    \brief Decrase a node's weight to 0, so it will not be sampled in weighted sample.
    Note:
    1. This function will destroy the tree structure.
    2. Because of floating point precision, the node's weight may not be exactly 0.
    Any value smaller than 1e-6 is regarded as 0.
  */
  void BuryNode(offsetT offset) {
    if (offset >= meta.size) { return; }
    auto node = items(offset);
    auto w = weight(node);
    if (w < 1e-6) { return; }
    node->cum_weight -= w;
    node->set_weight(0);
    while (node->has_father()) {
      node = items(node->father_offset());
      node->cum_weight -= w;
    }
  }

  void GetAllByTime(ProtoPtrList<EdgeInfo> *edge_info) const {
    offsetT cur = meta.time_head;
    for (size_t i = 0; i < meta.size; ++i) {
      CHECK(cur < meta.size)
          << "Get invalid offset " << cur
          << ". This is likely due to your specified type of Adaptor doesn't support Get by time order.";
      const CPTreapItem *cur_node = items(cur);
      std::string attr(attr_of_item(cur_node), edge_attr_size);
      auto edge = edge_info->Add();
      edge->set_id(cur_node->id());
      edge->set_weight(this->weight(cur_node));
      edge->set_timestamp(cur_node->timestamp_s());
      *(edge->mutable_edge_attr()) = std::move(attr);
      cur = cur_node->next_offset();
    }
  }

  bool Full() { return meta.size == capacity; }

  // Check whether the tree is valid. It will travese all nodes and check father and sons.
  bool IsValid() const;

  bool IsTimeChainValid() const {
    if (!IsTimeIndexed()) { return true; }
    offsetT cursor = meta.time_tail;
    offsetT checked_size = 0;
    auto x = CPTreapItem::invalid_offset;  // just for simplify code length.
    while (cursor != x && checked_size < meta.size) {
      if (items(cursor)->prev_id_ != x &&
          (items(items(cursor)->prev_id_)->next_id_ != cursor ||
           items(cursor)->timestamp_s() < items(items(cursor)->prev_id_)->timestamp_s())) {
        return false;
      }
      checked_size++;
      if (checked_size == meta.size && cursor != meta.time_head) { return false; }
      cursor = items(cursor)->prev_id_;
    }
    if (cursor != x || checked_size != meta.size) { return false; }
    return true;
  }

  bool IsWeightChainValid() const {
    if (!IsWeightIndexed()) { return true; }
    offsetT cursor = meta.weight_tail();
    offsetT checked_size = 0;
    auto x = CPTreapItem::invalid_offset;  // just for simplify code length.
    while (cursor != x && checked_size < meta.size) {
      auto prev_offset = items(cursor)->weight_prev_offset();
      if (prev_offset != x && (items(prev_offset)->weight_next_offset() != cursor ||
                               weight(items(cursor)) < weight(items(prev_offset)))) {
        return false;
      }
      checked_size++;
      if (checked_size == meta.size && cursor != meta.weight_head()) { return false; }
      cursor = prev_offset;
    }
    if (cursor != x || checked_size != meta.size) { return false; }
    return true;
  }

  std::string ToString() const {
    std::ostringstream oss;
    oss << EdgeListMeta::ToString() << ", size = " << meta.size << std::endl;
    LayerOrderTraverse(&oss);
    return oss.str();
  }

  void InOrderTraverseOP(offsetT offset, const std::function<void(offsetT)> &cb) const {
    auto node = items(offset);
    if (node->has_left_son()) { InOrderTraverseOP(node->left, cb); }
    cb(offset);
    if (node->has_right_son()) { InOrderTraverseOP(node->right, cb); }
  }

  void LayerOrderTraverse(std::ostringstream *oss) const {
    if (meta.size == 0) { return; }
    thread_local std::queue<std::tuple<offsetT, int>> q;
    CHECK(q.empty());
    q.emplace(meta.root, 0);
    while (!q.empty()) {
      auto elem = q.front();
      q.pop();
      auto offset = std::get<0>(elem);
      auto layer = std::get<1>(elem);
      auto node = items(offset);
      (*oss) << "[layer = " << layer << ", offset = " << offset << ", " << node->info()
             << ", weight = " << weight(node) << "]" << std::endl;
      if (node->has_left_son()) { q.emplace(node->left, layer + 1); }
      if (node->has_right_son()) { q.emplace(node->right, layer + 1); }
    }
  }

  void LayerOrderBury(float min_weight_required) {
    if (meta.size == 0) { return; }
    thread_local std::queue<offsetT> q;
    CHECK(q.empty());

    q.emplace(meta.root);
    while (!q.empty()) {
      auto offset = q.front();
      q.pop();
      auto node = items(offset);
      if (weight(node) < min_weight_required) {
        BuryNode(offset);
        if (node->has_left_son()) { q.emplace(node->left); }
        if (node->has_right_son()) { q.emplace(node->right); }
      }
    }
  }

  float weight(const CPTreapItem *item) const {
    if (__glibc_unlikely(IsPreciseWeight())) {  // NOLINT(readability/naming)
      return item->get_weight();
    }
    float cum_weight = item->cum_weight;
    if (item->has_left_son()) { cum_weight -= items(item->left)->cum_weight; }
    if (item->has_right_son()) { cum_weight -= items(item->right)->cum_weight; }
    return std::max(cum_weight, 0.0f);
  }

  uint32 item_size() const { return sizeof(CPTreapItem) + edge_attr_size; }
  uint32 pending_edge_size() const { return sizeof(PendingEdge) + edge_attr_size; }

  offsetT offset(const CPTreapItem *item) const {
    return (reinterpret_cast<const char *>(item) - reinterpret_cast<const char *>(items())) / item_size();
  }

  char *attr_block() { return reinterpret_cast<char *>(this + 1); }
  const char *attr_block() const { return reinterpret_cast<const char *>(this + 1); }

  char *IthEdgeAttrBlock(uint32 i) { return reinterpret_cast<char *>(items(i)) + sizeof(CPTreapItem); }
  const char *IthEdgeAttrBlock(uint32 i) const {
    return reinterpret_cast<const char *>(items(i)) + sizeof(CPTreapItem);
  }
  char *attr_of_item(CPTreapItem *item) { return reinterpret_cast<char *>(item) + sizeof(CPTreapItem); }
  const char *attr_of_item(const CPTreapItem *item) const {
    return reinterpret_cast<const char *>(item) + sizeof(CPTreapItem);
  }
  char *attr_of_pending_edge(PendingEdge *item) {
    return reinterpret_cast<char *>(item) + sizeof(PendingEdge);
  }
  const char *attr_of_pending_edge(const PendingEdge *item) const {
    return reinterpret_cast<const char *>(item) + sizeof(PendingEdge);
  }

  const CPTreapItem *items(offsetT offset = 0) const {
    return reinterpret_cast<const CPTreapItem *>(attr_block() + attr_size + offset * item_size());
  }
  CPTreapItem *items(offsetT offset = 0) {
    return reinterpret_cast<CPTreapItem *>(attr_block() + attr_size + offset * item_size());
  }

  const PendingEdge *edges() const {
    return reinterpret_cast<const PendingEdge *>(reinterpret_cast<const char *>(items()) +
                                                 capacity * item_size());
  }
  PendingEdge *edges() {
    return reinterpret_cast<PendingEdge *>(reinterpret_cast<char *>(items()) + capacity * item_size());
  }
  const PendingEdge *edges(offsetT offset) const {
    return reinterpret_cast<const PendingEdge *>(reinterpret_cast<const char *>(edges()) +
                                                 offset * pending_edge_size());
  }
  PendingEdge *edges(offsetT offset) {
    return reinterpret_cast<PendingEdge *>(reinterpret_cast<char *>(edges()) + offset * pending_edge_size());
  }

  // We have a linked list sorted by timestamp, and ### all nodes are in consecutive space ###.
  // Finding a value on a sorted linked list usually is O(n), but in this case can be optimized to O(sqrt(n))
  // We will use this optimization when list is long enough.
  static constexpr uint32 OPTIMIZE_SIZE_THRESHOLD = 50;
};

#pragma pack(pop)

/*!
  \brief CPTreapAdaptor contains interfaces to manipulate edge_list.
*/
template <typename CPTreapItem = CPTreapItemStandard>
class CPTreapAdaptor final : public EdgeListInterface {
 public:
  typedef CPTreapEdgeList<CPTreapItem> EdgeListType;

  CPTreapAdaptor(BlockStorageApi *attr_op, BlockStorageApi *edge_attr_op, const EdgeListConfig &config)
      : EdgeListInterface(attr_op, edge_attr_op, config) {
    CHECK_LT(config.edge_capacity_max_num, (offsetT)-1)
        << "Current cpt support edge capacity less than: " << (offsetT)-1
        << ", given: " << config.edge_capacity_max_num;
  }

  virtual ~CPTreapAdaptor() {}

  size_t GetMemorySize(int capacity = -1) const override {
    if (capacity == -1) { capacity = absl::GetFlag(FLAGS_cpt_dynamic_growth_base); }
    // the last uint64 is checksum
    return EdgeListType::MemorySize(this->attr_op_, this->edge_attr_op_, capacity, false) + sizeof(uint64);
  }

  // return buffered cpt edge list memory size which is a const
  size_t GetBufferedEdgeListMemorySize() const {
    return EdgeListType::MemorySize(this->attr_op_, this->edge_attr_op_, config_.edge_capacity_max_num, true);
  }

  // return cpt core memory size, no checksum or pending edges
  size_t GetCoreMemorySize(int capacity) const {
    if (capacity == -1) { capacity = absl::GetFlag(FLAGS_cpt_dynamic_growth_base); }
    return EdgeListType::MemorySize(this->attr_op_, this->edge_attr_op_, capacity, false);
  }

  base::KVData *InitBlock(MemAllocator *mem_allocator,
                          uint32_t *new_addr,
                          int add_size = 0,
                          char *old_edge_list = nullptr) const override;

  void Fill(char *edge_list, char *old_edge_list, const CommonUpdateItems &items) override;

  void FillRaw(char *edge_list, int slab_size, const CommonUpdateItems &items) override {
    auto edge_list_ptr = reinterpret_cast<EdgeListType *>(edge_list);
    edge_list_ptr->Initialize(this->attr_op_, this->edge_attr_op_, slab_size, false);
    if (!items.attr_update_info.empty()) {
      this->attr_op_->UpdateBlock(
          edge_list_ptr->attr_block(), items.attr_update_info.c_str(), items.attr_update_info.size());
    }
    edge_list_ptr->Add(items.ids,
                       items.id_weights,
                       config_.oversize_replace_strategy,
                       items.id_ts,
                       items.iewa,
                       items.id_attr_update_infos,
                       edge_attr_op_);
    SetCheckSumIfNeed(edge_list);
  }

  uint64 CalcCheckSum(const char *edge_list) const {
    auto edge_list_ptr = reinterpret_cast<const EdgeListType *>(edge_list);
    // no checksum in buffered edge list
    CHECK(!edge_list_ptr->BatchMergeEnabled());
    return base::CityHash64(edge_list, GetCoreMemorySize(edge_list_ptr->meta.size));
  }

  uint64 GetCheckSum(const char *edge_list) const {
    auto edge_list_ptr = reinterpret_cast<const EdgeListType *>(edge_list);
    // no checksum in buffered edge list
    CHECK(!edge_list_ptr->BatchMergeEnabled());
    auto checksum_ptr =
        reinterpret_cast<const uint64 *>(edge_list + GetCoreMemorySize(edge_list_ptr->capacity));
    return *checksum_ptr;
  }

  void SetCheckSumIfNeed(char *edge_list) const {
    auto edge_list_ptr = reinterpret_cast<EdgeListType *>(edge_list);
    // no checksum in buffered edge list
    if (edge_list_ptr->BatchMergeEnabled()) { return; }
    // checksum is the last 4bytes
    auto checksum = reinterpret_cast<uint64 *>(edge_list + GetCoreMemorySize(edge_list_ptr->capacity));
    // checksum is calculated from really used mem
    *checksum = CalcCheckSum(edge_list);
  }

  bool CanReuse(const char *edge_list, int appending_size) const override {
    auto edge_list_ptr = reinterpret_cast<const EdgeListType *>(edge_list);
    if (!edge_list_ptr->BatchMergeEnabled()) {
      if (appending_size < 0) { return true; }
      if (edge_list_ptr->CanTake(appending_size)) { return true; }
      // grow up
      if (edge_list_ptr->capacity < config_.edge_capacity_max_num) { return false; }
      // FLAGS_cpt_edge_buffer_capacity <= 0 means batch merge is disabled
      if (absl::GetFlag(FLAGS_cpt_edge_buffer_capacity) <= 0) { return true; }
      // batch merge will be turned on
      return false;
    }

    if (appending_size < 0) {
      // deletion in buffered edge list need replace update
      return false;
    }
    // buffer_size >= 0 means batch merge is already turned on
    // batch merge is triggered, switch to replace update
    if (edge_list_ptr->meta.buffer_size + appending_size > absl::GetFlag(FLAGS_cpt_edge_buffer_capacity)) {
      return false;
    }
    // flush new edges to buffer
    return true;
  }

  void Clean(char *edge_list) const override {
    auto edge_list_ptr = reinterpret_cast<EdgeListType *>(edge_list);
    edge_list_ptr->Initialize(
        this->attr_op_, this->edge_attr_op_, edge_list_ptr->capacity, edge_list_ptr->BatchMergeEnabled());
    SetCheckSumIfNeed(edge_list);
  };

  bool GetEdgeListInfo(const char *edge_list,
                       NodeAttr *node_attr,
                       ProtoPtrList<EdgeInfo> *edge_info) const override;

  bool Sample(const char *edge_list, const SamplingParam &param, EdgeListSlice *slice) const override;

  void ListExpandSchema(std::vector<uint32_t> *schema) const override {
    CHECK_LE(absl::GetFlag(FLAGS_cpt_dynamic_growth_base), this->config_.edge_capacity_max_num);
    uint32_t len = absl::GetFlag(FLAGS_cpt_dynamic_growth_base);
    while (len <= config_.edge_capacity_max_num) {
      schema->push_back(GetMemorySize(len));
      auto old_len = len;
      len = std::min((uint32_t)config_.edge_capacity_max_num,
                     (uint32_t)(len * absl::GetFlag(FLAGS_cpt_dynamic_growth_factor)));
      // Make sure `len` keeps growing in each loop to avoid infinite loop.
      len += (old_len == len);
    }
    if (absl::GetFlag(FLAGS_cpt_edge_buffer_capacity) > 0) {
      schema->push_back(GetBufferedEdgeListMemorySize());
    }
    CHECK_GT(schema->size(), 0) << "list expand schema get empty, check your capacity config.";
  }

  uint32 GetSize(const char *edge_list) const override {
    return reinterpret_cast<const EdgeListType *>(edge_list)->meta.size;
  }

  uint32 GetDegree(const char *edge_list) const override {
    return reinterpret_cast<const EdgeListType *>(edge_list)->out_degree;
  }

  virtual bool GetEdges(const char *edge_list,
                        const std::vector<uint64> &keys,
                        NodeAttr *node_attr,
                        ProtoPtrList<EdgeInfo> *edge_info) const {
    auto edge_list_ptr = reinterpret_cast<const EdgeListType *>(edge_list);
    if (node_attr) {
      node_attr->set_degree(edge_list_ptr->out_degree);
      *(node_attr->mutable_attr_info()) = std::move(this->AttrInfo(edge_list));
    }
    if (edge_info) {
      for (auto key : keys) {
        auto edge = edge_info->Add();
        offsetT i = edge_list_ptr->GetById(key, nullptr);
        if (i == CPTreapItem::invalid_offset) {  // query missed
          edge->set_id(0);
          edge->set_weight(0);
          edge->set_timestamp(0);
          *(edge->mutable_edge_attr()) = std::move("");
          continue;
        }
        edge->set_id(edge_list_ptr->items(i)->id());
        edge->set_weight(edge_list_ptr->weight(edge_list_ptr->items(i)));
        edge->set_timestamp(edge_list_ptr->items(i)->timestamp_s());
        std::string edge_attr(edge_list_ptr->IthEdgeAttrBlock(i), edge_list_ptr->edge_attr_size);
        *(edge->mutable_edge_attr()) = std::move(edge_attr);
      }
    }

    return true;
  }

  bool DeleteItems(char *edge_list, char *old_edge_list, const std::vector<uint64> &keys) override {
    auto edge_list_ptr = reinterpret_cast<EdgeListType *>(edge_list);
    if (old_edge_list != edge_list) {
      InitFromAnotherBufferedEdgeList(edge_list, old_edge_list);
      SetExpandMark(old_edge_list);
    }
    for (auto key : keys) { edge_list_ptr->DeleteById(key); }
    SetCheckSumIfNeed(edge_list);
    return true;
  };

  int TimeDecay(char *edge_list,
                char *old_edge_list,
                float degree_decay_ratio,
                float weight_decay_ratio,
                float delete_threshold_weight) const override {
    auto edge_list_ptr = reinterpret_cast<EdgeListType *>(edge_list);
    if (old_edge_list != edge_list && old_edge_list != nullptr) {
      InitFromAnotherBufferedEdgeList(edge_list, old_edge_list);
      SetExpandMark(old_edge_list);
    }
    edge_list_ptr->out_degree = (uint64_t)(floor(edge_list_ptr->out_degree * degree_decay_ratio));
    int res = edge_list_ptr->DecayAllWeights(weight_decay_ratio, delete_threshold_weight);
    SetCheckSumIfNeed(edge_list);
    return res;
  }

  int EdgeExpire(char *edge_list, char *old_edge_list, int expire_interval) const override {
    auto edge_list_ptr = reinterpret_cast<EdgeListType *>(edge_list);
    if (old_edge_list != edge_list && old_edge_list != nullptr) {
      InitFromAnotherBufferedEdgeList(edge_list, old_edge_list);
      SetExpandMark(old_edge_list);
    }
    int res = edge_list_ptr->CheckAllTimestamps(expire_interval);
    SetCheckSumIfNeed(edge_list);
    return res;
  }

  std::string AttrInfo(const char *edge_list) const override {
    auto edge_list_ptr = reinterpret_cast<const EdgeListType *>(edge_list);
    return this->attr_op_->SerializeBlock(edge_list_ptr->attr_block());
  }

  void SetExpandMark(char *edge_list) const override {
    auto edge_list_ptr = reinterpret_cast<EdgeListType *>(edge_list);
    edge_list_ptr->expand_mark = true;
    SetCheckSumIfNeed(edge_list);
  }

  bool GetExpandMark(const char *edge_list) const override {
    auto edge_list_ptr = reinterpret_cast<const EdgeListType *>(edge_list);
    return edge_list_ptr->expand_mark;
  }

 private:
  void InitFromAnotherBufferedEdgeList(char *edge_list, const char *old_edge_list) const {
    CHECK(edge_list != nullptr);
    CHECK(old_edge_list != nullptr);
    CHECK_NE(edge_list, old_edge_list);
    auto edge_list_ptr = reinterpret_cast<EdgeListType *>(edge_list);
    auto old_edge_list_ptr = reinterpret_cast<const EdgeListType *>(old_edge_list);
    CHECK(edge_list_ptr->BatchMergeEnabled());
    CHECK(old_edge_list_ptr->BatchMergeEnabled());
    memcpy(edge_list_ptr, old_edge_list, GetCoreMemorySize(old_edge_list_ptr->meta.size));
    // reset buffer_size since all pending edges will be merged now
    edge_list_ptr->meta.buffer_size = 0;
    // merge buffered edges first
    for (size_t i = 0; i < old_edge_list_ptr->meta.buffer_size; ++i) {
      const PendingEdge *edge = old_edge_list_ptr->edges(i);
      std::string edge_attr(edge_list_ptr->attr_of_pending_edge(edge), edge_attr_op_->MaxSize());
      edge_list_ptr->Add(edge->id,
                         edge->weight,
                         config_.oversize_replace_strategy,
                         edge->timestamp,
                         edge->iewa,
                         edge_attr,
                         edge_attr_op_);
    }
  }
};
}  // namespace chrono_graph

#include "cpt_edge_list-inl.h"
