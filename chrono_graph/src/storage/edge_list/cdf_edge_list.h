// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <algorithm>
#include <limits>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include "base/common/basic_types.h"
#include "base/common/gflags.h"
#include "base/hash/hash.h"
#include "base/random/pseudo_random.h"
#include "base/util/scope_exit.h"
#include "chrono_graph/src/base/gnn_type.h"
#include "chrono_graph/src/base/proto_helper.h"
#include "chrono_graph/src/base/util.h"
#include "chrono_graph/src/storage/edge_list/el_interface.h"

ABSL_DECLARE_FLAG(int32, cdf_dynamic_growth_base);
ABSL_DECLARE_FLAG(double, cdf_dynamic_growth_factor);
ABSL_DECLARE_FLAG(double, cdf_reformat_threshold);
ABSL_DECLARE_FLAG(std::string, cdf_insert_stat_key_ranges);
ABSL_DECLARE_FLAG(bool, cdf_allow_weighted_sample);

namespace chrono_graph {

class MemAllocator;

#pragma pack(push, 1)

// An element in CDF edge list
struct CDFEdgeListItem {
  uint64 id = 0;
  float weight = 0;
  // Timestamp in seconds, 0 is none.
  uint32 timestamp_s_ = 0;
};

/** \brief Use cumulative weight as weight meta to store sampling structure.
 * Suitable for static graph or periodically updated graph, with strict space requirement.
 *
 * Supported operations:
 * 1. Append write
 * 2. Time-based expiration
 * 3. Fast sampling operation.
 *
 * Pros: Fast sampling, suitable for static graph, with relatively small space occupation.
 * Cons: Does not support weight-based expiration, does not support deletion, does not support id-based
 * retrieval.
 *
 * Structure:
 * Each item stores id + weight + timestamp, where weight is the cumulative probability rather than the
 * original probability. After that, it is followed by edge_attr, whose length is determined by the
 * configuration, and is logically bound to the item, but not stored in the item's data structure.
 *
 * Insert: O(1)
 * Sample: O(logn)
 *
 * Modify with lock, read is lock-free. Need to make sure any intermediate status is safe for read.
 * That's because the node does not store any data structure related information (such as
 * parent-child nodes in tree).
 */
struct CDFEdgeList : public EdgeListMeta {
  uint32 start;
  uint32 size;
  // When expire happens, we don't have to update all cumulative weights.
  // Just accumulate a weight_bar, used as sampling lower bound.
  float weight_bar;

  static int MemorySize(BlockStorageApi *attr_op, BlockStorageApi *edge_attr_op, int capacity) {
    CHECK(attr_op && edge_attr_op) << "attr_op and edge_attr_op must be given!";
    return sizeof(CDFEdgeList) + attr_op->MaxSize() +
           capacity * (sizeof(CDFEdgeListItem) + edge_attr_op->MaxSize());
  }

  void Initialize(BlockStorageApi *attr_op, BlockStorageApi *edge_attr_op, int cap) {
    CHECK_GT(cap, 0) << "LoopList init fail, cap less than 1, cap = " << cap;
    EdgeListMeta::Initialize(attr_op, edge_attr_op, cap);
    size = 0;
    start = cap - 1;
    weight_bar = 0;
    attr_op->InitialBlock(attr_block(), attr_op->MaxSize());
    memset(reinterpret_cast<void *>(attr_block() + attr_size), 0, ItemsSize());
  }

  bool Add(const std::vector<uint64> &ids,
           const std::vector<float> &weights,
           EdgeListReplaceStrategy replace_type,
           const std::vector<uint32_t> &timestamp_s = {},
           const std::vector<std::string> &id_attrs = {},
           BlockStorageApi *edge_attr_op = nullptr);

  bool Add(const uint64 id,
           float weight,
           const uint32 timestamp_s,
           std::string id_attr,
           BlockStorageApi *edge_attr_op);

  // Assume: items after `offset` in the list are sorted by timestamp in descending order.
  // Effect: BubbleSort the item at `offset` to the right position.
  // Purpose: To keep the list sorted after each `Add`.
  void Sort(int offset);

  bool CanTake(int appending_size) const { return (capacity - size) >= appending_size; }

  // Clear weight_bar to avoid float overflow.
  void ReformatCDF() {
    if (weight_bar == 0) { return; }
    for (size_t i = 0; i < size; ++i) { Get(i)->weight -= weight_bar; }
    weight_bar = 0;
  }

  // offset = 0 is the latest item
  CDFEdgeListItem *Get(uint32 offset) {
    auto real_offset = (start + capacity - offset) % capacity;
    return reinterpret_cast<CDFEdgeListItem *>(attr_block() + attr_size +
                                               real_offset * (sizeof(CDFEdgeListItem) + edge_attr_size));
  }

  const CDFEdgeListItem *Get(uint32 offset) const {
    auto real_offset = (start + capacity - offset) % capacity;
    return reinterpret_cast<const CDFEdgeListItem *>(
        attr_block() + attr_size + real_offset * (sizeof(CDFEdgeListItem) + edge_attr_size));
  }

  float IthWeight(uint32 offset) const {
    if (!absl::GetFlag(FLAGS_cdf_allow_weighted_sample)) { return Get(offset)->weight; }
    return Get(offset)->weight - (((offset + 1) == capacity) ? weight_bar : Get(offset + 1)->weight);
  }

  /*!
    \brief Batch item read interface, in reverse order, that is, the offset 0 is the latest written item.
    \param ids result container.
  */
  void Get(std::vector<uint64> *ids) const {
    ids->clear();
    for (uint32 i = 0; i < size; ++i) { ids->emplace_back(Get(i)->id); }
  }

  /*!
    \brief Batch item read interface, in reverse order, that is, the offset 0 is the latest written item.
    \param ids result container.
    \param weights result container.
  */
  void Get(std::vector<uint64> *ids, std::vector<float> *weights) const {
    Get(ids);
    weights->clear();
    for (uint32 i = 0; i < size; ++i) { weights->emplace_back(IthWeight(i)); }
  }

  /*!
    \brief Time compexity: log(N)
    \return sample result id.
  */
  uint64 SampleOnce() const {
    uint32 offset = SampleEdge(0u, size);
    return Get(offset)->id;
  }

  /*!
    \brief Time compexity: log(N)
    \param min_offset Sample from offset no smaller than it (0 means sample from all)
    \param max_offset Sample from offset smaller than it (`size` means sample from all)
    \return offset of sampled edge.
  */
  uint32 SampleEdge(uint32 min_offset, uint32 max_offset) const {
    thread_local base::PseudoRandom pr(base::RandomSeed());
    float base = weight_bar;
    if (max_offset < size) base = Get(max_offset)->weight;
    float random = base + pr.GetDouble() * (Get(min_offset)->weight - base);
    int64 begin = 0, end = size - 1;
    while (begin <= end) {
      auto mid = (begin + end) / 2;
      if (Get(mid)->weight > random) {
        begin = mid + 1;
      } else {
        end = mid - 1;
      }
    }
    // Since random < Get(0)->weight
    CHECK_GE(end, 0);
    return (uint32)end;
  }

  /*!
    \brief Make all nodes' weight to decay_ratio * old_weight.
    \return Number of nodes deleted (CDF not support deletion, always return 0)
  */
  int DecayAllWeights(float decay_ratio) {
    for (uint32 i = 0; i < size; ++i) { Get(i)->weight = Get(i)->weight * decay_ratio; }
    weight_bar *= decay_ratio;
    return 0;
  }

  std::string ToString() const {
    std::ostringstream oss;
    oss << EdgeListMeta::ToString();
    oss << ", start = " << start;
    oss << ", size = " << size;
    oss << ", capacity = " << capacity;
    oss << ", weight_bar = " << weight_bar;
    for (size_t i = 0; i < size; ++i) {
      oss << ", v[" << i << "] = (" << Get(i)->id << ", " << IthWeight(i) << ")";
    }
    return oss.str();
  }

  char *attr_block() { return reinterpret_cast<char *>(this + 1); }
  const char *attr_block() const { return reinterpret_cast<const char *>(this + 1); }
  char *IthEdgeAttrBlock(uint32 i) { return reinterpret_cast<char *>(Get(i)) + sizeof(CDFEdgeListItem); }
  const char *IthEdgeAttrBlock(uint32 i) const {
    return reinterpret_cast<const char *>(Get(i)) + sizeof(CDFEdgeListItem);
  }

  size_t ItemsSize() { return capacity * (sizeof(CDFEdgeListItem) + edge_attr_size); }
  /*!
    \brief Find a pivot by timestamp.
    \param timestamp_s timestamp in second.
    \return offset of the pivot, which satisfies:
    When an item's offset in [ret, size), its timestamp <= `timstamp_s`
    When an item's offset in [0, ret), its timestamp > `timstamp_s`
  */
  uint32 FindPivotByTs(uint32 timestamp_s) const;
};
#pragma pack(pop)

/*!
  \brief CDFAdaptor contains interfaces to manipulate edge_list.
*/
class CDFAdaptor final : public EdgeListInterface {
 public:
  CDFAdaptor(BlockStorageApi *attr_op, BlockStorageApi *edge_attr_op, const EdgeListConfig &config)
      : EdgeListInterface(attr_op, edge_attr_op, config)
      , insert_range_matcher_(absl::GetFlag(FLAGS_cdf_insert_stat_key_ranges), ",") {}
  virtual ~CDFAdaptor() {}

  size_t GetMemorySize(int capacity = -1) const override {
    if (capacity == -1) { capacity = absl::GetFlag(FLAGS_cdf_dynamic_growth_base); }
    return CDFEdgeList::MemorySize(this->attr_op_, this->edge_attr_op_, capacity);
  }

  base::KVData *InitBlock(MemAllocator *mem_allocator,
                          uint32_t *new_addr,
                          int add_size = 0,
                          char *old_edge_list = nullptr) const override;

  void Fill(char *edge_list, char *old_edge_list, const CommonUpdateItems &items) override;

  void FillRaw(char *edge_list, int slab_size, const CommonUpdateItems &items) override;

  bool CanReuse(const char *edge_list, int appending_size) const override {
    if (appending_size < 0) { return true; }
    auto edge_list_ptr = reinterpret_cast<const CDFEdgeList *>(edge_list);
    return edge_list_ptr->CanTake(appending_size) || edge_list_ptr->capacity == config_.edge_capacity_max_num;
  }

  void Clean(char *edge_list) const override { NOT_REACHED() << "clean on cdf sample not implement yet."; };

  bool Sample(const char *edge_list, const SamplingParam &param, EdgeListSlice *slice) const override;

  bool GetEdgeListInfo(const char *edge_list,
                       NodeAttr *node_attr,
                       ProtoPtrList<EdgeInfo> *edge_info) const override {
    auto edge_list_ptr = reinterpret_cast<const CDFEdgeList *>(edge_list);
    if (node_attr) {
      node_attr->set_degree(GetDegree(edge_list));
      node_attr->set_edge_num(GetSize(edge_list));
      *(node_attr->mutable_attr_info()) = std::move(this->AttrInfo(edge_list));
    }

    if (edge_info) {
      for (size_t i = 0; i < edge_list_ptr->size; ++i) {
        auto edge = edge_info->Add();
        auto *item = edge_list_ptr->Get(i);
        edge->set_id(item->id);
        edge->set_weight(edge_list_ptr->IthWeight(i));
        edge->set_timestamp(item->timestamp_s_);
        std::string edge_attr(edge_list_ptr->IthEdgeAttrBlock(i), edge_list_ptr->edge_attr_size);
        edge->set_edge_attr(std::move(edge_attr));
      }
    }

    return true;
  };

  void ListExpandSchema(std::vector<uint32_t> *schema) const override {
    CHECK_LE(absl::GetFlag(FLAGS_cdf_dynamic_growth_base), this->config_.edge_capacity_max_num);
    uint32_t len = absl::GetFlag(FLAGS_cdf_dynamic_growth_base);
    while (len <= this->config_.edge_capacity_max_num) {
      schema->push_back(GetMemorySize(len));
      auto old_len = len;
      len = std::min((uint32_t)this->config_.edge_capacity_max_num,
                     (uint32_t)(len * absl::GetFlag(FLAGS_cdf_dynamic_growth_factor)));
      // Make sure `len` keeps growing in each loop to avoid infinite loop.
      len += (old_len == len);
    }
    CHECK_GT(schema->size(), 0) << "list expand schema get empty, check your capacity config.";
  }

  uint32 GetSize(const char *edge_list) const override {
    return reinterpret_cast<const CDFEdgeList *>(edge_list)->size;
  }

  uint32 GetDegree(const char *edge_list) const override {
    return reinterpret_cast<const CDFEdgeList *>(edge_list)->out_degree;
  }

  virtual bool GetEdges(const char *edge_list,
                        const std::vector<uint64> &keys,
                        NodeAttr *node_attr,
                        ProtoPtrList<EdgeInfo> *edge_info) const {
    LOG_EVERY_N_SEC(WARNING, 60) << "cdf edge list not support query items";
    return false;
  }

  bool DeleteItems(char *edge_list, const std::vector<uint64> &keys) override {
    LOG_EVERY_N_SEC(WARNING, 60) << "cdf edge list not support delete items";
    return false;
  };

  int TimeDecay(char *edge_list,
                char *old_edge_list,
                float degree_decay_ratio,
                float weight_decay_ratio,
                float delete_threshold_weight) const override {
    auto edge_list_ptr = reinterpret_cast<CDFEdgeList *>(edge_list);
    edge_list_ptr->out_degree = (uint64_t)(floor(edge_list_ptr->out_degree * degree_decay_ratio));
    // cdf does not support delete, so delete_threshold_weight is useless
    return edge_list_ptr->DecayAllWeights(weight_decay_ratio);
  }

  int EdgeExpire(char *edge_list, int expire_interval) const override {
    LOG_EVERY_N_SEC(WARNING, 60) << "cdf edge list doesn't support edge expire.";
    return 0;
  }

  std::string AttrInfo(const char *edge_list) const override {
    auto edge_list_ptr = reinterpret_cast<const CDFEdgeList *>(edge_list);
    return this->attr_op_->SerializeBlock(edge_list_ptr->attr_block());
  }

 private:
  RangeMatcher insert_range_matcher_;
};

}  // namespace chrono_graph
