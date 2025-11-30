// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "chrono_graph/src/storage/edge_list/cdf_edge_list.h"

#include <utility>
#include "base/common/gflags.h"
#include "base/common/time.h"
#include "chrono_graph/src/storage/edge_list/mem_allocator.h"

ABSL_FLAG(int32, cdf_dynamic_growth_base, 2, "Initial allocated space can store `X` items.");
ABSL_FLAG(double, cdf_dynamic_growth_factor, 2, "Expand factor when length exceeds limit");
ABSL_FLAG(double, cdf_reformat_threshold, 100000, "Trigger cdf reformat threshold");
ABSL_FLAG(std::string,
          cdf_insert_stat_key_ranges,
          "0,100,500,1000,3000,5000,10000,20000,30000,40000,50000,60000",
          "For monitor");
ABSL_FLAG(bool,
          cdf_allow_weighted_sample,
          true,
          "= true: Each node stores cumulative weight of itself and its sons, for weighted sample."
          "= false: Each node only stores weight of itself. Weighted sample will fail, but float overflow is "
          "less likely to happen");

namespace chrono_graph {
void CDFEdgeList::Sort(int offset) {
  for (int i = offset; i <= capacity - 2; ++i) {
    if (Get(i)->timestamp_s_ < Get(i + 1)->timestamp_s_) {
      // Swap item of i and i+1
      std::swap(Get(i)->id, Get(i + 1)->id);
      std::swap(Get(i)->timestamp_s_, Get(i + 1)->timestamp_s_);
      if (!absl::GetFlag(FLAGS_cdf_allow_weighted_sample)) {
        std::swap(Get(i)->weight, Get(i + 1)->weight);
      } else {
        float base_weight;
        if (i == capacity - 2) {
          base_weight = weight_bar;
        } else {
          base_weight = Get(i + 2)->weight;
        }
        Get(i + 1)->weight = base_weight + IthWeight(i);
      }
    } else {
      break;
    }
  }
}

uint32 CDFEdgeList::FindPivotByTs(uint32 threshold_ts) const {
  if (threshold_ts == 0) return 0;
  // end may be -1, can't use uint here
  int64 begin = 0, end = size - 1;
  while (begin <= end) {
    auto mid = (begin + end) / 2;
    if (Get(mid)->timestamp_s_ > threshold_ts) {
      begin = mid + 1;
    } else {
      end = mid - 1;
    }
  }
  // [begin, size) has timestamp <= threshold
  // [0, begin) has timestamp > threshold
  return (uint32)begin;
}

bool CDFEdgeList::Add(const std::vector<uint64> &ids,
                      const std::vector<float> &weights,
                      EdgeListReplaceStrategy replace_type,
                      const std::vector<uint32_t> &timestamp_s,
                      const std::vector<std::string> &id_attrs,
                      BlockStorageApi *edge_attr_op) {
  if (replace_type != EdgeListReplaceStrategy::LRU && replace_type != EdgeListReplaceStrategy::CANCEL) {
    LOG_EVERY_N_SEC(ERROR, 10) << "Add: given replace strategy not supported: " << replace_type;
    return false;
  }
  CHECK(start < capacity && size <= capacity)
      << "start = " << start << ", size = " << size << ", cap = " << capacity;
  // If weights is empty, all weights are 1.
  CHECK(weights.empty() || ids.size() == weights.size())
      << "Add edge item list error, ids and weights size not match, ids size = " << ids.size()
      << ", weights size = " << weights.size();
  CHECK(timestamp_s.empty() || ids.size() == timestamp_s.size())
      << "Add edge item list error, ids and ts size not match, ids size = " << ids.size()
      << ", ts size = " << timestamp_s.size();
  CHECK(id_attrs.empty() || ids.size() == id_attrs.size())
      << "Add edge item list error, ids and edge_attr size not match, ids size = " << ids.size()
      << ", edge_attr size = " << id_attrs.size();
  for (size_t i = 0; i < ids.size(); ++i) {
    if (replace_type == EdgeListReplaceStrategy::CANCEL && size == capacity) { break; }
    Add(ids[i],
        weights.empty() ? 1 : weights[i],
        timestamp_s.empty() ? 0 : timestamp_s[i],
        id_attrs.empty() ? "" : id_attrs[i],
        edge_attr_op);
  }
  return true;
}

bool CDFEdgeList::Add(const uint64 id,
                      float weight,
                      const uint32 timestamp_s,
                      std::string id_attr,
                      BlockStorageApi *edge_attr_op) {
  if (id == 0 || weight <= 0) {
    LOG_EVERY_N_SEC(WARNING, 5) << "cdf only insert id > 0(" << id << ") and weight > 0(" << weight << ")";
    return false;
  }

  // If attr size and attr_op not match, this insertion is invalid.
  if (!id_attr.empty()) {
    if (!edge_attr_op) {
      LOG_EVERY_N_SEC(ERROR, 5) << "attr op not specified when attr info exists!";
      return false;
    }
    if (id_attr.size() != edge_attr_size || edge_attr_op->MaxSize() != edge_attr_size) {
      LOG_EVERY_N_SEC(ERROR, 5) << "Input attr size: " << id_attr.size()
                                << ", op size: " << edge_attr_op->MaxSize()
                                << ", expected size: " << edge_attr_size;
      return false;
    }
  }
  out_degree++;

  // the edge list is full and new coming timestamp is oldder than the oldest item
  if (size == capacity && timestamp_s < Get(capacity - 1)->timestamp_s_) {
    LOG_EVERY_N_SEC(INFO, 10) << "discard the item due to timestamp: " << timestamp_s
                              << " is older than current oldest timestamp "
                              << Get(capacity - 1)->timestamp_s_;
    return true;
  }

  uint32 new_start = (start + 1 == capacity) ? 0 : start + 1;
  auto &item = *Get(capacity - 1);
  item.id = id;
  item.timestamp_s_ = timestamp_s;
  // Tricky here
  if (absl::GetFlag(FLAGS_cdf_allow_weighted_sample)) {
    weight_bar += IthWeight(capacity - 1);
    item.weight = weight + Get(0)->weight;
  } else {
    item.weight = weight;
  }
  if (!id_attr.empty()) {
    edge_attr_op->UpdateBlock(IthEdgeAttrBlock(capacity - 1), id_attr.c_str(), id_attr.size());
  }

  start = new_start;
  Sort(0);
  if (size < capacity) { size++; }
  if (absl::GetFlag(FLAGS_cdf_allow_weighted_sample) &&
      item.weight > absl::GetFlag(FLAGS_cdf_reformat_threshold)) {
    if (weight_bar == 0) {
      LOG_EVERY_N_SEC(WARNING, 5) << "cdf trying to Reformat when weight_bar = 0, skip. Consider set "
                                     "larger cdf_reformat_threshold. Weight sum = "
                                  << item.weight << ", size = " << size;
    } else {
      ReformatCDF();
    }
  }
  return true;
}

base::KVData *CDFAdaptor::InitBlock(MemAllocator *mem_allocator,
                                    uint32_t *new_addr,
                                    int add_size,
                                    char *old_edge_list) const {
  int new_size = add_size;
  if (old_edge_list != nullptr) {
    auto edge_list_ptr = reinterpret_cast<CDFEdgeList *>(old_edge_list);
    new_size += edge_list_ptr->size;
  }
  int capacity = absl::GetFlag(FLAGS_cdf_dynamic_growth_base);
  while (new_size > capacity && capacity < this->config_.edge_capacity_max_num) {
    // growing
    float t = capacity * absl::GetFlag(FLAGS_cdf_dynamic_growth_factor);
    capacity = ((int)t == capacity) ? capacity + 1 : (int)t;
    capacity = std::min(this->config_.edge_capacity_max_num, capacity);
  }
  base::KVData *ptr = mem_allocator->New(GetMemorySize(capacity), new_addr);
  if (ptr == nullptr) { return nullptr; }
  auto edge_list_ptr = reinterpret_cast<CDFEdgeList *>(ptr->data());
  edge_list_ptr->Initialize(this->attr_op_, this->edge_attr_op_, capacity);
  return ptr;
}

void CDFAdaptor::Fill(char *edge_list, char *old_edge_list, const CommonUpdateItems &items) {
  auto edge_list_ptr = reinterpret_cast<CDFEdgeList *>(edge_list);
  // Init from old_edge_list
  if (edge_list != old_edge_list && old_edge_list != nullptr) {
    auto old_edge_list_ptr = reinterpret_cast<CDFEdgeList *>(old_edge_list);
    int capacity = edge_list_ptr->capacity;
    // check capacity is calculated correctly
    CHECK(capacity >= old_edge_list_ptr->size + items.ids.size() ||
          capacity == this->config_.edge_capacity_max_num);
    memcpy(edge_list_ptr, old_edge_list, GetMemorySize(old_edge_list_ptr->size));
    // recover capacity
    edge_list_ptr->capacity = capacity;
    // reset start due to different capacities of new edge list and old edge list
    if (old_edge_list_ptr->size == 0) { edge_list_ptr->start = edge_list_ptr->capacity - 1; }
  } else if (old_edge_list == nullptr) {
    // new allocated
    CHECK(edge_list_ptr->capacity >= items.ids.size() ||
          edge_list_ptr->capacity == this->config_.edge_capacity_max_num);
  } else {
    // reuse
    CHECK(CanReuse(edge_list, items.ids.size()));
  }

  // set attr.
  if (!items.attr_update_info.empty()) {
    this->attr_op_->UpdateBlock(
        edge_list_ptr->attr_block(), items.attr_update_info.c_str(), items.attr_update_info.size());
  }
  auto begin = base::GetTimestamp();
  edge_list_ptr->Add(items.ids,
                     items.id_weights,
                     config_.oversize_replace_strategy,
                     items.id_ts,
                     items.id_attr_update_infos,
                     this->edge_attr_op_);
  auto end = base::GetTimestamp();
  PERF_AVG(end - begin,
           config_.relation_name,
           P_EL,
           "cdf.insert_cost",
           insert_range_matcher_.MatchKey(edge_list_ptr->size));
}

void CDFAdaptor::FillRaw(char *edge_list, int slab_size, const CommonUpdateItems &items) {
  auto edge_list_ptr = reinterpret_cast<CDFEdgeList *>(edge_list);
  edge_list_ptr->Initialize(this->attr_op_, this->edge_attr_op_, slab_size);
  if (!items.attr_update_info.empty()) {
    this->attr_op_->UpdateBlock(
        edge_list_ptr->attr_block(), items.attr_update_info.c_str(), items.attr_update_info.size());
  }
  edge_list_ptr->Add(items.ids,
                     items.id_weights,
                     config_.oversize_replace_strategy,
                     items.id_ts,
                     items.id_attr_update_infos,
                     edge_attr_op_);
}

bool CDFAdaptor::Sample(const char *edge_list, const SamplingParam &param, EdgeListSlice *slice) const {
  auto ptr = reinterpret_cast<const CDFEdgeList *>(edge_list);
  if (ptr->size == 0) { return true; }

  thread_local base::PseudoRandom s_random(base::RandomSeed());

  auto max_timestamp_s = 0;
  if (param.max_timestamp_s() > 0) {
    max_timestamp_s = param.max_timestamp_s();
    if (ptr->size > 0 && max_timestamp_s < ptr->Get(ptr->size - 1)->timestamp_s_) {
      std::string t_s = base::TimestampToString(param.max_timestamp_s());
      LOG_EVERY_N_SEC(INFO, 10) << "cdf sample early stop because all edge's timestamp is bigger than given; "
                                << "req.max_timestamp = " << t_s << "[" << param.max_timestamp_s() << "]"
                                << ", earliest edge timestamp = " << ptr->Get(ptr->size - 1)->timestamp_s_;
      return true;
    }
  }
  auto min_timestamp_s = 0;
  if (param.min_timestamp_s() > 0) {
    min_timestamp_s = param.min_timestamp_s();
    if (ptr->size > 0 && min_timestamp_s > ptr->Get(0u)->timestamp_s_) {
      std::string t_s = base::TimestampToString(param.min_timestamp_s());
      LOG_EVERY_N_SEC(INFO, 10)
          << "cdf sample early stop because all edge's timestamp is smaller than given; "
          << "req.min_timestamp = " << t_s << "[" << param.min_timestamp_s() << "]"
          << ", latest edge timestamp = " << ptr->Get(0u)->timestamp_s_;
      return true;
    }
  }

  // Valid range: [max_ts_pivot, min_ts_pivot)
  auto max_ts_pivot = max_timestamp_s == 0 ? 0 : ptr->FindPivotByTs(max_timestamp_s);
  auto min_ts_pivot = min_timestamp_s == 0 ? ptr->size : ptr->FindPivotByTs(min_timestamp_s);
  if (min_ts_pivot <= max_ts_pivot) {
    std::string max_t_s = base::TimestampToString(param.max_timestamp_s());
    std::string min_t_s = base::TimestampToString(param.min_timestamp_s());
    LOG_EVERY_N_SEC(INFO, 10)
        << "cdf sample early stop because all edge's timestamp is not in requested range; "
        << "req.max_timestamp = " << max_t_s << "[" << param.max_timestamp_s() << "]"
        << ", req.min_timestamp = " << min_t_s << "[" << param.min_timestamp_s() << "]"
        << ", earliest edge timestamp = " << ptr->Get(ptr->size - 1)->timestamp_s_
        << ", latest edge timestamp = " << ptr->Get(0u)->timestamp_s_;
    return true;
  }

  switch (param.strategy()) {
    case SamplingParam_SamplingStrategy_RANDOM:
      for (size_t i = 0; i < param.sampling_num(); ++i) {
        uint32 offset = s_random.GetInt(max_ts_pivot, min_ts_pivot - 1);
        const CDFEdgeListItem *edge = ptr->Get(offset);
        std::string edge_attr(ptr->IthEdgeAttrBlock(offset), ptr->edge_attr_size);
        slice->Add(edge->id, edge->timestamp_s_, ptr->IthWeight(offset), edge_attr);
      }
      return true;

    case SamplingParam_SamplingStrategy_MOST_RECENT:
      for (size_t i = max_ts_pivot; i < std::min(max_ts_pivot + param.sampling_num(), min_ts_pivot); ++i) {
        const CDFEdgeListItem *edge = ptr->Get(i);
        std::string edge_attr(ptr->IthEdgeAttrBlock(i), ptr->edge_attr_size);
        slice->Add(edge->id, edge->timestamp_s_, ptr->IthWeight(i), edge_attr);
      }
      return true;

    case SamplingParam_SamplingStrategy_LEAST_RECENT:
      // With larger offset, item is older. Thus, add item with larger offset first.
      for (int i = min_ts_pivot - 1; i >= max_ts_pivot && i >= min_ts_pivot - (uint32)param.sampling_num();
           --i) {
        const CDFEdgeListItem *edge = ptr->Get(i);
        std::string edge_attr(ptr->IthEdgeAttrBlock(i), ptr->edge_attr_size);
        slice->Add(edge->id, edge->timestamp_s_, ptr->IthWeight(i), edge_attr);
      }
      return true;

    case SamplingParam_SamplingStrategy_EDGE_WEIGHT:
      if (!absl::GetFlag(FLAGS_cdf_allow_weighted_sample)) {
        LOG_EVERY_N_SEC(ERROR, 5) << "cdf weight sample switch closed, so should not call it";
        return false;
      }
      for (size_t i = 0; i < param.sampling_num(); ++i) {
        uint32 offset = ptr->SampleEdge(max_ts_pivot, min_ts_pivot);
        const CDFEdgeListItem *edge = ptr->Get(offset);
        std::string edge_attr(ptr->IthEdgeAttrBlock(offset), ptr->edge_attr_size);
        slice->Add(edge->id, edge->timestamp_s_, ptr->IthWeight(offset), edge_attr);
      }
      return true;

    default:
      NOT_REACHED() << "cdf doesn't support sample type: "
                    << SamplingParam_SamplingStrategy_Name(param.strategy());
  }
  NOT_REACHED() << "illegal state, check case return break;";
  return false;
}

}  // namespace chrono_graph
