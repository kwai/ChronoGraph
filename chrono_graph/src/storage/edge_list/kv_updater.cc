// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "chrono_graph/src/storage/edge_list/kv_updater.h"

#include "chrono_graph/src/storage/edge_list/el_interface.h"
#include "chrono_graph/src/storage/edge_list/mem_allocator.h"

namespace chrono_graph {

base::KVData *GraphKvUpdater::InitKVData(uint32_t *new_addr,
                                         MemAllocator *malloc,
                                         int add_size,
                                         char *edge_list) const {
  base::KVData *cache = edge_list_adaptor_->InitBlock(malloc, new_addr, add_size, edge_list);
  if (cache == nullptr) {
    LOG_EVERY_N_SEC(ERROR, 10) << "Graph kv malloc a null block for relation " << relation_name_;
    return cache;
  }
  cache->expire_timet = base::GetTimestamp() / base::Time::kMicrosecondsPerSecond + expire_interval_s_;
  return cache;
}

uint32 GraphKvUpdater::UpdateHandler(
    uint32 old_addr, uint64 key, const char *log, int log_size, int dummy_expire_timet, base::MemKV *mem_kv) {
  CHECK_EQ(log_size, sizeof(CommonUpdateItems));
  const CommonUpdateItems *items = reinterpret_cast<const CommonUpdateItems *>(log);
  MemKVAllocator allocator(mem_kv, relation_name_);
  if (items->ids.empty() && items->attr_update_info.empty()) { return old_addr; }
  uint32 cur_addr = old_addr;
  base::KVData *cur_node = mem_kv->GetKVDataByAddr(cur_addr);
  char *old_edge_list = nullptr;
  if (cur_node == nullptr) {  // key didn't exist, alloc a new one
    cur_node = InitKVData(&cur_addr, &allocator, items->ids.size());
  } else {  // update old node
    old_edge_list = cur_node->data();
    if (!edge_list_adaptor_->CanReuse(old_edge_list, items->ids.size())) {
      cur_node = InitKVData(&cur_addr, &allocator, items->ids.size(), cur_node->data());
    }
  }

  if (cur_node == nullptr) {
    LOG_EVERY_N_SEC(ERROR, 10) << "failed to malloc node from pool, new addr:" << cur_addr;
    return old_addr;
  }

  edge_list_adaptor_->Fill(cur_node->data(), old_edge_list, *items);

  uint32_t base_time = base::GetTimestamp() / base::Time::kMicrosecondsPerSecond;
  if (items->use_item_ts_for_expire && !items->id_ts.empty()) { base_time = items->id_ts[0]; }

  if (items->expire_ts_update_flag) {
    auto old_expire_timet = cur_node->expire_timet;
    cur_node->expire_timet = base_time + expire_interval_s_;
    if (old_expire_timet > cur_node->expire_timet) { cur_node->expire_timet = old_expire_timet; }
  }

  return cur_addr;
}
uint32 GraphKvUpdater::DeleteEdgeHandler(uint32 addr,
                                         uint64 storage_key,
                                         const char *log,
                                         int log_size,
                                         int dummy_expire_timet,
                                         base::MemKV *mem_kv) {
  CHECK_EQ(log_size, sizeof(CommonUpdateItems));
  base::KVData *cur_node = mem_kv->GetKVDataByAddr(addr);
  if (cur_node == nullptr) {  // storage_key not found
    return addr;
  }

  const CommonUpdateItems *items = reinterpret_cast<const CommonUpdateItems *>(log);
  uint32 cur_addr = addr;
  char *old_edge_list = cur_node->data();
  // -1 means delete one edge
  if (!edge_list_adaptor_->CanReuse(old_edge_list, -1)) {
    MemKVAllocator allocator(mem_kv, relation_name_);
    cur_node = InitKVData(&cur_addr, &allocator, 0, cur_node->data());
    if (cur_node == nullptr) {
      LOG(ERROR) << "failed to malloc node from pool, new addr:" << cur_addr;
      return addr;
    }
  }
  edge_list_adaptor_->DeleteItems(cur_node->data(), old_edge_list, items->ids);
  if (edge_list_adaptor_->GetSize(cur_node->data()) == 0) {
    // expire immediately
    cur_node->expire_timet = base::GetTimestamp() / base::Time::kMicrosecondsPerSecond;
  }
  return cur_addr;
}

uint32 GraphKvUpdater::TimeDecayHandler(uint32 addr,
                                        uint64 storage_key,
                                        int *delete_count,
                                        float degree_decay_ratio,
                                        float weight_decay_ratio,
                                        float delete_threshold_weight,
                                        base::MemKV *mem_kv) {
  base::KVData *cur_node = mem_kv->GetKVDataByAddr(addr);
  if (cur_node == nullptr) { return addr; }

  uint32 cur_addr = addr;
  char *old_edge_list = cur_node->data();
  // only malloc in CPTreap buffered edge list
  // Expire may cause edge deletion, so pass -1 to CanReuse
  if (!edge_list_adaptor_->CanReuse(old_edge_list, -1)) {
    MemKVAllocator allocator(mem_kv, relation_name_);
    cur_node = InitKVData(&cur_addr, &allocator, 0, cur_node->data());
    if (cur_node == nullptr) {
      LOG(ERROR) << "failed to malloc node from pool, new addr:" << cur_addr;
      return addr;
    }
  }
  *delete_count += edge_list_adaptor_->TimeDecay(
      cur_node->data(), old_edge_list, degree_decay_ratio, weight_decay_ratio, delete_threshold_weight);
  if (edge_list_adaptor_->GetSize(cur_node->data()) == 0) {
    // expire immediately
    cur_node->expire_timet = base::GetTimestamp() / base::Time::kMicrosecondsPerSecond;
  }
  return cur_addr;
}

uint32 GraphKvUpdater::EdgeExpireHandler(
    uint32 addr, uint64 storage_key, int *delete_count, int expire_interval, base::MemKV *mem_kv) {
  base::KVData *cur_node = mem_kv->GetKVDataByAddr(addr);
  if (cur_node == nullptr) { return addr; }

  uint32 cur_addr = addr;
  char *old_edge_list = cur_node->data();
  // only malloc in CPTreap buffered edge list
  // Expire may cause edge deletion, so pass -1 to CanReuse
  if (!edge_list_adaptor_->CanReuse(old_edge_list, -1)) {
    MemKVAllocator allocator(mem_kv, relation_name_);
    cur_node = InitKVData(&cur_addr, &allocator, 0, cur_node->data());
    if (cur_node == nullptr) {
      LOG(ERROR) << "failed to malloc node from pool, new addr:" << cur_addr;
      return addr;
    }
  }
  *delete_count += edge_list_adaptor_->EdgeExpire(cur_node->data(), old_edge_list, expire_interval);
  if (edge_list_adaptor_->GetSize(cur_node->data()) == 0) {
    // expire immediately
    cur_node->expire_timet = base::GetTimestamp() / base::Time::kMicrosecondsPerSecond;
  }
  return cur_addr;
}

}  // namespace chrono_graph