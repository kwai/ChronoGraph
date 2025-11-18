// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "memtable/mem_kv.h"

#include <algorithm>
#include <string>

#include "absl/strings/str_join.h"
#include "base/common/file_util.h"
#include "base/common/logging.h"
#include "base/common/time.h"
#include "base/random/pseudo_random.h"
#include "base/thread/thread_pool.h"

ABSL_FLAG(double, multi_mem_kv_hash_ratio, 0.25, "hash ratio for each MemKV in MultiMemKV");
ABSL_FLAG(int32, mem_kv_max_block_item_num, 1024 * 1024, "max block number");

namespace base {

bool MemKV::Initialize(uint32_t capacity,
                       float hash_ratio,
                       uint64_t mem_limit,
                       uint32_t expire_ts,
                       uint32_t delay_ts,
                       const std::string &shm_path,
                       const int32_t part_id,
                       GenerateSlabHandler gen_slab_handler) {
  part_id_ = part_id;
  capacity_ = capacity;
  mem_limit_ = mem_limit;

  std::string shm_path_dict = "";
  std::string shm_path_value = "";
  if (!shm_path.empty()) {
    shm_path_dict = shm_path + "/dict";
    shm_path_value = shm_path + "/value";
  }

  std::vector<uint32_t> slabs_vec;
  if (gen_slab_handler) {
    gen_slab_handler(&slabs_vec);
  } else {
    MultiMemKV::GenerateSomeSlabs(&slabs_vec);
  }
  uint32_t slabs_len = slabs_vec.size();
  CHECK(slabs_len > 0) << "part_id:" << part_id << ", slabs_len:" << slabs_len;

  bool ret = hash_map_.Create(capacity, hash_ratio, shm_path_dict, part_id);

  ret &=
      node_pool_.Create(slabs_vec, absl::GetFlag(FLAGS_mem_kv_max_block_item_num), shm_path_value, part_id);
  if (!ret || hash_map_.Capacity() != hash_map_.CeilCapacity(capacity)) {
    LOG(ERROR) << "restore mem_kv failed, realloc again! ret: " << ret
               << ", new_cap: " << hash_map_.CeilCapacity(capacity) << ", old_cap: " << hash_map_.Capacity();
    hash_map_.Reset();
    node_pool_.Reset();

    // delete dir & recreate
    fs::remove_all(shm_path_dict + "/" + std::to_string(part_id));
    fs::remove_all(shm_path_value + "/" + std::to_string(part_id));

    ret = hash_map_.Create(capacity, hash_ratio, shm_path_dict, part_id);
    ret &=
        node_pool_.Create(slabs_vec, absl::GetFlag(FLAGS_mem_kv_max_block_item_num), shm_path_value, part_id);
    LOG(INFO) << "memkv realloc end, ret: " << ret << ", part:" << part_id;
  }

  expire_ts_ = expire_ts;
  erase_delay_ts_ = delay_ts;

  uint32_t now_ts = base::GetTimestamp() / 1000000;
  total_update_key_num_ = 0;
  total_update_byte_size_ = 0;

  uint32_t expire_nodes = 0;
  uint64_t expire_bytes = 0;
  uint32_t max_link_len = 0;
  uint32_t min_link_len = 1000;
  uint32_t bucket_num = 0;
  hash_map_.ConstTraverseOp(
      [&](uint64_t key, uint32_t value) {
        // do not calculate all nodes
        auto kv_node = reinterpret_cast<KVData *>(node_pool_.MemAddress(value));
        if (kv_node->expired()) {
          expire_nodes++;
          expire_bytes += kv_node->size();
        }
      },
      nullptr,
      &max_link_len,
      &min_link_len,
      &bucket_num);
  uint32_t end_ts = base::GetTimestamp() / 1000000;
  LOG(INFO) << "part_id:" << part_id << ", expire_nodes:" << expire_nodes << ", expire_bytes:" << expire_bytes
            << ", max_link_len:" << max_link_len << ", min_link_len:" << min_link_len
            << ", bucket_num:" << bucket_num << ", time_take_in_seconds:" << end_ts - now_ts;
  expire_nodes_ = expire_nodes;
  last_expire_ts_ = expire_nodes > 0 ? now_ts : 0;
  cached_node_expire_ts_ = 0;

  return ret;
}

bool MemKV::Update(uint64 key, const char *log, int log_size, int expire_timet) {
  return Update(key, log, log_size, expire_timet, DefaultUpdateHandler);
}

bool MemKV::Update(uint64 key,
                   const char *log,
                   int log_size,
                   int expire_timet,
                   UpdateHandler handler,
                   RecycleHandler recycle_handler,
                   RecycleWithKeyHandler recycle_with_key_handler) {
  absl::MutexLock lock(&modify_mutex_);

  uint32 old_addr = node_pool_.Null();
  auto old_node = hash_map_.Seek(key);
  if (old_node != nullptr) { old_addr = old_node->value; }

  uint32_t new_addr = handler(old_addr, key, log, log_size, expire_timet, this);
  if (new_addr == node_pool_.Null()) {
    if (old_addr == node_pool_.Null()) {
      LOG(ERROR) << "insert failed, key:" << key << ", log size:" << log_size;
      Recycle(recycle_handler, recycle_with_key_handler);
      return false;
    }  // else if (old_addr != node_pool_.Null())
    LOG(ERROR) << "update failed, key:" << key << ", log size:" << log_size;
    return false;
  }

  // reuse hash node if don't change expire_time
  // renew hash node if change expire_time
  uint32_t now_ts = base::GetTimestamp() / 1000000;
  if (old_node != nullptr && old_addr != new_addr) {
    expire_addrs_.push(std::pair<uint32_t, uint32_t>(old_node->value, now_ts));
    old_node->value = new_addr;
    if (old_addr == node_pool_.Null()) {
      // reuse mark-deleted node, increase size purposely
      hash_map_.IncValidSizePurposely();
    }
    VLOG(1) << "update exist key, old_addr:" << old_addr << ", new_addr:" << new_addr;
  } else if (old_node == nullptr && new_addr != node_pool_.Null()) {
    hash_map_.FirstInsert(key, new_addr);
  }

  total_update_key_num_++;
  total_update_byte_size_ += log_size;

  Recycle(recycle_handler, recycle_with_key_handler);

  VLOG(1) << "update one item, key:" << key << ", log size:" << log_size;
  return true;
}

// NOT THREAD-SAFE
uint32_t MemKV::MallocAddr(int malloc_size) {
  uint32_t now_ts = base::GetTimestamp() / 1000000;
  int size = malloc_size + sizeof(KVData);
  auto new_addr = node_pool_.Malloc(size);
  if (new_addr == node_pool_.Null()) {
    LOG_EVERY_N_SEC(ERROR, 10) << "malloc from node pool failed, size:" << malloc_size;
    return node_pool_.Null();
  }

  auto new_node = reinterpret_cast<KVData *>(node_pool_.MemAddress(new_addr));
  new_node->expire_timet = now_ts + expire_ts_;
  new_node->stored_size = size;
  memset(new_node->data(), 0, malloc_size);
  return new_addr;
}

std::string MemKV::GetInfo() const {
  std::ostringstream os;

  os << "MemKV:" << part_id_ << ",capacity:" << capacity_ << ", size:" << KeyNum()
     << ", Ratio:" << (double)KeyNum() / capacity_ << std::endl;
  os << "MemLimit:" << mem_limit_ << ", MemUse:" << MemUse() << ", MemAlloc:" << MemAlloc()
     << ", MemUse Ratio:" << (double)MemUse() / mem_limit_
     << ", MemAlloc Ratio:" << (double)MemAlloc() / mem_limit_ << std::endl
     << std::endl;
  os << hash_map_.GetInfo() << std::endl;
  os << node_pool_.GetInfo() << std::endl;

  os << "Counters:" << std::endl;
  os << "expire_addrs size:" << expire_addrs_.size() << ", expire_nodes:" << expire_nodes_
     << ", last_expire_ts:" << last_expire_ts_ << ", expire_ts:" << expire_ts_
     << ", erase_delay_ts:" << erase_delay_ts_ << std::endl;

  if (!expire_addrs_.empty()) {
    int from_now_on = expire_addrs_.front().second - base::GetTimestamp() / 1000000;
    os << "last_expire_addr:" << last_expire_addr_ << ", expire_addrs key:" << expire_addrs_.front().first
       << ", timet:" << expire_addrs_.front().second << ", from_now_on:" << from_now_on << std::endl;
  }

  os << "total_update_key_num:" << total_update_key_num_
     << ", total_update_byte_size:" << total_update_byte_size_ << std::endl;

  return os.str();
}

bool MemKV::MultiSync(const std::vector<KVSyncData> &datas) {
  uint32_t now_ts = base::GetTimestamp() / 1000000;
  for (auto &data : datas) {
    Update(std::get<0>(data), std::get<1>(data), std::get<2>(data), std::get<3>(data) - now_ts);
  }
  return true;
}

void MemKV::SyncAllKVFrom(MemKV *mem_from, uint32 now_ts) {
  if (mem_from == nullptr) return;
  uint64_t start_ts = base::GetTimestamp() / 1000L;
  for (size_t i = 0; i < ALWAYS_SYNC_PART_NUM; i++) {
    std::vector<uint64> keys;
    mem_from->GetPartKeys(ALWAYS_SYNC_PART_NUM, i, &keys);
    for (auto key : keys) {
      auto cache = mem_from->GetCache(key);
      if (cache->size() < 1024 * 1024) {
        Update(key, cache->data(), cache->size(), cache->expire_timet - now_ts);
      } else {
        LOG(ERROR) << "error len:" << key << ", ptr:" << reinterpret_cast<void *>(cache->data())
                   << ", size:" << cache->size() << ", expire:" << cache->expire_timet;
      }
    }
  }
  LOG(INFO) << "MemKV sync done, time cost: " << ((base::GetTimestamp() / 1000L) - start_ts)
            << "ms, origin_key_num:" << mem_from->KeyNum() << ", sync_key_num:" << KeyNum()
            << ", origin_mem_use: " << mem_from->MemUse() << ", sync_mem_use: " << MemUse();
}

void MemKV::Erase(uint64_t key) {
  absl::MutexLock lock(&modify_mutex_);
  auto val = hash_map_.EraseKey(key);
  auto now_ts = base::GetTimestamp() / 1000000;
  if (val != slab::SlabMempool32::NULL_VADDR) { expire_addrs_.push({val, now_ts}); }
}

bool MemKV::EraseKeyIf(uint64_t key, const std::function<bool(KVData *)> &condition) {
  absl::MutexLock lock(&modify_mutex_);
  auto kv_node = GetCache(key);
  if (kv_node == nullptr) { return false; }
  if (!condition(kv_node)) { return false; }
  auto val = hash_map_.EraseKey(key);
  if (val != slab::SlabMempool32::NULL_VADDR) {
    auto now_ts = base::GetTimestamp() / 1000000;
    expire_addrs_.push({val, now_ts});
  }
  return true;
}

uint32_t MemKV::EraseIf(std::function<bool(uint64, KVData *)> erase_handler) {
  uint64_t now_ts = base::GetTimestamp() / 1000;
  uint32_t erase_nodes = 0;
  uint32_t max_link_len = 0;
  uint32_t min_link_len = 1000;
  uint32_t bucket_num = 0;
  // Collect keys to be erased without lock
  std::vector<uint64_t> expire_keys;
  hash_map_.ConstTraverseOp(
      [&](uint64_t key, uint32_t value) {
        auto kv_data = reinterpret_cast<KVData *>(node_pool_.MemAddress(value));
        if (kv_data != nullptr && erase_handler(key, kv_data)) {
          expire_keys.push_back(key);
          erase_nodes++;
        }
      },
      nullptr,
      &max_link_len,
      &min_link_len,
      &bucket_num);

  if (erase_nodes == 0) { return 0; }

  LOG(INFO) << "Nodes to be erased: " << erase_nodes
            << ", time_take: " << (base::GetTimestamp() / 1000) - now_ts << " ms";

  now_ts = base::GetTimestamp() / 1000;
  {
    // Do erase with lock
    absl::MutexLock lock(&modify_mutex_);
    for (auto key : expire_keys) {
      auto val = hash_map_.EraseKey(key);
      if (val != slab::SlabMempool32::NULL_VADDR) { expire_addrs_.push({val, now_ts}); }
    }

    do {
      if (expire_addrs_.empty()) { break; }
      auto vaddr = expire_addrs_.front().first;
      auto timet = expire_addrs_.front().second;
      if (timet + erase_delay_ts_ >= now_ts) { break; }
      {
        expire_addrs_.pop();
        node_pool_.Free(vaddr);
      }
    } while (true);
  }
  LOG(INFO) << "Erase nodes with lock, time_take: " << (base::GetTimestamp() / 1000) - now_ts << " ms";
  return erase_nodes;
}

// Recycle only in single thread
// force renew every current node
void MemKV::Recycle(RecycleHandler recycle_handler, RecycleWithKeyHandler recycle_with_key_handler) {
  uint32_t now_ts = base::GetTimestamp() / 1000000;
  // recycle part
  do {
    bool force_recycle = RecycleAlarm();
    uint32_t recycle_nodes = 0;
    hash_map_.TraverseUntil([&](uint64_t key_t, uint32_t value_t) -> bool {
      // batch stop
      if (recycle_nodes >= BATCH_RECYCLE_NODE_SIZE) return false;

      last_expire_addr_ = value_t;

      // not force_recycle && cached last node expire ts
      if (!force_recycle && cached_node_expire_ts_ > now_ts) { return false; }

      // not force_recycle & not expired
      auto kv_node = reinterpret_cast<KVData *>(node_pool_.MemAddress(last_expire_addr_));
      if (!force_recycle && !kv_node->expired()) {
        cached_node_expire_ts_ = kv_node->expire_timet;
        return false;
      }

      LOG_EVERY_N_SEC(INFO, 10) << "part:" << part_id_ << " key:" << key_t << " is retired, addr:" << value_t
                                << " due to " << (kv_node->expired() ? "natural expire" : "force delete")
                                << " expire_timet:" << kv_node->expire_timet << " size:" << kv_node->size();

      if (recycle_with_key_handler) {
        recycle_with_key_handler(key_t, kv_node);
      } else if (recycle_handler) {
        recycle_handler(kv_node);
      }

      recycle_nodes += 1;
      last_expire_ts_ = now_ts;
      cached_node_expire_ts_ = 0;
      expire_addrs_.push(std::pair<uint32_t, uint32_t>(value_t, now_ts));
      // continue recycle
      return true;
    });

    if (force_recycle) {
      expire_nodes_ += recycle_nodes;
      LOG_EVERY_N_SEC(WARNING, 10) << "Part:" << part_id_ << " force recycle nodes: " << expire_nodes_
                                   << " done, time_take: " << base::GetTimestamp() / 1000000 - now_ts << " s";
    } else {
      expire_nodes_ = 0;
    }
  } while (false);

  do {
    if (expire_addrs_.empty()) { break; }

    auto vaddr = expire_addrs_.front().first;
    auto timet = expire_addrs_.front().second;
    if (timet + erase_delay_ts_ >= now_ts) { break; }
    {
      expire_addrs_.pop();
      node_pool_.Free(vaddr);
    }
  } while (true);
}

void MemKV::Clear() {
  LOG(INFO) << "start clear all expire addrs:" << expire_addrs_.size();
  while (!expire_addrs_.empty()) {
    auto vaddr = expire_addrs_.front().first;
    expire_addrs_.pop();
    node_pool_.Free(vaddr);
  }
  LOG(INFO) << "end clear all expire addrs:" << expire_addrs_.size();
}

bool MemKV::RecycleAlarm() {
  bool ret = false;
  if (mem_limit_ > 0) {
    auto allocated = MemAlloc();
    if (allocated >= mem_limit_ * RECYCLE_ALARM_ALLOC_FULL_RATIO) { node_pool_.SetAllocTotalFull(); }

    ret |= (MemUse() >= mem_limit_ * RECYCLE_ALARM_USE_RATIO);
    if (ret) {
      LOG_EVERY_N_SEC(WARNING, 10) << "MemKV hit mem limit, mem used: " << MemUse() << ", mem limit "
                                   << mem_limit_ * RECYCLE_ALARM_USE_RATIO;
    }
  }
  if (!ret && capacity_ > 0) {
    ret |= (KeyNum() >= capacity_ * RECYCLE_ALARM_USE_RATIO);
    if (ret) {
      LOG_EVERY_N_SEC(WARNING, 10) << "MemKV hit key num limit, key num: " << KeyNum()
                                   << ", valid key num: " << ValidKeyNum() << ", key num limit "
                                   << capacity_ * RECYCLE_ALARM_USE_RATIO;
    }
  }
  return ret;
}

void MemKV::GetSlabs(std::vector<SlabPair> *slabs) { node_pool_.GetSlabs(slabs); }

void MemKV::GetSlabInfos(std::vector<SlabInfoTuple> *slab_infos) const {
  node_pool_.GetSlabInfos(slab_infos);
}

void MemKV::TraverseOp(std::function<void(uint64_t, KVData *)> op, std::function<bool()> exit_op) {
  uint32_t max_link_len = 0;
  uint32_t min_link_len = 0;
  uint32_t bucket_num = 0;
  hash_map_.ConstTraverseOp(
      [op, this](uint64_t key, uint32_t value) {
        auto kv_node = reinterpret_cast<KVData *>(node_pool_.MemAddress(value));
        if (kv_node) op(key, kv_node);
      },
      exit_op,
      &max_link_len,
      &min_link_len,
      &bucket_num);
}

uint32 DefaultUpdateHandler(
    uint32 old_addr, uint64 key, const char *log, int size, int expire_timet, MemKV *table) {
  // KVData *old_node = table->GetKVDataByAddr(old_addr);
  uint32 new_addr = table->MallocAddr(size);
  auto new_node = table->GetKVDataByAddr(new_addr);
  if (new_node == nullptr) {
    LOG(ERROR) << "failed to malloc node from pool: " << new_addr << ", size:" << size;
    return slab::SlabMempool32::NULL_VADDR;
  }

  new_node->store(log, size);
  uint32_t now_ts = base::GetTimestamp() / 1000000;
  new_node->expire_timet = expire_timet == -1 ? (now_ts + table->ExpireTs()) : (now_ts + expire_timet);
  return new_addr;
}

MultiMemKV::MultiMemKV(const std::vector<std::string> &multi_shm_path,
                       int part_num,
                       uint64_t capacity,
                       int expire_ts,
                       uint64_t mem_limit,
                       GenerateSlabHandler gen_slab_handler) {
  CHECK(multi_shm_path.size() > 0);
  capacity_ = capacity;
  mem_limit_ = mem_limit;
  memkv_part_num_ = part_num;
  shm_path_ = multi_shm_path[0];
  expire_ts_ = expire_ts;
  shm_path_vec_ = multi_shm_path;

  for (size_t i = 0; i < part_num; i++) {
    auto ptr = new MemKV();
    memkv_ptr_vec_.push_back(ptr);
  }

  thread::ThreadPool thread_pool(part_num);
  for (size_t i = 0; i < part_num; i++) {
    thread_pool.AddTask([this, i, gen_slab_handler]() { this->InitializePart(i, gen_slab_handler); });
  }
  thread_pool.JoinAll();
  LOG(INFO) << "MultiMemKV initialize done, PartNum:" << memkv_part_num_ << ", Capacity:" << capacity_
            << ", MemLimit:" << mem_limit_ << ", ExpireTs:" << expire_ts_ << ", ShmPath:" << shm_path_
            << ", MultiPath:" << absl::StrJoin(shm_path_vec_, ",");
}

MultiMemKV::~MultiMemKV() {
  for (auto ptr : memkv_ptr_vec_) { delete ptr; }
  memkv_ptr_vec_.clear();
}

void MultiMemKV::InitializePart(int part_id, GenerateSlabHandler gen_slab_handler) {
  CHECK((part_id >= 0) && (part_id <= memkv_part_num_));
  memkv_ptr_vec_[part_id]->Initialize(capacity_ / memkv_part_num_,
                                      absl::GetFlag(FLAGS_multi_mem_kv_hash_ratio),
                                      mem_limit_ / memkv_part_num_,
                                      expire_ts_,
                                      10,
                                      GetMemKVShmPath(part_id),
                                      part_id,
                                      gen_slab_handler);
}

bool MultiMemKV::MultiSync(const std::vector<KVSyncData> &datas) {
  std::vector<std::vector<KVSyncData>> part_splits;
  part_splits.resize(PartNum());
  for (auto &data : datas) { part_splits[GetPart(std::get<0>(data))].emplace_back(std::move(data)); }
  for (int shard = 0; shard < PartNum(); ++shard) {
    if (!memkv_ptr_vec_[shard]->MultiSync(part_splits[shard])) { return false; }
  }
  return true;
}

void MultiMemKV::SyncAllKVFrom(MultiMemKV *mem_from, int sync_concurrency) {
  CHECK(mem_from != nullptr);
  CHECK_EQ(mem_from->PartNum(), memkv_part_num_);
  thread::ThreadPool thread_pool(sync_concurrency);
  uint64_t now_ts_64 = base::GetTimestamp() / 1000L;
  uint32_t now_ts = base::GetTimestamp() / 1000000;
  for (size_t i = 0; i < memkv_part_num_; i++) {
    thread_pool.AddTask([this, mem_from, now_ts, i]() {
      memkv_ptr_vec_[i]->SyncAllKVFrom(mem_from->memkv_ptr_vec_[i], now_ts);
    });
  }
  thread_pool.JoinAll();
  LOG(INFO) << "MultiMemKV sync done, time cost: " << ((base::GetTimestamp() / 1000L) - now_ts_64)
            << "ms, ori_key_num:" << mem_from->KeyNum() << ", sync_key_num:" << KeyNum()
            << ", ori_mem_use: " << mem_from->MemUse() << ", sync_mem_use: " << MemUse();
}

void MultiMemKV::GenerateSomeSlabs(std::vector<uint32_t> *slabs_vec) {
  for (size_t i = 5; i < 20; ++i) { slabs_vec->emplace_back(1 << i); }
}

std::string MultiMemKV::GetInfo() const {
  auto key_num = KeyNum();
  auto mem_use = MemUse();
  auto mem_alloc = MemAlloc();
  std::ostringstream os;
  os << "MemKV:" << memkv_part_num_ << ", Path:" << absl::StrJoin(shm_path_vec_, ",")
     << ", capacity:" << capacity_ << ", size:" << KeyNum() << ", Ratio:" << (double)key_num / capacity_
     << std::endl;
  os << "MemLimit:" << mem_limit_ << ", MemUse:" << mem_use << ", MemAlloc:" << mem_alloc
     << ", UseRatio:" << (double)mem_use / mem_limit_ << ", AllocRatio:" << (double)mem_alloc / mem_limit_
     << std::endl
     << std::endl;

  uint64_t all_parts_total_update_num = 0;
  for (size_t i = 0; i < memkv_part_num_; i++) {
    all_parts_total_update_num += memkv_ptr_vec_[i]->TotalUpdateNum();
  }
  os << "All parts total update num: " << all_parts_total_update_num << std::endl << std::endl;

  PseudoRandom s_random(base::GetTimestamp());
  uint64 r_num = s_random.GetUint64LT(memkv_part_num_);
  os << "MemKV" << r_num << ":" << reinterpret_cast<void *>(memkv_ptr_vec_[r_num]) << std::endl;
  os << memkv_ptr_vec_[r_num]->GetInfo();
  return os.str();
}

}  // namespace base
