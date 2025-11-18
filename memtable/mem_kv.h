// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "base/common/logging.h"
#include "base/common/time.h"
#include "memtable/hash_map/loop_hash_map.h"
#include "memtable/slab/slab_mempool32.h"
#include "memtable/util/constant.h"
#include "memtable/util/counter.h"

namespace base {

class MemKV {
 public:
  MemKV() {}
  ~MemKV() { Clear(); }

  bool Initialize(uint32_t capacity,
                  float hash_ratio,
                  uint64_t mem_limit,
                  uint32_t expire_ts,
                  uint32_t delay_ts,
                  const std::string &shm_path,
                  const int32_t part_id,
                  GenerateSlabHandler gen_slab_handler = nullptr);

  template <typename FixedType>
  FixedType *GetFixed(uint64 key) {
    auto node = GetCache(key);
    if (node == nullptr) { return nullptr; }

    if (node->size() != sizeof(FixedType)) {
      LOG(ERROR) << "get one item:" << key << ", stored_size:" << node->stored_size
                 << ", realsize:" << node->size() << ", ptr:" << reinterpret_cast<void *>(node);
      return nullptr;
    }

    return reinterpret_cast<FixedType *>(node->data());
  }

  KVReadData Get(uint64 key) {
    KVReadData read_data;

    auto kv_node = GetCache(key);
    if (kv_node == nullptr) { return read_data; }

    if (kv_node->size() <= 0) {
      LOG(ERROR) << "get one item:" << key << ", stored_size:" << kv_node->stored_size
                 << ", realsize:" << kv_node->size() << ", ptr:" << reinterpret_cast<void *>(kv_node);
      return read_data;
    }
    read_data.copy_from(kv_node);
    return read_data;
  }
  KVReadData ReadToBuffer(uint64 key, std::vector<char> *buffer, GetHandler get_handler = nullptr) {
    auto node = hash_map_.Seek(key);
    if (node == nullptr) { return {}; }

    uint32_t vaddr = node->value;
    return (node_pool_.ReadToBuffer(vaddr, buffer, get_handler));
  }

  LoopHashMap<uint64_t, uint32_t>::HashNodeT *SeekNodeOneStep(uint64 key) {
    return hash_map_.SeekNodeOneStep(key);
  }

  LoopHashMap<uint64_t, uint32_t>::HashNodeT *NextNodeOneStep(uint32 addr) { return hash_map_.GetNode(addr); }

  KVData *VaddrToKVData(uint32_t vaddr) { return reinterpret_cast<KVData *>(node_pool_.MemAddress(vaddr)); }

  absl::Mutex *GetModifyLockPtr() { return &modify_mutex_; }

  bool Update(uint64 key, const char *log, int log_size, int expire_timet = -1);
  bool Update(uint64 key,
              const char *log,
              int log_size,
              int expire_timet,
              UpdateHandler update_handler,
              RecycleHandler recycle_handler = nullptr,
              RecycleWithKeyHandler recycle_with_key_handler = nullptr);

  /**
   * \brief Erase nodes under certain condition immediately
   * \return erased node num
   */
  uint32_t EraseIf(std::function<bool(uint64, KVData *)> recyle_handler);
  // Only mark node as deleted, node will be released when recycle happens
  void Erase(uint64_t key);
  // Mark as deleted only if condition return true
  bool EraseKeyIf(uint64_t key, const std::function<bool(KVData *)> &condition);

  KVData *GetKVDataByAddr(uint32_t addr) {
    if (addr == node_pool_.Null()) { return nullptr; }
    return reinterpret_cast<base::KVData *>(node_pool_.MemAddress(addr));
  }
  uint32_t GetSlabLenByAddr(uint32_t addr) {
    if (addr == node_pool_.Null()) { return 0; }
    return node_pool_.GetSlabLenByVaddr(addr);
  }
  uint32_t MallocAddr(int malloc_size);
  bool MultiSync(const std::vector<KVSyncData> &datas);
  void GetPartKeys(int part_num, int part_id, std::vector<uint64> *keys) {
    hash_map_.GetPartKeys(part_num, part_id, keys);
  }
  void GetKeysAndValueSizes(std::vector<std::pair<uint64_t, size_t>> *results,
                            const std::function<bool(uint64_t, KVData *)> condition = nullptr,
                            const std::function<size_t(uint64_t, KVData *)> value_size_op = nullptr) {
    TraverseOp([results, condition, value_size_op](uint64_t key, KVData *value) {
      if (!condition || condition(key, value)) {
        results->emplace_back(key, value_size_op ? value_size_op(key, value) : value->size());
      }
    });
  };

  std::string GetInfo() const;

  // return total key num including those are marked as deleted
  int64 KeyNum() const { return hash_map_.Size(); }
  // return key num actually in use
  int64 ValidKeyNum() const { return hash_map_.ValidSize(); }
  uint32_t Capacity() const { return capacity_; }
  uint64_t MemLimit() const { return mem_limit_; }
  void SetMemLimit(uint64_t mem_limit) {
    absl::MutexLock lock(&modify_mutex_);
    auto old_limit = mem_limit_;
    mem_limit_ = mem_limit;
    // reset full flags first
    node_pool_.SetAllocTotalFull(false);
    auto alarm = RecycleAlarm();
    LOG(INFO) << "set mem_limit from " << old_limit << " to " << mem_limit << ", recycle alarm: " << alarm;
  }
  uint64_t MemUse() const { return (hash_map_.MemUse() + node_pool_.MemUse()); }
  uint64_t MemAlloc() const { return (hash_map_.MemUse() + node_pool_.MemAlloc()); }
  uint64_t MemInFreeList() const { return node_pool_.MemInFreeList(); }

  void SyncAllKVFrom(MemKV *mem_from, uint32 now_ts);
  void GetSlabs(std::vector<SlabPair> *slabs);
  void GetSlabInfos(std::vector<SlabInfoTuple> *slab_infos) const;

  void TraverseOp(std::function<void(uint64_t, KVData *)> op, std::function<bool()> exit_op = nullptr);

  uint64_t TotalUpdateNum() const { return total_update_key_num_; }

  void Flush() {
    absl::MutexLock lock(&modify_mutex_);
    hash_map_.Flush();
    node_pool_.Flush();
  }

  static constexpr size_t kMaxBatchSize = 16;
  void Get(const uint64 *keys, KVReadData *read_datas, int batch_size) {
    LoopHashMap<uint64_t, uint32_t>::HashNodeT *nodes[kMaxBatchSize];

    for (size_t i = 0; i < batch_size; i++) { nodes[i] = SeekNodeOneStep(keys[i]); }

    bool goon = true;
    while (goon) {
      goon = false;
      for (size_t i = 0; i < batch_size; ++i) {
        if (nodes[i] != nullptr && nodes[i]->key != keys[i]) {
          nodes[i] = NextNodeOneStep(nodes[i]->next);
          goon = true;
        }
      }
    }

    KVData *kv_datas[kMaxBatchSize];
    for (size_t i = 0; i < batch_size; ++i) {
      if (nodes[i] == nullptr) {
        kv_datas[i] = nullptr;
      } else {
        uint32_t vaddr = nodes[i]->value;
        auto data = VaddrToKVData(vaddr);
        __builtin_prefetch(data);
        kv_datas[i] = data;
      }
    }

    for (size_t i = 0; i < batch_size; i++) {
      if (kv_datas[i] != nullptr) {
        if (kv_datas[i]->size() <= 0) {
          LOG(ERROR) << "get one item:" << keys[i] << ", stored_size:" << kv_datas[i]->stored_size
                     << ", realsize:" << kv_datas[i]->size()
                     << ", ptr:" << reinterpret_cast<void *>(kv_datas[i]);
          read_datas[i].size = 0;
          read_datas[i].data = nullptr;
        } else {
          read_datas[i].copy_from(kv_datas[i]);
        }
      } else {
        read_datas[i].size = 0;
        read_datas[i].data = nullptr;
      }
    }
  }

  void BatchGet(const uint64 *keys, KVReadData *read_datas, int batch_size) {
    Get(keys, read_datas, batch_size);
  }

  void BatchGet(const uint64 *keys,
                size_t keys_size,
                std::function<void(size_t, uint64, KVReadData)> handler) {
    KVReadData read_datas[kMaxBatchSize];

    for (size_t i = 0; i < keys_size; i += kMaxBatchSize) {
      int batch = keys_size - i;
      if (batch > kMaxBatchSize) batch = kMaxBatchSize;

      BatchGet(keys + i, read_datas, batch);

      for (int j = 0; j < batch; ++j) { handler(i + j, keys[i + j], read_datas[j]); }
    }
  }

  uint32_t ExpireTs() { return expire_ts_; }

 private:
  KVData *GetCache(uint64 key) {
    auto node = hash_map_.Seek(key);
    if (node == nullptr) { return nullptr; }

    uint32_t vaddr = node->value;
    return reinterpret_cast<KVData *>(node_pool_.MemAddress(vaddr));
  }

  void Recycle(RecycleHandler recycle_hanler = nullptr,
               RecycleWithKeyHandler recycle_with_key_handler = nullptr);

  void Clear();

  bool NeedRecyclePart(uint32_t now_ts);
  bool RecycleAlarm();

 private:
  // when total full, cannot allocate new node
  static constexpr float RECYCLE_ALARM_ALLOC_FULL_RATIO = 1.02;
  // when almost full, trigger recycle when update
  static constexpr float RECYCLE_ALARM_USE_RATIO = 0.95;
  // only recycle one node
  static const uint32_t BATCH_RECYCLE_NODE_SIZE = 2;
  static const uint32_t ALWAYS_SYNC_PART_NUM = 1024;

  LoopHashMap<uint64_t, uint32_t> hash_map_;
  slab::SlabMempool32 node_pool_;

  int32_t part_id_;
  uint32_t capacity_;
  uint64_t mem_limit_;
  uint32_t expire_ts_;
  uint32_t erase_delay_ts_;
  uint32_t last_expire_addr_;

  uint64_t total_update_key_num_;
  uint64_t total_update_byte_size_;

  // restore need evaluate expire_nodes
  uint32_t expire_nodes_;
  uint32_t last_expire_ts_;

  // for write thread_safe
  mutable absl::Mutex modify_mutex_;
  std::queue<std::pair<uint32_t, uint32_t>> expire_addrs_;
  // assume expire time is non-decreasing
  mutable uint32_t cached_node_expire_ts_;

  DISALLOW_COPY_AND_ASSIGN(MemKV);
};

uint32 DefaultUpdateHandler(
    uint32 old_addr, uint64 key, const char *log, int size, int expire_timet, MemKV *table);

class MultiMemKV {
 public:
  // each MemKV's capacity must be within range of int32
  // their capacity sum up to MultiMemKV's capacity, which can be larger than int32_max
  MultiMemKV(const std::string &shm_path,
             int part_num,
             uint64_t capacity,
             int expire_ts,
             uint64_t mem_limit = 0,
             GenerateSlabHandler gen_slab_handler = nullptr)
      : MultiMemKV(
            std::vector<std::string>{shm_path}, part_num, capacity, expire_ts, mem_limit, gen_slab_handler) {}
  MultiMemKV(const std::vector<std::string> &multi_shm_path,
             int part_num,
             uint64_t capacity,
             int expire_ts,
             uint64_t mem_limit = 0,
             GenerateSlabHandler gen_slab_handler = nullptr);
  ~MultiMemKV();

  void InitializePart(int i, GenerateSlabHandler gen_slab_handler);

  KVReadData Get(uint64 key) { return memkv_ptr_vec_[GetPart(key)]->Get(key); }

  KVReadData ReadToBuffer(uint64 key, std::vector<char> *buffer, GetHandler get_handler = nullptr) {
    return memkv_ptr_vec_[GetPart(key)]->ReadToBuffer(key, buffer, get_handler);
  }

  using ReadData = KVReadData;
  static constexpr size_t kMaxBatchSize = 16;

  void Get(const uint64 *keys, KVReadData *read_datas, int batch_size) {
    LoopHashMap<uint64_t, uint32_t>::HashNodeT *nodes[kMaxBatchSize];
    MemKV *kv_parts[kMaxBatchSize];

    for (size_t i = 0; i < batch_size; i++) {
      kv_parts[i] = memkv_ptr_vec_[GetPart(keys[i])];
      nodes[i] = kv_parts[i]->SeekNodeOneStep(keys[i]);
    }

    bool goon = true;
    while (goon) {
      goon = false;
      for (size_t i = 0; i < batch_size; ++i) {
        if (nodes[i] != nullptr && nodes[i]->key != keys[i]) {
          nodes[i] = kv_parts[i]->NextNodeOneStep(nodes[i]->next);
          goon = true;
        }
      }
    }

    KVData *kv_datas[kMaxBatchSize];
    for (size_t i = 0; i < batch_size; ++i) {
      if (nodes[i] == nullptr) {
        kv_datas[i] = nullptr;
      } else {
        uint32_t vaddr = nodes[i]->value;
        auto data = kv_parts[i]->VaddrToKVData(vaddr);
        __builtin_prefetch(data);
        kv_datas[i] = data;
      }
    }

    for (size_t i = 0; i < batch_size; i++) {
      if (kv_datas[i] != nullptr) {
        if (kv_datas[i]->size() <= 0) {
          LOG(ERROR) << "get one item:" << keys[i] << ", stored_size:" << kv_datas[i]->stored_size
                     << ", realsize:" << kv_datas[i]->size()
                     << ", ptr:" << reinterpret_cast<void *>(kv_datas[i]);
          read_datas[i].size = 0;
          read_datas[i].data = nullptr;
        } else {
          read_datas[i].copy_from(kv_datas[i]);
        }
      } else {
        read_datas[i].size = 0;
        read_datas[i].data = nullptr;
      }
    }
  }

  void BatchGet(const uint64 *keys, KVReadData *read_datas, int batch_size) {
    Get(keys, read_datas, batch_size);
  }

  void BatchGet(const uint64 *keys,
                size_t keys_size,
                std::function<void(size_t, uint64, KVReadData)> handler) {
    KVReadData read_datas[kMaxBatchSize];

    for (size_t i = 0; i < keys_size; i += kMaxBatchSize) {
      int batch = keys_size - i;
      if (batch > kMaxBatchSize) batch = kMaxBatchSize;

      BatchGet(keys + i, read_datas, batch);

      for (int j = 0; j < batch; ++j) { handler(i + j, keys[i + j], read_datas[j]); }
    }
  }

  absl::Mutex *GetModifyLockPtr(uint64 key) { return memkv_ptr_vec_[GetPart(key)]->GetModifyLockPtr(); }

  bool Update(uint64 key, const char *log, int log_size, int expire_timet = -1) {
    return memkv_ptr_vec_[GetPart(key)]->Update(key, log, log_size, expire_timet);
  }
  bool Update(uint64 key,
              const char *log,
              int log_size,
              int expire_timet,
              UpdateHandler handler,
              RecycleHandler recycle_handler = nullptr,
              RecycleWithKeyHandler recycle_with_key_handler = nullptr) {
    return memkv_ptr_vec_[GetPart(key)]->Update(
        key, log, log_size, expire_timet, handler, recycle_handler, recycle_with_key_handler);
  }

  uint32_t EraseIf(int part_id, std::function<bool(uint64, KVData *)> erase_handler) {
    return memkv_ptr_vec_[part_id % memkv_part_num_]->EraseIf(erase_handler);
  }
  void Erase(uint64 key) { memkv_ptr_vec_[GetPart(key)]->Erase(key); }
  bool EraseKeyIf(uint64 key, const std::function<bool(KVData *)> &condition) {
    return memkv_ptr_vec_[GetPart(key)]->EraseKeyIf(key, condition);
  }

  bool MultiSync(const std::vector<KVSyncData> &datas);

  // sync all kv from another MultiMemKV, part_num must be equal
  // this function is blocking call
  void SyncAllKVFrom(MultiMemKV *mem_from, int sync_concurrency);

  void GetPartKeys(int shard_id, int part_num, int part_id, std::vector<uint64> *keys) {
    memkv_ptr_vec_[shard_id]->GetPartKeys(part_num, part_id, keys);
  }

  void GetKeysAndValueSizes(std::vector<std::pair<uint64_t, size_t>> *results,
                            const std::function<bool(uint64_t, KVData *)> condition = nullptr,
                            const std::function<size_t(uint64_t, KVData *)> value_size_op = nullptr) {
    for (auto memkv : memkv_ptr_vec_) memkv->GetKeysAndValueSizes(results, condition, value_size_op);
  };

  std::string GetInfo() const;
  uint64_t Capacity() const { return capacity_; }
  uint64_t MemLimit() const { return mem_limit_; }

  // return total key num including those are marked as deleted
  int64 KeyNum() const {
    int64 key_num = 0;
    for (auto mem_kv : memkv_ptr_vec_) key_num += mem_kv->KeyNum();
    return key_num;
  }
  // return key num actually in use
  int64 ValidKeyNum() const {
    int64 key_num = 0;
    for (auto mem_kv : memkv_ptr_vec_) key_num += mem_kv->ValidKeyNum();
    return key_num;
  }
  float OccupancyRate() const {
    static const float kEpsilon = 1e-6;
    float index_rate = KeyNum() / (capacity_ + kEpsilon);
    float mem_rate = MemUse() / (mem_limit_ + kEpsilon);
    return std::max(index_rate, mem_rate);
  }
  float ValidOccupancyRate() const {
    static const float kEpsilon = 1e-6;
    float index_rate = ValidKeyNum() / (capacity_ + kEpsilon);
    float mem_rate = MemUse() / (mem_limit_ + kEpsilon);
    return std::max(index_rate, mem_rate);
  }
  float MemAllocRate() const {
    static const float kEpsilon = 1e-6;
    return MemAlloc() / (mem_limit_ + kEpsilon);
  }
  uint64_t MemUse() const {
    uint64_t mem_use = 0;
    for (auto mem_kv : memkv_ptr_vec_) mem_use += mem_kv->MemUse();
    return mem_use;
  }
  uint64_t MemAlloc() const {
    uint64_t mem_use = 0;
    for (auto mem_kv : memkv_ptr_vec_) mem_use += mem_kv->MemAlloc();
    return mem_use;
  }
  uint64_t TotalMemInFreeList() const {
    uint64_t total_mem = 0;
    for (auto mem_kv : memkv_ptr_vec_) total_mem += mem_kv->MemInFreeList();
    return total_mem;
  }

  const std::string &ShmPath() const { return shm_path_; }
  int PartNum() const { return memkv_part_num_; }

  int GetPart(uint64 key) const { return GetHashWithLevel(key, 1) % memkv_part_num_; }

  void GetPartSlabInfos(int shard_id, std::vector<SlabInfoTuple> *slab_infos) const {
    memkv_ptr_vec_[shard_id]->GetSlabInfos(slab_infos);
  }

  static void GenerateSomeSlabs(std::vector<uint32_t> *slabs_vec);

  void TraverseOp(int shard_id,
                  std::function<void(uint64_t, KVData *)> op,
                  std::function<bool()> exit_op = nullptr) {
    memkv_ptr_vec_[shard_id]->TraverseOp(op, exit_op);
  }

  void SetMemLimit(uint64_t mem_limit) {
    mem_limit_ = mem_limit;
    for (auto mem_kv : memkv_ptr_vec_) mem_kv->SetMemLimit(mem_limit_ / memkv_part_num_);
  }

  MemKV *GetMemKVPart(int shard_id) {
    if (shard_id < 0 || shard_id >= memkv_part_num_) return nullptr;
    return memkv_ptr_vec_[shard_id];
  }

 private:
  const std::string &GetMemKVShmPath(int part_id) const {
    return shm_path_vec_[part_id % shm_path_vec_.size()];
  }

 private:
  int memkv_part_num_;
  uint64_t capacity_;
  uint64_t mem_limit_;
  std::string shm_path_;
  uint32_t expire_ts_;

  std::vector<MemKV *> memkv_ptr_vec_;

  // When using one data directory, this member has one element, which is the same as shm_path_
  // When using multiple data directories, this member has more than one element, and the first element is the
  // same as shm_path_
  std::vector<std::string> shm_path_vec_;
};

}  // namespace base
