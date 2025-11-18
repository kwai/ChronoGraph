// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "memtable/pool/mempool_helper.h"
#include "memtable/pool/shm_pool.h"
#include "memtable/util/constant.h"

#include "base/common/gflags.h"
#include "base/common/logging.h"

ABSL_DECLARE_FLAG(uint64, memtable_block_max_mem_size);

namespace base {
namespace slab {

class SlabMempool32 {
 public:
  typedef uint32_t Vaddr;
  static const Vaddr NULL_VADDR;

  struct MemBlock {
    char *start;
    uint32_t cur_index;
    uint32_t item_size;
    uint32_t free_item;
    uint32_t max_item_num;
    uint32_t used_node_num = 0;
  };

  struct MemSlab {
    uint32_t block_num = 0;
    int32_t cur_block_idx = -1;
    uint32_t free_list = -1;
    uint32_t node_capacity;
    uint32_t used_node_num;
    uint32_t free_list_node_num;
  };

 public:
  SlabMempool32();
  virtual ~SlabMempool32();

  bool Create(const std::vector<uint32_t> &slabs_vec,
              uint32_t max_block_item_num,
              const std::string &shm_path,
              const int32_t part_id);

  void Reset();

  inline Vaddr Malloc(int size) {
    int slab_idx = FindSlab(size);
    if (slab_idx < 0) {
      LOG(WARNING) << "failed to find item_size from slabs:" << size;
      return NULL_VADDR;
    }

    uint32_t item_size = slab_lens_[slab_idx];
    MemSlab &slab = mem_slabs_[slab_idx];
    // reuse block always no freelist
    if (slab.free_list != NULL_VADDR) { return MallocFromFreelist(&slab, item_size); }

    return MallocFromBlock(&slab, item_size);
  }

  inline bool Free(const Vaddr &addr) {
    if (addr == NULL_VADDR) { return true; }

    uint32_t block_idx = 0;
    uint32_t offset = 0;
    SplitVaddr(addr, &block_idx, &offset);

    if (block_idx >= *block_num_) {
      LOG(FATAL) << "invalid addr:" << addr << ", block_idx:" << block_idx << ", block_size:" << *block_num_;
      return false;
    }

    MemBlock &block = mem_blocks_[block_idx];
    if (offset >= block.free_item) {
      LOG(FATAL) << "invalid addr:" << addr << ", offset:" << offset << ", free_item:" << block.free_item
                 << ", Info:" << GetInfo();
      return false;
    }

    int slab_idx = FindSlab(block.item_size);
    if (slab_idx < 0 || (size_t)slab_idx >= *slab_len_) {
      LOG(FATAL) << "invalid slab_idx:" << slab_idx << ", slab_size:" << *slab_len_
                 << ", item_size:" << block.item_size << ", addr:" << addr;
      return false;
    }

    MemSlab &slab = mem_slabs_[slab_idx];

    *mem_consume_ -= block.item_size;
    *node_consume_ -= block.item_size;
    --(*node_num_);
    --slab.used_node_num;
    --block.used_node_num;
    CHECK_GE(block.used_node_num, 0U);

    VLOG(10) << "memconsume:" << *mem_consume_ << ",nodeconsume:" << *node_consume_
             << ",item_size:" << block.item_size;
    return PushIntoFreeList(addr, &slab);
  }

  inline bool PushIntoFreeList(const Vaddr &addr, MemSlab *slab) {
    uint32_t *p_addr = reinterpret_cast<uint32_t *>(MemAddress(addr));
    if (p_addr == nullptr) {
      LOG(FATAL) << "invalid vaddr:" << addr;
      return false;
    }

    *p_addr = slab->free_list;
    slab->free_list = addr;

    ++slab->free_list_node_num;
    VLOG(10) << reinterpret_cast<void *>(this) << "," << reinterpret_cast<void *>(slab)
             << ", push into free list, addr:" << addr << ", next_addr:" << *p_addr
             << ", used_num:" << slab->used_node_num << ", free_num:" << slab->free_list_node_num;
    return true;
  }

  Vaddr Null() const { return NULL_VADDR; }
  uint64_t MemAlloc() const { return *mem_allocated_; }
  uint64_t MemUse() const { return *mem_consume_; }
  uint64_t MemInFreeList() const {
    uint64_t mem_in_free_list = 0;
    for (uint32 i = 0; i < *slab_len_; i++) {
      uint32_t len = slab_lens_[i];
      auto &slab = mem_slabs_[i];
      if (slab.free_list != NULL_VADDR && slab.free_list_node_num > 0) {
        mem_in_free_list += len * slab.free_list_node_num;
      }
    }
    return mem_in_free_list;
  }
  std::string GetInfo() const;

  // For High Performance
  inline void *MemAddress(const Vaddr &addr) {
    if (addr == NULL_VADDR) { return nullptr; }

    uint32_t block_idx = 0;
    uint32_t offset = 0;
    SplitVaddr(addr, &block_idx, &offset);

    const MemBlock &block = mem_blocks_[block_idx];

    // void* cannot used for calculation
    return block.start + offset * block.item_size;
  }

  KVReadData ReadToBuffer(const Vaddr &addr, std::vector<char> *buffer, GetHandler get_handler = nullptr) {
    if (addr == NULL_VADDR) { return {}; }

    uint32_t block_idx = 0;
    uint32_t offset = 0;
    SplitVaddr(addr, &block_idx, &offset);

    const MemBlock &block = mem_blocks_[block_idx];
    // Avoid read amplification when item_size > value_size
    auto kv_data = reinterpret_cast<KVData *>(block.start + offset * block.item_size);
    auto read_size =
        get_handler ? get_handler(kv_data->data(), kv_data->size()) : kv_data->stored_size - sizeof(KVData);
    if (read_size <= 0) {
      LOG_EVERY_N_SEC(ERROR, 10) << "[memtable] stored value size <= 0";
      return {};
    }
    if (read_size > kv_data->size()) {
      LOG_EVERY_N_SEC(ERROR, 10) << "[memtable] read too large size (" << read_size
                                 << ") > actual read size (" << kv_data->size() << ")";
      return {};
    }
    if (buffer->size() < read_size) {
      LOG_EVERY_N_SEC(INFO, 10) << "[memtable] buffer resize from " << buffer->size() << " to " << read_size;
      buffer->resize(read_size);
    }
    int fd = block_fd_vec_[block_idx];
    if (fd < 0) {
      LOG_EVERY_N_SEC(ERROR, 10) << "[memtable] invalid block fd < 0";
      return {};
    }
    auto ret = pread(fd, buffer->data(), read_size, offset * block.item_size + sizeof(KVData));
    if (ret != read_size) {
      LOG_EVERY_N_SEC(ERROR, 10) << "[memtable] read_size(" << ret << ") not as expected(" << read_size
                                 << ")";
      return {};
    }
    KVReadData read_data;
    read_data.data = buffer->data();
    read_data.size = read_size;
    read_data.expire_timet = kv_data->expire_timet;
    return read_data;
  }

  inline uint32_t GetSlabLenByVaddr(const Vaddr &addr) {
    if (addr == NULL_VADDR) { return 0; }

    uint32_t block_idx = 0;
    uint32_t offset = 0;
    SplitVaddr(addr, &block_idx, &offset);

    return mem_blocks_[block_idx].item_size;
  }

  inline uint32_t GetBlockIdxFromVaddr(const Vaddr &vaddr) { return vaddr >> *idx2_bits_; }

  inline void SplitVaddr(const Vaddr &addr, uint32_t *block_idx, uint32_t *offset) const {
    *block_idx = addr >> *idx2_bits_;
    *offset = addr & *idx2_mask_;
  }

  inline Vaddr MakeVaddr(uint32_t block_idx, uint32_t offset) {
    return ((block_idx << *idx2_bits_) | offset);
  }

  inline int FindSlab(uint32_t size) {
    size_t beg = 0;
    size_t end = *slab_len_;
    while (beg < end) {
      size_t cur = beg + ((end - beg) >> 1);
      uint32_t cur_size = slab_lens_[cur];
      if (cur_size > size) {
        if (cur == 0 || slab_lens_[cur - 1] < size) return static_cast<int>(cur);
        end = cur;
      } else if (cur_size < size) {
        beg = cur + 1;
      } else {
        return static_cast<int>(cur);
      }
    }
    return -1;
  }

  void GetSlabs(std::vector<SlabPair> *slabs);
  void GetSlabInfos(std::vector<SlabInfoTuple> *slab_infos) const;

  void SetAllocTotalFull(bool full = true) { alloc_total_full_ = full; }

  void Flush() {
    if (mem_pool_) { mem_pool_->Flush(); }
  }

 protected:
  inline Vaddr MallocFromFreelist(MemSlab *slab, uint32_t item_size) {
    void *node_ptr = MemAddress(slab->free_list);
    if (node_ptr == nullptr) {
      LOG(FATAL) << "vaddr's node is null, something is wrong!" << slab->free_list << ",\nInfo:" << GetInfo();
      return NULL_VADDR;
    }

    // assume each item_size >= 4, free item store next_vaddr
    uint32_t *next_node_vaddr = static_cast<uint32_t *>(node_ptr);
    uint32_t ret_vaddr = slab->free_list;
    slab->free_list = *next_node_vaddr;

    ++(*node_num_);
    ++slab->used_node_num;
    --slab->free_list_node_num;
    *mem_consume_ += item_size;
    *node_consume_ += item_size;

    uint32_t block_idx = 0;
    uint32_t offset = 0;
    SplitVaddr(ret_vaddr, &block_idx, &offset);
    ++mem_blocks_[block_idx].used_node_num;

    LOG_EVERY_N_SEC(INFO, 10) << reinterpret_cast<void *>(this) << "," << reinterpret_cast<void *>(slab)
                              << ", malloc from free list, addr:" << ret_vaddr << ", item_size:" << item_size
                              << ", free_list:" << slab->free_list
                              << ", used_node_num:" << slab->used_node_num
                              << ", free_num:" << slab->free_list_node_num;
    return ret_vaddr;
  }

  inline Vaddr MallocFromBlock(MemSlab *slab, uint32_t item_size) {
    if (slab->block_num == 0) { return MallocFromNewBlock(slab, item_size); }

    size_t last_block_id = slab->cur_block_idx;
    if (last_block_id >= *block_num_) {
      LOG(FATAL) << "invalid block idx: " << last_block_id << ", block_size:" << *block_num_
                 << ", part_id_:" << *part_id_ << ", item_size:" << item_size;
      return NULL_VADDR;
    }

    MemBlock &block = mem_blocks_[last_block_id];
    if (block.free_item < block.max_item_num) {
      uint32_t vaddr = MakeVaddr(last_block_id, block.free_item);
      ++block.free_item;
      ++block.used_node_num;
      ++(*node_num_);
      ++slab->used_node_num;
      *mem_consume_ += item_size;
      *node_consume_ += item_size;
      return vaddr;
    }

    return MallocFromNewBlock(slab, item_size);
  }

  inline Vaddr MallocFromNewBlock(MemSlab *slab, uint32_t item_size) {
    if (alloc_total_full_) {
      LOG_EVERY_N_SEC(WARNING, 10) << "slab is total full, can not alloc new block"
                                   << ", only malloc from freelist"
                                   << ", part:" << *part_id_ << ", alloc:" << *mem_allocated_
                                   << ", use:" << *mem_consume_;
      return NULL_VADDR;
    }
    // exceed max block num, fail fast
    if (*block_num_ >= *max_block_num_) {
      LOG(FATAL) << "block_size:" << *block_num_ << " exceed max block num:" << *max_block_num_;
      return NULL_VADDR;
    }
    MemBlock new_block;
    uint64_t n_bytes = CalcBlockSize(&new_block, item_size);
    MemPool *now_pool = nullptr;
    uint32_t index_offset = 0;
    now_pool = mem_pool_;
    int fd = -1;
    new_block.start = reinterpret_cast<char *>(now_pool->Malloc(n_bytes, &fd));
    if (new_block.start == nullptr) {
      LOG(FATAL) << "failed to malloc new block, size:" << n_bytes << ", item_size:" << item_size;
      return NULL_VADDR;
    }

    *node_capacity_ += new_block.max_item_num;
    slab->node_capacity += new_block.max_item_num;

    new_block.cur_index = now_pool->IndexAt() + index_offset;
    new_block.item_size = item_size;
    new_block.free_item = 1U;  // 0 for now use
    new_block.used_node_num = 1;

    // find earliest free block
    uint32 block_idx = 0;
    for (; block_idx < *block_num_; block_idx++) {
      if (mem_blocks_[block_idx].start == nullptr) break;
    }

    // latest block
    if (block_idx >= *block_num_) { (*block_num_)++; }
    mem_blocks_[block_idx] = new_block;
    block_fd_vec_[block_idx] = fd;

    slab->cur_block_idx = block_idx;
    slab->block_num += 1;

    *mem_allocated_ += n_bytes;
    *mem_consume_ += item_size;
    *node_consume_ += item_size;

    ++(*node_num_);
    ++slab->used_node_num;
    return MakeVaddr(block_idx, 0);
  }

  bool CreateSlabs(const std::vector<uint32_t> &slabs_vec);
  bool CreateBlocks();
  void ComputeSplitBits(uint32_t max_block_item_num, uint32_t *idx1_bits, uint32_t *idx2_bits);
  uint64_t CalcBlockSize(MemBlock *block, uint32_t item_size);

  uint32_t MetaSize(uint32_t idx1_bits, uint32_t slabs_len) {
    uint32_t max_block_num = 1 << idx1_bits;
    return (sizeof(uint32_t) * 13 + sizeof(uint64_t) * 3 + sizeof(uint32_t) * slabs_len +
            sizeof(MemSlab) * slabs_len + sizeof(MemBlock) * max_block_num);
  }

  bool RestoreMeta(uint32_t idx1_bits, uint32_t slabs_len);
  void MallocMeta(uint32_t idx1_bits, uint32_t slabs_len);
  void LoadMeta(void *start, uint32_t slabs_len, uint32_t max_block_num);

  bool RestoreCheckValue();

 private:
  static const uint32_t MIN_SLAB_ITEM_SIZE = 4;
  static const uint32_t SUGGUEST_MAX_SLABS_LEN = 40;
  static const uint32_t MIN_IDX2_BITS = 10;
  static const uint32_t MAX_IDX2_BITS = 20;
  static const uint32_t REUSE_BLOCK_ITEM_SIZE_LIMIT = 64;
  static const uint32_t MAX_BLOCK_NUM = 1 << (32 - MIN_IDX2_BITS);
  static const uint32_t SLAB_MAGIC_NUM = 0xF1F2F3F4;
  static const uint32_t META_INDEX = 0;

  // valid guard
  uint32_t *magic_num_;

  // fixed max length when create
  uint32_t *slab_lens_;
  MemSlab *mem_slabs_;
  MemBlock *mem_blocks_;

  // block_idx | offset <==> idx1_bits_ | idx2_bits_
  uint32_t *idx1_bits_;
  uint32_t *idx2_bits_;
  uint32_t *idx2_mask_;

  uint32_t *max_block_num_;
  uint32_t *max_block_item_num_;

  // Mem partition
  int32_t *part_id_;

  uint32_t *block_num_;
  uint32_t *slab_len_;
  uint32_t *block_capacity_;
  uint32_t *node_num_;
  uint32_t *node_capacity_;

  uint64_t *mem_allocated_;
  uint64_t *mem_consume_;
  uint64_t *node_consume_;

  // Not Meta Varirable
  MemPool *mem_pool_ = nullptr;
  bool alloc_total_full_ = false;

  std::vector<int> block_fd_vec_;
};

}  // namespace slab
}  // namespace base
