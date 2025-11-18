// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "memtable/slab/slab_mempool32.h"

#include <algorithm>
#include <cstring>
#include "base/common/logging.h"

ABSL_FLAG(uint64, memtable_block_max_mem_size, 16 * 1024 * 1024, "max block max mem size");

namespace base {
namespace slab {

const SlabMempool32::Vaddr SlabMempool32::NULL_VADDR = 0xFFFFFFFF;

SlabMempool32::SlabMempool32() {
  LOG(INFO) << "memtable_block_max_mem_size: " << absl::GetFlag(FLAGS_memtable_block_max_mem_size);
  Reset();
}

SlabMempool32::~SlabMempool32() {
  for (size_t i = 0; i < *block_num_; i++) { mem_pool_->Free(mem_blocks_[i].start); }
  if (node_consume_) { mem_pool_->Free(node_consume_); }
  MempoolHelper::FreePool(mem_pool_);
}

bool SlabMempool32::RestoreMeta(uint32_t idx1_bits, uint32_t slabs_len) {
  uint64_t size = 0;
  auto start = mem_pool_->GetAddrByIndex(META_INDEX, &size);

  auto max_block_num = 1 << idx1_bits;
  auto meta_size = MetaSize(idx1_bits, slabs_len);
  LoadMeta(start, slabs_len, max_block_num);
  LOG(INFO) << "slab mempool restore, meta file size = " << size << ", max_block_num = " << max_block_num
            << ", meta_size = " << meta_size;
  if (*magic_num_ != SLAB_MAGIC_NUM) {
    LOG(ERROR) << "magic num not equal, " << *magic_num_ << " vs " << SLAB_MAGIC_NUM
               << ", offset = " << (reinterpret_cast<char *>(magic_num_) - reinterpret_cast<char *>(start));
    return false;
  }
  if (size != meta_size) {
    LOG(ERROR) << "meta size not qual, file_size: " << size << ", meta_size:" << meta_size;
    return false;
  }

  return true;
}

void SlabMempool32::MallocMeta(uint32_t idx1_bits, uint32_t slabs_len) {
  auto size = MetaSize(idx1_bits, slabs_len);
  auto start = reinterpret_cast<uint32_t *>(mem_pool_->Malloc(size));
  memset(start, 0, size);

  uint32_t max_block_num = 1 << idx1_bits;
  LoadMeta(start, slabs_len, max_block_num);

  *idx1_bits_ = idx1_bits;
  *idx2_bits_ = sizeof(Vaddr) * 8 - idx1_bits;
  *idx2_mask_ = (1U << *idx2_bits_) - 1;
  *max_block_item_num_ = 1U << *idx2_bits_;
  *magic_num_ = SLAB_MAGIC_NUM;
}

void SlabMempool32::LoadMeta(void *start, uint32_t slabs_len, uint32_t max_block_num) {
  node_consume_ = reinterpret_cast<uint64_t *>(start);
  mem_consume_ = node_consume_ + 1;
  mem_allocated_ = mem_consume_ + 1;

  node_capacity_ = reinterpret_cast<uint32_t *>(mem_allocated_ + 1);
  node_num_ = node_capacity_ + 1;
  block_capacity_ = node_num_ + 1;
  slab_len_ = block_capacity_ + 1;
  block_num_ = slab_len_ + 1;

  part_id_ = reinterpret_cast<int32_t *>(block_num_ + 1);

  max_block_item_num_ = reinterpret_cast<uint32_t *>(part_id_ + 1);
  max_block_num_ = max_block_item_num_ + 1;

  idx2_mask_ = max_block_num_ + 1;
  idx2_bits_ = idx2_mask_ + 1;
  idx1_bits_ = idx2_bits_ + 1;

  slab_lens_ = idx1_bits_ + 1;
  mem_slabs_ = reinterpret_cast<MemSlab *>(slab_lens_ + slabs_len);
  mem_blocks_ = reinterpret_cast<MemBlock *>(mem_slabs_ + slabs_len);

  magic_num_ = reinterpret_cast<uint32_t *>(mem_blocks_ + max_block_num);

  block_fd_vec_.resize(max_block_num, -1);
}

bool SlabMempool32::RestoreCheckValue() {
  for (size_t i = 0; i < *block_num_; i++) {
    auto &block = mem_blocks_[i];
    int block_index = block.cur_index;

    uint64_t size = 0;
    // normal pool
    if (block_index < MAX_BLOCK_NUM) {
      block.start =
          reinterpret_cast<char *>(mem_pool_->GetAddrByIndex(block_index, &size, &block_fd_vec_[i]));
    }

    if (block.start == nullptr) {
      LOG(FATAL) << "restore nullptr, part:" << *part_id_ << ", block:" << i << ", index:" << block_index;
      return false;
    }

    if (size != (block.item_size * block.max_item_num)) {
      LOG(FATAL) << "restore failed for block " << i << ", index:" << block_index
                 << ", addr:" << static_cast<void *>(block.start) << ", item_size:" << block.item_size
                 << ", max_item_num:" << block.max_item_num << ", size: " << size;
      return false;
    }

    LOG(INFO) << "Restore block " << i << " of part " << *part_id_ << ", block index: " << block.cur_index
              << ", item size: " << block.item_size << ", free item: " << block.free_item
              << ", max item num: " << block.max_item_num << ", used node num: " << block.used_node_num
              << ", fd: " << block_fd_vec_[i] << ", size: " << size;
  }

  LOG(INFO) << "restore value check of part " << *part_id_ << " done.\n" << GetInfo();
  return true;
}

bool SlabMempool32::Create(const std::vector<uint32_t> &slabs_vec,
                           uint32_t max_block_item_num,
                           const std::string &shm_path,
                           const int32_t part_id) {
  mem_pool_ = MempoolHelper::MallocPool(shm_path, part_id);

  uint32_t idx1_bits = 0;
  uint32_t idx2_bits = 0;
  ComputeSplitBits(max_block_item_num, &idx1_bits, &idx2_bits);
  auto slabs_len = slabs_vec.size();
  LOG(INFO) << "slab mempool create: max_block = " << max_block_item_num << ", idx1_bits = " << idx1_bits
            << ", idx2_bits = " << idx2_bits << ", slabs_len = " << slabs_len;

  if ((mem_pool_->CouldRestore() && mem_pool_->Restore())) {
    if (!RestoreMeta(idx1_bits, slabs_len)) {
      LOG(ERROR) << "restore meta failed, part:" << part_id;
      return false;
    }

    // abort if fail
    if (!RestoreCheckValue()) {
      LOG(ERROR) << "restore check value failed, part:" << part_id;
      return false;
    }

    return true;
  }

  MallocMeta(idx1_bits, slabs_len);

  *part_id_ = part_id;

  if (!CreateSlabs(slabs_vec)) {
    LOG(FATAL) << "create slabs failed! slabs_len:" << slabs_len
               << ", max_block_item_num:" << max_block_item_num;
    return false;
  }

  return CreateBlocks();
}

void SlabMempool32::Reset() {
  idx1_bits_ = nullptr;
  idx2_bits_ = nullptr;
  idx2_mask_ = nullptr;

  max_block_num_ = nullptr;
  max_block_item_num_ = nullptr;

  block_num_ = nullptr;
  slab_len_ = nullptr;
  block_capacity_ = nullptr;
  node_num_ = nullptr;
  node_capacity_ = nullptr;

  mem_allocated_ = nullptr;
  mem_consume_ = nullptr;
  node_consume_ = nullptr;

  part_id_ = nullptr;

  magic_num_ = nullptr;

  if (mem_pool_) {
    MempoolHelper::FreePool(mem_pool_);
    mem_pool_ = nullptr;
  }

  block_fd_vec_.clear();
}

void SlabMempool32::ComputeSplitBits(uint32_t max_block_item_num, uint32_t *idx1_bits, uint32_t *idx2_bits) {
  while ((1U << *idx2_bits) < max_block_item_num) { (*idx2_bits)++; }
  if (*idx2_bits < MIN_IDX2_BITS) {
    *idx2_bits = MIN_IDX2_BITS;
  } else if (*idx2_bits > MAX_IDX2_BITS) {
    *idx2_bits = MAX_IDX2_BITS;
  }

  *idx1_bits = sizeof(Vaddr) * 8 - *idx2_bits;
}

bool SlabMempool32::CreateSlabs(const std::vector<uint32_t> &slabs_vec) {
  auto slabs_len = slabs_vec.size();
  if (slabs_len == 0) {
    LOG(FATAL) << "invalid create slabs param, slab_len = " << slabs_len;
    return false;
  }

  if (slabs_len > SUGGUEST_MAX_SLABS_LEN) {
    LOG(WARNING) << "warning: slabs_len: " << slabs_len
                 << " is bigger than sugguest: " << SUGGUEST_MAX_SLABS_LEN;
  }

  for (uint32_t i = 0; i < slabs_len; i++) {
    uint32_t item_size = slabs_vec[i];
    if (item_size < MIN_SLAB_ITEM_SIZE) {
      LOG(FATAL) << "slab item size: " << item_size << " should be no less than: " << MIN_SLAB_ITEM_SIZE;
      continue;
    }
    slab_lens_[i] = item_size;
  }

  std::sort(slab_lens_, slab_lens_ + slabs_len);

  for (size_t i = 0; i < slabs_len; i++) {
    mem_slabs_[i].block_num = 0;
    mem_slabs_[i].cur_block_idx = -1;
    mem_slabs_[i].free_list = NULL_VADDR;
    mem_slabs_[i].node_capacity = 0;
    mem_slabs_[i].used_node_num = 0;
    mem_slabs_[i].free_list_node_num = 0;
  }
  *slab_len_ = slabs_len;

  uint64_t slab_len_size = slabs_len * sizeof(uint32_t);
  *mem_allocated_ += slab_len_size;
  *mem_consume_ += slab_len_size;

  uint64_t slab_size = slabs_len * sizeof(MemSlab);
  *mem_allocated_ += slab_size;
  *mem_consume_ += slab_size;
  LOG(INFO) << "create slabs done, slab_lens:" << slabs_len;
  return true;
}

// Blocks can not reallocate
bool SlabMempool32::CreateBlocks() {
  *max_block_num_ = 1 << *idx1_bits_;
  *block_capacity_ = 1 << *idx1_bits_;

  *block_num_ = 0;

  *mem_allocated_ += *block_capacity_ * (sizeof(MemBlock) + 4);
  *mem_consume_ += *block_capacity_ * (sizeof(MemBlock) + 4);
  return true;
}

uint64_t SlabMempool32::CalcBlockSize(MemBlock *block, uint32_t item_size) {
  uint32_t sug_item_num = absl::GetFlag(FLAGS_memtable_block_max_mem_size) / item_size;
  block->max_item_num = (sug_item_num < *max_block_item_num_) ? sug_item_num : *max_block_item_num_;

  return static_cast<uint64_t>(item_size) * block->max_item_num;
}

void SlabMempool32::GetSlabs(std::vector<SlabPair> *slabs) {
  for (size_t i = 0; i < *slab_len_; i++) {
    auto node_num = mem_slabs_[i].used_node_num + mem_slabs_[i].free_list_node_num;
    auto block_num = mem_slabs_[i].block_num;

    uint32 capacity = 0;
    if (block_num == 0) {
      MemBlock block;
      CalcBlockSize(&block, slab_lens_[i]);
      capacity = block.max_item_num;
    } else {
      capacity = mem_slabs_[i].node_capacity / block_num;
    }

    slabs->emplace_back(SlabPair(slab_lens_[i], node_num, block_num, capacity));
  }
}

void SlabMempool32::GetSlabInfos(std::vector<SlabInfoTuple> *slab_infos) const {
  slab_infos->clear();
  for (size_t i = 0; i < *slab_len_; i++) {
    slab_infos->emplace_back(SlabInfoTuple(slab_lens_[i],
                                           mem_slabs_[i].block_num,
                                           mem_slabs_[i].node_capacity,
                                           mem_slabs_[i].used_node_num,
                                           mem_slabs_[i].free_list_node_num));
  }
}

std::string SlabMempool32::GetInfo() const {
  std::ostringstream os;

  os << "SlabMempool32:" << std::endl;
  os << "Idx1:" << *idx1_bits_ << ", Idx2:" << *idx2_bits_ << ", max_block_num:" << *max_block_num_
     << ", max_block_item_num:" << *max_block_item_num_ << std::endl;
  os << "BlockCapacity:" << *block_capacity_ << ", BlockSize:" << *block_num_
     << ", NodeCapacity:" << *node_capacity_ << ", NodeNum:" << *node_num_ << std::endl;
  os << "MemAllocated:" << *mem_allocated_ << ", MemConsume:" << *mem_consume_
     << ", NodeConsumeMem:" << *node_consume_ << ", TotalFull:" << alloc_total_full_ << std::endl;

  os << std::endl << "Slabs:" << *slab_len_ << std::endl;
  for (size_t i = 0; i < *slab_len_; i++) {
    uint32_t len = slab_lens_[i];
    auto &slab = mem_slabs_[i];
    os << "Slab_" << len << ": BlockNum:" << slab.block_num << ", CurBlockIdx:" << slab.cur_block_idx;
    if (slab.cur_block_idx != -1) {
      auto &cur_block = mem_blocks_[slab.cur_block_idx];
      os << ", BlockFreeItem:" << cur_block.free_item << ", BlockMaxItem:" << cur_block.max_item_num
         << ", BlockUsedNodeNum:" << cur_block.used_node_num;
    }
    os << ", NodeCapacity:" << slab.node_capacity << ", UsedNodeNum:" << slab.used_node_num
       << ", FreeListNodeNum:" << slab.free_list_node_num << ", FreeAddr:" << slab.free_list << std::endl;
  }
  return os.str();
}

}  // namespace slab
}  // namespace base
