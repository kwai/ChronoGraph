// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "base/hash/hash.h"
#include "memtable/pool/mempool_helper.h"
#include "memtable/slab/slab_mempool32.h"
#include "memtable/util/constant.h"

namespace base {

// multi-read while one-write hash map
template <typename KeyT, typename ValueT>
class LoopHashMap {
 public:
  typedef TemplateHashNodeT<KeyT, ValueT> HashNodeT;

  LoopHashMap() {
    hash_table_ = nullptr;
    hash_size_ = nullptr;
    capacity_ = nullptr;
    valid_size_ = nullptr;
    start_entry_ = nullptr;
    next_entry_ = nullptr;
    memset(block_ptrs_, 0, sizeof(block_ptrs_));
    block_num_ = nullptr;
  }

  ~LoopHashMap() {
    if (hash_table_) { pool_->Free(hash_table_); }

    for (size_t i = 0; i < *block_num_; i++) {
      if (block_ptrs_[i]) { pool_->Free(block_ptrs_[i]); }
    }

    // free meta
    if (block_num_) { pool_->Free(block_num_); }

    slab::MempoolHelper::FreePool(pool_);
  }

  // create after construction
  bool Create(uint64_t node_num, float hash_ratio, const std::string &shm_path, const int32_t part_id) {
    pool_ = slab::MempoolHelper::MallocPool(shm_path, part_id);

    // return false if fail
    if (pool_->CouldRestore() && pool_->Restore()) {
      if (!RestoreMeta()) {
        LOG(ERROR) << "restore meta failed, " << part_id;
        return false;
      }

      if (!RestoreCheckValue()) {
        LOG(ERROR) << "restore check value failed, " << part_id;
        return false;
      }
      LOG(INFO) << "restore success";
      return true;
    }

    MallocMeta();

    uint64_t hash_size = node_num * hash_ratio;
    *capacity_ = CeilCapacity(node_num);

    auto max_used_block_num = (((*capacity_ - 1) & BLOCK_INDEX_MASK) >> BLOCK_INDEX_BITS) + 1;
    LOG(INFO) << "try to allocate all memory (total " << max_used_block_num
              << " blocks) for hashmap on initialization ...";
    while (*block_num_ < max_used_block_num) {
      if (!NewBlock()) {
        LOG(ERROR) << "alloc block memory failed, " << part_id;
        return false;
      }
    }

    return Resize(hash_size);
  }

  void Reset() {
    hash_table_ = nullptr;
    hash_size_ = nullptr;
    capacity_ = nullptr;
    valid_size_ = nullptr;
    start_entry_ = nullptr;
    next_entry_ = nullptr;
    memset(block_ptrs_, 0, sizeof(block_ptrs_));
    block_num_ = nullptr;

    if (pool_ != nullptr) {
      slab::MempoolHelper::FreePool(pool_);
      pool_ = nullptr;
    }
  }

  HashNodeT *Seek(const KeyT &key) const {
    uint32_t index = GetBucket(key);
    return GetNode(index, key);
  }

  HashNodeT *SeekNodeOneStep(const KeyT &key) const {
    uint32_t index = GetBucket(key);
    return GetNode(hash_table_[index]);
  }

  // first insert case: default value is not right -> FirstInsert
  // update case: right == Seek & Change Value
  bool Update(const KeyT &key, const ValueT &value) {
    auto node = GetOrInsertNode(key);
    node->value = value;
    return true;
  }

  bool FirstInsert(const KeyT &key, const ValueT &value) {
    auto addr = NewNode();
    auto node = GetNode(addr);
    node->value = value;
    return (InsertNode(key, addr) != nullptr);
  }

  HashNodeT *GetNode(uint32_t addr) const {
    if (addr == NULL_ADDR) { return nullptr; }
    uint32_t block = ((addr & BLOCK_INDEX_MASK) >> BLOCK_INDEX_BITS);
    uint32_t index = (addr & ADDR_INDEX_MASK);
    return &block_ptrs_[block][index];
  }

  // renew node, move to start
  HashNodeT *RenewNode(const KeyT &key) {
    uint32_t index = GetBucket(key);

    // Try find node in hashtable and replace it
    uint32_t addr = hash_table_[index];
    HashNodeT *node = GetNode(addr);

    HashNodeT *pre = nullptr;
    while (node != nullptr && node->key != key) {
      pre = node;
      // next = node->next
      addr = node->next;
      node = GetNode(addr);
    }

    // not found
    if (node == nullptr) { return nullptr; }

    CHECK(node->value != slab::SlabMempool32::NULL_VADDR)
        << "should not renew node whose value is NULL_VADDR";

    // Create new node
    auto new_addr = NewNode();
    auto new_node = GetNode(new_addr);

    new_node->key = node->key;
    new_node->value = node->value;
    new_node->next = node->next;

    // first node
    if (pre == nullptr) {
      hash_table_[index] = new_addr;
    } else {
      pre->next = new_addr;
    }

    node->value = slab::SlabMempool32::NULL_VADDR;
    node->next = NULL_ADDR;
    return node;
  }

  HashNodeT *InsertNode(const KeyT &key, uint32_t addr) {
    auto index = GetBucket(key);
    return InsertNode(index, key, addr);
  }

  // delete by key
  HashNodeT *Pop(const KeyT &key) {
    uint32_t index = GetBucket(key);
    return DeleteNode(index, key);
  }

  /** \brief Invalidate the key, remove it from linked list
   * This function is like (TraverseOp - ConstTraverseOp)
   * It only returns the value of the key, and the caller is responsible for
   * deleting it by the same way of TraverseOp.
   * \return NULL_VADDR if not found.
   */
  ValueT EraseKey(const KeyT &key) {
    uint32_t i = GetBucket(key);
    uint32_t addr = hash_table_[i];
    auto node = GetNode(addr);
    if (node == nullptr) return slab::SlabMempool32::NULL_VADDR;

    HashNodeT *prev_node = nullptr;
    do {
      auto cur_value = node->value;
      auto next_node = GetNode(node->next);
      // node is marked as deleted, do --(*valid_size_) here instead of ReleaseNode
      if (node->key == key) {
        // RemoveNodeFromLinkList to avoid the reused node being linked in two link list
        if (prev_node == nullptr) {
          hash_table_[i] = node->next;  // first node
        } else {
          prev_node->next = node->next;
        }
        node->next = NULL_ADDR;
        // ensure current node value is NULL_VADDR
        node->value = slab::SlabMempool32::NULL_VADDR;
        --(*valid_size_);
        // current node is removed, keep prev_node not changed
        return cur_value;
      } else {
        prev_node = node;
      }
      node = next_node;
    } while (node != nullptr);
    return slab::SlabMempool32::NULL_VADDR;
  }

  // NOT thread safe: first add node then change value
  ValueT &operator[](const KeyT &key) {
    auto node = GetOrInsertNode(key);
    return node->value;
  }

  // const traverse, do not modify node, linklist etc.
  void ConstTraverseOp(std::function<void(KeyT, ValueT)> op,
                       std::function<bool()> exit_op,
                       uint32_t *max_link_len,
                       uint32_t *min_link_len,
                       uint32_t *bucket_num) const {
    for (uint32_t i = 0; i < *hash_size_; i++) {
      uint32_t addr = hash_table_[i];
      auto node = GetNode(addr);
      if (node == nullptr) continue;

      uint32_t len = 0;
      do {
        op(node->key, node->value);
        node = GetNode(node->next);
        len++;
        if (exit_op && exit_op()) return;
      } while (node != nullptr);

      (*bucket_num)++;
      if (len > *max_link_len) *max_link_len = len;
      if (len < *min_link_len) *min_link_len = len;
    }
  }

  // op return new node value
  // if change node value to slab::SlabMempool32::NULL_VADDR, *valid_size_ will decrease
  // DO NOT use RenewNode inside op, avoid *valid_size_ being wrong
  // =================================== IMPORTANT ===================================
  // if this method may be used by multi threads,
  // please ENSURE you have LOCK this hashmap to avoid concurrency problem
  void TraverseOp(std::function<ValueT(KeyT, ValueT)> op,
                  std::function<bool()> exit_op,
                  uint32_t *max_link_len,
                  uint32_t *min_link_len,
                  uint32_t *bucket_num) {
    for (uint32_t i = 0; i < *hash_size_; i++) {
      uint32_t addr = hash_table_[i];
      auto node = GetNode(addr);
      if (node == nullptr) continue;

      uint32_t len = 0;
      HashNodeT *prev_node = nullptr;
      do {
        ValueT old_value = node->value;
        auto next_node = GetNode(node->next);
        auto new_value = op(node->key, node->value);
        node->value = new_value;
        // node is marked as deleted, do --(*valid_size_) here instead of ReleaseNode
        if (MT_UNLIKE(new_value == slab::SlabMempool32::NULL_VADDR) &&
            old_value != slab::SlabMempool32::NULL_VADDR) {
          // RemoveNodeFromLinkList to avoid the reused node being linked in two link list
          if (prev_node == nullptr) {
            hash_table_[i] = node->next;  // first node
          } else {
            prev_node->next = node->next;
          }
          node->next = NULL_ADDR;
          // ensure current node value is NULL_VADDR
          node->value = slab::SlabMempool32::NULL_VADDR;
          --(*valid_size_);
          // current node is removed, keep prev_node not changed
        } else {
          prev_node = node;
        }
        node = next_node;
        len++;
        if (exit_op && exit_op()) return;
      } while (node != nullptr);

      (*bucket_num)++;
      if (len > *max_link_len) *max_link_len = len;
      if (len < *min_link_len) *min_link_len = len;
    }
  }

  // can recycle by bucket
  // op return true to recycle the hash node, and there are two cases here:
  // 1. op leave node->value not changed, simply return true
  //    effect: TraverseUntil will remove node from link list and handle *valid_size_
  // 2. op change node->value to NULL_VADDR, then return true
  //    effect: TraverseUntil WILL NOT handle link list and *valid_size_, simply ReleaseNode
  // PLEASE ENSURE that in No.2 recycle case:
  //    node should be removed from link list inside op
  //    *valid_size_ should have been adjusted as well
  void TraverseUntil(std::function<bool(KeyT, ValueT)> op) {
    if (*start_entry_ == *next_entry_) return;
    HashNodeT *node = nullptr;
    auto addr = *start_entry_;
    do {
      if (MT_UNLIKE(addr == NULL_ADDR)) addr += 1;
      // NextEntry is just NULL + 1, break
      if (MT_UNLIKE(addr == *next_entry_)) break;

      node = GetNode(addr);
      CHECK(node);
      // unused node, should have been removed from link list
      // *valid_size_ should have been adjusted as well
      if (node->value == slab::SlabMempool32::NULL_VADDR) {
        ReleaseNode(addr, false);
        addr = (addr + 1) % *capacity_;
        continue;
      }

      if (!op(node->key, node->value)) break;

      // move start
      VLOG(10) << "node:" << node->key << "," << node->value << ",start:" << *start_entry_ << ",addr:" << addr
               << ",next:" << *next_entry_;

      // remove link list & release node
      if (node->value != slab::SlabMempool32::NULL_VADDR) {
        uint32_t index = GetBucket(node->key);
        RemoveNodeFromLinkList(index, node->key);
        // ensure current node value is NULL_VADDR
        node->next = NULL_ADDR;
        node->value = slab::SlabMempool32::NULL_VADDR;
        ReleaseNode(addr);
      } else {
        // node should be removed from link list inside op
        // *valid_size_ should have been adjusted as well
        ReleaseNode(addr, false);
      }

      addr = (addr + 1) % *capacity_;
    } while (addr != *next_entry_);
  }

  void GetPartKeys(int part_num, int part_id, std::vector<uint64> *keys) {
    keys->clear();
    for (uint32_t i = part_id; i < *hash_size_; i += part_num) {
      uint32_t addr = hash_table_[i];
      auto node = GetNode(addr);
      if (node == nullptr) continue;

      do {
        keys->push_back(node->key);
        node = GetNode(node->next);
      } while (node != nullptr);
    }
  }

  bool IsStartEntry(HashNodeT *node) { return node == GetNode(*start_entry_); }

  // used node num, including node whose value is NULL_VADDR
  uint32_t Size() const {
    if (*start_entry_ <= *next_entry_) {
      return *next_entry_ - *start_entry_;
    } else {
      // finally -1 bacause NULL_ADDR is not in use
      return (*capacity_) - (*start_entry_ - *next_entry_) - 1;
    }
  }
  // valid node num, DO NOT count node whose value is NULL_VADDR
  // ValidSize() should always <= Size()
  uint32_t ValidSize() const { return *valid_size_; }
  uint32_t HashSize() const { return *hash_size_; }
  uint32_t Capacity() const { return *capacity_; }
  uint32_t CeilCapacity(uint32_t capacity) const {
    return ((capacity + MAX_BLOCK_SIZE - 1) / MAX_BLOCK_SIZE) * MAX_BLOCK_SIZE;
  }
  double Ratio() const { return ((double)Size() * 100) / ((*capacity_) + 1); }

  uint64_t MemUse() const { return *total_mem_; }

  std::string GetInfo() const {
    std::ostringstream info;

    uint32_t max_link_len = 0;
    uint32_t min_link_len = INF_VALUE;
    uint32_t bucket_num = 0;

    ConstTraverseOp([](KeyT key, ValueT value) {}, nullptr, &max_link_len, &min_link_len, &bucket_num);

    info << "HashSize:" << *hash_size_ << ", Size:" << Size() << ", ValidSize: " << ValidSize()
         << ", Capacity:" << *capacity_ << ", Ratio: " << Ratio()
         << "%,\nAvgLen:" << (double)ValidSize() / (bucket_num + 1) << ", MaxLen:" << max_link_len
         << ", MinLen:" << min_link_len << ", NextAddr:" << *next_entry_ << ", StartAddr:" << *start_entry_
         << ", BlockNum:" << *block_num_ << ", TotalMem:" << *total_mem_ << std::endl;
    return info.str();
  }

  // DANGEROUS!! please make sure you know how to use it
  // now only for reusing mark-deleted node case
  void IncValidSizePurposely() { ++(*valid_size_); }

  void Flush() {
    if (pool_) { pool_->Flush(); }
  }

 protected:
  uint32_t GetBucket(const KeyT &key) const { return GetHashWithLevel(key, 2) % (*hash_size_); }

  // malloc node
  uint32_t NewNode() {
    if (MT_UNLIKE(((*next_entry_) + 1) % (*capacity_) == *start_entry_)) {
      LOG(FATAL) << "hash map is full, unpossible start_addr:" << *start_entry_
                 << ", next_addr:" << *next_entry_ << ", capacity: " << *capacity_;
      return NULL_ADDR;
    }

    uint32_t addr = *next_entry_;
    *next_entry_ = ((*next_entry_) + 1) % (*capacity_);
    if (MT_UNLIKE(*next_entry_ == NULL_ADDR)) {
      *next_entry_ += 1;
      if (MT_UNLIKE(((*next_entry_)) % (*capacity_) == *start_entry_)) {
        LOG(FATAL) << "hash map is full, unpossible start_addr:" << *start_entry_
                   << ", next_addr:" << *next_entry_ << ", capacity: " << *capacity_;
        return NULL_ADDR;
      }
    }

    uint32_t block = ((addr & BLOCK_INDEX_MASK) >> BLOCK_INDEX_BITS);
    // should Never happens
    if (MT_UNLIKE(block >= MAX_BLOCK_NUM)) {
      LOG(FATAL) << "failed to malloc node from pool, block:" << block << ", addr:" << addr
                 << ", start_addr:" << *start_entry_ << ", next_addr:" << *next_entry_;
      return NULL_ADDR;
    }

    if (block >= *block_num_) {
      if (!NewBlock()) { return NULL_ADDR; }
    }

    return addr;
  }

  HashNodeT *InsertNode(uint32_t index, const KeyT &key, uint32_t addr) {
    auto node = GetNode(addr);
    node->next = hash_table_[index];
    node->key = key;
    hash_table_[index] = addr;
    ++(*valid_size_);
    return node;
  }

  HashNodeT *GetNode(const uint32_t &index, const KeyT &key) const {
    HashNodeT *node = GetNode(hash_table_[index]);
    while (node != nullptr && node->key != key) { node = GetNode(node->next); }
    return node;
  }

  HashNodeT *GetOrInsertNode(const KeyT &key) {
    uint32_t index = GetBucket(key);
    auto node = GetNode(index, key);
    if (node == nullptr) { node = AddNode(index, key); }
    CHECK(node != nullptr);
    return node;
  }

  HashNodeT *RemoveNodeFromLinkList(const uint32_t &index, const KeyT &key) {
    uint32_t addr = hash_table_[index];
    HashNodeT *node = GetNode(addr);
    HashNodeT *pre = nullptr;
    while (node != nullptr && node->key != key) {
      pre = node;
      // next = node->next
      addr = node->next;
      node = GetNode(addr);
    }

    // not found
    if (node == nullptr) { return nullptr; }

    // first node
    if (pre == nullptr) {
      hash_table_[index] = node->next;
    } else {  // node->next = node->next->next
      pre->next = node->next;
    }

    // record self node
    node->next = addr;
    return node;
  }

  HashNodeT *DeleteNode(const uint32_t &index, const KeyT &key) {
    auto node = RemoveNodeFromLinkList(index, key);
    if (node != nullptr) {
      ReleaseNode(node->next);
      node->next = NULL_ADDR;
    }
    return node;
  }

  void ReleaseNode(uint32_t addr, bool modify_valid_size = true) {
    if (*start_entry_ != addr) {
      // set capacity_ to 0 to indicate fatal error happens
      // upper code should check the capacity_ and delete shm dir when it is 0
      // we can avoid coredump infinitely by this way
      *capacity_ = 0;
      NOT_REACHED()
          << "should free addr:" << *start_entry_ << " vs " << addr << "\nInfo:" << GetInfo()
          << "\nCapacity_ is set to 0 to indicate this fatal error, shm dir should be deleted when restart";
    }
    if (modify_valid_size) { --(*valid_size_); }
    *start_entry_ = ((*start_entry_) + 1) % (*capacity_);
    if (MT_UNLIKE(*start_entry_ == NULL_ADDR)) *start_entry_ += 1;
  }

  bool NewBlock() {
    try {
      auto new_block = reinterpret_cast<HashNodeT *>(pool_->Malloc(sizeof(HashNodeT) * MAX_BLOCK_SIZE));
      *(block_index_[*block_num_]) = pool_->IndexAt();
      block_ptrs_[(*block_num_)++] = new_block;
      *total_mem_ += sizeof(HashNodeT) * MAX_BLOCK_SIZE;
      return true;
    } catch (std::exception &e) {
      fprintf(stderr,
              "[%s, %d] [%s] malloc HashNode Block failed, block_index:%u\n",
              __FILE__,
              __LINE__,
              e.what(),
              *block_num_);
      return false;
    } catch (...) {
      fprintf(
          stderr, "[%s, %d] malloc HashNode Block failed, block_index:%u\n", __FILE__, __LINE__, *block_num_);
      return false;
    }
  }

  // insert head
  HashNodeT *AddNode(uint32_t index, const KeyT &key) {
    auto addr = NewNode();
    return InsertNode(index, key, addr);
  }

  bool Resize(uint64_t size) {
    uint32_t *p_hash_table = nullptr;

    uint64_t next_hash_size = NextPrime(size);
    if (hash_size_ != nullptr && next_hash_size <= *hash_size_) { return true; }

    uint64_t total_mem_size = next_hash_size * sizeof(uint32_t);
    try {
      p_hash_table = reinterpret_cast<uint32_t *>(pool_->Malloc(total_mem_size));
      LOG(INFO) << "malloc " << total_mem_size << " for hash table ";
    } catch (std::exception &e) {
      fprintf(stderr,
              "[%s, %d] [%s] malloc HashTable failed, size:%lu, mem:%lu\n",
              __FILE__,
              __LINE__,
              e.what(),
              next_hash_size,
              total_mem_size);
      return false;
    } catch (...) {
      fprintf(stderr,
              "[%s, %d] malloc HashTable failed, size:%lu, mem:%lu\n",
              __FILE__,
              __LINE__,
              next_hash_size,
              total_mem_size);
      return false;
    }

    if (hash_table_ == nullptr) {
      hash_table_ = p_hash_table;
    } else {
      for (uint32_t bucket_idx = 0; bucket_idx < *hash_size_; bucket_idx++) {
        uint32_t start_addr = hash_table_[bucket_idx];
        HashNodeT *node = GetNode(start_addr);
        while (node != nullptr) {
          uint32_t new_bucket = node->key % next_hash_size;
          uint32_t next_addr = node->next;
          node->next = p_hash_table[new_bucket];
          p_hash_table[new_bucket] = start_addr;

          hash_table_[bucket_idx] = next_addr;
          start_addr = next_addr;
          node = GetNode(start_addr);
        }
      }
      pool_->Free(hash_table_);
      hash_table_ = p_hash_table;
    }
    // NOT thread safe
    *table_index_ = pool_->IndexAt();

    *hash_size_ = next_hash_size;
    *total_mem_ += total_mem_size;
    return true;
  }

  uint32_t MetaSize() { return sizeof(uint32_t) * (8 + MAX_BLOCK_NUM) + sizeof(uint64_t); }

  bool RestoreMeta() {
    uint64_t size = 0;
    auto start = pool_->GetAddrByIndex(META_INDEX, &size);
    LoadMeta(start);
    if (*magic_num_ != MAGIC_NUM) {
      LOG(ERROR) << "magic num is not equal, " << *magic_num_ << " vs " << MAGIC_NUM;
      return false;
    }
    if (size != MetaSize()) {
      LOG(ERROR) << "size not equal, " << size << " vs " << MetaSize();
      return false;
    }
    return true;
  }

  void MallocMeta() {
    auto size = MetaSize();
    auto start = reinterpret_cast<uint32_t *>(pool_->Malloc(size));
    LoadMeta(start);
    *next_entry_ = 1;
    *start_entry_ = 1;
    *magic_num_ = MAGIC_NUM;
  }

  void LoadMeta(void *start) {
    block_num_ = reinterpret_cast<uint32_t *>(start);
    next_entry_ = block_num_ + 1;
    start_entry_ = next_entry_ + 1;
    valid_size_ = start_entry_ + 1;
    capacity_ = valid_size_ + 1;
    hash_size_ = capacity_ + 1;
    table_index_ = reinterpret_cast<int32_t *>(hash_size_ + 1);
    for (size_t i = 0; i < MAX_BLOCK_NUM; i++) { block_index_[i] = table_index_ + 1 + i; }
    total_mem_ = reinterpret_cast<uint64_t *>(table_index_ + 1 + MAX_BLOCK_NUM);
    magic_num_ = reinterpret_cast<uint32_t *>(total_mem_ + 1);
  }

  bool RestoreCheckValue() {
    for (size_t i = 0; i < *block_num_; i++) {
      int index = *(block_index_[i]);
      uint64_t size = 0;
      block_ptrs_[i] = reinterpret_cast<HashNodeT *>(pool_->GetAddrByIndex(index, &size));
      if (size != (MAX_BLOCK_SIZE * sizeof(HashNodeT))) {
        LOG(ERROR) << "idx: " << index << " restore block failed, size: " << size;
        return false;
      }
    }

    // Restore HashTable
    uint64_t size = 0;
    hash_table_ = reinterpret_cast<uint32_t *>(pool_->GetAddrByIndex(*table_index_, &size));
    if (size != ((*hash_size_) * sizeof(uint32_t))) {
      LOG(ERROR) << "size not equal hashsize, size: " << size << ", hashsize:" << *hash_size_;
      return false;
    }

    return true;
  }

 private:
  static const uint32_t INF_VALUE = 0xFFFFFFFF;
  static const uint32_t NULL_ADDR = 0;
  static const uint32_t ADDR_INDEX_MASK = 0x000FFFFF;
  static const uint32_t BLOCK_INDEX_MASK = 0xFFF00000;
  static const uint32_t BLOCK_INDEX_BITS = 20;
  // MAX NODE SIZE: 1024 * 1M * 16
  static const uint32_t MAX_BLOCK_NUM = 1024;
  static const uint32_t MAX_BLOCK_SIZE = 1048576;
  static const uint32_t MAX_LINK_LIST_LEN = 10;
  // meta index
  static const int META_INDEX = 0;
  static const uint32_t MAGIC_NUM = 0x2F2F3F4F;

  uint32_t *magic_num_;
  uint64_t *total_mem_;

  // meta index:0
  // table index & block index store in meta
  int32_t *block_index_[MAX_BLOCK_NUM];
  int32_t *table_index_;

  uint32_t *hash_size_;
  // capacity is limit
  uint32_t *capacity_;
  // node num actually in use, may change in these cases:
  // InsertNode / RenewNode / ReleaseNode / mark as deleted / reuse mark-deleted node
  // valid_size_ should be modified along with link list modification
  uint32_t *valid_size_;
  // calc used node num by *start_entry_ and *next_entry_
  uint32_t *start_entry_;
  uint32_t *next_entry_;

  uint32_t *block_num_;
  HashNodeT *block_ptrs_[MAX_BLOCK_NUM];

  uint32_t *hash_table_;

  slab::MemPool *pool_;
};

}  // namespace base
