// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <string>

#include "base/common/basic_types.h"
#include "base/common/gflags.h"
#include "memtable/pool/mem_pool.h"
#include "memtable/slab/slab_mempool32.h"

ABSL_DECLARE_FLAG(uint64, memtable_block_max_mem_size);

namespace base {
namespace slab {

class ShmPool : public MemPool {
 public:
  struct ShmBlock {
    std::string file_name;
    uint64_t size = 0;
    int fd = -1;
    int index = -1;
    void *data = nullptr;
    struct ShmBlock *next = nullptr;
  };

  explicit ShmPool(const std::string &dir_name, const int32_t part_id);
  ~ShmPool();

  void *Malloc(uint64_t size, int *fd = nullptr) override;
  void Free(void *ptr) override;

  int IndexAt() const override;
  void *GetAddrByIndex(int index, uint64_t *size, int *fd = nullptr) override;

  bool CouldRestore() const override;
  bool Restore() override;

  void Flush() override;

 private:
  bool CheckFileExistsAndSize(const std::string &file_name, uint64_t size);
  bool CheckFileExistsAndGetSize(const std::string &file_name, uint64_t *size);

  bool Load(ShmBlock *block);
  void Store(ShmBlock *block);
  // do not close fd
  void Sync(ShmBlock *block);

  bool Map(ShmBlock *block);

  std::string CurFileName();

 private:
  std::string dir_name_;
  int part_id_;
  int cur_block_idx_;
  ShmBlock *head_;
};

}  // namespace slab
}  // namespace base
