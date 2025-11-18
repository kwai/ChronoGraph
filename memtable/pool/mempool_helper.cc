// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "memtable/pool/mempool_helper.h"

#include <string>

namespace base {
namespace slab {

MemPool *MempoolHelper::MallocPool(const std::string &shm_path, const int32_t part_id) {
  MemPool *pool = nullptr;
  if (!shm_path.empty()) {
    pool = new ShmPool(shm_path, part_id);
  } else {
    pool = new MemPool();
  }
  return pool;
}

void MempoolHelper::FreePool(MemPool *ptr) {
  if (ptr) delete ptr;
}

}  // namespace slab
}  // namespace base
