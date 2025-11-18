// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <string>

#include "memtable/pool/mem_pool.h"
#include "memtable/pool/shm_pool.h"

namespace base {
namespace slab {

class MempoolHelper {
 public:
  static MemPool *MallocPool(const std::string &shm_path, const int32_t part_id);

  static void FreePool(MemPool *ptr);
};

}  // namespace slab
}  // namespace base
