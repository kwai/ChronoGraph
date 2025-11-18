// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <string>

#include "base/common/basic_types.h"
#include "base/common/logging.h"
#include "memtable/mem_kv.h"
#include "chrono_graph/src/base/perf_util.h"

namespace chrono_graph {

// Interface for custom memory allocator.
class MemAllocator {
 public:
  virtual base::KVData *New(int memory_size, uint32 *new_addr) = 0;
  virtual bool Free(base::KVData *memory_data) = 0;
  virtual ~MemAllocator() {}

 protected:
  MemAllocator() {}

 private:
  DISALLOW_COPY_AND_ASSIGN(MemAllocator);
};

class MemKVAllocator : public MemAllocator {
 public:
  explicit MemKVAllocator(base::MemKV *mem_kv, const std::string &relation_name)
      : mem_kv_(mem_kv), relation_name_(relation_name) {}

  base::KVData *New(int memory_size, uint32 *new_addr) override {
    uint32 t = *new_addr;
    *new_addr = mem_kv_->MallocAddr(memory_size);
    if (*new_addr == base::slab::SlabMempool32::NULL_VADDR) {
      *new_addr = t;
      PERF_SUM(1, relation_name_, P_EXCEPTION, "mem_kv.malloc_failed");
      return nullptr;
    }
    return mem_kv_->GetKVDataByAddr(*new_addr);
  }

  bool Free(base::KVData *memory_data) override {
    NOT_REACHED() << "SHOULD NOT CALL THIS!";
    return false;
  }

  ~MemKVAllocator() override {}

 private:
  base::MemKV *mem_kv_;
  std::string relation_name_;
  DISALLOW_COPY_AND_ASSIGN(MemKVAllocator);
};

}  // namespace chrono_graph