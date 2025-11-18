// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "memtable/pool/mem_pool.h"

namespace base {
namespace slab {

void *MemPool::Malloc(uint64_t size, int *fd) {
  auto ptr = new (std::nothrow) char[size]();
  return ptr;
}

void MemPool::Free(void *ptr) {
  delete[] reinterpret_cast<char *>(ptr);
  ptr = nullptr;
}

int MemPool::IndexAt() const { return -1; }

void *MemPool::GetAddrByIndex(int index, uint64_t *size, int *fd) {
  *size = 0;
  return nullptr;
}

bool MemPool::CouldRestore() const { return false; }

bool MemPool::Restore() { return false; }

// do nothing
void MemPool::Flush() {}

}  // namespace slab
}  // namespace base
