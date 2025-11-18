// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <string>
#include <vector>

namespace base {
namespace slab {

class MemPool {
 public:
  MemPool() {}
  virtual ~MemPool() {}

  virtual void *Malloc(uint64_t size, int *fd = nullptr);
  virtual void Free(void *ptr);

  // load from file interface
  virtual int IndexAt() const;
  virtual void *GetAddrByIndex(int index, uint64_t *size, int *fd = nullptr);
  virtual bool CouldRestore() const;
  virtual bool Restore();

  // flush data to shm, file, etc., do not close fd
  virtual void Flush();
};

}  // namespace slab
}  // namespace base
