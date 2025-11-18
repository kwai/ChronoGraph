// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "memtable/pool/shm_pool.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <filesystem>
#include <fstream>
#include <string>

#include <iostream>
#include "base/common/file_util.h"
#include "base/common/logging.h"

const std::vector<char> max_mem_block_size_buffer(absl::GetFlag(FLAGS_memtable_block_max_mem_size), 0);

namespace base {
namespace slab {

ShmPool::ShmPool(const std::string &dir_name, const int32_t part_id) {
  dir_name_ = dir_name;
  part_id_ = part_id;
  cur_block_idx_ = 0;
  head_ = nullptr;
}

ShmPool::~ShmPool() {
  auto p = head_;
  while (p) {
    Store(p);
    auto next = p->next;
    delete p;
    p = next;
  }
  head_ = nullptr;
  cur_block_idx_ = 0;
  dir_name_.clear();
}

bool ShmPool::Load(ShmBlock *block) {
  CHECK(block);  // Inner block check

  // no need to mmap
  if (block->data != nullptr && block->fd >= 0) return true;

  std::string file_name = block->file_name;
  uint64_t size = block->size;

  // if size not equal delete
  CheckFileExistsAndSize(file_name, size);

  if (!fs::exists(file_name)) {
    try {
      fs::create_directories(fs::path(file_name).parent_path().string());
    } catch (const fs::filesystem_error &e) { std::cerr << e.what() << std::endl; }
    std::ofstream output(file_name);
    if (!output) {
      LOG(ERROR) << "Unable to open file " << file_name;
      return false;
    }

    if (size != absl::GetFlag(FLAGS_memtable_block_max_mem_size)) {
      const int buffer_size = 4096;
      char buffer[buffer_size] = {0};

      for (uint64_t i = 0; i < size / buffer_size; ++i) {
        output.write(buffer, buffer_size);
        if (!output.good()) {
          LOG(ERROR) << "File " << file_name << " can't be written to at offset " << i * buffer_size;
          return false;
        }
      }

      if (size % buffer_size != 0) {
        output.write(buffer, size % buffer_size);
        if (!output.good()) {
          LOG(ERROR) << "File " << file_name << " can't be written to at offset "
                     << size / buffer_size * buffer_size;
          return false;
        }
      }
    } else {
      output.write(max_mem_block_size_buffer.data(), max_mem_block_size_buffer.size());
      if (!output.good()) {
        LOG(ERROR) << "Error occurred while writing to file " << file_name;
        return false;
      }
    }
  }

  if (!CheckFileExistsAndSize(file_name, size)) { return false; }

  return Map(block);
}

bool ShmPool::Map(ShmBlock *block) {
  auto fd = open(block->file_name.c_str(), O_RDWR);
  if (fd < 0) {
    LOG(ERROR) << "Failed to open file " << block->file_name << ": " << strerror(errno);
    return false;
  }

  auto last_ptr = block->data;
  block->data = mmap(nullptr, block->size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (block->data == MAP_FAILED) {
    LOG(ERROR) << "Failed to map file:" << block->file_name << ", size: " << block->size
               << ", part_id:" << part_id_ << ", ptr:" << block->data << ", last_ptr:" << last_ptr
               << ", errno:" << errno << ", msg:" << strerror(errno);
    return false;
  }
  block->fd = fd;
  LOG(INFO) << "Create ShmFile: " << block->file_name << ", size: " << block->size << ", part_id:" << part_id_
            << ", ptr:" << block->data << ", last_ptr:" << last_ptr;
  return true;
}

void ShmPool::Store(ShmBlock *block) {
  CHECK(block);  // inner block check
  if (block->fd >= 0 && block->data != nullptr) {
    auto start_time = absl::Now();
    msync(block->data, block->size, MS_SYNC);
    auto sync_time = absl::Now();
    munmap(block->data, block->size);
    auto unmap_time = absl::Now();
    close(block->fd);
    auto close_time = absl::Now();
    block->fd = -1;
    LOG(INFO) << "unmap shm file:" << block->file_name << ", fd:" << block->fd << ", size:" << block->size
              << ", part_id:" << part_id_ << ",ptr:" << block->data
              << ", sync cost:" << absl::ToInt64Microseconds(sync_time - start_time)
              << "us, unmap cost:" << absl::ToInt64Microseconds(unmap_time - sync_time)
              << "us, close cost:" << absl::ToInt64Microseconds(close_time - unmap_time) << "us";
  }
}

void *ShmPool::Malloc(uint64_t size, int *fd) {
  auto new_block = new (std::nothrow) ShmBlock();
  new_block->file_name = CurFileName();
  new_block->size = size;
  new_block->index = cur_block_idx_;

  if (!Load(new_block)) {
    delete new_block;
    return nullptr;
  }
  new_block->next = head_;
  head_ = new_block;
  cur_block_idx_++;
  if (fd) { *fd = new_block->fd; }

  return new_block->data;
}

void ShmPool::Free(void *ptr) {
  auto p = head_;
  while (p) {
    if (ptr == p->data) break;
    p = p->next;
  }

  Store(p);
}

int ShmPool::IndexAt() const {
  if (head_ == nullptr) { return -1; }

  return head_->index;
}

void *ShmPool::GetAddrByIndex(int index, uint64_t *size, int *fd) {
  auto p = head_;
  while (p) {
    if (p->index == index) {
      *size = p->size;
      if (fd) { *fd = p->fd; }
      return p->data;
    }
    p = p->next;
  }

  return nullptr;
}

bool ShmPool::CouldRestore() const { return true; }

bool ShmPool::Restore() {
  std::string file_name = CurFileName();
  uint64_t size = 0;
  while (CheckFileExistsAndGetSize(file_name, &size)) {
    auto new_block = new (std::nothrow) ShmBlock();
    new_block->file_name = file_name;
    new_block->size = size;
    new_block->index = cur_block_idx_;

    // abort process if false, should never happend
    if (!Map(new_block)) {
      LOG(FATAL) << "restore shmpool failed, abort:" << file_name;
      return false;
    }
    new_block->next = head_;
    head_ = new_block;

    cur_block_idx_++;
    file_name = CurFileName();
  }

  return (head_ != nullptr);
}

void ShmPool::Sync(ShmBlock *block) {
  CHECK(block);  // inner block check
  if (block->fd >= 0 && block->data != nullptr) {
    msync(block->data, block->size, MS_SYNC);
    LOG(INFO) << "sync shm file: " << block->file_name << ", fd:" << block->fd << ", size:" << block->size
              << ", part_id:" << part_id_ << ",ptr:" << block->data;
  }
}

void ShmPool::Flush() {
  auto p = head_;
  while (p) {
    Sync(p);
    p = p->next;
  }
}

bool ShmPool::CheckFileExistsAndSize(const std::string &file_name, uint64_t size) {
  if (fs::exists(file_name)) {
    uint64 get_size = 0;
    bool succ = base::GetFileSize(file_name, &get_size);
    if (!succ) {
      fs::remove(file_name);
      return false;
    }

    if (get_size != size) {
      LOG(ERROR) << file_name << " Size Error: get_size:" << get_size << " vs size:" << size;
      fs::remove(file_name);
      return false;
    }
  }

  return true;
}

bool ShmPool::CheckFileExistsAndGetSize(const std::string &file_name, uint64_t *size) {
  if (!fs::exists(file_name)) {
    LOG(WARNING) << "File Not Exists: " << file_name;
    return false;
  }
  return base::GetFileSize(file_name, size);
}

std::string ShmPool::CurFileName() {
  return (dir_name_ + "/" + std::to_string(part_id_) + "/" + std::to_string(cur_block_idx_));
}

}  // namespace slab
}  // namespace base
