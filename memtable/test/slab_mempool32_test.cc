// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "memtable/slab/slab_mempool32.h"

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <string>
#include <utility>

#include "base/common/file_util.h"
#include "base/common/logging.h"
#include "base/hash/hash.h"
#include "base/random/pseudo_random.h"
#include "base/testing/gtest.h"

const std::string kTestSlabPoolRootDir = "/dev/shm/slab";

namespace base {

class SlabMempool32Test : public ::testing::Test {
  void SetUp() override { fs::remove_all(kTestSlabPoolRootDir); }
  void TearDown() override { fs::remove_all(kTestSlabPoolRootDir); }
};

void fake_slabs(std::vector<uint32_t> *slabs) {
  for (size_t i = 0; i < 8; i++) { slabs->emplace_back(1 << (i + 2)); }
  slabs->emplace_back(48);
}

void fake_large_slabs(uint32_t slabs[], uint32_t *slab_len) {
  slabs[0] = 1024;
  slabs[1] = 2048;
  slabs[2] = 4096;
  *slab_len = 3;
}

TEST_F(SlabMempool32Test, test_normal_right) {
  slab::SlabMempool32 node_pool;

  std::vector<uint32_t> slabs;
  fake_slabs(&slabs);

  bool ret = node_pool.Create(slabs, 1024 * 1024, "", 0);
  CHECK(ret);

  auto valid_min_addr = node_pool.Malloc(2);
  CHECK_NE(slab::SlabMempool32::NULL_VADDR, valid_min_addr);

  auto min_addr = node_pool.Malloc(4);
  CHECK_NE(slab::SlabMempool32::NULL_VADDR, min_addr);

  auto max_addr = node_pool.Malloc(1 << 9);
  CHECK_NE(slab::SlabMempool32::NULL_VADDR, max_addr);

  auto invalid_max_addr = node_pool.Malloc((1 << 9) + 1);
  CHECK_EQ(slab::SlabMempool32::NULL_VADDR, invalid_max_addr);

  std::cout << "slab mem normal:\n" << node_pool.GetInfo() << std::endl;
}

TEST_F(SlabMempool32Test, test_high_perf) {
  slab::SlabMempool32 node_pool;

  std::vector<uint32_t> slabs;
  fake_slabs(&slabs);

  bool ret = node_pool.Create(slabs, 1024 * 1024, "", 0);
  CHECK(ret);

  std::vector<uint32_t> addrs_4;
  for (size_t i = 0; i < 10000000; i++) {
    auto addr = node_pool.Malloc(4);
    CHECK_NE(slab::SlabMempool32::NULL_VADDR, addr);
    uint32_t *p = static_cast<uint32_t *>(node_pool.MemAddress(addr));
    *p = 0;
    addrs_4.push_back(addr);
  }
  std::vector<uint32_t> addrs_255;
  for (size_t i = 0; i < 800; i++) {
    auto addr = node_pool.Malloc(255);
    CHECK_NE(slab::SlabMempool32::NULL_VADDR, addr);
    uint32_t *p = static_cast<uint32_t *>(node_pool.MemAddress(addr));
    *p = 0;
    addrs_255.push_back(addr);
  }

  for (size_t i = 0; i < 5000000; i++) {
    auto addr = addrs_4[i];
    CHECK(node_pool.Free(addr));
  }
  for (size_t i = 0; i < 400; i++) {
    auto addr = addrs_255[i];
    CHECK(node_pool.Free(addr));
  }
  std::cout << "slab mem high:\n" << node_pool.GetInfo() << std::endl;
}

TEST_F(SlabMempool32Test, test_high_perf_with_shm_restore_normal) {
  std::vector<uint32_t> slabs;
  fake_slabs(&slabs);

  slab::SlabMempool32 *node_pool = new slab::SlabMempool32();
  bool ret = node_pool->Create(slabs, 1024 * 1024, kTestSlabPoolRootDir, 0);
  CHECK(ret);

  std::vector<uint32_t> addrs_4;
  for (size_t i = 0; i < 10000000; i++) {
    auto addr = node_pool->Malloc(4);
    CHECK_NE(slab::SlabMempool32::NULL_VADDR, addr);
    addrs_4.push_back(addr);
  }

  PseudoRandom s_random;
  std::vector<uint32_t> addrs_255;
  std::vector<std::string> values_255;
  for (size_t i = 0; i < 800; i++) {
    std::string r_str = s_random.GetString(255 - 8);
    auto addr = node_pool->Malloc(255);
    CHECK_NE(slab::SlabMempool32::NULL_VADDR, addr);

    auto node = reinterpret_cast<KVData *>(node_pool->MemAddress(addr));
    node->store(r_str.c_str(), r_str.size());
    addrs_255.push_back(addr);
    values_255.push_back(r_str);
  }

  for (size_t i = 0; i < 5000000; i++) {
    auto addr = addrs_4[i];
    CHECK(node_pool->Free(addr));
  }
  for (size_t i = 0; i < 400; i++) {
    auto addr = addrs_255[i];
    CHECK(node_pool->Free(addr));
  }

  std::string origin_info = node_pool->GetInfo();
  std::cout << "slab mem shm origin:\n" << node_pool->GetInfo() << std::endl;
  delete node_pool;

  slab::SlabMempool32 restore_pool;
  ret = restore_pool.Create(slabs, 1024 * 1024, kTestSlabPoolRootDir, 0);
  CHECK(ret);
  std::cout << "slab mem shm restore:\n" << restore_pool.GetInfo() << std::endl;

  CHECK_EQ(origin_info, restore_pool.GetInfo());
  for (int i = 400; i < 800; i++) {
    auto node = reinterpret_cast<KVData *>(restore_pool.MemAddress(addrs_255[i]));
    CHECK_EQ(node->size(), 255 - 8);
    CHECK_EQ(std::string(node->data(), node->size()), values_255[i]);
  }
}

}  // namespace base
