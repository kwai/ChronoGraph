// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "memtable/hash_map/loop_hash_map.h"

#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "base/common/file_util.h"
#include "base/common/logging.h"
#include "base/common/time.h"
#include "base/testing/gtest.h"

namespace base {

TEST(LoopHashMapTest, test_normal_right) {
  LoopHashMap<uint64_t, uint32_t> hash_map;
  hash_map.Create(1000000, 0.5, "", 0);
  CHECK_EQ(0, hash_map.Size());
  CHECK_EQ(0, hash_map.ValidSize());
  CHECK_NE(0, hash_map.HashSize());

  for (size_t i = 0; i < 100000; i++) { hash_map.Update(i, i / 2); }
  CHECK_EQ(0, hash_map[1]);
  CHECK_EQ(1, hash_map[3]);
  CHECK_EQ(2, hash_map[5]);
  CHECK_EQ(3, hash_map[6]);
  CHECK_EQ(4, hash_map[9]);
  CHECK_EQ(100000, hash_map.Size());
  CHECK_EQ(100000, hash_map.ValidSize());

  CHECK(hash_map.Pop(0) != nullptr);
  CHECK(hash_map.Pop(1) != nullptr);
  CHECK(hash_map.Pop(2) != nullptr);
  hash_map[8] = 8;
  CHECK_EQ(8, hash_map[8]);
  std::cout << "hash_map.GetInfo():\n" << hash_map.GetInfo();
}

TEST(LoopHashMapTest, test_high_perf) {
  LoopHashMap<uint64_t, uint32_t> hash_map;
  hash_map.Create(1000000, 0.5, "", 0);
  CHECK_EQ(0, hash_map.Size());
  CHECK_EQ(0, hash_map.ValidSize());
  CHECK_NE(0, hash_map.HashSize());

  for (size_t i = 0; i < 1000000; i++) { hash_map.Update(i, i / 2); }
  CHECK_EQ(0, hash_map[1]);
  CHECK_EQ(1, hash_map[3]);
  CHECK_EQ(2, hash_map[5]);
  CHECK_EQ(3, hash_map[6]);
  CHECK_EQ(50000, hash_map[100000]);
  CHECK_EQ(1000000, hash_map.Size());
  CHECK_EQ(1000000, hash_map.ValidSize());

  auto start = base::GetTimestamp();
  for (size_t i = 0; i < 1000000; i++) { hash_map.Seek(i); }
  auto end = base::GetTimestamp();
  std::cout << "time_interval:" << end - start << std::endl;
}

TEST(LoopHashMapTest, test_traverse_until_perform_right) {
  LoopHashMap<uint64_t, uint32_t> hash_map;
  hash_map.Create(2000000, 0.5, "", 0);
  CHECK_EQ(0, hash_map.Size());
  CHECK_EQ(0, hash_map.ValidSize());
  CHECK_NE(0, hash_map.HashSize());

  for (size_t i = 0; i < 1000000; i++) { hash_map.Update(i, i / 2); }
  CHECK_EQ(0, hash_map[1]);
  CHECK_EQ(1, hash_map[3]);
  CHECK_EQ(2, hash_map[5]);
  CHECK_EQ(3, hash_map[6]);
  CHECK_EQ(50000, hash_map[100000]);
  CHECK_EQ(1000000, hash_map.Size());
  CHECK_EQ(1000000, hash_map.ValidSize());

  auto start = base::GetTimestamp();
  for (size_t i = 0; i < 1000000; i++) { hash_map.Seek(i); }

  std::vector<std::pair<uint64_t, uint32_t>> recycle_values;
  hash_map.TraverseUntil([&](uint64_t key, uint32_t value) -> bool {
    if (key < 10) {
      recycle_values.emplace_back(std::pair<uint64_t, uint32_t>(key, value));
      return true;
    }
    return false;
  });

  for (auto pair : recycle_values) {
    LOG(INFO) << "key:" << pair.first << ",value:" << pair.second;
    auto node = hash_map.Seek(pair.first);
    CHECK(node == nullptr);
  }
  CHECK_EQ(1000000 - 10, hash_map.Size());
  CHECK_EQ(1000000 - 10, hash_map.ValidSize());
  auto end = base::GetTimestamp();
  std::cout << "time_interval:" << end - start << std::endl;
}

TEST(LoopHashMapTest, test_high_perf_with_shm_right) {
  LoopHashMap<uint64_t, uint32_t> hash_map;
  hash_map.Create(1000000, 0.5, "./loop_dict", 0);
  CHECK_EQ(0, hash_map.Size());
  CHECK_EQ(0, hash_map.ValidSize());
  CHECK_NE(0, hash_map.HashSize());

  for (size_t i = 0; i < 1000000; i++) { hash_map.Update(i, i / 2); }
  CHECK_EQ(0, hash_map[1]);
  CHECK_EQ(1, hash_map[3]);
  CHECK_EQ(2, hash_map[5]);
  CHECK_EQ(3, hash_map[6]);
  CHECK_EQ(50000, hash_map[100000]);
  CHECK_EQ(1000000, hash_map.Size());
  CHECK_EQ(1000000, hash_map.ValidSize());

  auto start = base::GetTimestamp();
  for (size_t i = 0; i < 1000000; i++) { hash_map.Seek(i); }
  auto end = base::GetTimestamp();
  std::cout << "time_interval:" << end - start << std::endl;
  fs::remove_all("./loop_dict");
}

TEST(LoopHashMapTest, test_high_perf_with_shm_restore_right) {
  LoopHashMap<uint64_t, uint32_t> hash_map;
  hash_map.Create(1000000, 0.5, "./loop_dict", 0);
  CHECK_EQ(0, hash_map.Size());
  CHECK_EQ(0, hash_map.ValidSize());
  CHECK_NE(0, hash_map.HashSize());

  for (size_t i = 0; i < 1000000; i++) { hash_map.Update(i, i / 2); }
  CHECK_EQ(0, hash_map[1]);
  CHECK_EQ(1, hash_map[3]);
  CHECK_EQ(2, hash_map[5]);
  CHECK_EQ(3, hash_map[6]);
  CHECK_EQ(50000, hash_map[100000]);
  CHECK_EQ(1000000, hash_map.Size());
  CHECK_EQ(1000000, hash_map.ValidSize());

  auto start = base::GetTimestamp();
  for (size_t i = 0; i < 1000000; i++) { hash_map.Seek(i); }
  auto end = base::GetTimestamp();
  std::cout << "time_interval:" << end - start << std::endl;

  LoopHashMap<uint64_t, uint32_t> restore_map;
  restore_map.Create(1000000, 0.5, "./loop_dict", 0);
  CHECK_EQ(hash_map.Size(), restore_map.Size());
  CHECK_EQ(hash_map.ValidSize(), restore_map.ValidSize());
  CHECK_EQ(hash_map.HashSize(), restore_map.HashSize());

  start = base::GetTimestamp();
  for (size_t i = 0; i < 1000000; i++) {
    auto origin = hash_map.Seek(i);
    auto restore_value = restore_map.Seek(i);
    CHECK_EQ(origin->value, restore_value->value) << "index:" << i;
  }
  end = base::GetTimestamp();
  std::cout << "time_interval:" << end - start << std::endl;

  fs::remove_all("./loop_dict");
}

TEST(LoopHashMapTest, test_size_and_renew_right) {
  LoopHashMap<uint64_t, uint32_t> hash_map;
  hash_map.Create(1000000, 0.5, "./loop_dict", 0);
  CHECK_EQ(0, hash_map.Size());
  CHECK_EQ(0, hash_map.ValidSize());
  CHECK_NE(0, hash_map.HashSize());

  for (size_t i = 0; i < 1000000; i++) { hash_map.Update(i, i / 2); }
  CHECK_EQ(0, hash_map[1]);
  CHECK_EQ(1, hash_map[3]);
  CHECK_EQ(2, hash_map[5]);
  CHECK_EQ(3, hash_map[6]);
  CHECK_EQ(50000, hash_map[100000]);
  CHECK_EQ(1000000, hash_map.Size());
  CHECK_EQ(1000000, hash_map.ValidSize());

  for (size_t i = 0; i < 10000; i++) { hash_map.RenewNode(i); }
  for (size_t i = 0; i < 10000; i++) { CHECK_EQ(i / 2, hash_map[i]); }
  LOG(INFO) << hash_map.GetInfo();
  CHECK_EQ(1000000 + 10000, hash_map.Size());
  CHECK_EQ(1000000, hash_map.ValidSize());

  auto start = base::GetTimestamp();
  for (size_t i = 0; i < 1000000; i++) { hash_map.Seek(i); }
  auto end = base::GetTimestamp();
  std::cout << "time_interval:" << end - start << std::endl;

  LoopHashMap<uint64_t, uint32_t> restore_map;
  restore_map.Create(1000000, 0.5, "./loop_dict", 0);
  CHECK_EQ(hash_map.Size(), restore_map.Size());
  CHECK_EQ(hash_map.ValidSize(), restore_map.ValidSize());
  CHECK_EQ(hash_map.HashSize(), restore_map.HashSize());

  start = base::GetTimestamp();
  for (size_t i = 0; i < 1000000; i++) {
    auto origin = hash_map.Seek(i);
    auto restore_value = restore_map.Seek(i);
    CHECK_EQ(origin->value, restore_value->value) << "index:" << i;
  }
  end = base::GetTimestamp();
  std::cout << "time_interval:" << end - start << std::endl;

  fs::remove_all("./loop_dict");
}

}  // namespace base
