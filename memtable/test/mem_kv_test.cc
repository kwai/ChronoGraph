// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "memtable/mem_kv.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <future>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "base/common/file_util.h"
#include "base/common/logging.h"
#include "base/common/sleep.h"
#include "base/common/time.h"
#include "base/random/pseudo_random.h"
#include "base/testing/gtest.h"
#include "base/thread/thread_pool.h"

ABSL_DECLARE_FLAG(uint64, memtable_block_max_mem_size);
ABSL_FLAG(int32, multi_memkv_sync_concurrency_for_test, 8, "MultiMemKV sync concurrency for test");
ABSL_FLAG(int32, memkv_max_key_for_test_erase_if, 10000, "max key for test MemKV.EraseIf");
ABSL_FLAG(int32, memkv_concurrency_test_thread_num, 4, "thread num for MemKV concurrency test");
ABSL_FLAG(int32, memkv_concurrency_test_loop_num, 2, "loop num per thread for MemKV concurrency test");

const std::string kTestMemKVRootDir = "/dev/shm/memkv";
const std::string kTestMemKVIndexDir = "/dev/shm/index";

namespace base {

class MemKVTest : public ::testing::Test {
  void SetUp() override {
    fs::remove_all(kTestMemKVRootDir);
    fs::remove_all(kTestMemKVIndexDir);
  }
  void TearDown() override {
    fs::remove_all(kTestMemKVRootDir);
    fs::remove_all(kTestMemKVIndexDir);
  }
};

class MultiMemKVTest : public ::testing::Test {
  void SetUp() override {
    fs::remove_all(kTestMemKVRootDir);
    fs::remove_all(kTestMemKVIndexDir);
  }
  void TearDown() override {
    fs::remove_all(kTestMemKVRootDir);
    fs::remove_all(kTestMemKVIndexDir);
  }
};

TEST_F(MemKVTest, test_normal_right) {
  MemKV mem_kv;

  mem_kv.Initialize(1 << 20, 0.5, 1 << 30, 3600, 10, "", 0);

  PseudoRandom s_random;
  std::vector<std::string> random_strs;
  for (size_t i = 0; i < 10000; i++) {
    int r_size = s_random.GetInt(8, 512);
    std::string r_str = s_random.GetString(r_size);
    bool ret = mem_kv.Update(i, r_str.data(), r_str.size());
    CHECK(ret);
    random_strs.emplace_back(r_str);
  }

  std::cout << "mem kv normal: " << mem_kv.GetInfo();

  KVReadData data = mem_kv.Get(1);
  CHECK_EQ(data.size, random_strs[1].size());
}

TEST_F(MemKVTest, test_loop_malloc_free_right) {
  LOG(INFO) << "Running test_loop_malloc_free_right, this may cost 1-2 minute...";
  MemKV mem_kv;

  mem_kv.Initialize(1000000, 0.5, 10UL << 31, 300, 3, "", 0);

  PseudoRandom s_random;
  std::vector<std::string> random_strs;
  for (size_t i = 0; i < 3 * 1000000; i++) {
    int r_size = s_random.GetInt(8, 512);
    std::string r_str = s_random.GetString(r_size);
    bool ret = mem_kv.Update(i, r_str.data(), r_str.size());
    CHECK(ret);
    random_strs.emplace_back(r_str);
  }

  std::cout << "mem kv loop: " << mem_kv.GetInfo();

  KVReadData data = mem_kv.Get(3000000 - 1);
  CHECK_EQ(data.size, random_strs[3000000 - 1].size());
}

TEST_F(MemKVTest, test_high_perf_right) {
  MemKV mem_kv;

  mem_kv.Initialize(1 << 20, 0.5, 1 << 30, 3600, 10, "", 0);

  PseudoRandom s_random;
  std::vector<std::string> random_strs;
  for (size_t i = 0; i < 10000; i++) {
    int r_size = s_random.GetInt(8, 512);
    std::string r_str = s_random.GetString(r_size);
    bool ret = mem_kv.Update(i, r_str.data(), r_str.size());
    CHECK(ret);
    random_strs.emplace_back(r_str);
  }

  std::cout << "mem kv high: " << mem_kv.GetInfo();

  for (size_t i = 0; i < 10000; i++) {
    KVReadData data = mem_kv.Get(i);
    CHECK_EQ(data.size, random_strs[i].size());
  }
}

void WriteMemThread(MemKV *p_mem, int i) {
  PseudoRandom s_random(i);
  for (size_t i = 0; i < 10000; i++) {
    int r_size = s_random.GetInt(8, 512);
    std::string r_str = s_random.GetString(r_size);
    bool ret = p_mem->Update(i, r_str.data(), r_str.size());
    CHECK(ret);
  }
}

TEST_F(MemKVTest, test_high_perf_multi_write_right) {
  MemKV mem_kv;
  mem_kv.Initialize(1 << 20, 0.5, 1 << 30, 3600, 10, "", 0);

  thread::ThreadPool tp(12);

  for (size_t i = 0; i < tp.ThreadNum(); i++) {
    tp.AddTask([&mem_kv, i]() { WriteMemThread(&mem_kv, i); });
  }
  tp.JoinAll();

  std::cout << "mem kv high: " << mem_kv.GetInfo();

  for (size_t i = 0; i < 10000; i++) {
    KVReadData data = mem_kv.Get(i);
    CHECK(nullptr != data.data);
    CHECK_NE(0, data.size);
  }
}

TEST_F(MemKVTest, test_mem_kv_with_shm_restore_right) {
  MemKV *mem_kv = new MemKV();
  bool ret = mem_kv->Initialize(1 << 20, 0.5, 1 << 30, 3600, 10, kTestMemKVRootDir, 0);
  CHECK(ret);

  std::vector<std::string> random_strs;
  PseudoRandom s_random;
  for (size_t i = 0; i < 10000; i++) {
    int r_size = s_random.GetInt(8, 512);
    std::string r_str = s_random.GetString(r_size);
    bool ret = mem_kv->Update(i, r_str.data(), r_str.size());
    CHECK(ret);
    random_strs.emplace_back(r_str);
  }

  std::string origin_info = mem_kv->GetInfo();
  delete mem_kv;

  MemKV restore_kv;
  CHECK(restore_kv.Initialize(1 << 20, 0.5, 1 << 30, 3600, 10, kTestMemKVRootDir, 0));

  std::string restore_info = restore_kv.GetInfo();

  std::cout << "mem kv origin: " << origin_info;
  std::cout << "mem kv restore: " << restore_info;

  for (size_t i = 0; i < 10000; i++) {
    auto read_data = restore_kv.Get(i);
    CHECK_EQ(std::string(read_data.data, read_data.size), random_strs[i]);
  }
}

void WriteMultiMemThread(MultiMemKV *p_mem, int index, int write_num = 100000) {
  PseudoRandom s_random(index);
  for (size_t i = 0; i < write_num; i++) {
    int r_size = s_random.GetInt(8, 2048);
    std::string r_str = s_random.GetString(r_size);
    uint64_t key = s_random.GetInt(index * 1000000, (index + 1) * 1000000);
    bool ret = p_mem->Update(key, r_str.data(), r_str.size());
    CHECK(ret);
  }
}

TEST_F(MultiMemKVTest, test_mem_kv_restore_fd_right) {
  auto mem_kv = new MultiMemKV(kTestMemKVRootDir, 8, 1 << 20, 3600, 1L << 40);

  std::vector<std::string> random_strs;
  PseudoRandom s_random;
  for (size_t i = 0; i < 10000; i++) {
    int r_size = s_random.GetInt(8, 512);
    std::string r_str = s_random.GetString(r_size);
    bool ret = mem_kv->Update(i, r_str.data(), r_str.size());
    CHECK(ret);
    random_strs.emplace_back(r_str);
  }
  std::vector<char> buf;
  for (size_t i = 0; i < 10000; i++) {
    auto read_data = mem_kv->Get(i);
    CHECK_EQ(std::string(read_data.data, read_data.size), random_strs[i]);
    auto data = mem_kv->ReadToBuffer(i, &buf);
    CHECK(data.size > 0);
    CHECK_EQ(std::string(data.data, data.size), random_strs[i]);
    CHECK_EQ(std::string(buf.data(), data.size), random_strs[i]);
  }

  delete mem_kv;

  auto restore_kv = new MultiMemKV(kTestMemKVRootDir, 8, 1 << 20, 3600, 1L << 40);

  for (size_t i = 0; i < 10000; i++) {
    auto read_data = restore_kv->Get(i);
    CHECK_EQ(std::string(read_data.data, read_data.size), random_strs[i]);
    auto data = restore_kv->ReadToBuffer(i, &buf);
    CHECK(data.size > 0);
    CHECK_EQ(std::string(data.data, data.size), random_strs[i]);
    CHECK_EQ(std::string(buf.data(), data.size), random_strs[i]);
  }
  delete restore_kv;
}

TEST_F(MultiMemKVTest, test_mem_kv_read_buffer_resize) {
  auto mem_kv = new MultiMemKV(kTestMemKVRootDir, 8, 1 << 20, 3600, 1L << 40);
  auto key_num = 10000;
  std::vector<std::string> random_strs;
  PseudoRandom s_random;
  for (size_t i = 0; i < key_num; i++) {
    int r_size = s_random.GetInt(8, 512);
    std::string r_str = s_random.GetString(r_size);
    bool ret = mem_kv->Update(i, r_str.data(), r_str.size());
    CHECK(ret);
    random_strs.emplace_back(r_str);
  }
  for (size_t i = 0; i < key_num; i++) {
    for (int j = 0; j < 1000; j++) {
      std::vector<char> buf(j, 0);
      auto data = mem_kv->ReadToBuffer(i, &buf);
      CHECK(data.size > 0);
      if (j < random_strs[i].size()) {
        CHECK_EQ(buf.size(), random_strs[i].size());
      } else {
        CHECK_EQ(buf.size(), j);
      }
      CHECK_EQ(std::string(data.data, data.size), random_strs[i]);
      CHECK_EQ(std::string(buf.data(), data.size), random_strs[i]);
    }
  }
  // no data
  std::vector<char> buf(1000, 0);
  for (int i = key_num + 1; i < 2 * key_num; i++) { CHECK(mem_kv->ReadToBuffer(i, &buf).size == 0); }

  delete mem_kv;
}

TEST_F(MultiMemKVTest, test_mem_kv_read_buffer_handler) {
  auto mem_kv = new MultiMemKV(kTestMemKVRootDir, 8, 1 << 20, 3600, 1L << 40);
  auto key_num = 10000;
  std::vector<std::string> random_strs;
  std::vector<std::string> used_strs;
  PseudoRandom s_random;
  std::vector<char> str(1024);
  for (size_t i = 0; i < key_num; i++) {
    int r_size = s_random.GetInt(8, 512);
    std::string r_str = s_random.GetString(r_size);
    int use_len = s_random.GetInt(0, r_size);
    memcpy(str.data(), &use_len, sizeof(use_len));
    memcpy(str.data() + sizeof(use_len), r_str.data(), r_size);
    bool ret = mem_kv->Update(i, str.data(), sizeof(use_len) + r_size);
    CHECK(ret);
    random_strs.emplace_back(std::string(str.data(), str.data() + sizeof(use_len) + r_size));
    used_strs.emplace_back(std::string(str.data(), str.data() + sizeof(use_len) + use_len));
  }
  for (size_t i = 0; i < key_num; i++) {
    std::vector<char> buf(1000, 0);
    auto data = mem_kv->ReadToBuffer(i, &buf);
    CHECK(data.size > 0);
    CHECK_EQ(std::string(data.data, data.size), random_strs[i]);
    CHECK_EQ(std::string(buf.data(), data.size), random_strs[i]);

    auto handler = [](const char *log, int size) {
      return *(reinterpret_cast<const int *>(log)) + sizeof(int);
    };
    buf.clear();
    data = mem_kv->ReadToBuffer(i, &buf, handler);
    CHECK_EQ(data.size, used_strs[i].size());
    CHECK_EQ(std::string(data.data, data.size), used_strs[i]);
    CHECK_EQ(std::string(buf.data(), data.size), used_strs[i]);

    auto invalid_handler = [](const char *log, int size) { return 1024; };
    buf.clear();
    data = mem_kv->ReadToBuffer(i, &buf, invalid_handler);
    CHECK(data.size == 0);
  }

  delete mem_kv;
}

TEST_F(MultiMemKVTest, test_single_part_calc_slabs_right) {
  auto single_mem_write = new MultiMemKV(kTestMemKVRootDir, 1, 1 << 26, 100, 1L << 40);
  WriteMultiMemThread(single_mem_write, 0);
  auto last_key_num = single_mem_write->KeyNum();
  delete single_mem_write;

  auto single_mem_load = new MultiMemKV(kTestMemKVRootDir, 1, 1 << 26, 100, 1L << 40);
  CHECK_EQ(last_key_num, single_mem_load->KeyNum());
  delete single_mem_load;
}

void MemKVUpdateSameKey() {
  MemKV mem_kv;
  mem_kv.Initialize(1 << 20, 0.5, 1 << 30, 3600, 10, "", 0);

  const uint64 key = 12345ul;
  const std::string old_value = "old value";
  const std::string new_value = "new value new";

  {
    bool ret = mem_kv.Update(key, old_value.data(), old_value.size());
    ASSERT_TRUE(ret);

    EXPECT_EQ(1, mem_kv.KeyNum());

    auto kv = mem_kv.Get(key);
    EXPECT_EQ(old_value.size(), kv.size);
    EXPECT_TRUE(strncmp(old_value.c_str(), kv.data, strlen(old_value.c_str())) == 0);
  }
  {
    bool ret = mem_kv.Update(key, new_value.data(), new_value.size());
    ASSERT_TRUE(ret);

    EXPECT_EQ(1, mem_kv.KeyNum());

    auto kv = mem_kv.Get(key);
    EXPECT_EQ(new_value.size(), kv.size);
    EXPECT_TRUE(strncmp(new_value.c_str(), kv.data, strlen(new_value.c_str())) == 0);
  }
  {
    bool ret = mem_kv.Update(key, old_value.data(), old_value.size());
    ASSERT_TRUE(ret);

    EXPECT_EQ(1, mem_kv.KeyNum());

    auto kv = mem_kv.Get(key);
    EXPECT_EQ(old_value.size(), kv.size);
    EXPECT_TRUE(strncmp(old_value.c_str(), kv.data, strlen(old_value.c_str())) == 0);
  }
}

TEST_F(MemKVTest, update_same_key) { MemKVUpdateSameKey(); }

TEST_F(MemKVTest, test_erase_key_right) {
  MemKV mem_kv;

  mem_kv.Initialize(1 << 20, 0.5, 1 << 30, 3600, 10, "", 0);

  PseudoRandom s_random;
  std::string r_str = s_random.GetString(8);
  bool ret = mem_kv.Update(1, r_str.data(), r_str.size());
  CHECK(ret);
  CHECK_EQ(mem_kv.KeyNum(), 1);
  mem_kv.Erase(1);
  CHECK_EQ(mem_kv.KeyNum(), 1);
  CHECK_EQ(mem_kv.ValidKeyNum(), 0);
  KVReadData data = mem_kv.Get(1);
  CHECK(data.data == nullptr);
}

TEST_F(MemKVTest, test_erase_key_if_right) {
  MemKV mem_kv;

  mem_kv.Initialize(1 << 20, 0.5, 1 << 30, 3600, 10, "", 0);

  PseudoRandom s_random;
  std::string r_str = s_random.GetString(8);
  bool ret = mem_kv.Update(1, r_str.data(), r_str.size());
  CHECK(ret);
  CHECK_EQ(mem_kv.KeyNum(), 1);
  ret = mem_kv.EraseKeyIf(1, [&](KVData *kv_data) {
    CHECK_EQ(kv_data->size(), r_str.size());
    CHECK_EQ(memcmp(r_str.data(), kv_data->data(), r_str.size()), 0);
    return false;
  });
  ASSERT_FALSE(ret);
  CHECK_EQ(mem_kv.KeyNum(), 1);
  CHECK_EQ(mem_kv.ValidKeyNum(), 1);
  ret = mem_kv.EraseKeyIf(1, [&](KVData *kv_data) {
    CHECK_EQ(kv_data->size(), r_str.size());
    CHECK_EQ(memcmp(r_str.data(), kv_data->data(), r_str.size()), 0);
    return true;
  });
  ASSERT_TRUE(ret);
  CHECK_EQ(mem_kv.KeyNum(), 1);
  CHECK_EQ(mem_kv.ValidKeyNum(), 0);
  KVReadData data = mem_kv.Get(1);
  CHECK(data.data == nullptr);
}

TEST_F(MemKVTest, test_erase_if_right) {
  MemKV mem_kv;

  mem_kv.Initialize(1 << 20, 0.5, 1 << 30, 3600, 10, "", 0);

  static const int kMaxKey = absl::GetFlag(FLAGS_memkv_max_key_for_test_erase_if);
  static const int kHalfKey = kMaxKey / 2;

  LOG(INFO) << "kMaxKey: " << kMaxKey << ", kHalfKey: " << kHalfKey;

  PseudoRandom s_random;
  for (size_t i = 0; i < kMaxKey; i++) {
    int size = (i < kHalfKey) ? 8 : 16;
    std::string r_str = s_random.GetString(size);
    bool ret = mem_kv.Update(i, r_str.data(), r_str.size());
    CHECK(ret);
  }
  CHECK_EQ(mem_kv.KeyNum(), kMaxKey);
  CHECK_EQ(mem_kv.ValidKeyNum(), kMaxKey);

  LOG(INFO) << "init 0-" << kMaxKey - 1 << " finished";

  uint32_t erased_nodes;
  // erased first half nodes
  erased_nodes = mem_kv.EraseIf([](uint64 key, base::KVData *data) { return key < kHalfKey; });

  CHECK_EQ(erased_nodes, kHalfKey);

  CHECK_EQ(mem_kv.KeyNum(), kMaxKey);
  // actually use half keys now
  CHECK_EQ(mem_kv.ValidKeyNum(), kHalfKey);

  LOG(INFO) << "erase first half finished";

  // data should miss now
  for (size_t i = 0; i < kMaxKey; i++) {
    KVReadData data = mem_kv.Get(i);
    if (i < kHalfKey) {
      CHECK(data.data == nullptr);
    } else {
      CHECK_EQ(data.size, 16);
    }
  }

  // update will trigger recycle, first half should be released now
  std::string s("fakedata");
  CHECK(mem_kv.Update(kHalfKey + 1, s.data(), s.size()));
  LOG(INFO) << "update " << kHalfKey + 1 << " finished";

  CHECK_EQ(mem_kv.KeyNum(), kHalfKey);

  erased_nodes = mem_kv.EraseIf([](uint64 key, base::KVData *data) { return true; });

  CHECK_EQ(erased_nodes, kHalfKey);

  // actually use 0 keys now
  CHECK_EQ(mem_kv.ValidKeyNum(), 0);

  LOG(INFO) << "erase all nodes finished";

  // data should miss now
  for (size_t i = 0; i < kMaxKey; i++) {
    KVReadData data = mem_kv.Get(i);
    CHECK(data.data == nullptr);
  }

  // update will trigger recycle
  for (int i = kHalfKey - 1000; i < kHalfKey + 1000; i++) { CHECK(mem_kv.Update(i, s.data(), s.size())); }
  LOG(INFO) << "update " << kHalfKey - 1000 << " to " << kHalfKey + 1000 << " finished";

  CHECK_EQ(mem_kv.ValidKeyNum(), 2000);

  // remove all key
  erased_nodes = mem_kv.EraseIf([](uint64 key, base::KVData *data) { return key < kHalfKey + 1000; });
  CHECK_EQ(erased_nodes, 2000);

  CHECK_EQ(mem_kv.ValidKeyNum(), 0);

  // update will trigger recycle
  CHECK(mem_kv.Update(kMaxKey + 1, s.data(), s.size()));
  LOG(INFO) << "update " << kMaxKey + 1 << " finished";

  // only 10001 left
  CHECK_EQ(mem_kv.ValidKeyNum(), 1);
}

TEST_F(MemKVTest, test_set_mem_limit) {
  MemKV mem_kv;
  // 16 MB
  constexpr const int kBlockSize = 16 * (1L << 20);
  // 512 KB
  constexpr const int kValueSize = 512 * 1024 - sizeof(KVData);
  int orig_mem_limit = 3 * kBlockSize;
  mem_kv.Initialize(1000, 0.5, orig_mem_limit, 3600, 10, "", 0);
  PseudoRandom s_random;
  std::vector<std::string> random_strs;
  // 16M / 512K = 32
  for (size_t i = 0; i < 96; i++) {
    std::string r_str = s_random.GetString(kValueSize);
    bool ret = mem_kv.Update(i, r_str.data(), r_str.size());
    CHECK(ret);
    random_strs.emplace_back(r_str);
  }

  LOG(INFO) << "memkv keynum: " << mem_kv.KeyNum();

  KVReadData data;
  data = mem_kv.Get(95);
  CHECK_EQ(data.size, random_strs[95].size());

  // total full will trigger update fail and recycle 2 keys.
  std::string r_str = s_random.GetString(kValueSize);
  bool ret = mem_kv.Update(96, r_str.data(), r_str.size());
  CHECK(!ret);

  // Increase mem_limit
  mem_kv.SetMemLimit(orig_mem_limit + kBlockSize);
  for (int i = 96; i < 96 + 5; i++) {
    std::string r_str = s_random.GetString(kValueSize);
    bool ret = mem_kv.Update(i, r_str.data(), r_str.size());
    CHECK(ret);
    random_strs.emplace_back(r_str);
  }

  // 23
  LOG(INFO) << "memkv keynum: " << mem_kv.KeyNum();

  data = mem_kv.Get(96);
  CHECK_EQ(data.size, random_strs[96].size());

  // Decrease mem_limit
  mem_kv.SetMemLimit(orig_mem_limit);
  for (int i = 96 + 5; i < 96 + 10; i++) {
    std::string r_str = s_random.GetString(kValueSize);
    bool ret = mem_kv.Update(i, r_str.data(), r_str.size());
    CHECK(ret);
    random_strs.emplace_back(r_str);
  }

  // 18
  LOG(INFO) << "memkv keynum: " << mem_kv.KeyNum();

  int last_valid_key = 106 - mem_kv.KeyNum();
  for (size_t i = 0; i < last_valid_key; i++) {
    data = mem_kv.Get(i);
    CHECK_EQ(data.size, 0);
  }
  for (int i = last_valid_key; i < 105; i++) {
    data = mem_kv.Get(i);
    CHECK_EQ(data.size, random_strs[i].size());
  }
}

void MemKVConcurrencyTestThread(MemKV *mem_kv, int index) {
  PseudoRandom s_random(index);
  UpdateHandler update_handler =
      [&](uint32 old_addr, uint64 key, const char *log, int size, int expire_time, MemKV *mem_kv) -> uint32 {
    bool is_create = (mem_kv->GetKVDataByAddr(old_addr) == nullptr);
    uint32_t cur_addr = old_addr;
    if (is_create) { cur_addr = mem_kv->MallocAddr(size); }
    auto cur_node = mem_kv->GetKVDataByAddr(cur_addr);
    if (cur_node == nullptr) {
      LOG_EVERY_N(WARNING, 10000) << "malloc addr may fail, cur_node is nullptr";
      return cur_addr;
    } else if (is_create) {
      memcpy(cur_node->data(), log, size);
    } else {
      // refresh expire_timet
      uint32_t now_ts = base::GetTimestamp() / 1000000;
      cur_node->expire_timet = now_ts + 1;
    }
    return cur_addr;
  };
  for (size_t i = 0; i < absl::GetFlag(FLAGS_memkv_concurrency_test_loop_num); i++) {
    LOG(INFO) << "thread " << index << " concurrency test, loop: " << i;
    for (int j = 0; j < 200000; j++) {
      int r_size = 1;  // use same value size to share the same slab
      std::string r_str = s_random.GetString(r_size);
      uint64 key = s_random.GetInt(1, 100000);
      bool ret = mem_kv->Update(key, r_str.data(), r_size, -1, update_handler);
      CHECK(ret);
    }

    LOG(INFO) << "update finished, memkv info: " << mem_kv->GetInfo();

    mem_kv->EraseIf([](uint64 key, KVData *value) { return true; });
    LOG(INFO) << "erase finished, memkv info: " << mem_kv->GetInfo();
    base::SleepForSeconds(1);
  }
}

void MemKVGetInfoThread(MemKV *mem_kv, std::atomic_bool *stop, int index) {
  while (!stop->load()) {
    std::string info = mem_kv->GetInfo();
    if (index == 0) { LOG_EVERY_N(INFO, 1000) << info; }
    base::SleepForMilliseconds(1);
  }
}

void MemKVUpdateAndGetInfoConcurrently() {
  MemKV mem_kv;

  mem_kv.Initialize(10000, 0.25, 1 << 30, 1, 1, "", 0);

  std::vector<std::string> random_strs;

  LOG(INFO) << "FLAGS_memkv_concurrency_test_thread_num: "
            << absl::GetFlag(FLAGS_memkv_concurrency_test_thread_num)
            << ", FLAGS_memkv_concurrency_test_loop_num: "
            << absl::GetFlag(FLAGS_memkv_concurrency_test_loop_num);

  thread::ThreadPool thread_pool(absl::GetFlag(FLAGS_memkv_concurrency_test_thread_num));
  for (size_t i = 0; i < absl::GetFlag(FLAGS_memkv_concurrency_test_thread_num); i++) {
    thread_pool.AddTask([&mem_kv, i]() { MemKVConcurrencyTestThread(&mem_kv, i); });
  }
  std::atomic_bool stop{false};
  thread::ThreadPool backgroud_pool(8);
  for (size_t i = 0; i < 8; i++) {
    backgroud_pool.AddTask([&mem_kv, &stop, i]() { MemKVGetInfoThread(&mem_kv, &stop, i); });
  }
  thread_pool.JoinAll();
  stop.store(true);
  backgroud_pool.JoinAll();
  LOG(INFO) << "MemKV concurrency test finished";
}

TEST_F(MemKVTest, concurrency_test) { MemKVUpdateAndGetInfoConcurrently(); }

// use /dev/shm for test quickly, needs about 6GB memory in total
TEST_F(MultiMemKVTest, test_sync_all_kv_right) {
  const auto kTestSyncFromDir = kTestMemKVRootDir + "/from";
  const auto kTestSyncToDir = kTestMemKVRootDir + "/to";
  const auto kTestSyncCapacity = 1 << 25;

  int shard_num = 5;

  auto memkv_from = new MultiMemKV(kTestSyncFromDir, shard_num, kTestSyncCapacity, 100, 1L << 40);
  WriteMultiMemThread(memkv_from, 0);
  auto memkv_from_key_num = memkv_from->KeyNum();

  LOG(INFO) << "memkv_from MemUse: " << memkv_from->MemUse();

  auto memkv_to = new MultiMemKV(kTestSyncToDir, shard_num, kTestSyncCapacity, 100, 1L << 40);

  LOG(INFO) << "start sync, concurrency: " << absl::GetFlag(FLAGS_multi_memkv_sync_concurrency_for_test);
  memkv_to->SyncAllKVFrom(memkv_from, absl::GetFlag(FLAGS_multi_memkv_sync_concurrency_for_test));

  auto memkv_to_key_num = memkv_to->KeyNum();

  LOG(INFO) << "memkv_to MemUse: " << memkv_to->MemUse();

  CHECK_EQ(memkv_from_key_num, memkv_to_key_num);

  std::vector<uint64> keys;
  int part_num = 1024;
  for (size_t i = 0; i < shard_num; i++) {
    for (int j = 0; j < part_num; j++) {
      memkv_from->GetPartKeys(i, part_num, j, &keys);
      for (auto key : keys) {
        KVReadData data_from = memkv_from->Get(key);
        KVReadData data_to = memkv_to->Get(key);
        CHECK_EQ(std::string(data_from.data, data_from.size), std::string(data_to.data, data_to.size));
      }
    }
  }

  delete memkv_from;
  delete memkv_to;
}

TEST_F(MultiMemKVTest, test_traverse_op) {
  int shard_num = 5;
  auto memkv = new MultiMemKV(kTestMemKVRootDir, shard_num, 1 << 26, 100, 1L << 40);
  WriteMultiMemThread(memkv, 0);
  auto key_num = memkv->KeyNum();
  LOG(INFO) << "memkv MemUse: " << memkv->MemUse();

  // traverse all
  int thread_num = 10;
  std::vector<std::future<void>> fs;
  std::vector<uint64_t> counts(thread_num, 0);
  for (size_t i = 0; i < thread_num; ++i) {
    auto f = std::async([memkv, i, &counts] {
      auto op = [i, &counts](uint64_t key, base::KVData *data) { ++(counts[i]); };
      for (int shard = 0; shard < memkv->PartNum(); ++shard) { memkv->TraverseOp(shard, op); }
    });
    fs.push_back(std::move(f));
  }
  for (auto &f : fs) f.wait();
  for (int count : counts) { CHECK_EQ(count, key_num); }

  // traverse with exit_op
  fs.clear();
  counts.assign(thread_num, 0);
  std::atomic<bool> stop(false);
  auto exit_op = [&stop]() -> bool { return stop; };
  for (size_t i = 0; i < thread_num; ++i) {
    auto f = std::async([memkv, i, &counts, &exit_op] {
      auto op = [i, &counts](uint64_t key, base::KVData *data) { ++(counts[i]); };
      for (int shard = 0; shard < memkv->PartNum(); ++shard) { memkv->TraverseOp(shard, op, exit_op); }
    });
    fs.push_back(std::move(f));
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(1));
  stop = true;
  for (auto &f : fs) f.wait();
  for (int count : counts) { CHECK_LT(count, key_num); }

  delete memkv;
}

TEST_F(MultiMemKVTest, test_mem_kv_multi_get_for_prefetch) {
  auto mem_kv = new MultiMemKV(kTestMemKVRootDir, 8, 1 << 20, 3600, 1L << 40);

  std::vector<std::string> random_strs;
  PseudoRandom s_random;
  for (size_t i = 0; i < 10000; i++) {
    int r_size = s_random.GetInt(8, 512);
    std::string r_str = s_random.GetString(r_size);
    bool ret = mem_kv->Update(i, r_str.data(), r_str.size());
    CHECK(ret);
    random_strs.emplace_back(r_str);
  }

  constexpr size_t max_batch = MultiMemKV::kMaxBatchSize;
  uint64 keys[max_batch];
  KVReadData read_datas[max_batch];

  std::vector<uint64> all_keys;

  // NOTE: use i++ instead of i += batch, ensure cover all keys
  for (size_t i = 0; i < 10000; i++) {
    auto batch = std::min(10000 - i, max_batch);
    for (int j = 0; j < batch; j++) { keys[j] = i + j; }
    mem_kv->BatchGet(keys, read_datas, batch);

    for (int j = 0; j < batch; j++) {
      CHECK_EQ(std::string(read_datas[j].data, read_datas[j].size), random_strs[i + j]);
    }

    all_keys.push_back(i);
  }

  mem_kv->BatchGet(all_keys.data(), all_keys.size(), [&](size_t idx, uint64, KVReadData read_data) {
    CHECK_EQ(std::string(read_data.data, read_data.size), random_strs[idx]);
  });

  std::vector<uint64> should_miss_keys{1000001, 1000002, 1000003, 1000004, 1000005};
  mem_kv->BatchGet(
      should_miss_keys.data(), should_miss_keys.size(), [&](size_t idx, uint64, KVReadData read_data) {
        CHECK_EQ(read_data.size, 0);
        CHECK_EQ(read_data.data, nullptr);
      });

  delete mem_kv;
}

TEST_F(MultiMemKVTest, test_mem_kv_get_value_size) {
  auto mem_kv = new MultiMemKV(kTestMemKVRootDir, 8, 1 << 20, 3600, 1L << 40);

  std::vector<std::string> random_strs;
  PseudoRandom s_random;
  for (size_t i = 0; i < 10000; i++) {
    int r_size = s_random.GetInt(8, 512);
    std::string r_str = s_random.GetString(r_size);
    bool ret = mem_kv->Update(i, r_str.data(), r_str.size());
    CHECK(ret);
    random_strs.emplace_back(r_str);
  }
  std::vector<std::pair<uint64_t, size_t>> result;
  mem_kv->GetKeysAndValueSizes(&result);
  CHECK(result.size() == 10000);
  for (auto kv : result) {
    auto key = kv.first;
    CHECK(kv.second == random_strs[key].size());
  }
  result.clear();
  mem_kv->GetKeysAndValueSizes(&result, [](uint64_t key, KVData *value) -> bool { return key % 2 == 1; });
  CHECK(result.size() == 5000);
  for (auto kv : result) {
    auto key = kv.first;
    CHECK(kv.second == random_strs[key].size());
  }
  result.clear();
  mem_kv->GetKeysAndValueSizes(
      &result,
      [](uint64_t key, KVData *value) -> bool { return key % 3 == 0; },
      [](uint64_t key, KVData *value) -> size_t { return value->stored_size; });
  CHECK(result.size() == 10000 / 3 + 1);
  for (auto kv : result) {
    auto key = kv.first;
    CHECK(kv.second == random_strs[key].size() + sizeof(uint32_t) + sizeof(int));
  }
  delete mem_kv;
}

}  // namespace base
