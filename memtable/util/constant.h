// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <cstdint>
#include <cstring>
#include <ctime>
#include <functional>
#include <tuple>
#include <vector>

namespace base {

#pragma pack(push, 1)
struct KVData {
  uint32_t expire_timet = 0;
  uint32_t stored_size = 0;

  char *data() { return reinterpret_cast<char *>(this + 1); }
  uint32_t size() {
    int real_size = stored_size - sizeof(stored_size) - sizeof(expire_timet);
    return (real_size < 0 ? 0 : real_size);
  }

  void store(const char *s_data, int s_size) {
    if (s_size <= 0) return;
    stored_size = s_size + sizeof(stored_size) + sizeof(expire_timet);
    memcpy(data(), s_data, s_size);
  }

  // 0 is never expire
  bool expired() { return !(expire_timet == 0 || expire_timet > time(NULL)); }
};

template <typename KeyT, typename ValueT>
struct TemplateHashNodeT {
  KeyT key;
  ValueT value;
  uint32_t next;
};
#pragma pack(pop)

static const uint64_t PrimeListS[] = {
    53ul,        97ul,        193ul,       389ul,       769ul,        1543ul,       3079ul,
    6151ul,      12289ul,     24593ul,     49157ul,     98317ul,      196613ul,     393241ul,
    786433ul,    1572869ul,   3145739ul,   6291469ul,   12582917ul,   25165843ul,   50331653ul,
    100663319ul, 200000033ul, 201326611ul, 210000047ul, 220000051ul,  230000059ul,  240000073ul,
    250000103ul, 260000137ul, 270000161ul, 280000241ul, 290000251ul,  300000277ul,  310000283ul,
    320000287ul, 330000371ul, 340000387ul, 350000411ul, 360000451ul,  370000489ul,  380000519ul,
    390000521ul, 400000543ul, 402653189ul, 805306457ul, 1610612741ul, 3221225473ul, 4294967291ul};

inline uint64_t NextPrime(uint64_t size) {
  uint32_t prime_size = sizeof(PrimeListS) / sizeof(PrimeListS[0]);
  const uint64_t *first = PrimeListS;
  const uint64_t *last = PrimeListS + prime_size;
  const uint64_t *pos = std::lower_bound(first, last, size);
  return (pos == last) ? *(last - 1) : *pos;
}

#define MT_UNLIKE(x) __builtin_expect((x), 0)

class MemKV;

typedef std::function<uint32_t(uint32_t, uint64_t, const char *, int, int, MemKV *)> UpdateHandler;
// (key, data, size) -> read_size
typedef std::function<uint32_t(const char *, int)> GetHandler;
typedef std::function<void(KVData *)> RecycleHandler;
typedef std::function<void(uint64_t, KVData *)> RecycleWithKeyHandler;
typedef std::function<void(std::vector<uint32_t> *)> GenerateSlabHandler;
// len, node_num, block_num, capacity
typedef std::tuple<uint32_t, uint32_t, uint32_t, uint32_t> SlabPair;
// len, block_num, node_cap, node_use, node_free
typedef std::tuple<uint32_t, uint32_t, uint32_t, uint32_t, uint32_t> SlabInfoTuple;

// key | data | size | expire_timet
typedef std::tuple<uint64_t, const char *, int, uint32_t> KVSyncData;
struct KVReadData {
  int size = 0;
  const char *data = nullptr;
  uint32_t expire_timet = 0;

  void copy_from(KVData *kv_data) {
    size = kv_data->size();
    data = kv_data->data();
    expire_timet = kv_data->expire_timet;
  }

  bool expired() { return !(expire_timet == 0 || expire_timet > time(NULL)); }
};

}  // namespace base
