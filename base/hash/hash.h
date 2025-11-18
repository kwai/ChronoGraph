// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <string>
#include <vector>

#include "base/common/time.h"
#include "base/hash/city.h"

namespace base {

inline uint64 GetHash(uint64 key) {
  return base::CityHash64(reinterpret_cast<const char *>(&key), sizeof(uint64));
}

inline uint64 GetHash(const std::string &key) { return base::CityHash64(key.data(), key.size()); }

// For situations that you need to shard by hash, and split buckets by hash inside each shard.
inline uint64 GetHashWithLevel(uint64 key, int level) {
  return base::CityHash64WithSeeds(reinterpret_cast<const char *>(&key), sizeof(uint64), kInt32Max, level);
}

inline uint64 GetHashWithLevel(const std::string &key, int level) {
  return base::CityHash64WithSeeds(key.data(), key.size(), kInt32Max, level);
}

inline uint64 GetJoinHash(uint64 hash, uint64 join_value) {
  return base::CityHash64WithSeed(reinterpret_cast<const char *>(&hash), sizeof(uint64), GetHash(join_value));
}

inline uint64 GetJoinHash(uint64 hash, const std::string &join_value) {
  return base::CityHash64WithSeed(reinterpret_cast<const char *>(&hash), sizeof(uint64), GetHash(join_value));
}

inline uint64 RandomSeed() { return GetHash(base::GetTimestamp()); }

}  // namespace base