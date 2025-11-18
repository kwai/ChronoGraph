// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <string>

namespace chrono_graph {

// Read-Only Global Variables that should be set during initialization.

extern int global_shard_id;
extern int global_shard_num;

extern std::string global_exp_name;

void SetShardId(int id);
void SetShardNum(int num);
void SetExpName(std::string exp_name);

// GraphKV is designed to be sharded, this function judge whether the given key should be stored on this host.
template <typename T>
bool HitLocalGraphKV(T key) {
  return (key % global_shard_num) == global_shard_id;
}

}  // namespace chrono_graph
