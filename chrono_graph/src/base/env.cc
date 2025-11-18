// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "chrono_graph/src/base/env.h"

#include <string>

namespace chrono_graph {

int global_shard_num = 1;
int global_shard_id = 0;
std::string global_exp_name = "";

void SetShardId(int id) { global_shard_id = id; }
void SetShardNum(int num) { global_shard_num = num; }
void SetExpName(std::string exp_name) { global_exp_name = exp_name; }

}  // namespace chrono_graph
