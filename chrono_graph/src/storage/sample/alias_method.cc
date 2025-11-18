// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "chrono_graph/src/storage/sample/alias_method.h"

#include <vector>
#include "base/common/logging.h"
#include "base/hash/hash.h"
#include "base/random/pseudo_random.h"

namespace chrono_graph {

AliasMethod::AliasMethod(const std::vector<float> &weights) { Reset(weights); }

bool AliasMethod::Reset(const std::vector<float> &weights) {
  if (weights.empty()) {
    samples_count_ = 0;
    alias_.clear();
    probs_.clear();
    return false;
  }
  samples_count_ = weights.size();
  alias_.resize(weights.size());
  probs_.resize(weights.size());
  Build(weights);
  return true;
}

void AliasMethod::Build(const std::vector<float> &weights) {
  CHECK(!weights.empty()) << "alias method build by an empty weights array!";
  std::size_t count = weights.size();

  std::vector<int32_t> high_set;  // offsets with prob > 1.0
  std::vector<int32_t> low_set;   // offsets with prob < 1.0
  high_set.reserve(count / 2 + 1);
  low_set.reserve(count / 2 + 1);

  // initialize.
  float sum = std::accumulate(weights.begin(), weights.end(), 0.0);
  for (std::size_t offset = 0; offset < count; offset++) {
    alias_[offset] = offset;
    probs_[offset] = (weights[offset] / sum) * count;  // normalize weight
    if (probs_[offset] < 1) {
      low_set.push_back(offset);
    } else if (probs_[offset] > 1) {
      high_set.push_back(offset);
    }
  }

  // update.
  std::size_t low_num = low_set.size();
  std::size_t high_num = high_set.size();
  while (low_num > 0 && high_num > 0) {
    int32_t low_idx = low_set[--low_num];
    int32_t high_idx = high_set[--high_num];
    probs_[high_idx] = probs_[high_idx] - (1 - probs_[low_idx]);
    alias_[low_idx] = high_idx;
    if (probs_[high_idx] < 1.0) {
      low_set[low_num++] = high_idx;
    } else if (probs_[high_idx] > 1.0) {
      high_set[high_num++] = high_idx;
    }
  }

  // float calculation may cause precision loss, just ignore the final imbalance.
  while (low_num > 0) { probs_[low_set[--low_num]] = 1.0; }
  while (high_num > 0) { probs_[high_set[--high_num]] = 1.0; }
}

double AliasMethod::UniformRandom(int count) {
  thread_local base::PseudoRandom random(base::RandomSeed());
  return random.GetDouble() * count;
}

bool AliasMethod::Sample(size_t sample_num, std::vector<size_t> *sampled_offset) {
  if (samples_count_ == 0) { return false; }
  sampled_offset->clear();
  sampled_offset->resize(sample_num);
  for (size_t i = 0; i < sample_num; i++) {
    double rand = UniformRandom(samples_count_);
    auto idx = static_cast<size_t>(rand);
    (*sampled_offset)[i] = ((rand - idx) < probs_[idx]) ? idx : alias_[idx];
  }
  return true;
}

}  // namespace chrono_graph
