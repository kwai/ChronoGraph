// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

namespace chrono_graph {

template <typename ElementT>
bool AliasMethod::SampleAndSet(size_t sample_num, ElementT *result, std::function<ElementT(size_t)> op) {
  if (samples_count_ == 0) { return false; }
  for (size_t i = 0; i < sample_num; i++) {
    double rand = UniformRandom(samples_count_);
    auto idx = static_cast<size_t>(rand);
    size_t index = ((rand - idx) < probs_[idx]) ? idx : alias_[idx];
    result[i] = op(index);
  }
  return true;
}

}  // namespace chrono_graph