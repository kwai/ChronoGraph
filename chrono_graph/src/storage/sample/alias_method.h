// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <utility>
#include <vector>
#include "base/common/basic_types.h"
#include "base/common/logging.h"

namespace chrono_graph {

/**
 * AliasMethod: efficient algorithm for sampling from a discrete probability distribution.
 * build time O(n), sample time O(1).
 * \ref https://en.wikipedia.org/wiki/Alias_method
 */
class AliasMethod {
 public:
  AliasMethod() = delete;

  AliasMethod(AliasMethod &&am) {
    samples_count_ = am.samples_count_;
    alias_ = std::move(am.alias_);
    probs_ = std::move(am.probs_);
  }

  AliasMethod &operator=(AliasMethod &&am) {
    samples_count_ = am.samples_count_;
    alias_ = std::move(am.alias_);
    probs_ = std::move(am.probs_);
    return *this;
  }

  explicit AliasMethod(const std::vector<float> &weights);

  bool Reset(const std::vector<float> &weights);

  /**
   * \return false when sample pool is empty
   */
  bool Sample(size_t sample_num, std::vector<size_t> *sampled_offset);

  /**
   * \tparam ElementT type of sampled item.
   * \param result pointer to sampled result.
   * \param op input = sampled offset, output = correspond element.
   * \return false when sample pool is empty
   */
  template <typename ElementT>
  bool SampleAndSet(size_t sample_num, ElementT *result, std::function<ElementT(size_t)> op);

 private:
  void Build(const std::vector<float> &weights);
  double UniformRandom(int count);

  int32_t samples_count_;
  std::vector<size_t> alias_;
  std::vector<float> probs_;
  DISALLOW_COPY_AND_ASSIGN(AliasMethod);
};

}  // namespace chrono_graph

#include "alias_method-inl.h"
