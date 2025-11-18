// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <string>

#include "absl/random/random.h"
#include "base/common/basic_types.h"

namespace base {

const uint64 kDefaultSeed = 0xDEADBEAFFACEFEED;

class PseudoRandom {
 public:
  // Init with the given seed.
  explicit PseudoRandom(uint64 seed);

  // Init with the default seed |kDefaultSeed|, equivalent to PseudoRandom(kDefaultSeed)
  PseudoRandom();

  ~PseudoRandom();

  // Reset the random number generator with |seed|
  void Reset(uint64 seed);

  // Returns a random number in range [min, max]
  // NOTE: both min and max are inclusive.
  int GetInt(int min, int max);

  // Returns a random double in range [0, 1)
  // NOTE: 0 is inclusive and 1 is exclusive.
  double GetDouble();

  // Returns a random number in range [0, kUint64Max]
  // NOTE: both 0 and kUint64Max are inclusive.
  uint64 GetUint64();

  // Returns a random unsigned 64bit number less than |max|, i.e. in range [0, max).
  // NOTE: 0 is inclusive and max is exclusive.
  // NOTE: return 0 when parameter max is 0
  uint64 GetUint64LT(uint64 max);

  // Fills |output_length| bytes of |output| with random data.
  void GetBytes(void *output, size_t output_length);

  // Fills a string of length |length| with with random data and returns it.
  //
  // Not that this is a variation of |RandBytes| with a different return type.
  std::string GetString(size_t length);

  // Returns a random number in range [0, max).
  // NOTE: 0 is inclusive and max is exclusive.
  //
  // Note that this can be used as an adapter for std::random_shuffle():
  // Given a pre-populated |std::vector<int> myvector|, shuffle it as
  //   std::random_shuffle(myvector.begin(), myvector.end(), base::PseudoRandom(base::GetTimestamp()));
  uint64 operator()(uint64 max);

 private:
  absl::BitGen gen_;

  DISALLOW_COPY_AND_ASSIGN(PseudoRandom);
};

}  // namespace base
