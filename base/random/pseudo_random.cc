// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "base/random/pseudo_random.h"

#include <random>

#include "base/common/logging.h"

namespace base {

PseudoRandom::PseudoRandom(uint64 seed) { Reset(seed); }

PseudoRandom::PseudoRandom() { Reset(kDefaultSeed); }

void PseudoRandom::Reset(uint64 seed) {
  std::seed_seq seed_seq{seed};
  gen_ = absl::BitGen(seed_seq);
}

PseudoRandom::~PseudoRandom() {}

// Returns a random number in range [min, max].
int PseudoRandom::GetInt(int min, int max) {
  DCHECK_LE(min, max);
  return min + GetUint64LT(max - min + 1);
}

// Returns a random double in range [0, 1).
double PseudoRandom::GetDouble() { return absl::Uniform(gen_, 0.0, 1.0); }

// Returns a random number in range [0, kuint64max].
uint64 PseudoRandom::GetUint64() {
  return absl::Uniform<uint64>(absl::IntervalClosed, gen_, 0UL, kUint64Max);
}

// Returns a random number in range [0, max).
// NOTE: return 0 when parameter max is 0
uint64 PseudoRandom::GetUint64LT(uint64 max) {
  DCHECK_LT(0u, max);
  if (max == 0) return 0;
  uint64 a;
  do { a = GetUint64(); } while (a >= kUint64Max - (kUint64Max % max));
  return a % max;
}

// Fills |output_length| bytes of |output| with random data.
void PseudoRandom::GetBytes(void *output, size_t output_length) {
  uint8 *data = reinterpret_cast<uint8 *>(output);

  size_t len = output_length / 8 * 8;
  for (size_t i = 0; i < len; i += 8) {
    uint64 t = GetUint64();
    *reinterpret_cast<uint64 *>(data + i) = t;
  }

  for (size_t i = len; i < output_length; ++i) {
    uint64 t = GetUint64();
    data[i] = (reinterpret_cast<char *>(&t))[0];
  }
}

static char *WriteIntoImpl(std::string *str, size_t length_with_null) {
  str->reserve(length_with_null);
  str->resize(length_with_null - 1);
  return &((*str)[0]);
}

// Fills a string of length |length| with with random data and returns it.
//
// Not that this is a variation of |RandBytes| with a different return type.
std::string PseudoRandom::GetString(size_t length) {
  std::string result;
  GetBytes(WriteIntoImpl(&result, length + 1), length);
  return result;
}

// Note that this can be used as an adapter for std::random_shuffle():
// Given a pre-populated |std::vector<int> myvector|, shuffle it as
//   std::random_shuffle(myvector.begin(), myvector.end(), base::RandGenerator);
uint64 PseudoRandom::operator()(uint64 max) { return GetUint64LT(max); }

}  // namespace base
