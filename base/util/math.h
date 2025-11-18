// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <cmath>

namespace base {

static const double kDefaultEpsilon = 1e-5;

inline bool IsEqual(double d1, double d2, double epsilon = kDefaultEpsilon) {
  return std::abs(d1 - d2) < epsilon;
}

inline bool IsGreater(double d1, double d2, double epsilon = kDefaultEpsilon) { return d1 - d2 > epsilon; }

inline bool IsGreaterEqual(double d1, double d2, double epsilon = kDefaultEpsilon) {
  return d1 - d2 >= -epsilon;
}

inline bool IsLess(double d1, double d2, double epsilon = kDefaultEpsilon) { return d1 - d2 < -epsilon; }

inline bool IsLessEqual(double d1, double d2, double epsilon = kDefaultEpsilon) { return d1 - d2 <= epsilon; }

inline bool IsZero(double d, double epsilon = kDefaultEpsilon) { return std::abs(d) < epsilon; }

inline bool IsGreaterThanZero(double d, double epsilon = kDefaultEpsilon) { return d > epsilon; }

inline bool IsGreaterEqualThanZero(double d, double epsilon = kDefaultEpsilon) { return d >= -epsilon; }

inline bool IsLessThanZero(double d, double epsilon = kDefaultEpsilon) { return d < -epsilon; }

inline bool IsLessEqualThanZero(double d, double epsilon = kDefaultEpsilon) { return d <= epsilon; }

}  // namespace base
