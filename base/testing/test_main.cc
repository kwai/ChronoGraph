// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include "base/common/gflags.h"
#include "base/common/logging.h"
#include "base/testing/gtest.h"

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  InitLogging();

  return RUN_ALL_TESTS();
}