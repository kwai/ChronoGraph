// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#include <string>

#include "base/common/logging.h"
#include "base/testing/gtest.h"
#include "chrono_graph/src/base/util.h"

namespace chrono_graph {

TEST(BaseUtilTest, GetTSFromDTPathTEST) {
  std::string dt_path =
      "viewfs://hadoop-xx-cluster/home/fake/service_name=fake2/relation_name=fake3/dt=20210526";
  uint32 ts = GetTimestampFromDTPath(dt_path);
  ASSERT_EQ(ts, 1621958400);

  // Test valid dt inside
  dt_path = "viewfs://hadoop-xx-cluster/home/fake/service_name=fake2/relation_name=fake3/dt=20210601dummy";
  ts = GetTimestampFromDTPath(dt_path);
  ASSERT_EQ(ts, 1622476800);

  // Test path without dt
  dt_path =
      "viewfs://hadoop-xx-cluster/home/fake/service_name=fake2/relation_name=fake3/.hive-staging_hive_file";
  ts = GetTimestampFromDTPath(dt_path);
  ASSERT_EQ(ts, 0);

  // Test invalid dt inside
  dt_path = "viewfs://hadoop-dt=cluster/home/fake/service_name=fake2/relation_name=fake3/dt=20210601";
  ts = GetTimestampFromDTPath(dt_path);
  ASSERT_EQ(ts, 0);

  // Test dt with no value
  dt_path = "viewfs://hadoop-xx-cluster/home/fake/service_name=fake2/relation_name=fake3/dt=";
  ts = GetTimestampFromDTPath(dt_path);
  ASSERT_EQ(ts, 0);

  // Test dt with hour & minute
  dt_path = "viewfs://hadoop-xx-cluster/home/fake/service_name=fake2/relation_name=fake3/dt=202108131804";
  ts = GetTimestampFromDTPath(dt_path);
  ASSERT_EQ(ts, 1628849040);
}

TEST(BaseUtilTest, RangeMatcher) {
  RangeMatcher rm("0,10,20,30,50,80,100", ",");
  CHECK_EQ(rm.MatchKey(35), "30-50");
  CHECK_EQ(rm.MatchKey(20), "20-30");
  CHECK_EQ(rm.MatchKey(-10), "floor-0");
  CHECK_EQ(rm.MatchKey(120), "100-upper");
}

}  // namespace chrono_graph
