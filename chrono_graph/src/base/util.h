// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "absl/time/time.h"
#include "base/common/basic_types.h"
#include "base/common/logging.h"
#include "base/common/string.h"
#include "chrono_graph/src/base/env.h"

namespace chrono_graph {

/**
 * \brief Transform timestamp to seconds.
 * \param timestamp Input timestamp in seconds, milliseconds or microseconds.
 *                  If the timestamp is negative, it will be regarded as 0.
 * \return timestamp in seconds.
 */
inline uint32 ConvertTimestampToSec(int64 timestamp) {
  const int64 kYearUs = 365ul * 24ul * 3600ul * 1000000ul;
  const int64 kYearMs = 365ul * 24ul * 3600ul * 1000ul;
  const int64 kYearSec = 365ul * 24ul * 3600ul;
  const int64 kMaxYear = 137;  // uint32 timestamp in seconds can represent at most 136.2 year
  if (timestamp < 0) {
    LOG_EVERY_N_SEC(ERROR, 10) << "invalid timestamp: " << timestamp << ". Regard it as 0.";
    return 0;
  }
  if (timestamp < kMaxYear * kYearSec) return timestamp;
  if (timestamp < kMaxYear * kYearMs) { return timestamp / 1000; }
  if (timestamp < kMaxYear * kYearUs) { return timestamp / 1000 / 1000; }
  LOG_EVERY_N_SEC(ERROR, 10) << "timestamp too large: " << timestamp << ". Regard it as 0.";
  return 0;
}

/**
 * \brief Parse timestamp in second from input dt_path
 * \param dt_path Formatted as "dt=YYYYmmdd" or "dt=YYYYmmddHHMM"
 * \return 0 if parse failed, otherwise return timestamp in second.
 */
inline uint32 GetTimestampFromDTPath(const std::string &dt_path) {
  int n;
  if ((n = dt_path.find("dt=")) == std::string::npos) { return 0; }
  std::string sub = dt_path.substr(n + 3, 12);
  std::string YYYY, mm, dd, HH, MM;
  try {
    YYYY = sub.substr(0, 4);
    mm = sub.substr(4, 2);
    dd = sub.substr(6, 2);
  } catch (...) {
    LOG(WARNING) << "Invalid format of dt_path: " << dt_path;
    return 0;
  }
  HH = "00";
  MM = "00";
  int hour, minute;
  bool parsed_hour_min = true;
  if (sub.length() == 12) {
    try {
      hour = std::stoi(sub.substr(8, 2));
      if (hour < 24 && hour >= 0) { HH = sub.substr(8, 2); }
    } catch (...) { parsed_hour_min = false; }
    try {
      minute = std::stoi(sub.substr(10, 2));
      if (minute < 60 && minute >= 0) { MM = sub.substr(10, 2); }
    } catch (...) { parsed_hour_min = false; }
  } else {
    parsed_hour_min = false;
  }
  if (!parsed_hour_min) {
    HH = "00";
    MM = "00";
  }
  absl::Time absl_time;
  try {
    absl::CivilSecond civil_time(std::stoi(YYYY), std::stoi(mm), std::stoi(dd), std::stoi(HH), std::stoi(MM));
    absl::TimeZone shanghai_tz;
    absl::LoadTimeZone("Asia/Shanghai", &shanghai_tz);
    absl_time = absl::FromCivil(civil_time, shanghai_tz);
  } catch (...) {
    LOG(WARNING) << "stoi failed: " << YYYY << ", " << mm << ", " << dd << ", " << HH << ", " << MM;
    return 0;
  }

  return absl::ToUnixSeconds(absl_time);
  ;
}

/**
 * \brief Used for range matching.
 * Example: Initialized range string is 0,10,20,50,100.
 * MatchKey input 25, it matches [20, 50), so return "20-50"
 * MatchKey input 1000, return "upper"
 */
class RangeMatcher {
 public:
  RangeMatcher() = delete;
  explicit RangeMatcher(const std::string &range, const std::string &delim = ",") {
    std::vector<std::string> ranges = absl::StrSplit(range, delim);

    for (const auto &range : ranges) {
      int64 v;
      CHECK(absl::SimpleAtoi(range, &v))
          << "range matcher parse fail: " << range << " is not a number, total value = " << range
          << ", delim = " << delim;
      ranges_.push_back(v);
    }
    GenerateRangeKeys();
  }

  explicit RangeMatcher(std::vector<int64> &&range) {
    ranges_ = std::move(range);
    GenerateRangeKeys();
  }

  std::string MatchKey(int64 value) const {
    for (size_t i = 0; i < ranges_.size(); ++i) {
      if (value < ranges_[i]) { return range_keys_[i]; }
    }
    return *(range_keys_.end() - 1);
  }
  ~RangeMatcher() {}

 private:
  void GenerateRangeKeys() {
    std::sort(ranges_.begin(), ranges_.end());
    std::string b = "floor";
    for (auto range : ranges_) {
      range_keys_.push_back(b + "-" + std::to_string(range));
      b = std::to_string(range);
    }
    range_keys_.push_back(b + "-" + "upper");
  }

  std::vector<int64> ranges_;
  std::vector<std::string> range_keys_;
};

}  // namespace chrono_graph
