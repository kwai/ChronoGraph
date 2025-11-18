#pragma once

#include <string>

namespace base {

class PerfWrapper {
 public:
  // Reports a metric point for monitoring.
  // Points are aggregated in dashboards by sum(count).
  static void CountLogStash(int64_t count,
                            const std::string &ns,
                            const std::string &sub_tag,
                            const std::string &ext1 = "",
                            const std::string &ext2 = "",
                            const std::string &ext3 = "",
                            const std::string &ext4 = "",
                            const std::string &ext5 = "",
                            const std::string &ext6 = "");
  // Points are aggregated in dashboards by avg(count).
  static void IntervalLogStash(int64_t count,
                               const std::string &ns,
                               const std::string &sub_tag,
                               const std::string &ext1 = "",
                               const std::string &ext2 = "",
                               const std::string &ext3 = "",
                               const std::string &ext4 = "",
                               const std::string &ext5 = "",
                               const std::string &ext6 = "");
};

}  // namespace base
