#pragma once

#include <memory>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "base/common/basic_types.h"
#include "base/common/gflags.h"
#include "base/common/logging.h"

ABSL_DECLARE_FLAG(int32, rw_mutex_slice_num_per_part);

namespace thread {

class RWMutexManager {
 public:
  explicit RWMutexManager(int part_num)
      : part_num_(part_num),
        slice_num_(absl::GetFlag(FLAGS_rw_mutex_slice_num_per_part)) {
    auto num = part_num_ * slice_num_;
    rw_mutexes_.resize(num);
    for (int i = 0; i < num; ++i) {
      rw_mutexes_[i] = std::make_unique<absl::Mutex>();
    }
  }
  ~RWMutexManager() {}

  absl::Mutex *GetMutexByPartKey(int part_id, uint64 key) {
    return (rw_mutexes_[GetMutexIndex(part_id, key)]).get();
  }

  int GetMutexIndex(int part_id, uint64 key) {
    CHECK_LT(part_id, part_num_)
        << "Invalid part_id: " << part_id << ", part_num: " << part_num_;
    return part_id * slice_num_ + key % slice_num_;
  }

 private:
  // number of MemKV parts
  int part_num_;
  // number of mutex slices per MemKV part
  int slice_num_;
  std::vector<std::unique_ptr<absl::Mutex>> rw_mutexes_;
};

}  // namespace thread
