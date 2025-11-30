#include "base/thread/rw_mutex_manager.h"

ABSL_FLAG(int32, rw_mutex_slice_num_per_part, 1024, "read-write mutex slice num per MemKV part");