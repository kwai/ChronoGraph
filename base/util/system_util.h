// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <unistd.h>
#include <string>

namespace base {

/** \brief Try getting hostname
 * \return Empty string if failed.
 */
inline std::string GetHostName() {
  char host_name_buf[256];
  if (gethostname(host_name_buf, sizeof(host_name_buf)) == 0) { return std::string(host_name_buf); }
  return "";
}

};  // namespace base