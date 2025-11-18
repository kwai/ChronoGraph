// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <vector>

namespace chrono_graph {

class Bitmap {
 public:
  /*!
   * \brief resize the bitmap to be certain size
   * \param size the size of bitmap
   */
  inline void Resize(size_t size) { data.resize((size + 31U) >> 5, 0); }

  /*!
   * \brief query the i-th position of bitmap
   * \param i the position in
   */
  inline bool Get(size_t i) const { return (data[i >> 5] >> (i & 31U)) & 1U; }

  /*!
   * \brief set i-th position to true
   * \param i position index
   */
  inline void Set(size_t i) { data[i >> 5] |= (1 << (i & 31U)); }

  /*! \brief reset the bitmap, set all places to false */
  inline void Reset() { std::fill(data.begin(), data.end(), 0U); }

 private:
  /*! \brief internal data structure */
  std::vector<uint32_t> data;
};

}  // namespace chrono_graph
