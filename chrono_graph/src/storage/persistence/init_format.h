// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <string>
#include <vector>
#include "base/common/basic_types.h"
#include "base/common/logging.h"
#include "base/common/string.h"

namespace chrono_graph {

#define FIELD_DELIMITER "|"
#define NODE_DELIMITER ","
#define ATTR_DELIMITER ","
#define STRUCT_DELIMITER "\003"
#define STRUCT_ATTR_DELIMITER "\004"
// hive format
#define EMPTY_ATTR "\\N"

// edge format
// id (required) | weight | int_attrs | float_attrs
class GraphOneEdgeStr {
 public:
  GraphOneEdgeStr(const std::string &data) : data_(data) {
    fields_ = absl::StrSplit(data, STRUCT_DELIMITER);
    if (fields_.size() != 2 && fields_.size() != 4 && fields_.size() != 1) {
      LOG_EVERY_N(WARNING, 1000) << "Illegal data line edge parse, data = " << data;
      is_valid_ = false;
    }
  }
  ~GraphOneEdgeStr() {}
  bool IsValid() { return is_valid_; }
  bool GetID(uint64 *id) {
    if (!is_valid_) { return false; }
    if (!absl::SimpleAtoi(fields_[0], id)) {
      LOG_EVERY_N(WARNING, 1000) << "Illegal data line in id, data = " << data_ << ", in id = " << fields_[0];
      return false;
    }
    return true;
  }
  bool GetWeight(double *weight) {
    if (!is_valid_ || fields_.size() < 2) { return false; }
    if (!absl::SimpleAtod(fields_[1], weight)) {
      LOG_EVERY_N(WARNING, 1000) << "Illegal data line dest weight, data = " << data_
                                 << ", field = " << fields_[1];
      return false;
    }
    return true;
  }
  bool GetEdgeAttrs(std::vector<int64> *int_attrs, std::vector<float> *float_attrs) {
    if (!is_valid_ || fields_.size() != 4) { return false; }

    std::vector<std::string> int_attrs_str = absl::StrSplit(fields_[2], STRUCT_ATTR_DELIMITER);
    int_attrs->resize(int_attrs_str.size());
    // if int attr is empty, just return true
    for (size_t i = 0; fields_[2] != EMPTY_ATTR && i < int_attrs_str.size(); ++i) {
      if (!absl::SimpleAtoi(int_attrs_str[i], &((*int_attrs)[i]))) {
        LOG_EVERY_N(WARNING, 1000) << "Illegal data line edge int attr, data = " << data_
                                   << ", field = " << fields_[2] << ", offset = " << i
                                   << ", attr = " << int_attrs_str[i];
        return false;
      }
    }
    std::vector<std::string> float_attrs_str = absl::StrSplit(fields_[3], STRUCT_ATTR_DELIMITER);
    float_attrs->resize(float_attrs_str.size());
    // if float attr is empty, just return true
    for (size_t i = 0; fields_[3] != EMPTY_ATTR && i < float_attrs_str.size(); ++i) {
      if (!absl::SimpleAtof(float_attrs_str[i], &((*float_attrs)[i]))) {
        LOG_EVERY_N(WARNING, 1000) << "Illegal data line edge float attr, data = " << data_
                                   << ", field = " << fields_[3] << ", offset = " << i
                                   << ", attr = " << float_attrs_str[i];
        return false;
      }
    }
    return true;
  }

 private:
  bool is_valid_ = true;
  std::string data_;
  std::vector<std::string> fields_;
};

// one line format
// src_id | int_attrs | float_attrs | dest_ids
class GraphOneLineStr {
 public:
  GraphOneLineStr(const std::string &data) : data_(data) { is_valid_ = Init(data); }
  ~GraphOneLineStr() {}
  bool Init(const std::string &data) {
    fields_ = absl::StrSplit(data, FIELD_DELIMITER);
    if (fields_.size() != 4) {
      LOG_EVERY_N(WARNING, 1000) << "Illegal data line, fields size not match(" << fields_.size()
                                 << "), data = " << data;
      return false;
    }
    return true;
  }

  bool GetSrcID(uint64 *id) {
    if (!is_valid_) { return false; }
    if (!absl::SimpleAtoi(fields_[0], id)) {
      LOG_EVERY_N(WARNING, 1000) << "Illegal data line in id, data = " << data_ << ", in id = " << fields_[0];
      return false;
    }
    return true;
  }

  bool GetIntAttrs(std::vector<int64> *int_attrs) {
    if (!is_valid_) { return false; }
    std::vector<std::string> int_attrs_str = absl::StrSplit(fields_[1], ATTR_DELIMITER);
    int_attrs->resize(int_attrs_str.size());
    // if int attr is empty, just return true
    for (size_t i = 0; fields_[1] != EMPTY_ATTR && i < int_attrs_str.size(); ++i) {
      if (!absl::SimpleAtoi(int_attrs_str[i], &((*int_attrs)[i]))) {
        LOG_EVERY_N(WARNING, 1000) << "Illegal data line int attr, data = " << data_ << ", offset = " << i
                                   << ", attr = " << int_attrs_str[i] << ", field = " << fields_[1];
        return false;
      }
    }
    return true;
  }
  bool GetFloatAttrs(std::vector<float> *float_attrs) {
    if (!is_valid_) { return false; }
    std::vector<std::string> float_attrs_str = absl::StrSplit(fields_[2], ATTR_DELIMITER);
    float_attrs->resize(float_attrs_str.size());
    // if int attr is empty, just return true
    for (size_t i = 0; fields_[2] != EMPTY_ATTR && i < float_attrs_str.size(); ++i) {
      if (!absl::SimpleAtof(float_attrs_str[i], &((*float_attrs)[i]))) {
        LOG_EVERY_N(WARNING, 1000) << "Illegal data line int attr, data = " << data_ << ", offset = " << i
                                   << ", attr = " << float_attrs_str[i] << ", field = " << fields_[2];
        return false;
      }
    }
    return true;
  }
  bool HasDestId() {
    if (!is_valid_) { return false; }
    if (fields_[3] == EMPTY_ATTR || fields_[3] == "") { return false; }
    return true;
  }
  bool GetEdgesInfo(std::vector<std::string> *dests) {
    if (!is_valid_) { return false; }
    *dests = absl::StrSplit(fields_[3], NODE_DELIMITER);
    return true;
  }

 private:
  bool is_valid_ = false;
  std::string data_;
  std::vector<std::string> fields_;
};

}  // namespace chrono_graph