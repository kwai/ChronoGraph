# coding=utf-8

# Copyright (c) 2025 Kuaishou Technology
# SPDX-License-Identifier: MIT

from typing import List, Dict


class LayerQueryResult:
  # List of current layer ids.
  ids: List[int]
  timestamps: List[int]
  weights: List[float]
  # Size matches ids, indicating whether the id exists. This is usually used when retrieving attributes.
  # For example, when you take a point from U2I and get its attributes from I2U, it may no longer exist.
  attr_exist: List[bool]
  # degrees and id correspond to out-degree information.
  degrees: List[int]
  # int64 attribute, length = ids * int_attr_len
  int_attrs: List[List[int]]
  # float attribute, length = ids * float_attr_len
  float_attrs: List[List[float]]
  # Alias ​​for the current layer result.
  name: str

  def __init__(self):
    self.ids = None
    self.timestamps = None
    self.weights = None
    self.attr_exist = None
    self.degrees = None
    self.int_attrs = None
    self.float_attrs = None
    self.name = "UNDEFINED"

  def __repr__(self):
    return "[name = " + self.name \
        + "; ids = " + str(self.ids) \
        + "; timestamps = " + str(self.timestamps) \
        + "; weights = " + str(self.weights) \
        + "; attr_exist = " + str(self.attr_exist) \
        + "; degrees = " + str(self.degrees) \
        + "; int_attrs = " + str(self.int_attrs) \
        + "; float_attrs = " + str(self.float_attrs) \
        + "]"

  def __getitem__(self, key: str):
    return self.__dict__[key]


class QueryResults:
  results: Dict[str, LayerQueryResult]

  def __init__(self):
    self.results = {}

  def __repr__(self):
    return str(self.results)

  def __getitem__(self, key: str):
    return self.results[key]
