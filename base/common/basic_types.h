// Copyright (c) 2025 Kuaishou Technology
// SPDX-License-Identifier: MIT

#pragma once

#include <chrono>
#include <cstdarg>
#include <cstdio>

typedef signed char schar;
typedef signed char int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;

typedef unsigned char uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;

const uint8 kUint8Max = static_cast<uint8>(0xFF);
const uint16 kUint16Max = static_cast<uint16>(0xFFFF);
const uint32 kUint32Max = static_cast<uint32>(0xFFFFFFFF);
const uint64 kUint64Max = static_cast<uint64>(0xFFFFFFFFFFFFFFFFULL);

const int8 kInt8Min = static_cast<int8>(0x80);
const int8 kInt8Max = static_cast<int8>(0x7F);
const int16 kInt16Min = static_cast<int16>(0x8000);
const int16 kInt16Max = static_cast<int16>(0x7FFF);
const int32 kInt32Min = static_cast<int32>(0x80000000);
const int32 kInt32Max = static_cast<int32>(0x7FFFFFFF);
const int64 kInt64Min = static_cast<int64>(0x8000000000000000LL);
const int64 kInt64Max = static_cast<int64>(0x7FFFFFFFFFFFFFFFLL);

// A macro to disallow the copy constructor and operator= functions
// This should be used in the private: declarations for a class
#ifndef DISALLOW_COPY_AND_ASSIGN
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
 private:                                  \
  TypeName(const TypeName &);              \
  void operator=(const TypeName &)
#endif
