/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_FP16_H_
#define VALKEYSEARCH_SRC_INDEXES_FP16_H_

#include <cstddef>

using float16 = _Float16;
static_assert(sizeof(float16) == 2, "float16 must be 2 bytes");

inline float float16_to_float(float16 v) { return static_cast<float>(v); }
inline float16 float_to_float16(float v) { return static_cast<float16>(v); }

#endif  // VALKEYSEARCH_SRC_INDEXES_FP16_H_
