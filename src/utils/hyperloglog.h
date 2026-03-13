/*
 * Copyright (c) 2014, Redis Ltd.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/*
 * HyperLogLog probabilistic cardinality estimation.
 *
 * Standalone dense-only HLL extracted from the Valkey server implementation:
 * https://github.com/valkey-io/valkey/blob/unstable/src/hyperloglog.c
 *
 * Uses MurmurHash64A and the improved Ertl estimator with P=14
 * (16384 registers, 6-bit packed, ~0.81% standard error).
 */

#ifndef VALKEYSEARCH_SRC_UTILS_HYPERLOGLOG_H_
#define VALKEYSEARCH_SRC_UTILS_HYPERLOGLOG_H_

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define HLL_P 14
#define HLL_Q (64 - HLL_P)
#define HLL_REGISTERS (1 << HLL_P) /* 16384 */
#define HLL_P_MASK (HLL_REGISTERS - 1)
#define HLL_BITS 6
#define HLL_REGISTER_MAX ((1 << HLL_BITS) - 1)
/* Dense size: 6 bits * 16384 registers = 12288 bytes. */
#define HLL_DENSE_SIZE (((HLL_REGISTERS * HLL_BITS) + 7) / 8)

struct HLL {
  uint64_t cached_card; /* Cached cardinality; UINT64_MAX = invalid. */
  uint8_t registers[HLL_DENSE_SIZE + 1]; /* +1 for safe macro access. */
};

/* Initialize an HLL to the empty state. */
void hll_init(struct HLL *hll);

/* Add a raw buffer to the HLL (hashed with MurmurHash64A). */
void hll_add(struct HLL *hll, const void *buf, size_t len);

/* Estimate the cardinality. Result is cached until next add. */
uint64_t hll_count(const struct HLL *hll);

#ifdef __cplusplus
}
#endif

#endif /* VALKEYSEARCH_SRC_UTILS_HYPERLOGLOG_H_ */
