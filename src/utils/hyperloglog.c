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
 * Standalone dense-only HyperLogLog extracted from the Valkey server
 * implementation. All Valkey-server-specific code (sparse encoding,
 * commands, robj, sds) has been removed. Only the core algorithm,
 * MurmurHash64A, dense register macros, and the Ertl estimator remain.
 *
 * Source: https://github.com/valkey-io/valkey/blob/unstable/src/hyperloglog.c
 */

#include "hyperloglog.h"

#include <math.h>
#include <string.h>

#define HLL_INVALIDATE_CACHE(h) ((h)->cached_card = UINT64_MAX)
#define HLL_VALID_CACHE(h) ((h)->cached_card != UINT64_MAX)

/* ========================= Dense register macros ========================= */

/* Store the value of the register at position 'regnum' into variable
 * 'target'. 'p' is an array of unsigned bytes. */
#define HLL_DENSE_GET_REGISTER(target, p, regnum)               \
  do {                                                          \
    uint8_t *_p = (uint8_t *)(p);                               \
    unsigned long _byte = (regnum) * HLL_BITS / 8;              \
    unsigned long _fb = (regnum) * HLL_BITS & 7;                \
    unsigned long _fb8 = 8 - _fb;                               \
    unsigned long b0 = _p[_byte];                               \
    unsigned long b1 = _p[_byte + 1];                           \
    (target) = ((b0 >> _fb) | (b1 << _fb8)) & HLL_REGISTER_MAX; \
  } while (0)

/* Set the value of the register at position 'regnum' to 'val'.
 * 'p' is an array of unsigned bytes. */
#define HLL_DENSE_SET_REGISTER(p, regnum, val)     \
  do {                                             \
    uint8_t *_p = (uint8_t *)(p);                  \
    unsigned long _byte = (regnum) * HLL_BITS / 8; \
    unsigned long _fb = (regnum) * HLL_BITS & 7;   \
    unsigned long _fb8 = 8 - _fb;                  \
    unsigned long _v = (val);                      \
    _p[_byte] &= ~(HLL_REGISTER_MAX << _fb);       \
    _p[_byte] |= _v << _fb;                        \
    _p[_byte + 1] &= ~(HLL_REGISTER_MAX >> _fb8);  \
    _p[_byte + 1] |= _v >> _fb8;                   \
  } while (0)

#define HLL_ALPHA_INF 0.721347520444481703680 /* 0.5/ln(2) */

/* ========================= MurmurHash64A ================================= */

/* MurmurHash2, 64 bit version. Modified for endian neutrality. */
static uint64_t MurmurHash64A(const void *key, int len, unsigned int seed) {
  const uint64_t m = 0xc6a4a7935bd1e995ULL;
  const int r = 47;
  uint64_t h = seed ^ ((uint64_t)len * m);
  const uint8_t *data = (const uint8_t *)key;
  const uint8_t *end = data + (len - (len & 7));

  while (data != end) {
    uint64_t k;

#if (BYTE_ORDER == LITTLE_ENDIAN) || (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
    memcpy(&k, data, sizeof(uint64_t));
#else
    k = (uint64_t)data[0];
    k |= (uint64_t)data[1] << 8;
    k |= (uint64_t)data[2] << 16;
    k |= (uint64_t)data[3] << 24;
    k |= (uint64_t)data[4] << 32;
    k |= (uint64_t)data[5] << 40;
    k |= (uint64_t)data[6] << 48;
    k |= (uint64_t)data[7] << 56;
#endif

    k *= m;
    k ^= k >> r;
    k *= m;
    h ^= k;
    h *= m;
    data += 8;
  }

  switch (len & 7) {
    case 7:
      h ^= (uint64_t)data[6] << 48; /* fall-thru */
    case 6:
      h ^= (uint64_t)data[5] << 40; /* fall-thru */
    case 5:
      h ^= (uint64_t)data[4] << 32; /* fall-thru */
    case 4:
      h ^= (uint64_t)data[3] << 24; /* fall-thru */
    case 3:
      h ^= (uint64_t)data[2] << 16; /* fall-thru */
    case 2:
      h ^= (uint64_t)data[1] << 8; /* fall-thru */
    case 1:
      h ^= (uint64_t)data[0];
      h *= m; /* fall-thru */
  };

  h ^= h >> r;
  h *= m;
  h ^= h >> r;
  return h;
}

/* ========================= HLL algorithm ================================= */

/* Given a string element, returns the length of the pattern 000..1 of
 * the element hash. As a side effect 'regp' is set to the register
 * index this element hashes to. */
static int hllPatLen(const unsigned char *ele, size_t elesize, long *regp) {
  uint64_t hash, index;
  int count;

  hash = MurmurHash64A(ele, (int)elesize, 0xadc83b19ULL);
  index = hash & HLL_P_MASK;
  hash >>= HLL_P;
  hash |= ((uint64_t)1 << HLL_Q);
  count = 1;
  count += __builtin_ctzll(hash);
  *regp = (long)index;
  return count;
}

/* Set the dense HLL register at 'index' to 'count' if the current
 * value is smaller. Returns 1 if updated, 0 otherwise. */
static int hllDenseSet(uint8_t *registers, long index, uint8_t count) {
  uint8_t oldcount;
  HLL_DENSE_GET_REGISTER(oldcount, registers, index);
  if (count > oldcount) {
    HLL_DENSE_SET_REGISTER(registers, index, count);
    return 1;
  }
  return 0;
}

/* Add an element to the dense HLL. Returns 1 if a register was
 * updated, 0 otherwise. */
static int hllDenseAdd(uint8_t *registers, const unsigned char *ele,
                       size_t elesize) {
  long index;
  uint8_t count = (uint8_t)hllPatLen(ele, elesize, &index);
  return hllDenseSet(registers, index, count);
}

/* Compute the register histogram in the dense representation.
 * Optimized unrolled loop for P=14, BITS=6. */
static void hllDenseRegHisto(uint8_t *registers, int *reghisto) {
  if (HLL_REGISTERS == 16384 && HLL_BITS == 6) {
    uint8_t *r = registers;
    unsigned long r0, r1, r2, r3, r4, r5, r6, r7;
    unsigned long r8, r9, r10, r11, r12, r13, r14, r15;
    for (int j = 0; j < 1024; j++) {
      r0 = r[0] & 63;
      r1 = (r[0] >> 6 | r[1] << 2) & 63;
      r2 = (r[1] >> 4 | r[2] << 4) & 63;
      r3 = (r[2] >> 2) & 63;
      r4 = r[3] & 63;
      r5 = (r[3] >> 6 | r[4] << 2) & 63;
      r6 = (r[4] >> 4 | r[5] << 4) & 63;
      r7 = (r[5] >> 2) & 63;
      r8 = r[6] & 63;
      r9 = (r[6] >> 6 | r[7] << 2) & 63;
      r10 = (r[7] >> 4 | r[8] << 4) & 63;
      r11 = (r[8] >> 2) & 63;
      r12 = r[9] & 63;
      r13 = (r[9] >> 6 | r[10] << 2) & 63;
      r14 = (r[10] >> 4 | r[11] << 4) & 63;
      r15 = (r[11] >> 2) & 63;

      reghisto[r0]++;
      reghisto[r1]++;
      reghisto[r2]++;
      reghisto[r3]++;
      reghisto[r4]++;
      reghisto[r5]++;
      reghisto[r6]++;
      reghisto[r7]++;
      reghisto[r8]++;
      reghisto[r9]++;
      reghisto[r10]++;
      reghisto[r11]++;
      reghisto[r12]++;
      reghisto[r13]++;
      reghisto[r14]++;
      reghisto[r15]++;

      r += 12;
    }
  } else {
    for (int j = 0; j < HLL_REGISTERS; j++) {
      unsigned long reg;
      HLL_DENSE_GET_REGISTER(reg, registers, j);
      reghisto[reg]++;
    }
  }
}

/* Helper function sigma as defined in
 * "New cardinality estimation algorithms for HyperLogLog sketches"
 * Otmar Ertl, arXiv:1702.01284 */
static double hllSigma(double x) {
  if (x == 1.) return INFINITY;
  double zPrime;
  double y = 1;
  double z = x;
  do {
    x *= x;
    zPrime = z;
    z += x * y;
    y += y;
  } while (zPrime != z);
  return z;
}

/* Helper function tau as defined in
 * "New cardinality estimation algorithms for HyperLogLog sketches"
 * Otmar Ertl, arXiv:1702.01284 */
static double hllTau(double x) {
  if (x == 0. || x == 1.) return 0.;
  double zPrime;
  double y = 1.0;
  double z = 1 - x;
  do {
    x = sqrt(x);
    zPrime = z;
    y *= 0.5;
    z -= pow(1 - x, 2) * y;
  } while (zPrime != z);
  return z / 3;
}

/* Return the approximated cardinality using the improved Ertl estimator. */
static uint64_t hllCountRegisters(uint8_t *registers) {
  double m = HLL_REGISTERS;
  double E;
  int j;
  int reghisto[64] = {0};

  hllDenseRegHisto(registers, reghisto);

  double z = m * hllTau((m - reghisto[HLL_Q + 1]) / (double)m);
  for (j = HLL_Q; j >= 1; --j) {
    z += reghisto[j];
    z *= 0.5;
  }
  z += m * hllSigma(reghisto[0] / (double)m);
  E = llroundl(HLL_ALPHA_INF * m * m / z);

  return (uint64_t)E;
}

/* ========================= Public API ==================================== */

void hll_init(struct HLL *hll) {
  memset(hll, 0, sizeof(*hll));
  HLL_INVALIDATE_CACHE(hll);
}

void hll_add(struct HLL *hll, const void *buf, size_t len) {
  if (hllDenseAdd(hll->registers, (const unsigned char *)buf, len)) {
    HLL_INVALIDATE_CACHE(hll);
  }
}

uint64_t hll_count(const struct HLL *hll) {
  if (HLL_VALID_CACHE(hll)) {
    return hll->cached_card;
  }
  uint64_t card = hllCountRegisters(((struct HLL *)hll)->registers);
  ((struct HLL *)hll)->cached_card = card;
  return card;
}
