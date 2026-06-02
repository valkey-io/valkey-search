#ifndef THIRD_PARTY_HNSWLIB_SIMSIMD_H_
#define THIRD_PARTY_HNSWLIB_SIMSIMD_H_

#include <cstddef>

#include "third_party/simsimd/c/lib.c"

#include "third_party/simsimd/include/simsimd/simsimd.h"
#include "third_party/simsimd/include/simsimd/types.h"


inline float InnerProductDistanceSimsimd(const void *pVect1, const void *pVect2,
                                         const void *qty_ptr) {
  simsimd_size_t dim = *static_cast<const size_t *>(qty_ptr);
  const simsimd_f32_t *vec1 = static_cast<const simsimd_f32_t *>(pVect1);
  const simsimd_f32_t *vec2 = static_cast<const simsimd_f32_t *>(pVect2);
  simsimd_distance_t distance;
  simsimd_dot_f32(vec1, vec2, dim, &distance);
  return 1.0f -  distance;
}

inline float L2SqrSimsimd(const void *pVect1, const void *pVect2,
                          const void *qty_ptr) {
  simsimd_size_t dim = *static_cast<const size_t *>(qty_ptr);
  const simsimd_f32_t *vec1 = static_cast<const simsimd_f32_t *>(pVect1);
  const simsimd_f32_t *vec2 = static_cast<const simsimd_f32_t *>(pVect2);
  simsimd_distance_t distance;
  simsimd_l2sq_f32(vec1, vec2, dim, &distance);
  return distance;
}

#endif  // THIRD_PARTY_HNSWLIB_SIMSIMD_H_
