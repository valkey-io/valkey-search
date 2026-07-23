#ifndef THIRD_PARTY_HNSWLIB_SIMSIMD_H_
#define THIRD_PARTY_HNSWLIB_SIMSIMD_H_

#include <cstddef>

// SimSIMD's dynamic-dispatch unit (c/lib.c) DEFINES the simsimd_* entry points
// with external linkage and is meant to be compiled exactly once. This header
// is pulled into many translation units, so it must include only the
// declarations; c/lib.c is compiled once as the `simsimd` static library (see
// CMakeLists.txt). Including it here instead produces duplicate symbols at link
// time -- a hard error under macOS/Mach-O ld.
#include "third_party/simsimd/include/simsimd/simsimd.h"
#include "third_party/simsimd/include/simsimd/types.h"

inline float InnerProductDistanceSimsimd(const void *pVect1, const void *pVect2,
                                         const void *qty_ptr,
                                         float reciprocal_mag_product) {
  simsimd_size_t dim = *static_cast<const size_t *>(qty_ptr);
  const simsimd_f32_t *vec1 = static_cast<const simsimd_f32_t *>(pVect1);
  const simsimd_f32_t *vec2 = static_cast<const simsimd_f32_t *>(pVect2);
  simsimd_distance_t distance;
  simsimd_dot_f32(vec1, vec2, dim, &distance);
  return 1.0f - (distance * reciprocal_mag_product);
}

inline float L2SqrSimsimd(const void *pVect1, const void *pVect2,
                          const void *qty_ptr,
                          [[maybe_unused]] float product_magnitude) {
  simsimd_size_t dim = *static_cast<const size_t *>(qty_ptr);
  const simsimd_f32_t *vec1 = static_cast<const simsimd_f32_t *>(pVect1);
  const simsimd_f32_t *vec2 = static_cast<const simsimd_f32_t *>(pVect2);
  simsimd_distance_t distance;
  simsimd_l2sq_f32(vec1, vec2, dim, &distance);
  return distance;
}

#endif  // THIRD_PARTY_HNSWLIB_SIMSIMD_H_
