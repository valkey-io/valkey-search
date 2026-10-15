#pragma once

#include "hnswlib.h"

#if defined(USE_SIMSIMD)
#include "third_party/hnswlib/simsimd.h"
#endif

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-function"
namespace hnswlib {

static double L2SqrFP64(const void *pVect1v, const void *pVect2v,
                        const void *qty_ptr,
                        [[maybe_unused]] double product_magnitude) {
  const double *pVect1 = static_cast<const double *>(pVect1v);
  const double *pVect2 = static_cast<const double *>(pVect2v);
  const size_t qty = *static_cast<const size_t *>(qty_ptr);
  double res = 0.0;
  for (size_t i = 0; i < qty; ++i) {
    const double t = pVect1[i] - pVect2[i];
    res += t * t;
  }
  return res;
}

#if defined(USE_SIMSIMD)
static double L2SqrFP64Simsimd(const void *pVect1, const void *pVect2,
                               const void *qty_ptr,
                               [[maybe_unused]] double product_magnitude) {
  simsimd_size_t dim = *static_cast<const size_t *>(qty_ptr);
  const simsimd_f64_t *vec1 = static_cast<const simsimd_f64_t *>(pVect1);
  const simsimd_f64_t *vec2 = static_cast<const simsimd_f64_t *>(pVect2);
  simsimd_distance_t distance;
  simsimd_l2sq_f64(vec1, vec2, dim, &distance);
  return distance;
}
#endif

class L2SpaceFP64 : public SpaceInterface<double> {
  DISTFUNC<double> fstdistfunc_;
  size_t data_size_;
  size_t dim_;

 public:
  explicit L2SpaceFP64(size_t dim)
      : data_size_(dim * sizeof(double)), dim_(dim) {
#if defined(USE_SIMSIMD)
    fstdistfunc_ = L2SqrFP64Simsimd;
#else
    fstdistfunc_ = L2SqrFP64;
#endif
  }

  size_t get_data_size() { return data_size_; }
  DISTFUNC<double> get_dist_func() { return fstdistfunc_; }
  void *get_dist_func_param() { return &dim_; }
};

}  // namespace hnswlib
#pragma GCC diagnostic pop
