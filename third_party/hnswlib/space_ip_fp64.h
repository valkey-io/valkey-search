#pragma once

#include "hnswlib.h"

#if defined(USE_SIMSIMD)
#include "third_party/hnswlib/simsimd.h"
#endif

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-function"
namespace hnswlib {

static double InnerProductFP64(const void *pVect1v, const void *pVect2v,
                               const void *qty_ptr) {
  const double *pVect1 = static_cast<const double *>(pVect1v);
  const double *pVect2 = static_cast<const double *>(pVect2v);
  const size_t qty = *static_cast<const size_t *>(qty_ptr);
  double res = 0.0;
  for (size_t i = 0; i < qty; ++i) {
    res += pVect1[i] * pVect2[i];
  }
  return res;
}

static double InnerProductDistanceFP64(const void *pVect1, const void *pVect2,
                                       const void *qty_ptr,
                                       double reciprocal_mag_product) {
  return 1.0 -
         (InnerProductFP64(pVect1, pVect2, qty_ptr) * reciprocal_mag_product);
}

#if defined(USE_SIMSIMD)
static double InnerProductDistanceFP64Simsimd(const void *pVect1,
                                              const void *pVect2,
                                              const void *qty_ptr,
                                              double reciprocal_mag_product) {
  simsimd_size_t dim = *static_cast<const size_t *>(qty_ptr);
  const simsimd_f64_t *vec1 = static_cast<const simsimd_f64_t *>(pVect1);
  const simsimd_f64_t *vec2 = static_cast<const simsimd_f64_t *>(pVect2);
  simsimd_distance_t distance;
  simsimd_dot_f64(vec1, vec2, dim, &distance);
  return 1.0 - (distance * reciprocal_mag_product);
}
#endif

class InnerProductSpaceFP64 : public SpaceInterface<double> {
  DISTFUNC<double> fstdistfunc_;
  size_t data_size_;
  size_t dim_;

 public:
  explicit InnerProductSpaceFP64(size_t dim)
      : data_size_(dim * sizeof(double)), dim_(dim) {
#if defined(USE_SIMSIMD)
    fstdistfunc_ = InnerProductDistanceFP64Simsimd;
#else
    fstdistfunc_ = InnerProductDistanceFP64;
#endif
  }

  size_t get_data_size() { return data_size_; }
  DISTFUNC<double> get_dist_func() { return fstdistfunc_; }
  void *get_dist_func_param() { return &dim_; }
};

}  // namespace hnswlib
#pragma GCC diagnostic pop
