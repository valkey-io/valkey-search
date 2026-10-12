#pragma once

#include "hnswlib.h"

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

class L2SpaceFP64 : public SpaceInterface<double> {
  DISTFUNC<double> fstdistfunc_;
  size_t data_size_;
  size_t dim_;

 public:
  explicit L2SpaceFP64(size_t dim)
      : fstdistfunc_(L2SqrFP64), data_size_(dim * sizeof(double)), dim_(dim) {}

  size_t get_data_size() { return data_size_; }
  DISTFUNC<double> get_dist_func() { return fstdistfunc_; }
  void *get_dist_func_param() { return &dim_; }
};

}  // namespace hnswlib
