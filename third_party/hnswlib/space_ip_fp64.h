#pragma once

#include "hnswlib.h"

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

class InnerProductSpaceFP64 : public SpaceInterface<double> {
  DISTFUNC<double> fstdistfunc_;
  size_t data_size_;
  size_t dim_;

 public:
  explicit InnerProductSpaceFP64(size_t dim)
      : fstdistfunc_(InnerProductDistanceFP64),
        data_size_(dim * sizeof(double)),
        dim_(dim) {}

  size_t get_data_size() { return data_size_; }
  DISTFUNC<double> get_dist_func() { return fstdistfunc_; }
  void *get_dist_func_param() { return &dim_; }
};

}  // namespace hnswlib
