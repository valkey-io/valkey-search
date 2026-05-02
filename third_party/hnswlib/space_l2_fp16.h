#pragma once
#include "hnswlib.h"
#include "src/indexes/fp16.h"

#ifdef VMSDK_ENABLE_MEMORY_ALLOCATION_OVERRIDES
  #include "vmsdk/src/memory_allocation_overrides.h" // IWYU pragma: keep
#endif

#if defined(USE_SIMSIMD)
#include "third_party/hnswlib/simsimd.h"
#endif

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-function"
namespace hnswlib {

static float
L2SqrFP16(const void *pVect1v, const void *pVect2v, const void *qty_ptr) {
    float16 *pVect1 = (float16 *) pVect1v;
    float16 *pVect2 = (float16 *) pVect2v;
    size_t qty = *((size_t *) qty_ptr);

    float res = 0;
    for (size_t i = 0; i < qty; i++) {
        float t = static_cast<float>(pVect1[i]) - static_cast<float>(pVect2[i]);
        res += t * t;
    }
    return res;
}

#if defined(USE_SIMSIMD)
static float
L2SqrFP16Simsimd(const void *pVect1, const void *pVect2, const void *qty_ptr) {
    simsimd_size_t dim = *static_cast<const size_t *>(qty_ptr);
    const simsimd_f16_t *vec1 = static_cast<const simsimd_f16_t *>(pVect1);
    const simsimd_f16_t *vec2 = static_cast<const simsimd_f16_t *>(pVect2);
    simsimd_distance_t distance;
    simsimd_l2sq_f16(vec1, vec2, dim, &distance);
    return static_cast<float>(distance);
}
#endif

class L2SpaceFP16 : public SpaceInterface<float> {
    DISTFUNC<float> fstdistfunc_;
    size_t data_size_;
    size_t dim_;

 public:
    L2SpaceFP16(size_t dim) {
#if defined(USE_SIMSIMD)
        fstdistfunc_ = L2SqrFP16Simsimd;
#else
        fstdistfunc_ = L2SqrFP16;
#endif
        dim_ = dim;
        data_size_ = dim * sizeof(float16);
    }

    size_t get_data_size() {
        return data_size_;
    }

    DISTFUNC<float> get_dist_func() {
        return fstdistfunc_;
    }

    void *get_dist_func_param() {
        return &dim_;
    }

    ~L2SpaceFP16() {}
};

}  // namespace hnswlib
#pragma GCC diagnostic pop
