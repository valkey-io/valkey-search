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
#pragma GCC diagnostic ignored "-Wunused-variable"
namespace hnswlib {

static float
InnerProductFP16(const void *pVect1, const void *pVect2, const void *qty_ptr) {
    float16 *pVect1f = (float16 *) pVect1;
    float16 *pVect2f = (float16 *) pVect2;
    size_t qty = *((size_t *) qty_ptr);
    float res = 0;
    for (size_t i = 0; i < qty; i++) {
        res += static_cast<float>(pVect1f[i]) * static_cast<float>(pVect2f[i]);
    }
    return res;
}

static float
InnerProductDistanceFP16(const void *pVect1, const void *pVect2, const void *qty_ptr) {
    return 1.0f - InnerProductFP16(pVect1, pVect2, qty_ptr);
}

#if defined(USE_SIMSIMD)
static float
InnerProductDistanceFP16Simsimd(const void *pVect1, const void *pVect2, const void *qty_ptr) {
    simsimd_size_t dim = *static_cast<const size_t *>(qty_ptr);
    const simsimd_f16_t *vec1 = static_cast<const simsimd_f16_t *>(pVect1);
    const simsimd_f16_t *vec2 = static_cast<const simsimd_f16_t *>(pVect2);
    simsimd_distance_t distance;
    simsimd_dot_f16(vec1, vec2, dim, &distance);
    return 1.0f - static_cast<float>(distance);
}
#endif

class InnerProductSpaceFP16 : public SpaceInterface<float> {
    DISTFUNC<float> fstdistfunc_;
    size_t data_size_;
    size_t dim_;

 public:
    InnerProductSpaceFP16(size_t dim) {
#if defined(USE_SIMSIMD)
        fstdistfunc_ = InnerProductDistanceFP16Simsimd;
#else
        fstdistfunc_ = InnerProductDistanceFP16;
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

    ~InnerProductSpaceFP16() {}
};

}  // namespace hnswlib
#pragma GCC diagnostic pop
