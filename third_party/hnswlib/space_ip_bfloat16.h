#pragma once
#include "hnswlib.h"
#include "src/indexes/bfloat16.h"

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
InnerProductBF16(const void *pVect1, const void *pVect2, const void *qty_ptr) {
    bfloat16 *pVect1f = (bfloat16 *) pVect1;
    bfloat16 *pVect2f = (bfloat16 *) pVect2;
    size_t qty = *((size_t *) qty_ptr);
    float res = 0;
    for (size_t i = 0; i < qty; i++) {
        res += static_cast<float>(pVect1f[i]) * static_cast<float>(pVect2f[i]);
    }
    return res;
}

static float
InnerProductDistanceBF16(const void *pVect1, const void *pVect2, const void *qty_ptr) {
    return 1.0f - InnerProductBF16(pVect1, pVect2, qty_ptr);
}

#if defined(USE_SIMSIMD)
static float
InnerProductDistanceBF16Simsimd(const void *pVect1, const void *pVect2, const void *qty_ptr) {
    simsimd_size_t dim = *static_cast<const size_t *>(qty_ptr);
    const simsimd_bf16_t *vec1 = static_cast<const simsimd_bf16_t *>(pVect1);
    const simsimd_bf16_t *vec2 = static_cast<const simsimd_bf16_t *>(pVect2);
    simsimd_distance_t distance;
    simsimd_dot_bf16(vec1, vec2, dim, &distance);
    return 1.0f - static_cast<float>(distance);
}
#endif

class InnerProductSpaceBF16 : public SpaceInterface<float> {
    DISTFUNC<float> fstdistfunc_;
    size_t data_size_;
    size_t dim_;

 public:
    InnerProductSpaceBF16(size_t dim) {
#if defined(USE_SIMSIMD)
        fstdistfunc_ = InnerProductDistanceBF16Simsimd;
#else
        fstdistfunc_ = InnerProductDistanceBF16;
#endif
        dim_ = dim;
        data_size_ = dim * sizeof(bfloat16);
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

    ~InnerProductSpaceBF16() {}
};

}  // namespace hnswlib
#pragma GCC diagnostic pop
