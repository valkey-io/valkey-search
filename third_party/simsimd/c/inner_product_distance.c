// VALKEYSEARCH: Out-of-line definition of InnerProductDistanceSimsimd().
//
// The previous inline C++ wrapper called the simsimd_dot_f32() dispatcher and
// then scaled the result by the reciprocal-magnitude argument. Because that
// argument had to survive a real call (all vector registers are caller-saved),
// it was spilled to and reloaded from the stack on every distance evaluation,
// and every evaluation went through three calls:
//   fstdistfunc_ -> wrapper -> simsimd_dot_f32 dispatcher -> kernel.
//
// Here, for every ISA SimSIMD dispatches f32 dot products to (Skylake/AVX-512,
// Haswell/AVX2, SVE, NEON and serial), the kernel is inlined into a fused
// function compiled with the same target, so the scaling happens in registers.
// InnerProductDistanceSimsimd() itself only loads the fused variant selected
// for the running CPU and tail-calls it, keeping nothing live across the call.
// The arithmetic is identical to the previous wrapper.
//
// This translation unit disables dynamic dispatch so that it does not
// re-define the simsimd_* dispatch symbols provided by lib.c. The target
// gating mirrors lib.c, so the same kernels are available in both.
#define SIMSIMD_DYNAMIC_DISPATCH 0
#define SIMSIMD_NATIVE_F16 0
#define SIMSIMD_NATIVE_BF16 0

#if !defined(SIMSIMD_TARGET_NEON) && (defined(__APPLE__) || defined(__linux__))
#define SIMSIMD_TARGET_NEON 1
#endif
#if !defined(SIMSIMD_TARGET_SVE) && (defined(__linux__))
#define SIMSIMD_TARGET_SVE 1
#endif
#if !defined(SIMSIMD_TARGET_HASWELL) && \
    (defined(_MSC_VER) || defined(__APPLE__) || defined(__linux__))
#define SIMSIMD_TARGET_HASWELL 1
#endif
#if !defined(SIMSIMD_TARGET_SKYLAKE) && \
    (defined(_MSC_VER) || defined(__linux__))
#define SIMSIMD_TARGET_SKYLAKE 1
#endif

#include <stddef.h>

#include "third_party/simsimd/include/simsimd/dot.h"

typedef float (*InnerProductDistanceFn)(const void *a, const void *b,
                                        const void *qty_ptr,
                                        float reciprocal_mag_product);

// Defines a fused variant for `isa`. `target_attr` must match (or be a
// superset of) the target of simsimd_dot_f32_<isa>(), otherwise the compiler
// refuses to inline the kernel.
#define DEFINE_FUSED_INNER_PRODUCT(isa, target_attr)                          \
  target_attr static float InnerProductDistance_##isa(                        \
      const void *a, const void *b, const void *qty_ptr,                      \
      float reciprocal_mag_product) {                                         \
    simsimd_distance_t distance;                                              \
    simsimd_dot_f32_##isa((simsimd_f32_t const *)a, (simsimd_f32_t const *)b, \
                          *(size_t const *)qty_ptr, &distance);               \
    return (float)(1.0 - distance * (double)reciprocal_mag_product);          \
  }

DEFINE_FUSED_INNER_PRODUCT(serial, )

// Capability probes exported by lib.c.
#if SIMSIMD_TARGET_ARM
#if SIMSIMD_TARGET_NEON
int simsimd_uses_neon(void);
DEFINE_FUSED_INNER_PRODUCT(neon, __attribute__((target("+simd"))))
#endif
#if SIMSIMD_TARGET_SVE
int simsimd_uses_sve(void);
DEFINE_FUSED_INNER_PRODUCT(sve, __attribute__((target("+sve"))))
#endif
#endif  // SIMSIMD_TARGET_ARM

#if SIMSIMD_TARGET_X86
#if SIMSIMD_TARGET_HASWELL
int simsimd_uses_haswell(void);
DEFINE_FUSED_INNER_PRODUCT(haswell, __attribute__((target("avx2,f16c,fma"))))
#endif
#if SIMSIMD_TARGET_SKYLAKE
int simsimd_uses_skylake(void);
DEFINE_FUSED_INNER_PRODUCT(
    skylake, __attribute__((target("avx512f,avx512vl,avx512bw,bmi2"))))
#endif
#endif  // SIMSIMD_TARGET_X86

// Same preference order as simsimd_find_metric_punned() for f32 dot products.
static InnerProductDistanceFn SelectInnerProductDistance(void) {
#if SIMSIMD_TARGET_ARM
#if SIMSIMD_TARGET_SVE
  if (simsimd_uses_sve()) {
    return InnerProductDistance_sve;
  }
#endif
#if SIMSIMD_TARGET_NEON
  if (simsimd_uses_neon()) {
    return InnerProductDistance_neon;
  }
#endif
#endif  // SIMSIMD_TARGET_ARM
#if SIMSIMD_TARGET_X86
#if SIMSIMD_TARGET_SKYLAKE
  if (simsimd_uses_skylake()) {
    return InnerProductDistance_skylake;
  }
#endif
#if SIMSIMD_TARGET_HASWELL
  if (simsimd_uses_haswell()) {
    return InnerProductDistance_haswell;
  }
#endif
#endif  // SIMSIMD_TARGET_X86
  return InnerProductDistance_serial;
}

static float ResolveInnerProductDistance(const void *a, const void *b,
                                         const void *qty_ptr,
                                         float reciprocal_mag_product);

// Starts out pointing at the resolver, which replaces it with the selected
// variant on first use. This is a constant initializer, so no load-time
// constructor is needed. Concurrent first calls may each resolve; they all
// store the same value.
static InnerProductDistanceFn inner_product_distance_impl =
    ResolveInnerProductDistance;

static float ResolveInnerProductDistance(const void *a, const void *b,
                                         const void *qty_ptr,
                                         float reciprocal_mag_product) {
  InnerProductDistanceFn fn = SelectInnerProductDistance();
  __atomic_store_n(&inner_product_distance_impl, fn, __ATOMIC_RELAXED);
  return fn(a, b, qty_ptr, reciprocal_mag_product);
}

float InnerProductDistanceSimsimd(const void *pVect1, const void *pVect2,
                                  const void *qty_ptr,
                                  float reciprocal_mag_product) {
  return __atomic_load_n(&inner_product_distance_impl, __ATOMIC_RELAXED)(
      pVect1, pVect2, qty_ptr, reciprocal_mag_product);
}
