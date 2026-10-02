// VALKEYSEARCH: Out-of-line fused COSINE distance kernels.
//
// The previous inline C++ wrappers called a simsimd_dot_*() dispatcher and
// then scaled the result by the reciprocal-magnitude argument. Because that
// argument had to survive a real call (all vector registers are caller-saved),
// it was spilled to and reloaded from the stack on every distance evaluation,
// and every evaluation went through three calls:
//   fstdistfunc_ -> wrapper -> simsimd_dot_* dispatcher -> kernel.
//
// Here, for every storage type and ISA SimSIMD dispatches to, the dot-product
// kernel is inlined into a fused function compiled for the same target. The
// scaling then stays in registers. Each exported wrapper loads a fused variant
// selected once for the running CPU and tail-calls it.
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
#if !defined(SIMSIMD_TARGET_GENOA) && (defined(__linux__))
#define SIMSIMD_TARGET_GENOA 1
#endif
#if !defined(SIMSIMD_TARGET_SAPPHIRE) && (defined(__linux__))
#define SIMSIMD_TARGET_SAPPHIRE 1
#endif

#include <stddef.h>

#include "third_party/simsimd/include/simsimd/dot.h"

typedef float (*InnerProductDistanceFn)(const void *a, const void *b,
                                        const void *qty_ptr,
                                        float reciprocal_mag_product);

// FLOAT32 preserves PR #1487's existing final multiply in double precision.
#define DEFINE_FUSED_F32_INNER_PRODUCT(isa, target_attr)                      \
  target_attr static float InnerProductDistance_F32_##isa(                    \
      const void *a, const void *b, const void *qty_ptr,                      \
      float reciprocal_mag_product) {                                         \
    simsimd_distance_t distance;                                              \
    simsimd_dot_f32_##isa((simsimd_f32_t const *)a, (simsimd_f32_t const *)b, \
                          *(size_t const *)qty_ptr, &distance);               \
    return (float)(1.0 - distance * (double)reciprocal_mag_product);          \
  }

// FP16 and BF16 preserve their existing float-scale arithmetic.
#define DEFINE_FUSED_HALF_INNER_PRODUCT(suffix, type, isa, target_attr) \
  target_attr static float InnerProductDistance_##suffix##_##isa(       \
      const void *a, const void *b, const void *qty_ptr,                \
      float reciprocal_mag_product) {                                   \
    simsimd_distance_t distance;                                        \
    simsimd_dot_##type##_##isa((simsimd_##type##_t const *)a,           \
                               (simsimd_##type##_t const *)b,           \
                               *(size_t const *)qty_ptr, &distance);    \
    return 1.0f - ((float)distance * reciprocal_mag_product);           \
  }

DEFINE_FUSED_F32_INNER_PRODUCT(serial, )
DEFINE_FUSED_HALF_INNER_PRODUCT(F16, f16, serial, )
DEFINE_FUSED_HALF_INNER_PRODUCT(BF16, bf16, serial, )

// Capability probes exported by lib.c.
int simsimd_uses_neon(void);
int simsimd_uses_neon_f16(void);
int simsimd_uses_neon_bf16(void);
int simsimd_uses_neon_fhm(void);
int simsimd_uses_sve(void);
int simsimd_uses_haswell(void);
int simsimd_uses_skylake(void);
int simsimd_uses_genoa(void);
int simsimd_uses_sapphire(void);

#if SIMSIMD_TARGET_ARM
#if SIMSIMD_TARGET_NEON
DEFINE_FUSED_F32_INNER_PRODUCT(neon, __attribute__((target("+simd"))))
DEFINE_FUSED_HALF_INNER_PRODUCT(F16, f16, neon,
                                __attribute__((target("+simd+fp16"))))
DEFINE_FUSED_HALF_INNER_PRODUCT(BF16, bf16, neon_shift,
                                __attribute__((target("+simd"))))
#endif
#if SIMSIMD_TARGET_NEON_FHM
DEFINE_FUSED_HALF_INNER_PRODUCT(
    F16, f16, fhm, __attribute__((target("arch=armv8.4-a+simd+fp16+fp16fml"))))
#endif
#if SIMSIMD_TARGET_NEON_BF16
DEFINE_FUSED_HALF_INNER_PRODUCT(
    BF16, bf16, neon, __attribute__((target("arch=armv8.6-a+simd+bf16"))))
#endif
#if SIMSIMD_TARGET_SVE
DEFINE_FUSED_F32_INNER_PRODUCT(sve, __attribute__((target("+sve"))))
#endif
#endif  // SIMSIMD_TARGET_ARM

#if SIMSIMD_TARGET_X86
#if SIMSIMD_TARGET_HASWELL
DEFINE_FUSED_F32_INNER_PRODUCT(haswell,
                               __attribute__((target("avx2,f16c,fma"))))
DEFINE_FUSED_HALF_INNER_PRODUCT(F16, f16, haswell,
                                __attribute__((target("avx2,f16c,fma"))))
DEFINE_FUSED_HALF_INNER_PRODUCT(BF16, bf16, haswell,
                                __attribute__((target("avx2,f16c,fma"))))
#endif
#if SIMSIMD_TARGET_SKYLAKE
DEFINE_FUSED_F32_INNER_PRODUCT(
    skylake, __attribute__((target("avx512f,avx512vl,avx512bw,bmi2"))))
#endif
#if SIMSIMD_TARGET_GENOA
DEFINE_FUSED_HALF_INNER_PRODUCT(
    BF16, bf16, genoa,
    __attribute__((target("avx512f,avx512vl,bmi2,avx512bw,avx512bf16"))))
#endif
#if SIMSIMD_TARGET_SAPPHIRE
DEFINE_FUSED_HALF_INNER_PRODUCT(
    F16, f16, sapphire,
    __attribute__((target("avx512f,avx512vl,bmi2,avx512bw,avx512fp16"))))
#endif
#endif  // SIMSIMD_TARGET_X86

// The selectors mirror simsimd_find_metric_punned() for each dot-product
// datatype. The FP16 selector deliberately skips SVE: the bundled SVE dot
// kernel accumulates in f16 and can overflow for normal vector dimensions.
static InnerProductDistanceFn SelectF32InnerProductDistance(void) {
#if SIMSIMD_TARGET_ARM
#if SIMSIMD_TARGET_SVE
  if (simsimd_uses_sve()) {
    return InnerProductDistance_F32_sve;
  }
#endif
#if SIMSIMD_TARGET_NEON
  if (simsimd_uses_neon()) {
    return InnerProductDistance_F32_neon;
  }
#endif
#endif  // SIMSIMD_TARGET_ARM
#if SIMSIMD_TARGET_X86
#if SIMSIMD_TARGET_SKYLAKE
  if (simsimd_uses_skylake()) {
    return InnerProductDistance_F32_skylake;
  }
#endif
#if SIMSIMD_TARGET_HASWELL
  if (simsimd_uses_haswell()) {
    return InnerProductDistance_F32_haswell;
  }
#endif
#endif  // SIMSIMD_TARGET_X86
  return InnerProductDistance_F32_serial;
}

static InnerProductDistanceFn SelectF16InnerProductDistance(void) {
#if SIMSIMD_TARGET_ARM
#if SIMSIMD_TARGET_NEON_FHM
  if (simsimd_uses_neon_fhm()) {
    return InnerProductDistance_F16_fhm;
  }
#endif
#if SIMSIMD_TARGET_NEON
  if (simsimd_uses_neon_f16()) {
    return InnerProductDistance_F16_neon;
  }
#endif
#endif  // SIMSIMD_TARGET_ARM
#if SIMSIMD_TARGET_X86
#if SIMSIMD_TARGET_SAPPHIRE
  if (simsimd_uses_sapphire()) {
    return InnerProductDistance_F16_sapphire;
  }
#endif
#if SIMSIMD_TARGET_HASWELL
  if (simsimd_uses_haswell()) {
    return InnerProductDistance_F16_haswell;
  }
#endif
#endif  // SIMSIMD_TARGET_X86
  return InnerProductDistance_F16_serial;
}

static InnerProductDistanceFn SelectBF16InnerProductDistance(void) {
#if SIMSIMD_TARGET_ARM
#if SIMSIMD_TARGET_NEON_BF16
  if (simsimd_uses_neon_bf16()) {
    return InnerProductDistance_BF16_neon;
  }
#endif
#if SIMSIMD_TARGET_NEON
  if (simsimd_uses_neon()) {
    return InnerProductDistance_BF16_neon_shift;
  }
#endif
#endif  // SIMSIMD_TARGET_ARM
#if SIMSIMD_TARGET_X86
#if SIMSIMD_TARGET_HASWELL
  if (simsimd_uses_haswell()) {
    return InnerProductDistance_BF16_haswell;
  }
#endif
#if SIMSIMD_TARGET_GENOA
  if (simsimd_uses_genoa()) {
    return InnerProductDistance_BF16_genoa;
  }
#endif
#endif  // SIMSIMD_TARGET_X86
  return InnerProductDistance_BF16_serial;
}

static float ResolveF32InnerProductDistance(const void *a, const void *b,
                                            const void *qty_ptr,
                                            float reciprocal_mag_product);
static float ResolveF16InnerProductDistance(const void *a, const void *b,
                                            const void *qty_ptr,
                                            float reciprocal_mag_product);
static float ResolveBF16InnerProductDistance(const void *a, const void *b,
                                             const void *qty_ptr,
                                             float reciprocal_mag_product);

// Every dispatch pointer starts at its resolver. Concurrent first calls may
// resolve more than once, but each stores the same selected function.
static InnerProductDistanceFn f32_inner_product_distance_impl =
    ResolveF32InnerProductDistance;
static InnerProductDistanceFn f16_inner_product_distance_impl =
    ResolveF16InnerProductDistance;
static InnerProductDistanceFn bf16_inner_product_distance_impl =
    ResolveBF16InnerProductDistance;

#define DEFINE_RESOLVER(type, impl)                                   \
  static float Resolve##type##InnerProductDistance(                   \
      const void *a, const void *b, const void *qty_ptr,              \
      float reciprocal_mag_product) {                                 \
    InnerProductDistanceFn fn = Select##type##InnerProductDistance(); \
    __atomic_store_n(&impl, fn, __ATOMIC_RELAXED);                    \
    return fn(a, b, qty_ptr, reciprocal_mag_product);                 \
  }

DEFINE_RESOLVER(F32, f32_inner_product_distance_impl)
DEFINE_RESOLVER(F16, f16_inner_product_distance_impl)
DEFINE_RESOLVER(BF16, bf16_inner_product_distance_impl)

float InnerProductDistanceSimsimd(const void *pVect1, const void *pVect2,
                                  const void *qty_ptr,
                                  float reciprocal_mag_product) {
  return __atomic_load_n(&f32_inner_product_distance_impl, __ATOMIC_RELAXED)(
      pVect1, pVect2, qty_ptr, reciprocal_mag_product);
}

float InnerProductDistanceFP16Simsimd(const void *pVect1, const void *pVect2,
                                      const void *qty_ptr,
                                      float reciprocal_mag_product) {
  return __atomic_load_n(&f16_inner_product_distance_impl, __ATOMIC_RELAXED)(
      pVect1, pVect2, qty_ptr, reciprocal_mag_product);
}

float InnerProductDistanceBF16Simsimd(const void *pVect1, const void *pVect2,
                                      const void *qty_ptr,
                                      float reciprocal_mag_product) {
  return __atomic_load_n(&bf16_inner_product_distance_impl, __ATOMIC_RELAXED)(
      pVect1, pVect2, qty_ptr, reciprocal_mag_product);
}
