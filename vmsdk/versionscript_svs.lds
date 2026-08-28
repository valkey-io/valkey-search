GLIBC_2.2.5 {
global:
  malloc;
  free;
  calloc;
  realloc;
};

GLIBC_2.16 {
global:
  aligned_alloc;
} GLIBC_2.2.5;

GLIBCXX_3.4 {
global:
  _Znwm;
  _Znam;
  _ZdlPv;
  _ZdaPv;
  _ZnwmRKSt9nothrow_t;
  _ZnamRKSt9nothrow_t;
  _ZdlPvRKSt9nothrow_t;
  _ZdaPvRKSt9nothrow_t;
};

CXXABI_1.3.9 {
global:
  _ZdlPvm;
  _ZdaPvm;
} GLIBCXX_3.4;

CXXABI_1.3.11 {
global:
  _ZnwmSt11align_val_t;
  _ZnwmSt11align_val_tRKSt9nothrow_t;
  _ZnamSt11align_val_t;
  _ZnamSt11align_val_tRKSt9nothrow_t;
  _ZdlPvSt11align_val_t;
  _ZdlPvSt11align_val_tRKSt9nothrow_t;
  _ZdlPvmSt11align_val_t;
  _ZdaPvSt11align_val_t;
  _ZdaPvSt11align_val_tRKSt9nothrow_t;
  _ZdaPvmSt11align_val_t;
} CXXABI_1.3.9;

VALKEY_SEARCH {
global:
  *;
local:
  __wrap_*;
  __real_*;
};
