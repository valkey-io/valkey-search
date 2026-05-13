// LD_PRELOAD mmap/munmap tracer. Logs each anonymous mmap of size >= MIN_MB
// with a short backtrace. Call from the benchmark with:
//   gcc -O2 -shared -fPIC mmap_trace.c -o mmap_trace.so -ldl
//   MMAP_MIN_MB=16 LD_PRELOAD=./mmap_trace.so ./bench_svs 100000 768 fp32
//
// Only traces *anonymous* private mmaps (the kind that show up as "(anon)"
// in smaps). File-backed mmaps of .so libraries are skipped.

#define _GNU_SOURCE
#include <dlfcn.h>
#include <execinfo.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

static void* (*real_mmap)(void*, size_t, int, int, int, off_t) = NULL;
static int (*real_munmap)(void*, size_t) = NULL;

static size_t min_bytes = 16 * 1024 * 1024;  // 16 MiB default
static FILE* logf = NULL;
static atomic_size_t total_anon_mapped = 0;

// Lazy init — called from mmap() on first invocation because
// __attribute__((constructor)) can race with dl linker doing its own mmaps.
static void ensure_init(void) {
  if (real_mmap) return;
  real_mmap = dlsym(RTLD_NEXT, "mmap");
  real_munmap = dlsym(RTLD_NEXT, "munmap");
  const char* env = getenv("MMAP_MIN_MB");
  if (env) min_bytes = (size_t)atoi(env) * 1024 * 1024;
  const char* out = getenv("MMAP_LOG");
  if (!out) out = "/tmp/mmap_trace.log";
  logf = fopen(out, "w");
  if (logf) {
    setvbuf(logf, NULL, _IOLBF, 0);
    fprintf(logf, "# tracing mmap >= %zu MiB anonymous regions\n",
            min_bytes / 1048576);
  }
}

void* mmap(void* addr, size_t length, int prot, int flags, int fd,
           off_t offset) {
  ensure_init();
  void* r = real_mmap(addr, length, prot, flags, fd, offset);
  int is_anon = (flags & MAP_ANONYMOUS) != 0 || fd == -1;
  if (logf && is_anon && length >= min_bytes && r != MAP_FAILED) {
    atomic_fetch_add(&total_anon_mapped, length);
    fprintf(logf, "\nMMAP %p size=%zu MiB prot=0x%x flags=0x%x total_live=%zu MiB\n",
            r, length / 1048576, prot, flags,
            atomic_load(&total_anon_mapped) / 1048576);
    void* bt[20];
    int n = backtrace(bt, 20);
    char** syms = backtrace_symbols(bt, n);
    if (syms) {
      for (int i = 1; i < n && i < 12; i++) {
        fprintf(logf, "  #%d %s\n", i, syms[i]);
      }
      free(syms);
    }
  }
  return r;
}

int munmap(void* addr, size_t length) {
  ensure_init();
  int r = real_munmap(addr, length);
  if (logf && length >= min_bytes) {
    atomic_fetch_sub(&total_anon_mapped, length);
    fprintf(logf, "MUNMAP %p size=%zu MiB total_live=%zu MiB\n",
            addr, length / 1048576,
            atomic_load(&total_anon_mapped) / 1048576);
  }
  return r;
}
