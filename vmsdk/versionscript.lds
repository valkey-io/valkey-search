{
global:
  *;
local:
  /*
   * The module defines the C allocator entry points itself; see
   * vmsdk/src/memory_allocation_c_api.cc. Listing them here keeps them out of
   * the dynamic symbol table, which hides them from the rest of the process
   * and -- because a local symbol cannot be preempted -- makes every reference
   * from inside the module bind to them at link time.
   *
   * That binding is the whole mechanism. A dlopened library's symbol lookups
   * search the global scope first, and libc.so.6 defines malloc, so without
   * this these definitions would simply be ignored -- even by the module's own
   * operator new.
   */
  malloc;
  free;
  calloc;
  realloc;
  aligned_alloc;
  posix_memalign;
  valloc;
  malloc_usable_size;
  strdup;
  realpath;
  getcwd;
  /*
   * operator new/delete come from the statically linked libstdc++. Keep them
   * unexported so the module cannot interpose them for anything else in the
   * process.
   */
  _Znwm*;
  _Znam*;
  _ZdlPv*;
  _ZdaPv*;
  /*
   * libstdc++ symbols instantiated in direct translation units (in debug
   * builds without inlining) must remain local so they do not collide with
   * libstdc++.so.6.
   */
  _ZNSt*;
  _ZNKSt*;
  _ZSt*;
  _ZTISt*;
  _ZTVSt*;
  _ZTSSt*;
};
