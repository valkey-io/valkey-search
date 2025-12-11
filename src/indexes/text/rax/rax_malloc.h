/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef RAX_ALLOC_H
#define RAX_ALLOC_H
#include <stddef.h>

/* Override with the wrappers provided by VMSDK. */
extern void* __wrap_malloc(size_t size);
extern void __wrap_free(void* ptr);
extern void* __wrap_realloc(void* ptr, size_t size);
extern int __wrap_malloc_usable_size(void* ptr);

#define rax_malloc __wrap_malloc
#define rax_realloc __wrap_realloc
#define rax_free __wrap_free
#define rax_ptr_alloc_size(ptr) ((size_t)__wrap_malloc_usable_size(ptr))

#endif
