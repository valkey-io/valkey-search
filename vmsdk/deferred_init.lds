/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 * Defer the module's C++ static initializers until after ValkeyModule_Alloc is
 * known.
 *
 * All memory allocated by the module must come from ValkeyModule_Alloc/Free,
 * but their addresses are not known until ValkeyModule_OnLoad runs. Static
 * initializers normally run at dlopen(), i.e. before that. This script renames
 * the output section holding their function pointers, which removes the
 * DT_INIT_ARRAY entry the dynamic loader would have walked. The array is
 * instead walked explicitly from ValkeyModule_OnLoad once the allocator is
 * established -- see vmsdk/src/deferred_init.cc.
 *
 * This is an addition to the default linker script (INSERT), not a replacement.
 * The input-section rules mirror the default .init_array rules exactly, so
 * initialization priority ordering is preserved. crtbegin/crtend are excluded
 * so that frame_dummy stays in the real .init_array and still runs at dlopen().
 */
SECTIONS
{
  .vmsdk_init_array :
  {
    PROVIDE_HIDDEN (__vmsdk_init_array_start = .);
    KEEP (*(SORT_BY_INIT_PRIORITY(.init_array.*) SORT_BY_INIT_PRIORITY(.ctors.*)))
    KEEP (*(EXCLUDE_FILE (*crtbegin.o *crtbegin?.o *crtend.o *crtend?.o) .init_array .ctors))
    PROVIDE_HIDDEN (__vmsdk_init_array_end = .);
  }
} INSERT BEFORE .init_array;
