/* Rax -- A radix tree implementation.
 *
 * Copyright (c) 2017-2018, Redis Ltd.
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef RAX_H
#define RAX_H

#ifdef __cplusplus
#include <cstddef>
#include <cstdint>
#include <memory_resource>
#include <string_view>
#else
#include <stddef.h>
#include <stdint.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* Representation of a radix tree as implemented in this file, that contains
 * the strings "foo", "foobar" and "footer" after the insertion of each
 * word. When the node represents a key inside the radix tree, we write it
 * between [], otherwise it is written between ().
 *
 * This is the vanilla representation:
 *
 *              (f) ""
 *                \
 *                (o) "f"
 *                  \
 *                  (o) "fo"
 *                    \
 *                  [t   b] "foo"
 *                  /     \
 *         "foot" (e)     (a) "foob"
 *                /         \
 *      "foote" (r)         (r) "fooba"
 *              /             \
 *    "footer" []             [] "foobar"
 *
 * However, this implementation implements a very common optimization where
 * successive nodes having a single child are "compressed" into the node
 * itself as a string of characters, each representing a next-level child,
 * and only the link to the node representing the last character node is
 * provided inside the representation. So the above representation is turned
 * into:
 *
 *                  ["foo"] ""
 *                     |
 *                  [t   b] "foo"
 *                  /     \
 *        "foot" ("er")    ("ar") "foob"
 *                 /          \
 *       "footer" []          [] "foobar"
 *
 * However this optimization makes the implementation a bit more complex.
 * For instance if a key "first" is added in the above radix tree, a
 * "node splitting" operation is needed, since the "foo" prefix is no longer
 * composed of nodes having a single child one after the other. This is the
 * above tree and the resulting node splitting after this event happens:
 *
 *
 *                    (f) ""
 *                    /
 *                 (i o) "f"
 *                 /   \
 *    "firs"  ("rst")  (o) "fo"
 *              /        \
 *    "first" []       [t   b] "foo"
 *                     /     \
 *           "foot" ("er")    ("ar") "foob"
 *                    /          \
 *          "footer" []          [] "foobar"
 *
 * Similarly after deletion, if a new chain of nodes having a single child
 * is created (the chain must also not include nodes that represent keys),
 * it must be compressed back into a single node.
 */

#define RAX_NODE_MAX_SIZE ((1 << 29) - 1)
typedef struct vs_raxNode {
  uint32_t is_key : 1;     /* Does this node contain a key? */
  uint32_t is_null : 1;    /* Associated value is NULL (don't store it). */
  uint32_t is_compr : 1;   /* Node is compressed. */
  uint32_t size : 29;      /* Number of children, or compressed string len. */
  uint64_t subtree_items;  // SEARCH
  /* Data layout is as follows:
   *
   * If node is not compressed we have 'size' bytes, one for each children
   * character, and 'size' vs_raxNode pointers, point to each child node.
   * Note how the character is not stored in the children but in the
   * edge of the parents:
   *
   * [header is_compr=0][abc][a-ptr][b-ptr][c-ptr](value-ptr?)
   *
   * if node is compressed (is_compr bit is 1) the node has 1 children.
   * In that case the 'size' bytes of the string stored immediately at
   * the start of the data section, represent a sequence of successive
   * nodes linked one after the other, for which only the last one in
   * the sequence is actually represented as a node, and pointed to by
   * the current compressed node.
   *
   * [header is_compr=1][xyz][z-ptr](value-ptr?)
   *
   * Both compressed and not compressed nodes can represent a key
   * with associated data in the radix tree at any level (not just terminal
   * nodes).
   *
   * If the node has an associated key (is_key=1) and is not NULL
   * (is_null=0), then after the vs_raxNode pointers pointing to the
   * children, an additional value pointer is present (as you can see
   * in the representation above as "value-ptr" field).
   */
  unsigned char data[];
} vs_raxNode;

typedef struct vs_rax {
  vs_raxNode *head;  /* Pointer to root node of tree */
  uint64_t numele;   /* Number of keys in the tree */
  uint64_t numnodes; /* Number of rax nodes in the tree */
  size_t alloc_size; /* Total allocation size of the tree in bytes */
} vs_rax;

/* Stack data structure used by vs_raxLowWalk() in order to, optionally, return
 * a list of parent nodes to the caller. The nodes do not have a "parent"
 * field for space concerns, so we use the auxiliary stack when needed. */
#define RAX_STACK_STATIC_ITEMS 32
typedef struct vs_raxStack {
  void **stack; /* Points to static_items or an heap allocated array. */
  size_t items, maxitems; /* Number of items contained and total space. */
  /* Up to RAX_STACK_STATIC_ITEMS items we avoid to allocate on the heap
   * and use this static array of pointers instead. */
  void *static_items[RAX_STACK_STATIC_ITEMS];
  int oom; /* True if pushing into this stack failed for OOM at some point. */
} vs_raxStack;

/* Optional callback used for iterators and be notified on each rax node,
 * including nodes not representing keys. */
typedef int (*vs_raxNodeCallback)(vs_raxNode **noderef);

/* Radix tree iterator state is encapsulated into this data structure. */
#define RAX_ITER_STATIC_LEN 128
#define RAX_ITER_JUST_SEEKED                                              \
  (1 << 0)                    /* Iterator was just seeked. Return current \
                                 element for the first iteration and      \
                                 clear the flag. */
#define RAX_ITER_EOF (1 << 1) /* End of iteration reached. */
#define RAX_ITER_SAFE                                \
  (1 << 2) /* Safe iterator, allows operations while \
              iterating. But it is slower. */
#define RAX_ITER_SUB_TREE \
  (1 << 3) /* SEARCH - restrict iteration to sub-tree. */

typedef struct vs_raxIterator {
  int flags;
  vs_rax *rt;         /* Radix tree we are iterating. */
  unsigned char *key; /* The current string. */
  void *data;         /* Data associated to this key. */
  size_t key_len;     /* Current key length. */
  size_t key_max;     /* Max key len the current key buffer can hold. */
  unsigned char key_static_string[RAX_ITER_STATIC_LEN];
  vs_raxNode *node;  /* Current node. Only for unsafe iteration. */
  vs_raxStack stack; /* Stack used for unsafe iteration. */
  vs_raxNodeCallback
      node_cb;      /* Optional node callback. Normally set to NULL. */
  vs_raxNode *head; /* SEARCH - Used to limit iteration to a subtree */
} vs_raxIterator;

/* BEGIN SEARCH */
/* Used to modify the subtree_items count */
typedef enum { kNone, kAdd, kSubtract } item_count_op;

/* Callback type for vs_raxMutate. Receives current value (NULL if key doesn't
 * exist) and the passed through caller context. Returns new value (NULL to
 * delete the key). */
typedef void *(*vs_raxMutateCallback)(void *current_value,
                                      void *caller_context);

uint32_t vs_raxGetSubtreeItemCount(vs_rax *rax, unsigned char *s, size_t len);
int vs_raxSeekSubTree(vs_raxIterator *it, unsigned char *ele, size_t len);
int vs_raxMutate(vs_rax *rax, unsigned char *s, size_t len,
                 vs_raxMutateCallback callback, void *caller_context,
                 item_count_op op);
/* END SEARCH */

/* Exported C API. */
vs_rax *vs_raxNew(void);
int vs_raxInsert(vs_rax *rax, unsigned char *s, size_t len, void *data,
                 void **old);
int vs_raxTryInsert(vs_rax *rax, unsigned char *s, size_t len, void *data,
                    void **old);
int vs_raxRemove(vs_rax *rax, unsigned char *s, size_t len, void **old);
int vs_raxFind(vs_rax *rax, unsigned char *s, size_t len, void **value);
void vs_raxFree(vs_rax *rax);
void vs_raxFreeWithCallback(vs_rax *rax, void (*free_callback)(void *));
void vs_raxStart(vs_raxIterator *it, vs_rax *rt);
int vs_raxSeek(vs_raxIterator *it, const char *op, unsigned char *ele,
               size_t len);
int vs_raxNext(vs_raxIterator *it);
int vs_raxPrev(vs_raxIterator *it);
int vs_raxRandomWalk(vs_raxIterator *it, size_t steps);
int vs_raxCompare(vs_raxIterator *iter, const char *op, unsigned char *key,
                  size_t key_len);
void vs_raxStop(vs_raxIterator *it);
int vs_raxEOF(vs_raxIterator *it);
void vs_raxShow(vs_rax *rax);
uint64_t vs_raxSize(vs_rax *rax);
size_t vs_raxAllocSize(vs_rax *rax);
unsigned long vs_raxTouch(vs_raxNode *n);
void vs_raxSetDebugMsg(int onoff);

/* Internal API. May be used by the node callback in order to access rax nodes
 * in a low level way, so this function is exported as well. */
void vs_raxSetData(vs_raxNode *n, void *data);

#ifdef __cplusplus
}
#endif

#ifdef __cplusplus
namespace valkey_search {

// RaxTree implements a high-performance C++ wrapper around the vs_rax radix
// tree with the following features:
// * Capacity Quantization & Growth Over-allocation: Rounds node allocations up
// to power-of-two capacity buckets
//   (32B, 64B, 128B, ...), ensuring child edge additions do not trigger
//   repeated reallocations or memory copies.
// * Quantized Size-Class Free-Lists: Maintains an array of LIFO free-lists
// indexed by power-of-two bucket size,
//   enabling O(1) recycling of split and compressed nodes without invoking
//   allocator functions.
// * Thread-Local Memory Resource Routing (RaxPmrGuard): Routes C-style
// malloc/free hooks directly to thread-local
//   memory pools, avoiding global heap mutex contention during multi-threaded
//   indexing.
// * Callback-Based Bulk Deallocation: Supports custom deallocation callbacks
// (`FreeWithCallback`) during tree
//   teardown to safely clean up associated leaf set structures in a single
//   pass.
class RaxTree {
 public:
  explicit RaxTree(std::pmr::memory_resource *res = nullptr);
  ~RaxTree();

  RaxTree(const RaxTree &) = delete;
  RaxTree &operator=(const RaxTree &) = delete;

  RaxTree(RaxTree &&other) noexcept;
  RaxTree &operator=(RaxTree &&other) noexcept;

  int Insert(std::string_view key, void *data, void **old_data = nullptr);
  int Remove(std::string_view key, void **old_data = nullptr);
  void *Find(std::string_view key) const;
  int Mutate(std::string_view key, vs_raxMutateCallback mutate,
             void *caller_context, item_count_op op = kNone);
  size_t GetSubtreeItemCount(std::string_view prefix) const;
  vs_rax *GetRax() const { return rax_; }
  uint64_t Size() const;
  size_t GetAllocSize() const;
  void FreeWithCallback(void (*free_callback)(void *));

  // Custom allocation hooks used by this tree instance
  void *AllocateNode(size_t size);
  void *ReallocateNode(void *ptr, size_t new_size);
  void FreeNode(void *ptr);
  int UsableSize(void *ptr);

  static constexpr int kNumBuckets = 8;
  static size_t GetBucketIndex(size_t size);
  static size_t GetBucketSize(size_t bucket_idx);

 private:
  vs_rax *rax_{nullptr};
  std::pmr::memory_resource *res_{nullptr};
  void *free_lists_[kNumBuckets]{nullptr};
  uint8_t free_counts_[kNumBuckets]{0};
};

}  // namespace valkey_search
#endif

#endif  // RAX_H
