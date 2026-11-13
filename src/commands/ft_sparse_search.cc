/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <bit>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "absl/base/internal/endian.h"
#include "absl/status/status.h"
#include "src/commands/commands.h"
#include "src/query/sparse_search.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {
namespace {

// Validates command arguments, borrows posting strings for synchronous search,
// and writes nested ID/score pairs or packed uint32/FP64 records. Returns an
// error for invalid arguments, non-string keys, or malformed posting data.
absl::Status SearchPacked(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                          int argc, bool binary) {
  vmsdk::VerifyMainThread();
  long long k = 0;
  long long dimensions = 0;
  if (argc < 3 ||
      ValkeyModule_StringToLongLong(argv[1], &k) != VALKEYMODULE_OK ||
      ValkeyModule_StringToLongLong(argv[2], &dimensions) != VALKEYMODULE_OK ||
      k < 1 || k > query::kSparseMaxTopK || dimensions < 1 ||
      dimensions > query::kSparseMaxDimensions || argc != 3 + 2 * dimensions) {
    return absl::InvalidArgumentError(
        "Expected K M key weight [key weight ...]");
  }

  std::vector<vmsdk::UniqueValkeyOpenKey> keys;
  std::vector<query::SparsePostingList> lists;
  keys.reserve(dimensions);
  lists.reserve(dimensions);
  for (int i = 0; i < dimensions; ++i) {
    double weight = 0;
    if (ValkeyModule_StringToDouble(argv[4 + 2 * i], &weight) !=
        VALKEYMODULE_OK) {
      return absl::InvalidArgumentError("Invalid query weight");
    }
    keys.push_back(vmsdk::MakeUniqueValkeyOpenKey(ctx, argv[3 + 2 * i],
                                                  VALKEYMODULE_READ));
    const int type = ValkeyModule_KeyType(keys.back().get());
    if (type == VALKEYMODULE_KEYTYPE_EMPTY) {
      lists.push_back({{}, weight});
      continue;
    }
    if (type != VALKEYMODULE_KEYTYPE_STRING) {
      return absl::InvalidArgumentError(VALKEYMODULE_ERRORMSG_WRONGTYPE);
    }
    size_t length = 0;
    const char *data =
        ValkeyModule_StringDMA(keys.back().get(), &length, VALKEYMODULE_READ);
    lists.push_back({absl::string_view(data, length), weight});
  }

  // ponytail: DMA views require synchronous main-thread execution. Background
  // search would need an index-owned immutable snapshot, not borrowed key data.
  auto hits = query::SparseSearch(lists, k);
  if (!hits.ok()) {
    return hits.status();
  }
  if (binary) {
    constexpr size_t kReplyBytes = sizeof(uint32_t) + sizeof(double);
    std::string reply(hits->size() * kReplyBytes, '\0');
    for (size_t i = 0; i < hits->size(); ++i) {
      char *row = reply.data() + i * kReplyBytes;
      absl::little_endian::Store32(row, (*hits)[i].id);
      absl::little_endian::Store64(row + sizeof(uint32_t),
                                   std::bit_cast<uint64_t>((*hits)[i].score));
    }
    ValkeyModule_ReplyWithStringBuffer(ctx, reply.data(), reply.size());
  } else {
    ValkeyModule_ReplyWithArray(ctx, hits->size());
    for (const auto &hit : *hits) {
      ValkeyModule_ReplyWithArray(ctx, 2);
      ValkeyModule_ReplyWithLongLong(ctx, hit.id);
      ValkeyModule_ReplyWithDouble(ctx, hit.score);
    }
  }
  return absl::OkStatus();
}

}  // namespace

// Executes FT.SPARSESEARCH and replies with ranked [document ID, score] pairs.
absl::Status FTSparseSearchCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                               int argc) {
  return SearchPacked(ctx, argv, argc, false);
}

// Executes FT.SPARSESEARCHBIN with little-endian uint32/FP64 result records.
absl::Status FTSparseSearchBinCmd(ValkeyModuleCtx *ctx,
                                  ValkeyModuleString **argv, int argc) {
  return SearchPacked(ctx, argv, argc, true);
}

}  // namespace valkey_search
