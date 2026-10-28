/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */
#include "src/attribute.h"

#include "src/index_schema.h"

namespace valkey_search {

int Attribute::RespondWithInfo(ValkeyModuleCtx* ctx,
                               const IndexSchema* index_schema) const {
  ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_LEN);
  ValkeyModule_ReplyWithSimpleString(ctx, "identifier");
  ValkeyModule_ReplyWithSimpleString(ctx, GetIdentifier().c_str());
  ValkeyModule_ReplyWithSimpleString(ctx, "attribute");
  ValkeyModule_ReplyWithSimpleString(ctx, GetAlias().c_str());
  ValkeyModule_ReplyWithSimpleString(ctx, "user_indexed_memory");
  ValkeyModule_ReplyWithLongLong(ctx, index_schema->GetSize(GetAlias()));
  int added_fields = index_->RespondWithInfo(ctx);
  // Redis reports these as bare tokens, which no generic key/value parser can
  // read, so they are reported as pairs like CASESENSITIVE. Each is reported
  // only where it can mean something: Redis rejects SORTABLE on a vector, and
  // UNF suppresses a normalization that a number never has.
  const auto indexer_type = index_->GetIndexerType();
  if (!indexes::IsVectorIndex(indexer_type)) {
    ValkeyModule_ReplyWithSimpleString(ctx, "SORTABLE");
    ValkeyModule_ReplyWithSimpleString(ctx, options_.sortable ? "1" : "0");
    added_fields += 2;
  }
  if (indexer_type == indexes::IndexerType::kTag ||
      indexer_type == indexes::IndexerType::kText) {
    ValkeyModule_ReplyWithSimpleString(ctx, "UNF");
    ValkeyModule_ReplyWithSimpleString(ctx, options_.unf ? "1" : "0");
    added_fields += 2;
  }
  ValkeyModule_ReplySetArrayLength(ctx, added_fields + 6);
  return 1;
}
}  // namespace valkey_search
