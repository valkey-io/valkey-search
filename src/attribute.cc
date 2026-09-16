/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */
#include "src/attribute.h"

#include "src/index_schema.h"
#include "src/valkey_search_options.h"

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
  // read, so they are reported as pairs like the other attribute properties.
  added_fields += VALKEY_SEARCH_COMPATIBILITY_FIX(
      1, 3, 0, "ft_info_sortable_flags",
      [&]() {
        int emitted = 0;
        if (sortable_) {
          ValkeyModule_ReplyWithSimpleString(ctx, "sortable");
          ValkeyModule_ReplyWithSimpleString(ctx, "1");
          emitted += 2;
          if (unf_) {
            ValkeyModule_ReplyWithSimpleString(ctx, "unf");
            ValkeyModule_ReplyWithSimpleString(ctx, "1");
            emitted += 2;
          }
        }
        return emitted;
      },
      []() { return 0; });
  ValkeyModule_ReplySetArrayLength(ctx, added_fields + 6);
  return 1;
}
}  // namespace valkey_search
