/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
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

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "src/acl.h"
#include "src/commands/commands.h"
#include "src/schema_manager.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

absl::Status FTDropIndexCmd(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
                            int argc) {
  if (argc != 2) {
    return absl::InvalidArgumentError(vmsdk::WrongArity(kDropIndexCommand));
  }
  auto index_schema_name = vmsdk::ToStringView(argv[1]);

  VMSDK_ASSIGN_OR_RETURN(
      auto index_schema,
      SchemaManager::Instance().GetIndexSchema(ValkeyModule_GetSelectedDb(ctx),
                                               index_schema_name));
  static const auto permissions =
      PrefixACLPermissions(kDropIndexCmdPermissions, kDropIndexCommand);
  VMSDK_RETURN_IF_ERROR(
      AclPrefixCheck(ctx, permissions, index_schema->GetKeyPrefixes()));

  VMSDK_RETURN_IF_ERROR(SchemaManager::Instance().RemoveIndexSchema(
      ValkeyModule_GetSelectedDb(ctx), index_schema_name));
  ValkeyModule_ReplyWithSimpleString(ctx, "OK");
  ValkeyModule_ReplicateVerbatim(ctx);
  return absl::OkStatus();
}
}  // namespace valkey_search
