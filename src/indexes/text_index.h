#ifndef VALKEY_SEARCH_INDEXES_TEXT_TEXT_H_
#define VALKEY_SEARCH_INDEXES_TEXT_TEXT_H_

#include <optional>
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"
#include "src/attribute_data_type.h"
#include "src/index_schema.pb.h"
#include "src/indexes/index_base.h"
#include "src/rdb_serialization.h"
#include "src/utils/string_interning.h"

namespace valkey_search {

// Forward declare the types
namespace text {
class RadixTree {
public:
  RadixTree() = default;
};
}

using Key = InternedStringPtr;

namespace indexes {

struct Text : public IndexBase {
  // Constructor
  Text(const data_model::TextIndex& text_index_proto);

  // Virtual methods from IndexBase
  absl::StatusOr<bool> AddRecord(const InternedStringPtr& key,
                                 absl::string_view data) override;
  absl::StatusOr<bool> RemoveRecord(const InternedStringPtr& key,
                                    DeletionType deletion_type) override;
  absl::StatusOr<bool> ModifyRecord(const InternedStringPtr& key,
                                    absl::string_view data) override;
  int RespondWithInfo(ValkeyModuleCtx* ctx) const override;
  bool IsTracked(const InternedStringPtr& key) const override;
  absl::Status SaveIndex(RDBChunkOutputStream chunked_out) const override;
  std::unique_ptr<data_model::Index> ToProto() const override;
  uint64_t GetRecordCount() const override;

  text::RadixTree prefix_;
  std::optional<text::RadixTree> suffix_;

  absl::flat_hash_map<Key, text::RadixTree> reverse_;

  absl::flat_hash_set<Key> untracked_keys_;
};

}  // namespace indexes
}  // namespace valkey_search

#endif
