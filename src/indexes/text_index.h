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

#include <concepts>
#include <memory>

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

  using Key = vmsdk::InternedStringPtr;
  using Position = uint32_t;

  // Each text field is assigned a unique number within the containing index, this is used
  // by the Postings object to identify fields.
  size_t text_field_number;
  std::shared_ptr<TextIndex> text_



struct TextIndex {
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

  //
  // The main query data structure maps Words into Postings objects. This
  // is always done with a prefix tree. Optionally, a suffix tree can also be maintained.
  // But in any case for the same word the two trees must point to the same Postings object,
  // which is owned by this pair of trees. Plus, updates to these two trees need
  // to be atomic when viewed externally. The locking provided by the RadixTree object
  // is NOT quite sufficient to guarantee that the two trees are always in lock step.
  // thus this object becomes responsible for cross-tree locking issues.
  // Multiple locking strategies are possible. TBD (a shared-ed word lock table should work well)
  //
  std::shared_ptr<RadixTree<std::unique_ptr<Postings *>, false>>> prefix_;
  std::optional<text::RadixTree> suffix_;

  absl::flat_hash_map<Key, text::RadixTree> reverse_;

  absl::flat_hash_set<Key> untracked_keys_;
};

struct IndexSchemaText{
  //
  // This is the main index of all Text fields in this index schema
  //
  TextIndex corpus_;
  //
  // To support the Delete record and the post-filtering case, there is a separate
  // table of postings that are indexed by Key.
  //
  // This object must also ensure that updates of this object are multi-thread safe.
  //
  absl::flat_hash_map<Key, TextIndex>> by_key_;
};

}  // namespace indexes
}  // namespace valkey_search

#endif
