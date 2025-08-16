#ifndef _VALKEY_SEARCH_INDEXES_TEXT_TEXT_H
#define _VALKSY_SEARCH_INDEXES_TEXT_TEXT_H

/*

External API for text subsystem

*/

#include <concepts>
#include <memory>

#include "src/utils/string_interning.h"

namespace valkey_search {
namespace text {

using Key = vmsdk::InternedStringPtr;
using Position = uint32_t;

using Byte = uint8_t;
using Char = uint32_t;

struct TextFieldIndex : public indexes::IndexBase {
  TextIndex(...);
  ~TextIndex();

  virtual absl::StatusOr<bool> AddRecord(const InternedStringPtr& key,
                                         absl::string_view data) override;
  virtual absl::StatusOr<bool> RemoveRecord(
      const InternedStringPtr& key, DeletionType deletion_type) override;
  virtual absl::StatusOr<bool> ModifyRecord(const InternedStringPtr& key,
                                            absl::string_view data) override;
  virtual int RespondWithInfo(RedisModuleCtx* ctx) const override;
  virtual bool IsTracked(const InternedStringPtr& key) const override;
  virtual absl::Status SaveIndex(RDBOutputStream& rdb_stream) const override;

  virtual std::unique_ptr<data_model::Index> ToProto() const override;
  virtual void ForEachTrackedKey(
      absl::AnyInvocable<void(const InternedStringPtr&)> fn) const override;

  virtual uint64_t GetRecordCount() const override;

 private:
  // Each text field is assigned a unique number within the containing index,
  // this is used by the Postings object to identify fields.
  size_t text_field_number;
  // The per-index text index.
  std::shared_ptr<TextIndex> text_
}

struct TextIndex {
  //
  // The main query data structure maps Words into Postings objects. This
  // is always done with a prefix tree. Optionally, a suffix tree can also be
  // maintained. But in any case for the same word the two trees must point to
  // the same Postings object, which is owned by this pair of trees. Plus,
  // updates to these two trees need to be atomic when viewed externally. The
  // locking provided by the RadixTree object is NOT quite sufficient to
  // guarantee that the two trees are always in lock step. thus this object
  // becomes responsible for cross-tree locking issues. Multiple locking
  // strategies are possible. TBD (a shared-ed word lock table should work well)
  //
  std::shared_ptr < RadixTree < std::unique_ptr<Postings*>, false >>> prefix_;
  std::optional<RadixTree<Postings*, true>> suffix_;
};

//
// this is a logical extension of the index-schema. could easily be merged into
// that object.
//
struct IndexSchemaText
    //
    // This is the main index of all Text fields in this index schema
    //
    TextIndex corpus_;
//
// To support the Delete record and the post-filtering case, there is a separate
// table of postings that are indexed by Key.
//
// This object must also ensure that updates of this object are multi-thread
// safe.
//
absl::flat_hash_map < Key, TextIndex >> by_key_;
};

}  // namespace text
}  // namespace valkey_search

#endif
