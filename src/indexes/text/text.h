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

struct TextIndex : public indexes::IndexBase {
  TextIndex(...);
  ~TextIndex();

  virtual absl::StatusOr<bool> AddRecord(const InternedStringPtr& key,
                                         absl::string_view data) override;
  virtual absl::StatusOr<bool> RemoveRecord(const InternedStringPtr& key,
                                            DeletionType deletion_type) override;
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
  //
  // The main query data structure maps Words into Postings objects. This
  // is always done with a prefix tree. Optionally, a suffix tree can also be maintained.
  // But in any case for the same word the two trees must point to the same Postings object,
  // which is owned by this pair of trees. Plus, updates to these two trees need
  // to be atomic when viewed externally. The locking provided by the RadixTree object
  // is NOT quite sufficient to guarantee that the two trees are always in lock step.
  // Multiple locking strategies are possible. TBD (a shareded word lock table should work well)
  //
  RadixTree<std::unique_ptr<Postings *>, false>> prefix_;
  std::optional<RadixTree<Postings *, true>> suffix_;

  //
  // To support the Delete record and the post-filtering case, there is a separate
  // table of postings that are indexed by Key.
  //
  // This object must also ensure that updates of this object are multi-thread safe.
  //
  absl::flat_hash_map<InternedStringPtr, RadixTree<std::unique_ptr<Postings *>, false>> by_key_;
};

}  // namespace text
}  // namespace valkey_search

#endif