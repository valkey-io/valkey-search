/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/commands/ft_debug_indexstats.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/attribute.h"
#include "src/index_schema.h"
#include "src/indexes/index_base.h"
#include "src/indexes/numeric.h"
#include "src/indexes/tag.h"
#include "src/indexes/text.h"
#include "src/indexes/text/posting.h"
#include "src/indexes/text/rax_wrapper.h"
#include "src/indexes/text/text_index.h"
#include "src/indexes/vector_flat.h"
#include "src/indexes/vector_hnsw.h"
#include "src/schema_manager.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

// ============================================================================
// Histogram helper: linear with last-bucket overflow.
//
// Result is a StatPairs with `n_buckets` entries. Bucket label "i" counts
// samples equal to i for i in [0, n_buckets-2]; the last entry is labeled
// ">=N-1" and counts samples >= n_buckets-1.
// ============================================================================

// Builds a linear histogram with last-bucket overflow.
//
// Buckets represent values [skip_first .. n_buckets-1]; the final bucket is
// labeled ">=N-1" and absorbs anything beyond. Pass skip_first=1 for
// histograms whose underlying domain can't be 0 (e.g. keys-per-word in a
// posting list — every tracked word has at least one key, so the "0" bucket
// would always be empty).
template <typename Iterable>
static StatPairs MakeLinearOverflowHistogram(size_t n_buckets,
                                             const Iterable& samples,
                                             size_t skip_first = 0) {
  std::vector<int64_t> counts(n_buckets, 0);
  for (auto sample : samples) {
    size_t idx = static_cast<size_t>(sample);
    if (idx >= n_buckets) {
      idx = n_buckets - 1;
    }
    counts[idx]++;
  }
  StatPairs out;
  out.reserve(n_buckets - skip_first);
  for (size_t i = skip_first; i < n_buckets; ++i) {
    std::string label;
    if (i + 1 == n_buckets && n_buckets > 1) {
      label = absl::StrCat(">=", i);
    } else {
      label = absl::StrCat(i);
    }
    out.emplace_back(std::move(label), StatValue{counts[i]});
  }
  return out;
}

// ============================================================================
// Per-attribute collectors (return a StatPairs containing the attribute's
// type-specific stats; the caller wraps with attributeAlias/Identifier).
// ============================================================================

namespace {

constexpr size_t kKeysHistogramBuckets = 10;

StatPairs CollectTagStats(const indexes::Tag& tag) {
  // Walk tracked keys; for each key, fetch its tag set and accumulate:
  //   - per-tag key counts (for keysPerTag histogram and numUniqueTags)
  //   - tags-per-key statistics (avg/max + key-with-most-tags)
  absl::flat_hash_map<std::string, size_t> tag_to_key_count;
  size_t tracked_keys_count = 0;
  size_t total_tags_per_key = 0;
  size_t max_tags_per_key = 0;
  std::string key_with_most_tags;

  tag.ForEachTrackedKey([&](const InternedStringPtr& key) -> absl::Status {
        bool case_sensitive_unused = false;
        const auto* tags_set = tag.GetValue(key, case_sensitive_unused);
        if (tags_set != nullptr) {
          ++tracked_keys_count;
          const size_t this_count = tags_set->size();
          total_tags_per_key += this_count;
          if (this_count > max_tags_per_key) {
            max_tags_per_key = this_count;
            key_with_most_tags = std::string(key->Str());
          }
          for (absl::string_view tag_view : *tags_set) {
            ++tag_to_key_count[std::string(tag_view)];
          }
        }
        return absl::OkStatus();
      })
      .IgnoreError();

  std::vector<size_t> keys_per_tag;
  keys_per_tag.reserve(tag_to_key_count.size());
  for (const auto& entry : tag_to_key_count) {
    keys_per_tag.push_back(entry.second);
  }

  StatPairs out;
  out.emplace_back("attributeType", StatValue{"TAG"});
  out.emplace_back("numUniqueTags",
                   StatValue{static_cast<int64_t>(tag_to_key_count.size())});
  out.emplace_back("keysPerTagHistogram",
                   StatValue{MakeLinearOverflowHistogram(kKeysHistogramBuckets,
                                                         keys_per_tag)});
  const double avg = tracked_keys_count == 0
                         ? 0.0
                         : static_cast<double>(total_tags_per_key) /
                               static_cast<double>(tracked_keys_count);
  out.emplace_back("tagsPerKeyAvg",
                   StatValue{absl::StrFormat("%.2f", avg)});
  out.emplace_back("tagsPerKeyMax",
                   StatValue{static_cast<int64_t>(max_tags_per_key)});
  out.emplace_back("keyWithMostTags", StatValue{key_with_most_tags});
  return out;
}

StatPairs CollectNumericStats(const indexes::Numeric& num) {
  StatPairs out;
  out.emplace_back("attributeType", StatValue{"NUMERIC"});
  out.emplace_back("numNumbers",
                   StatValue{static_cast<int64_t>(num.GetTrackedKeyCount())});
  out.emplace_back(
      "numSegmentTreeNodes",
      StatValue{static_cast<int64_t>(num.GetSegmentTreeNodeCount())});
  return out;
}

StatPairs CollectTextStats(const indexes::Text& text) {
  StatPairs out;
  out.emplace_back("attributeType", StatValue{"TEXT"});
  out.emplace_back("numKeys",
                   StatValue{static_cast<int64_t>(text.GetTrackedKeyCount())});
  return out;
}

template <typename T>
StatPairs CollectVectorFlatStats(const indexes::VectorFlat<T>& vf) {
  absl::ReaderMutexLock lock(&vf.GetResizeMutex());
  const size_t num_vectors = vf.GetCurrentElementCount();
  const size_t num_chunks = vf.GetChunkCount();
  const size_t per_chunk = vf.GetElementsPerChunk();
  const size_t capacity = num_chunks * per_chunk;
  const int util_percent =
      capacity == 0
          ? 0
          : static_cast<int>((num_vectors * 100ULL) / capacity);

  StatPairs out;
  out.emplace_back("attributeType", StatValue{"VECTOR_FLAT"});
  out.emplace_back("numVectors", StatValue{static_cast<int64_t>(num_vectors)});
  out.emplace_back("numChunks", StatValue{static_cast<int64_t>(num_chunks)});
  out.emplace_back("chunkUtilizationPercent", StatValue{util_percent});
  return out;
}

template <typename T>
StatPairs CollectVectorHnswStats(const indexes::VectorHNSW<T>& vh) {
  absl::ReaderMutexLock lock(&vh.GetResizeMutex());
  const size_t num_vectors = vh.GetCurrentElementCount();
  const size_t num_chunks = vh.GetChunkCount();
  const size_t per_chunk = vh.GetElementsPerChunk();
  const size_t capacity = num_chunks * per_chunk;
  const int util_percent =
      capacity == 0
          ? 0
          : static_cast<int>((num_vectors * 100ULL) / capacity);
  const int num_layers = vh.GetNumLayers();
  const size_t num_deleted = vh.GetDeletedCount();
  const std::vector<size_t> degree_hist = vh.GetLevel0OutDegreeHistogram();

  StatPairs hist_pairs;
  hist_pairs.reserve(degree_hist.size());
  for (size_t i = 0; i < degree_hist.size(); ++i) {
    hist_pairs.emplace_back(absl::StrCat(i),
                            StatValue{static_cast<int64_t>(degree_hist[i])});
  }

  StatPairs out;
  out.emplace_back("attributeType", StatValue{"VECTOR_HNSW"});
  out.emplace_back("numVectors", StatValue{static_cast<int64_t>(num_vectors)});
  out.emplace_back("numChunks", StatValue{static_cast<int64_t>(num_chunks)});
  out.emplace_back("chunkUtilizationPercent", StatValue{util_percent});
  out.emplace_back("numLayers", StatValue{num_layers});
  out.emplace_back("numDeletedVectors",
                   StatValue{static_cast<int64_t>(num_deleted)});
  out.emplace_back("outDegreeHistogram", StatValue{std::move(hist_pairs)});
  return out;
}

StatPairs CollectAttributeStats(const Attribute& attr) {
  auto idx = attr.GetIndex();
  switch (idx->GetIndexerType()) {
    case indexes::IndexerType::kTag:
      return CollectTagStats(*static_cast<indexes::Tag*>(idx.get()));
    case indexes::IndexerType::kNumeric:
      return CollectNumericStats(*static_cast<indexes::Numeric*>(idx.get()));
    case indexes::IndexerType::kText:
      return CollectTextStats(*static_cast<indexes::Text*>(idx.get()));
    case indexes::IndexerType::kFlat:
      return CollectVectorFlatStats(
          *static_cast<indexes::VectorFlat<float>*>(idx.get()));
    case indexes::IndexerType::kHNSW:
      return CollectVectorHnswStats(
          *static_cast<indexes::VectorHNSW<float>*>(idx.get()));
    default: {
      StatPairs out;
      out.emplace_back("attributeType", StatValue{"UNKNOWN"});
      return out;
    }
  }
}

// Index-level stats (text-cross-field).
StatPairs CollectIndexLevelStats(const IndexSchema& schema) {
  StatPairs out;

  size_t num_unique_words = 0;
  std::vector<size_t> keys_per_word;
  std::string word_with_most_keys;
  size_t max_keys_for_word = 0;

  auto text_schema = schema.GetTextIndexSchema();
  if (text_schema) {
    auto text_index = text_schema->GetTextIndex();
    if (text_index) {
      const auto& prefix = text_index->GetPrefix();
      num_unique_words = prefix.GetTotalUniqueWordCount();
      auto wi = prefix.GetWordIterator("");
      while (!wi.Done()) {
        auto postings = wi.GetPostingsTarget();
        size_t key_count = postings ? postings->GetKeyCount() : 0;
        keys_per_word.push_back(key_count);
        if (key_count > max_keys_for_word) {
          max_keys_for_word = key_count;
          word_with_most_keys = std::string(wi.GetWord());
        }
        wi.Next();
      }
    }
  }

  out.emplace_back("numUniqueWords",
                   StatValue{static_cast<int64_t>(num_unique_words)});
  // skip_first=1: every word in the prefix tree has >=1 key, so the "0"
  // bucket would always be empty.
  out.emplace_back(
      "keysPerWordHistogram",
      StatValue{MakeLinearOverflowHistogram(kKeysHistogramBuckets,
                                            keys_per_word, /*skip_first=*/1)});
  out.emplace_back("wordWithMostKeys", StatValue{word_with_most_keys});
  return out;
}

}  // namespace

// ============================================================================
// Top-level collector
// ============================================================================

StatPairs CollectIndexStats(const IndexSchema& schema,
                            const std::vector<std::string>& fields) {
  StatPairs root;
  root.emplace_back("indexName", StatValue{schema.GetName()});
  root.emplace_back("indexLevel", StatValue{CollectIndexLevelStats(schema)});

  StatPairs attrs;
  const auto& attr_map = schema.GetAttributes();

  if (fields.empty()) {
    for (const auto& entry : attr_map) {
      const Attribute& attr = entry.second;
      StatPairs body;
      body.emplace_back("attributeIdentifier",
                        StatValue{attr.GetIdentifier()});
      auto type_specific = CollectAttributeStats(attr);
      for (auto& kv : type_specific) {
        body.emplace_back(std::move(kv.first), std::move(kv.second));
      }
      attrs.emplace_back(attr.GetAlias(), StatValue{std::move(body)});
    }
  } else {
    for (const auto& field : fields) {
      auto it = attr_map.find(field);
      if (it == attr_map.end()) {
        // Unknown field — caller should have validated; emit nothing here.
        // (IndexStatsCmd validates up-front and returns an error.)
        continue;
      }
      StatPairs body;
      body.emplace_back("attributeIdentifier",
                        StatValue{it->second.GetIdentifier()});
      auto type_specific = CollectAttributeStats(it->second);
      for (auto& kv : type_specific) {
        body.emplace_back(std::move(kv.first), std::move(kv.second));
      }
      attrs.emplace_back(it->second.GetAlias(), StatValue{std::move(body)});
    }
  }

  root.emplace_back("attributes", StatValue{std::move(attrs)});
  return root;
}

// ============================================================================
// Emitters
// ============================================================================

void EmitResp(ValkeyModuleCtx* ctx, const StatPairs& kv) {
  ValkeyModule_ReplyWithArray(ctx, static_cast<long>(kv.size() * 2));
  for (const auto& entry : kv) {
    const std::string& key = entry.first;
    const StatValue& val = entry.second;
    ValkeyModule_ReplyWithSimpleString(ctx, key.c_str());
    std::visit(
        [&](const auto& x) {
          using X = std::decay_t<decltype(x)>;
          if constexpr (std::is_same_v<X, int64_t>) {
            ValkeyModule_ReplyWithLongLong(ctx,
                                           static_cast<long long>(x));
          } else if constexpr (std::is_same_v<X, std::string>) {
            ValkeyModule_ReplyWithStringBuffer(ctx, x.data(), x.size());
          } else if constexpr (std::is_same_v<X, StatPairs>) {
            EmitResp(ctx, x);
          }
        },
        val.v);
  }
}

void BuildLogLineRec(const StatPairs& kv, int indent, std::string& out) {
  const std::string pad(static_cast<size_t>(indent) * 2, ' ');
  for (const auto& entry : kv) {
    const std::string& key = entry.first;
    const StatValue& val = entry.second;
    std::visit(
        [&](const auto& x) {
          using X = std::decay_t<decltype(x)>;
          if constexpr (std::is_same_v<X, int64_t>) {
            absl::StrAppend(&out, pad, key, ": ", x, "\n");
          } else if constexpr (std::is_same_v<X, std::string>) {
            absl::StrAppend(&out, pad, key, ": \"", x, "\"\n");
          } else if constexpr (std::is_same_v<X, StatPairs>) {
            absl::StrAppend(&out, pad, key, ":\n");
            BuildLogLineRec(x, indent + 1, out);
          }
        },
        val.v);
  }
}

std::string BuildLogLine(const StatPairs& kv) {
  std::string out;
  BuildLogLineRec(kv, 0, out);
  return out;
}

// ----------------------------------------------------------------------------
// Single-line rendering for sectioned logging. Each StatValue is rendered
// inline so that one section fits in one log line (avoids Valkey's per-line
// LOG_MAX_LEN truncation).
// ----------------------------------------------------------------------------

static void RenderInlineInto(const StatValue& v, std::string& out);

static void RenderInlinePairsInto(const StatPairs& kv, std::string& out) {
  out.push_back('{');
  bool first = true;
  for (const auto& entry : kv) {
    if (!first) {
      out.push_back(' ');
    }
    first = false;
    absl::StrAppend(&out, entry.first, "=");
    RenderInlineInto(entry.second, out);
  }
  out.push_back('}');
}

static void RenderInlineInto(const StatValue& v, std::string& out) {
  std::visit(
      [&](const auto& x) {
        using X = std::decay_t<decltype(x)>;
        if constexpr (std::is_same_v<X, int64_t>) {
          absl::StrAppend(&out, x);
        } else if constexpr (std::is_same_v<X, std::string>) {
          absl::StrAppend(&out, "\"", x, "\"");
        } else if constexpr (std::is_same_v<X, StatPairs>) {
          RenderInlinePairsInto(x, out);
        }
      },
      v.v);
}

// Emits one log line per top-level entry, and (when the top-level entry is
// the `attributes` map) one log line per attribute. This keeps every line
// well under Valkey's per-line log size cap for any realistic schema.
static void LogStatsBySection(ValkeyModuleCtx* ctx,
                              absl::string_view header,
                              const StatPairs& root) {
  for (const auto& entry : root) {
    const std::string& key = entry.first;
    const StatValue& val = entry.second;
    const StatPairs* nested = std::get_if<StatPairs>(&val.v);
    if (key == "attributes" && nested != nullptr) {
      for (const auto& attr : *nested) {
        std::string body;
        RenderInlineInto(attr.second, body);
        VMSDK_LOG(NOTICE, ctx)
            << header << " attribute " << attr.first << ": " << body;
      }
    } else {
      std::string body;
      RenderInlineInto(val, body);
      VMSDK_LOG(NOTICE, ctx) << header << " " << key << ": " << body;
    }
  }
}

// ============================================================================
// Top-level handler — FT._DEBUG INDEXSTATS <index> [<field> ...]
// ============================================================================

absl::Status IndexStatsCmd(ValkeyModuleCtx* ctx, vmsdk::ArgsIterator& itr) {
  std::string index_name;
  VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, index_name));
  VMSDK_ASSIGN_OR_RETURN(auto index_schema,
                         SchemaManager::Instance().GetIndexSchema(
                             ValkeyModule_GetSelectedDb(ctx), index_name));

  std::vector<std::string> fields;
  while (itr.HasNext()) {
    std::string field;
    VMSDK_RETURN_IF_ERROR(vmsdk::ParseParamValue(itr, field));
    fields.push_back(std::move(field));
  }

  vmsdk::ReaderMutexLock lock(&index_schema->GetTimeSlicedMutex());

  // Validate all field names up-front; reject unknown fields with no partial
  // reply.
  if (!fields.empty()) {
    const auto& attrs = index_schema->GetAttributes();
    for (const auto& f : fields) {
      if (attrs.find(f) == attrs.end()) {
        return absl::InvalidArgumentError(
            absl::StrCat("Unknown attribute: ", f));
      }
    }
  }

  StatPairs result = CollectIndexStats(*index_schema, fields);

  const std::string header = absl::StrCat("INDEXSTATS ", index_name);
  LogStatsBySection(ctx, header, result);

  EmitResp(ctx, result);
  return absl::OkStatus();
}

}  // namespace valkey_search
