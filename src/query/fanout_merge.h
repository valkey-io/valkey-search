/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

// Shared by the coordinator's fan-out merges (fanout.cc and the bounded
// content fan-out), so that both decode shard replies and rank candidates
// identically.

#ifndef VALKEYSEARCH_SRC_QUERY_FANOUT_MERGE_H_
#define VALKEYSEARCH_SRC_QUERY_FANOUT_MERGE_H_

#include <bit>
#include <cstdint>
#include <optional>
#include <utility>

#include "src/coordinator/coordinator.pb.h"
#include "src/indexes/scoring/scorer.h"
#include "src/indexes/vector_base.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/cluster_map.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/type_conversions.h"

namespace valkey_search::query::fanout {

// The slot fingerprint a request to `node` carries; `fallback` when the
// node's shard is unknown (unit tests).
uint64_t TargetSlotFingerprint(const vmsdk::cluster_map::NodeInfo &node,
                               uint64_t fallback);

struct NeighborComparator {
  bool operator()(const indexes::Neighbor &a,
                  const indexes::Neighbor &b) const {
    // Max heap on distance, to pop off the furthest vector during KNN
    // aggregation. Hybrid text=>[KNN] sets score to text relevance, so key on
    // distance (not score) to evict correctly. Non-vector queries all have
    // distance 0 and are never evicted (see AddResult), falling to the
    // tie-break.
    if (a.distance != b.distance) {
      return a.distance < b.distance;
    }
    // Secondary sort: by key for consistent ordering when distances are equal.
    // The full string compare is required because for external keys there is no
    // guarantee of the stability of the InternedStringPtr across invocations.
    return a.external_id->Str() > b.external_id->Str();
  }
};

// Converts one shard reply entry to a Neighbor, taking its content. An entry
// marked content_omitted (bounded content fan-out) has no attribute_contents;
// any other entry has them, possibly empty.
inline indexes::Neighbor DecodeNeighborEntry(
    coordinator::NeighborEntry &neighbor_entry) {
  std::optional<RecordsMap> attribute_contents;
  if (!neighbor_entry.content_omitted()) {
    auto &contents = attribute_contents.emplace();
    for (const auto &attribute_content : neighbor_entry.attribute_contents()) {
      auto identifier =
          vmsdk::MakeUniqueValkeyString(attribute_content.identifier());
      auto identifier_view = vmsdk::ToStringView(identifier.get());
      contents.emplace(identifier_view,
                       RecordsMapValue(std::move(identifier),
                                       vmsdk::MakeUniqueValkeyString(
                                           attribute_content.content())));
    }
  }
  indexes::Neighbor neighbor{StringInternStore::Intern(neighbor_entry.key()),
                             neighbor_entry.distance(),
                             std::move(attribute_contents)};
  // Single-VR model: the VR distance is carried in Neighbor::distance
  // above. Cluster merge concatenates per-shard matches; the merged set is
  // ordered like any non-vector result.
  // The wire format is unchanged; a compound-OR match with no VR distance
  // is transmitted as a +inf distance, so reconstruct has_vr_distance from
  // the bit pattern rather than adding a proto field. Only +inf is the
  // marker, since no radius includes a +inf distance; an IP distance of
  // -inf is a VR distance and is kept. Compare the bits: -ffast-math makes
  // float comparisons with infinity unreliable.
  neighbor.has_vr_distance =
      std::bit_cast<uint32_t>(neighbor_entry.distance()) !=
      std::bit_cast<uint32_t>(indexes::scoring::PositiveInf());
  neighbor.score = neighbor_entry.score();
  return neighbor;
}

}  // namespace valkey_search::query::fanout

#endif  // VALKEYSEARCH_SRC_QUERY_FANOUT_MERGE_H_
