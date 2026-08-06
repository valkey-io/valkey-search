#ifndef THIRD_PARTY_HNSWLIB_IOSTREAM_H_
#define THIRD_PARTY_HNSWLIB_IOSTREAM_H_

#include <stdlib.h>

#include <cstddef>
#include <cstdint>
#include <fstream>
#include <ios>
#include <memory>
#include <string>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"

#ifdef VMSDK_ENABLE_MEMORY_ALLOCATION_OVERRIDES
#include "vmsdk/src/memory_allocation_overrides.h"  // IWYU pragma: keep
#endif

namespace hnswlib {

class VectorTracker {
 public:
  virtual ~VectorTracker() = default;
  virtual char *TrackVector(uint64_t internal_id, char *vector, size_t len) = 0;

  // Two-phase tracking for the HNSW load path, where a legacy RDB may carry the
  // same label on several slots. StageVector interns the bytes and holds an
  // owning ref keyed by slot (returning the data pointer) so a later duplicate
  // can't overwrite and free an earlier slot. CommitStagedVector moves that ref
  // into the by-label map under the slot's final label once live/tombstone
  // slots are distinguished.
  virtual char *StageVector(uint64_t slot, char *vector, size_t len) = 0;
  virtual void CommitStagedVector(uint64_t slot, uint64_t label) = 0;
  virtual void ClearStagedVectors() = 0;
};

class InputStream {
 public:
  virtual ~InputStream() = default;
  virtual absl::StatusOr<std::unique_ptr<std::string>> LoadChunk() = 0;
};

class OutputStream {
 public:
  virtual ~OutputStream() = default;
  virtual absl::Status SaveChunk(const char *data, size_t len) = 0;
};

}  // namespace hnswlib

#endif  // THIRD_PARTY_HNSWLIB_IOSTREAM_H_
