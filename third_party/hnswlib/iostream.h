#ifndef THIRD_PARTY_HNSWLIB_IOSTREAM_H_
#define THIRD_PARTY_HNSWLIB_IOSTREAM_H_

#include <cstddef>
#include <memory>
#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"

#ifdef VMSDK_ENABLE_MEMORY_ALLOCATION_OVERRIDES
#include "vmsdk/src/memory_allocation_overrides.h"  // IWYU pragma: keep
#endif

namespace hnswlib {

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
