/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VMSDK_SRC_UTILS_H_
#define VMSDK_SRC_UTILS_H_
#include <absl/strings/str_format.h>

#include <optional>
#include <string>
#include <utility>

#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "absl/strings/string_view.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"
namespace vmsdk {

class StopWatch {
 public:
  StopWatch() { Reset(); }
  ~StopWatch() = default;
  void Reset() { start_time_ = absl::Now(); }
  absl::Duration Duration() const { return absl::Now() - start_time_; }

 private:
  absl::Time start_time_;
};
// Timer creation from background threads is not safe. The event loop of Redis/
// Valkey releases the GIL, and during this period also checks the timer data
// structure for the next pending timer, meaning there is no way to safely
// create a timer from a background thread.
//
// This function creates a timer from a background thread by creating a task
// that is added to the event loop, which then creates the timer.
int StartTimerFromBackgroundThread(ValkeyModuleCtx *ctx, mstime_t period,
                                   ValkeyModuleTimerProc callback, void *data);
struct TimerDeletionTask {
  ValkeyModuleCtx *ctx;
  ValkeyModuleTimerID timer_id;
  absl::AnyInvocable<void(void *)> user_data_deleter;
};
int StopTimerFromBackgroundThread(
    ValkeyModuleCtx *ctx, ValkeyModuleTimerID timer_id,
    absl::AnyInvocable<void(void *)> user_data_deleter);

bool verifyLoadedOnlyOnce();
void TrackCurrentAsMainThread();
bool IsMainThread();
inline void VerifyMainThread() { CHECK(IsMainThread()); }

// MainThreadAccessGuard ensures that all access to the underlying data
// structure is done on the main thread.
template <typename T>
class MainThreadAccessGuard {
 public:
  MainThreadAccessGuard() = default;
  MainThreadAccessGuard(const T &var) : var_(var) {}
  MainThreadAccessGuard(T &&var) noexcept : var_(std::move(var)) {}
  MainThreadAccessGuard &operator=(MainThreadAccessGuard<T> const &other) {
    VerifyMainThread();
    var_ = other.var_;
    return *this;
  }
  MainThreadAccessGuard &operator=(MainThreadAccessGuard<T> &&other) noexcept {
    VerifyMainThread();
    var_ = std::move(other.var_);
    return *this;
  }
  T &Get() {
    VerifyMainThread();
    return var_;
  }
  const T &Get() const {
    VerifyMainThread();
    return var_;
  }

 private:
  T var_;
};

int RunByMain(absl::AnyInvocable<void()> fn, bool force_async = false);

std::string WrongArity(absl::string_view cmd);

inline std::ostream &operator<<(std::ostream &os, ValkeyModuleString *s) {
  return os << (*(std::string *)s);
}

//
// Parse out a hash tag from a string view
//
std::optional<absl::string_view> ParseHashTag(absl::string_view);

bool IsRealUserClient(ValkeyModuleCtx *ctx);
bool MultiOrLua(ValkeyModuleCtx *ctx);

size_t DisplayAsSIBytes(size_t value, char *buffer, size_t buffer_size);

std::string PrintableBytes(absl::string_view sv);
std::string StringToHex(std::string_view s);

// Checks if a numeric value falls within an optional inclusive range [min,
// max]. The range is inclusive: a value is considered valid if min <= value <=
// max. If either boundary is not specified (`std::nullopt`), that check is
// skipped.
absl::Status VerifyRange(long long num_value, std::optional<long long> min,
                         std::optional<long long> max);
std::optional<std::string> JsonUnquote(absl::string_view sv);

//
// Class for Valkey Version
//
class ValkeyVersion {
 public:
  constexpr ValkeyVersion(uint16_t major, uint8_t minor, uint8_t patch)
      : version_((static_cast<unsigned>(major) << 16) |
                 (static_cast<unsigned>(minor) << 8) |
                 static_cast<unsigned>(patch)) {}
  constexpr ValkeyVersion(int version) : version_(version) {}
  unsigned Major() const { return (version_ >> 16) & 0xFFFF; }
  unsigned Minor() const { return (version_ >> 8) & 0xFF; }
  unsigned Patch() const { return (version_) & 0xFF; }
  operator unsigned() const { return version_; }
  std::string ToString() const {
    return absl::StrFormat("%d.%d.%d", Major(), Minor(), Patch());
  }
  int ToInt() const { return version_; }

  auto operator<=>(const ValkeyVersion &other) const = default;

  template <typename Sink>
  friend void AbslStringify(Sink &sink, const ValkeyVersion &sv) {
    absl::Format(&sink, "%d.%d.%d", sv.Major(), sv.Minor(), sv.Patch());
  }

 private:
  unsigned version_;
};

inline std::ostream &operator<<(std::ostream &os, const ValkeyVersion &sv) {
  return os << sv.ToString();
}

struct JsonQuotedStringView {
  absl::string_view view_;
  friend std::ostream &operator<<(std::ostream &os,
                                  const JsonQuotedStringView &js);
};

#define VMSDK_NON_COPYABLE(ClassName)    \
  ClassName(const ClassName &) = delete; \
  ClassName &operator=(const ClassName &) = delete

#define VMSDK_NON_MOVABLE(ClassName) \
  ClassName(ClassName &&) = delete;  \
  ClassName &operator=(ClassName &&) = delete

#define VMSDK_NON_COPYABLE_NON_MOVABLE(ClassName) \
  VMSDK_NON_COPYABLE(ClassName);                  \
  VMSDK_NON_MOVABLE(ClassName)

struct SocketAddress {
  std::string primary_endpoint;
  uint16_t port;

  auto operator<=>(const SocketAddress &) const = default;
};

// ValkeySelectDbGuard allows for selecting into the specified DB number while
// ensuring the DB is selected back to the previous DB afterwards.
class ValkeySelectDbGuard {
 public:
  ValkeySelectDbGuard(ValkeyModuleCtx *ctx, int db_index) : ctx_(ctx) {
    old_db_ = ValkeyModule_GetSelectedDb(ctx_);
    if (old_db_ != db_index) {
      if (ValkeyModule_SelectDb(ctx_, db_index) == VALKEYMODULE_OK) {
        switched_ = true;
      }
    }
  }

  ~ValkeySelectDbGuard() {
    if (switched_) {
      ValkeyModule_SelectDb(ctx_, old_db_);
    }
  }

  ValkeySelectDbGuard(const ValkeySelectDbGuard &) = delete;
  ValkeySelectDbGuard &operator=(const ValkeySelectDbGuard &) = delete;

 private:
  ValkeyModuleCtx *ctx_;
  int old_db_;
  bool switched_ = false;
};

}  // namespace vmsdk

// Hash specialization for SocketAddress
namespace std {
template <>
struct hash<vmsdk::SocketAddress> {
  size_t operator()(const vmsdk::SocketAddress &addr) const {
    size_t h1 = std::hash<std::string>{}(addr.primary_endpoint);
    size_t h2 = std::hash<uint16_t>{}(addr.port);
    return h1 ^ (h2 << 1);
  }
};
}  // namespace std

#endif  // VMSDK_SRC_UTILS_H_
