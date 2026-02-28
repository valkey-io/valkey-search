/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VMSDK_SRC_TESTING_INFRA_MODULE
#define VMSDK_SRC_TESTING_INFRA_MODULE

#include <atomic>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ios>
#include <optional>
#include <ostream>
#include <sstream>
#include <string>
#include <vector>

#include "absl/base/call_once.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/strings/str_format.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

class MockValkeyModule {
 public:
  MockValkeyModule() {
    ON_CALL(*this, EventLoopAddOneShot)
        .WillByDefault(
            [](ValkeyModuleEventLoopOneShotFunc callback, void *data) -> int {
              if (callback) {
                callback(data);
              }
              return 0;
            });
  };
  MOCK_METHOD(ValkeyModuleBlockedClient *, BlockClientOnAuth,
              (ValkeyModuleCtx * ctx, ValkeyModuleAuthCallback reply_callback,
               void (*free_privdata)(ValkeyModuleCtx *, void *)));
  MOCK_METHOD(ValkeyModuleBlockedClient *, BlockClient,
              (ValkeyModuleCtx * ctx, ValkeyModuleCmdFunc reply_callback,
               ValkeyModuleCmdFunc timeout_callback,
               void (*free_privdata)(ValkeyModuleCtx *, void *),
               long long timeout_ms));  // NOLINT
  MOCK_METHOD(void, Log,
              (ValkeyModuleCtx * ctx, const char *levelstr, const char *msg));
  MOCK_METHOD(void, LogIOError,
              (ValkeyModuleIO * io, const char *levelstr, const char *msg));
  MOCK_METHOD(int, UnblockClient,
              (ValkeyModuleBlockedClient * bc, void *privdata));
  MOCK_METHOD(void *, GetBlockedClientPrivateData, (ValkeyModuleCtx * ctx));
  MOCK_METHOD(int, AuthenticateClientWithACLUser,
              (ValkeyModuleCtx * ctx, const char *name, size_t len,
               ValkeyModuleUserChangedFunc callback, void *privdata,
               uint64_t *client_id));
  MOCK_METHOD(void, ACLAddLogEntryByUserName,
              (ValkeyModuleCtx * ctx, ValkeyModuleString *user,
               ValkeyModuleString *object,
               ValkeyModuleACLLogEntryReason reason));
  MOCK_METHOD(int, EventLoopAdd,
              (int fd, int mask, ValkeyModuleEventLoopFunc func,
               void *user_data));
  MOCK_METHOD(int, EventLoopAddOneShot,
              (ValkeyModuleEventLoopOneShotFunc func, void *user_data));
  MOCK_METHOD(int, EventLoopDel, (int fd, int mask));
  MOCK_METHOD(ValkeyModuleTimerID, CreateTimer,
              (ValkeyModuleCtx * ctx, mstime_t period,
               ValkeyModuleTimerProc callback, void *data));
  MOCK_METHOD(int, StopTimer,
              (ValkeyModuleCtx * ctx, ValkeyModuleTimerID id, void **data));
  MOCK_METHOD(void, SetModuleOptions, (ValkeyModuleCtx * ctx, int options));
  MOCK_METHOD(unsigned long long, GetClientId,  // NOLINT
              (ValkeyModuleCtx *ctx));
  MOCK_METHOD(int, GetClientInfoById, (void *ci, uint64_t id));
  MOCK_METHOD(int, SubscribeToKeyspaceEvents,
              (ValkeyModuleCtx * ctx, int types,
               ValkeyModuleNotificationFunc cb));
  MOCK_METHOD(int, KeyExists, (ValkeyModuleCtx * ctx, ValkeyModuleString *key));
  MOCK_METHOD(ValkeyModuleKey *, OpenKey,
              (ValkeyModuleCtx * ctx, ValkeyModuleString *key, int flags));
  MOCK_METHOD(int, HashExternalize,
              (ValkeyModuleKey * key, ValkeyModuleString *field,
               ValkeyModuleHashExternCB fn, void *privdata));
  MOCK_METHOD(int, GetApi, (const char *name, void *func));
  MOCK_METHOD(mstime_t, GetExpire, (ValkeyModuleKey * key));
  MOCK_METHOD(int, HashGet,
              (ValkeyModuleKey * key, int flags, const char *field,
               int *exists_out, void *terminating_null));
  MOCK_METHOD(int, HashGet,
              (ValkeyModuleKey * key, int flags, const char *field,
               ValkeyModuleString **value_out, void *terminating_null));
  MOCK_METHOD(int, HashSet,
              (ValkeyModuleKey * key, int flags, ValkeyModuleString *field,
               ValkeyModuleString *value_out, void *terminating_null));
  MOCK_METHOD(void, CloseKey, (ValkeyModuleKey * key));
  MOCK_METHOD(int, CreateCommand,
              (ValkeyModuleCtx * ctx, const char *name,
               ValkeyModuleCmdFunc cmdfunc, const char *strflags, int firstkey,
               int lastkey, int keystep));
  MOCK_METHOD(int, KeyType, (ValkeyModuleKey * key));
  MOCK_METHOD(int, ModuleTypeSetValue,
              (ValkeyModuleKey * key, ValkeyModuleType *mt, void *value));
  MOCK_METHOD(int, DeleteKey, (ValkeyModuleKey * key));
  MOCK_METHOD(int, RegisterInfoFunc,
              (ValkeyModuleCtx * ctx, ValkeyModuleInfoFunc cb));
  MOCK_METHOD(int, Init,
              (ValkeyModuleCtx * ctx, const char *name, int ver, int apiver));
  MOCK_METHOD(int, ReplyWithArray,
              (ValkeyModuleCtx * ctx, long len));  // NOLINT
  MOCK_METHOD(void, ReplySetArrayLength,
              (ValkeyModuleCtx * ctx, long len));  // NOLINT
  MOCK_METHOD(int, ReplyWithLongLong,
              (ValkeyModuleCtx * ctx, long long ll));  // NOLINT
  MOCK_METHOD(int, ReplyWithSimpleString,
              (ValkeyModuleCtx * ctx, const char *str));
  MOCK_METHOD(int, ReplyWithString,
              (ValkeyModuleCtx * ctx, ValkeyModuleString *str));
  MOCK_METHOD(int, ReplyWithDouble, (ValkeyModuleCtx * ctx, double val));
  MOCK_METHOD(int, ReplyWithCString, (ValkeyModuleCtx * ctx, const char *str));
  MOCK_METHOD(int, ReplyWithStringBuffer,
              (ValkeyModuleCtx * ctx, const char *buf, size_t len));
  MOCK_METHOD(int, FreeString,
              (ValkeyModuleCtx * ctx, ValkeyModuleString *str));
  MOCK_METHOD(ValkeyModuleType *, CreateDataType,
              (ValkeyModuleCtx * ctx, const char *name, int encver,
               ValkeyModuleTypeMethods *typemethods));
  MOCK_METHOD(int, ReplyWithError, (ValkeyModuleCtx * ctx, const char *err));
  MOCK_METHOD(int, ScanKey,
              (ValkeyModuleKey * key, ValkeyModuleScanCursor *cursor,
               ValkeyModuleScanKeyCB fn, void *privdata));
  MOCK_METHOD(ValkeyModuleScanCursor *, ScanCursorCreate, ());
  MOCK_METHOD(void, ScanCursorDestroy, (ValkeyModuleScanCursor * cursor));
  MOCK_METHOD(int, SubscribeToServerEvent,
              (ValkeyModuleCtx * ctx, ValkeyModuleEvent event,
               ValkeyModuleEventCallback cb));
  MOCK_METHOD(int, Scan,
              (ValkeyModuleCtx * ctx, ValkeyModuleScanCursor *cursor,
               ValkeyModuleScanCB fn, void *privdata));
  MOCK_METHOD(int, ReplicateVerbatim, (ValkeyModuleCtx * ctx));
  MOCK_METHOD(int, Replicate,
              (ValkeyModuleCtx * ctx, const char *cmdname, const char *fmt));
  MOCK_METHOD(ValkeyModuleType *, ModuleTypeGetType, (ValkeyModuleKey * key));
  MOCK_METHOD(ValkeyModuleCtx *, GetDetachedThreadSafeContext,
              (ValkeyModuleCtx * ctx));
  MOCK_METHOD(ValkeyModuleCtx *, GetThreadSafeContext,
              (ValkeyModuleBlockedClient * bc));
  MOCK_METHOD(void, FreeThreadSafeContext, (ValkeyModuleCtx * ctx));
  MOCK_METHOD(int, SelectDb, (ValkeyModuleCtx * ctx, int newid));
  MOCK_METHOD(int, GetSelectedDb, (ValkeyModuleCtx * ctx));
  MOCK_METHOD(void *, ModuleTypeGetValue, (ValkeyModuleKey * key));
  MOCK_METHOD(unsigned long long, DbSize, (ValkeyModuleCtx * ctx));  // NOLINT
  MOCK_METHOD(int, InfoAddSection,
              (ValkeyModuleInfoCtx * ctx, const char *str));
  MOCK_METHOD(int, InfoBeginDictField,
              (ValkeyModuleInfoCtx * ctx, const char *str));
  MOCK_METHOD(int, InfoEndDictField, (ValkeyModuleInfoCtx * ctx));
  MOCK_METHOD(int, InfoAddFieldLongLong,
              (ValkeyModuleInfoCtx * ctx, const char *str,
               long long field));  // NOLINT
  MOCK_METHOD(int, InfoAddFieldCString,
              (ValkeyModuleInfoCtx * ctx, const char *str, const char *field));
  MOCK_METHOD(int, InfoAddFieldDouble,
              (ValkeyModuleInfoCtx * ctx, const char *str, double field));
  MOCK_METHOD(int, RegisterStringConfig,
              (ValkeyModuleCtx * ctx, const char *name, const char *default_val,
               unsigned int flags, ValkeyModuleConfigGetStringFunc getfn,
               ValkeyModuleConfigSetStringFunc setfn,
               ValkeyModuleConfigApplyFunc applyfn, void *privdata));
  MOCK_METHOD(int, RegisterEnumConfig,
              (ValkeyModuleCtx * ctx, const char *name, int default_val,
               unsigned int flags, const char **enum_values,
               const int *int_values, int num_enum_vals,
               ValkeyModuleConfigGetEnumFunc getfn,
               ValkeyModuleConfigSetEnumFunc setfn,
               ValkeyModuleConfigApplyFunc applyfn, void *privdata));
  MOCK_METHOD(int, RegisterNumericConfig,
              (ValkeyModuleCtx * ctx, const char *name, long long default_val,
               unsigned int flags, long long min, long long max,
               ValkeyModuleConfigGetNumericFunc getfn,
               ValkeyModuleConfigSetNumericFunc setfn,
               ValkeyModuleConfigApplyFunc applyfn, void *privdata));
  MOCK_METHOD(int, RegisterBoolConfig,
              (ValkeyModuleCtx * ctx, const char *name, int default_val,
               unsigned int flags, ValkeyModuleConfigGetBoolFunc getfn,
               ValkeyModuleConfigSetBoolFunc setfn,
               ValkeyModuleConfigApplyFunc applyfn, void *privdata));
  MOCK_METHOD(int, LoadConfigs, (ValkeyModuleCtx * ctx));
  MOCK_METHOD(int, SetConnectionProperties,
              (const ValkeyModuleConnectionProperty *properties, int length));
  MOCK_METHOD(int, SetShardId, (const char *shard_id, int len));
  MOCK_METHOD(int, GetClusterInfo, (void *cli));
  MOCK_METHOD(const char *, GetMyShardID, ());
  MOCK_METHOD(int, GetContextFlags, (ValkeyModuleCtx * ctx));
  MOCK_METHOD(uint64_t, LoadUnsigned, (ValkeyModuleIO * io));
  MOCK_METHOD(int64_t, LoadSigned, (ValkeyModuleIO * io));
  MOCK_METHOD(double, LoadDouble, (ValkeyModuleIO * io));
  MOCK_METHOD(char *, LoadStringBuffer, (ValkeyModuleIO * io, size_t *lenptr));
  MOCK_METHOD(ValkeyModuleString *, LoadString, (ValkeyModuleIO * io));
  MOCK_METHOD(void, SaveUnsigned, (ValkeyModuleIO * io, uint64_t val));
  MOCK_METHOD(void, SaveSigned, (ValkeyModuleIO * io, int64_t val));
  MOCK_METHOD(void, SaveDouble, (ValkeyModuleIO * io, double val));
  MOCK_METHOD(void, SaveStringBuffer,
              (ValkeyModuleIO * io, const char *str, size_t len));
  MOCK_METHOD(int, IsIOError, (ValkeyModuleIO * io));
  MOCK_METHOD(int, ReplicationSetMasterCrossCluster,
              (ValkeyModuleCtx * ctx, const char *ip, const int port));
  MOCK_METHOD(int, ReplicationUnsetMasterCrossCluster, (ValkeyModuleCtx * ctx));
  MOCK_METHOD(const char *, GetMyClusterID, ());
  MOCK_METHOD(int, GetClusterNodeInfo,
              (ValkeyModuleCtx * ctx, const char *id, char *ip, char *master_id,
               int *port, int *flags));
  MOCK_METHOD(int, ReplicationSetSecondaryCluster,
              (ValkeyModuleCtx * ctx, bool is_secondary_cluster));
  MOCK_METHOD(ValkeyModuleCrossClusterReplicasList *,
              GetCrossClusterReplicasList, ());
  MOCK_METHOD(void, FreeCrossClusterReplicasList,
              (ValkeyModuleCrossClusterReplicasList * list));
  MOCK_METHOD(void *, Alloc, (size_t size));
  MOCK_METHOD(void, Free, (void *ptr));
  MOCK_METHOD(void *, Realloc, (void *ptr, size_t size));
  MOCK_METHOD(void *, Calloc, (size_t nmemb, size_t size));
  MOCK_METHOD(size_t, MallocUsableSize, (void *ptr));
  MOCK_METHOD(size_t, GetClusterSize, ());
  MOCK_METHOD(ValkeyModuleCallReply *, Call,
              (ValkeyModuleCtx * ctx, const char *cmd, const char *fmt,
               const char *arg1, const char *arg2));
  MOCK_METHOD(ValkeyModuleCallReply *, Call,
              (ValkeyModuleCtx * ctx, const char *cmd, const char *fmt,
               const char *arg1, const char *arg2, size_t arg2_len,
               const char *arg3, size_t arg3_len));
  MOCK_METHOD(void *, GetSharedAPI, (ValkeyModuleCtx * ctx, const char *arg1));
  MOCK_METHOD(ValkeyModuleCallReply *, Call,
              (ValkeyModuleCtx * ctx, const char *cmd, const char *fmt,
               const char *arg1));
  MOCK_METHOD(ValkeyModuleCallReply *, CallReplyArrayElement,
              (ValkeyModuleCallReply * reply, size_t index));
  MOCK_METHOD(int, CallReplyMapElement,
              (ValkeyModuleCallReply * reply, size_t index,
               ValkeyModuleCallReply **key, ValkeyModuleCallReply **val));
  MOCK_METHOD(const char *, CallReplyStringPtr,
              (ValkeyModuleCallReply * reply, size_t *len));
  MOCK_METHOD(void, FreeCallReply, (ValkeyModuleCallReply * reply));
  MOCK_METHOD(void, RegisterClusterMessageReceiver,
              (ValkeyModuleCtx * ctx, uint8_t type,
               ValkeyModuleClusterMessageReceiver callback));
  MOCK_METHOD(int, SendClusterMessage,
              (ValkeyModuleCtx * ctx, const char *target_id, uint8_t type,
               const char *msg, uint32_t len));
  MOCK_METHOD(char **, GetClusterNodesList,
              (ValkeyModuleCtx * ctx, size_t *numnodes));
  MOCK_METHOD(ValkeyModuleCtx *, GetContextFromIO, (ValkeyModuleIO * rdb));
  MOCK_METHOD(int, GetDbIdFromIO, (ValkeyModuleIO * rdb));
  MOCK_METHOD(void, FreeClusterNodesList, (char **ids));
  MOCK_METHOD(int, CallReplyType, (ValkeyModuleCallReply * reply));
  MOCK_METHOD(size_t, CallReplyLength, (ValkeyModuleCallReply * reply));
  MOCK_METHOD(long long, CallReplyInteger, (ValkeyModuleCallReply * reply));
  MOCK_METHOD(ValkeyModuleString *, CreateStringFromCallReply,
              (ValkeyModuleCallReply * reply));
  MOCK_METHOD(int, WrongArity, (ValkeyModuleCtx * ctx));
  MOCK_METHOD(int, Fork, (ValkeyModuleForkDoneHandler cb, void *user_data));
  MOCK_METHOD(int, ExitFromChild, (int retcode));
  MOCK_METHOD(ValkeyModuleRdbStream *, RdbStreamCreateFromRioHandler,
              (const ValkeyModuleRIOHandler *handler));
  MOCK_METHOD(int, RdbLoad,
              (ValkeyModuleCtx * ctx, ValkeyModuleRdbStream *stream,
               int flags));
  MOCK_METHOD(int, RdbSave,
              (ValkeyModuleCtx * ctx, ValkeyModuleRdbStream *stream,
               int flags));
  MOCK_METHOD(void, RdbStreamFree, (ValkeyModuleRdbStream * stream));
  MOCK_METHOD(ValkeyModuleString *, GetCurrentUserName,
              (ValkeyModuleCtx * ctx));
  MOCK_METHOD(long long, Milliseconds, ());
};
// NOLINTBEGIN(readability-identifier-naming)
// Global kMockValkeyModule is a fake Valkey module used for static wrappers
// around MockValkeyModule methods.
inline MockValkeyModule *kMockValkeyModule VALKEYMODULE_ATTR;

inline void TestValkeyModule_Log(ValkeyModuleCtx *ctx [[maybe_unused]],
                                 const char *levelstr [[maybe_unused]],
                                 const char *fmt [[maybe_unused]], ...) {
  char out[2048];
  va_list args;
  va_start(args, fmt);
  vsnprintf(out, sizeof(out), fmt, args);
  va_end(args);
  printf("TestValkey[%s]: %s\n", levelstr, out);
  kMockValkeyModule->Log(ctx, levelstr, out);
}

inline void TestValkeyModule_LogIOError(ValkeyModuleIO *io [[maybe_unused]],
                                        const char *levelstr [[maybe_unused]],
                                        const char *fmt [[maybe_unused]], ...) {
  char out[2048];
  va_list args;
  va_start(args, fmt);
  vsnprintf(out, sizeof(out), fmt, args);
  va_end(args);
  printf("TestValkey[%s]: %s\n", levelstr, out);
  kMockValkeyModule->LogIOError(io, levelstr, out);
}

inline int TestValkeyModule_BlockedClientMeasureTimeStart(
    ValkeyModuleBlockedClient *bc [[maybe_unused]]) {
  return 0;
}

inline int TestValkeyModule_BlockedClientMeasureTimeEnd(
    ValkeyModuleBlockedClient *bc [[maybe_unused]]) {
  return 0;
}

inline int TestValkeyModule_UnblockClient(ValkeyModuleBlockedClient *bc,
                                          void *privdata) {
  return kMockValkeyModule->UnblockClient(bc, privdata);
}

struct ValkeyModuleBlockedClient {};

// Simple test implementation of ValkeyModuleString
struct ValkeyModuleString {
  std::string data;
  std::atomic<int> cnt{1};
};

class ReplyCapture {
 public:
  struct ReplyElement;
  struct ReplyArray {
    long target_length;  // NOLINT
    std::vector<ReplyElement> elements;
  };
  struct ReplyElement {
    std::optional<long long> long_long_value;  // NOLINT
    std::optional<std::string> simple_string_value;
    std::optional<std::string> string_value;
    std::optional<std::string> err_string_value;
    std::optional<ReplyArray> array_value;
    std::optional<double> double_value;
  };
  void ReplyWithArray(long len) {  // NOLINT
    auto result = AllocateElement();
    result->array_value = ReplyArray{.target_length = len};
    if (len == VALKEYMODULE_POSTPONED_ARRAY_LEN) {
      curr_postponed_length_arrays.push_back(result);
    }
  }
  void ReplySetArrayLength(long len) {  // NOLINT
    EXPECT_FALSE(curr_postponed_length_arrays.empty());
    curr_postponed_length_arrays.back()->array_value->target_length = len;
    curr_postponed_length_arrays.pop_back();
  }
  void ReplyWithLongLong(long long val) {  // NOLINT
    auto result = AllocateElement();
    result->long_long_value = val;
  }
  void ReplyWithSimpleString(const char *msg) {
    auto result = AllocateElement();
    result->simple_string_value = msg;
  }
  void ReplyWithString(ValkeyModuleString *str) {
    auto result = AllocateElement();
    result->string_value = vmsdk::ToStringView(str);
  }
  void ReplyWithCString(const char *str) {
    auto result = AllocateElement();
    result->string_value = std::string(str);
  }
  void ReplyWithStringBuffer(const char *buf, size_t len) {
    auto result = AllocateElement();
    result->string_value = std::string(buf, len);
  }
  void ReplyWithError(const char *str) {
    auto result = AllocateElement();
    result->err_string_value = std::string(str);
  }
  void ReplyWithError(ValkeyModuleString *str) {
    auto result = AllocateElement();
    result->err_string_value = vmsdk::ToStringView(str);
  }
  void ReplyWithDouble(double val) {
    auto result = AllocateElement();
    result->double_value = val;
  }
  std::string GetReply() const {
    std::string result;
    for (auto &reply_element : captured_elements) {
      result += ToString(reply_element);
    }
    return result;
  }
  void ClearReply() { captured_elements.clear(); }
  static std::string ToString(const ReplyElement &element) {
    if (element.long_long_value.has_value()) {
      return absl::StrFormat(":%lld\r\n", *element.long_long_value);
    } else if (element.simple_string_value.has_value()) {
      return absl::StrFormat("+%s\r\n", *element.simple_string_value);
    } else if (element.string_value.has_value()) {
      return absl::StrFormat("$%d\r\n%s\r\n", element.string_value->size(),
                             *element.string_value);
    } else if (element.err_string_value.has_value()) {
      return absl::StrFormat("-%s\r\n", *element.err_string_value);
    } else if (element.array_value.has_value()) {
      std::string result =
          absl::StrFormat("*%d\r\n", element.array_value->target_length);
      for (auto &array_element : element.array_value->elements) {
        result += ToString(array_element);
      }
      return result;
    } else if (element.double_value.has_value()) {
      return absl::StrFormat("%g\r\n", *element.double_value);
    } else {
      return "";
    }
  }

 private:
  ReplyElement *AllocateElement(std::optional<ReplyArray> &array_opt) {
    if (!array_opt.has_value()) {
      return nullptr;
    }
    auto &array = array_opt.value();
    if (!array.elements.empty()) {
      auto result = AllocateElement(array.elements.back().array_value);
      if (result) {
        return result;
      }
    }
    if (array.target_length == VALKEYMODULE_POSTPONED_ARRAY_LEN ||
        array.elements.size() < (size_t)array.target_length) {
      array.elements.push_back(ReplyElement{});
      return &array.elements.back();
    }
    return nullptr;
  }
  ReplyElement *AllocateElement() {
    if (captured_elements.empty()) {
      captured_elements.push_back(ReplyElement{});
      return &captured_elements.back();
    }
    auto result = AllocateElement(captured_elements.back().array_value);
    if (result) {
      return result;
    }
    captured_elements.push_back(ReplyElement{});
    return &captured_elements.back();
  }
  std::vector<ReplyElement *> curr_postponed_length_arrays;
  std::vector<ReplyElement> captured_elements;
};

struct RegisteredKey {
  std::string key;
  void *data;
  ValkeyModuleType *module_type;
  bool operator==(const RegisteredKey &other) const {
    return key == other.key && data == other.data &&
           module_type == other.module_type;
  }
};

struct ValkeyModuleCtx {
  ReplyCapture reply_capture;
  absl::flat_hash_map<std::string, RegisteredKey> registered_keys;
};

struct ValkeyModuleIO {};

class InfoCapture {
 public:
  void InfoAddSection(const char *str) { info_ << str << std::endl; }
  void InfoAddFieldLongLong(const char *str, long long field,  // NOLINT
                            int in_dict_field) {
    if (in_dict_field) {
      info_ << str << "=" << field << ",";
    } else {
      info_ << str << ": " << field << std::endl;
    }
  }
  void InfoAddFieldCString(const char *str, const char *field,
                           int in_dict_field) {
    if (in_dict_field) {
      info_ << str << "=" << field << ",";
    } else {
      info_ << str << ": '" << field << "'" << std::endl;
    }
  }
  void InfoAddFieldDouble(const char *str, double field, int in_dict_field) {
    if (in_dict_field) {
      info_ << str << "=" << field << ",";
    } else {
      info_ << str << ": " << field << std::endl;
    }
  }
  void InfoEndDictField() {
    if (!info_.str().empty() && info_.str().back() == ',') {
      info_.seekp(-1, std::ios_base::end);
    }
    info_ << std::endl;
  }
  void InfoBeginDictField(const char *str, int in_dict_field) {
    if (in_dict_field) {
      InfoEndDictField();
    }
    info_ << str << ":";
  }
  std::string GetInfo() const { return info_.str(); }

 private:
  std::stringstream info_;
};

struct ValkeyModuleInfoCtx {
  InfoCapture info_capture;
  int in_dict_field = 0;
};

struct ValkeyModuleKey {
  ValkeyModuleCtx *ctx;
  std::string key;
};

inline const char *TestValkeyModule_StringPtrLen(const ValkeyModuleString *str,
                                                 size_t *len) {
  if (len != nullptr) {
    *len = str->data.size();
  }
  return str->data.c_str();
}

inline ValkeyModuleString *TestValkeyModule_CreateStringPrintf(
    ValkeyModuleCtx *ctx [[maybe_unused]], const char *fmt, ...) {
  char out[1024];
  va_list args;
  va_start(args, fmt);
  vsnprintf(out, sizeof(out), fmt, args);
  va_end(args);
  return new ValkeyModuleString{std::string(out)};
}

inline void *TestValkeyModule_GetBlockedClientPrivateData(
    ValkeyModuleCtx *ctx) {
  return kMockValkeyModule->GetBlockedClientPrivateData(ctx);
}

inline ValkeyModuleBlockedClient *TestValkeyModule_BlockClientOnAuth(
    ValkeyModuleCtx *ctx, ValkeyModuleAuthCallback reply_callback,
    void (*free_privdata)(ValkeyModuleCtx *, void *)) {
  return kMockValkeyModule->BlockClientOnAuth(ctx, reply_callback,
                                              free_privdata);
}

inline ValkeyModuleBlockedClient *TestValkeyModule_BlockClient(
    ValkeyModuleCtx *ctx, ValkeyModuleCmdFunc reply_callback,
    ValkeyModuleCmdFunc timeout_callback,
    void (*free_privdata)(ValkeyModuleCtx *, void *),
    long long timeout_ms) {  // NOLINT
  return kMockValkeyModule->BlockClient(ctx, reply_callback, timeout_callback,
                                        free_privdata, timeout_ms);
}

inline int TestValkeyModule_AuthenticateClientWithACLUser(
    ValkeyModuleCtx *ctx, const char *name, size_t len,
    ValkeyModuleUserChangedFunc callback, void *privdata, uint64_t *client_id) {
  return kMockValkeyModule->AuthenticateClientWithACLUser(
      ctx, name, len, callback, privdata, client_id);
}

inline void TestValkeyModule_ACLAddLogEntryByUserName(
    ValkeyModuleCtx *ctx, ValkeyModuleString *user, ValkeyModuleString *object,
    ValkeyModuleACLLogEntryReason reason) {
  return kMockValkeyModule->ACLAddLogEntryByUserName(ctx, user, object, reason);
}

inline ValkeyModuleString *TestValkeyModule_CreateString(ValkeyModuleCtx *ctx
                                                         [[maybe_unused]],
                                                         const char *ptr,
                                                         size_t len) {
  return new ValkeyModuleString{std::string(ptr, len)};
}

inline void TestValkeyModule_FreeString(ValkeyModuleCtx *ctx [[maybe_unused]],
                                        ValkeyModuleString *str) {
  str->cnt--;
  if (str->cnt == 0) {
    delete str;
  }
}

inline void TestValkeyModule_RetainString(ValkeyModuleCtx *ctx [[maybe_unused]],
                                          ValkeyModuleString *str) {
  str->cnt++;
}

inline int TestValkeyModule_EventLoopAdd(int fd, int mask,
                                         ValkeyModuleEventLoopFunc func,
                                         void *user_data) {
  return kMockValkeyModule->EventLoopAdd(fd, mask, func, user_data);
}

inline int TestValkeyModule_EventLoopAddOneShot(
    ValkeyModuleEventLoopOneShotFunc func, void *user_data) {
  return kMockValkeyModule->EventLoopAddOneShot(func, user_data);
}

inline int TestValkeyModule_EventLoopDel(int fd, int mask) {
  return kMockValkeyModule->EventLoopDel(fd, mask);
}

inline ValkeyModuleTimerID TestValkeyModule_CreateTimer(
    ValkeyModuleCtx *ctx, mstime_t period, ValkeyModuleTimerProc callback,
    void *data) {
  return kMockValkeyModule->CreateTimer(ctx, period, callback, data);
}

inline int TestValkeyModule_StopTimer(ValkeyModuleCtx *ctx,
                                      ValkeyModuleTimerID id, void **data) {
  return kMockValkeyModule->StopTimer(ctx, id, data);
}

inline void TestValkeyModule_SetModuleOptions(ValkeyModuleCtx *ctx,
                                              int options) {
  return kMockValkeyModule->SetModuleOptions(ctx, options);
}

inline unsigned long long TestValkeyModule_GetClientId(  // NOLINT
    ValkeyModuleCtx *ctx) {
  return kMockValkeyModule->GetClientId(ctx);
}

inline int TestValkeyModule_GetClientInfoById(void *ci, uint64_t id) {
  return kMockValkeyModule->GetClientInfoById(ci, id);
}

inline int TestValkeyModule_SubscribeToKeyspaceEvents(
    ValkeyModuleCtx *ctx, int types, ValkeyModuleNotificationFunc cb) {
  return kMockValkeyModule->SubscribeToKeyspaceEvents(ctx, types, cb);
}

inline int TestValkeyModule_KeyExistsDefaultImpl(ValkeyModuleCtx *ctx,
                                                 ValkeyModuleString *key) {
  if (ctx != nullptr) {
    return ctx->registered_keys.contains(key->data) ? 1 : 0;
  }
  return 0;
}

inline int TestValkeyModule_HashExternalizeDefaultImpl(
    ValkeyModuleKey *key, ValkeyModuleString *field,
    ValkeyModuleHashExternCB fn, void *privdata) {
  return VALKEYMODULE_OK;
}

inline int TestValkeyModule_GetApiDefaultImpl(const char *name, void *func) {
  return VALKEYMODULE_OK;
}

inline int TestValkeyModule_KeyExists(ValkeyModuleCtx *ctx,
                                      ValkeyModuleString *key) {
  return kMockValkeyModule->KeyExists(ctx, key);
}

inline ValkeyModuleKey *TestValkeyModule_OpenKeyDefaultImpl(
    ValkeyModuleCtx *ctx, ValkeyModuleString *key, int flags) {
  return new ValkeyModuleKey{ctx, key->data};
}

inline ValkeyModuleKey *TestValkeyModule_OpenKey(ValkeyModuleCtx *ctx,
                                                 ValkeyModuleString *key,
                                                 int flags) {
  return kMockValkeyModule->OpenKey(ctx, key, flags);
}

inline int TestValkeyModule_HashExternalize(ValkeyModuleKey *key,
                                            ValkeyModuleString *field,
                                            ValkeyModuleHashExternCB fn,
                                            void *privdata) {
  return kMockValkeyModule->HashExternalize(key, field, fn, privdata);
}

inline int TestValkeyModule_GetApi(const char *name, void *func) {
  return kMockValkeyModule->GetApi(name, func);
}

inline mstime_t TestValkeyModule_GetExpire(ValkeyModuleKey *key) {
  return kMockValkeyModule->GetExpire(key);
}

inline int TestValkeyModule_HashGet(ValkeyModuleKey *key, int flags, ...) {
  va_list args;
  va_start(args, flags);

  const char *field = va_arg(args, const char *);
  int result;
  if (flags & VALKEYMODULE_HASH_EXISTS) {
    int *exists_out = va_arg(args, int *);
    void *terminating_null = va_arg(args, void *);
    result = kMockValkeyModule->HashGet(key, flags, field, exists_out,
                                        terminating_null);
  } else {
    ValkeyModuleString **value_out = va_arg(args, ValkeyModuleString **);
    void *terminating_null = va_arg(args, void *);
    result = kMockValkeyModule->HashGet(key, flags, field, value_out,
                                        terminating_null);
  }

  va_end(args);
  return result;
}

inline int TestValkeyModule_HashSet(ValkeyModuleKey *key, int flags, ...) {
  va_list args;
  va_start(args, flags);

  ValkeyModuleString *field = va_arg(args, ValkeyModuleString *);
  ValkeyModuleString *value = va_arg(args, ValkeyModuleString *);
  void *terminating_null = va_arg(args, void *);
  int result =
      kMockValkeyModule->HashSet(key, flags, field, value, terminating_null);
  va_end(args);
  return result;
}

struct ValkeyModuleScanCursor {
  int cursor{0};
};

inline int TestValkeyModule_ScanKey(ValkeyModuleKey *key,
                                    ValkeyModuleScanCursor *cursor,
                                    ValkeyModuleScanKeyCB fn, void *privdata) {
  return kMockValkeyModule->ScanKey(key, cursor, fn, privdata);
}

inline ValkeyModuleScanCursor *TestValkeyModule_ScanCursorCreate() {
  return new ValkeyModuleScanCursor();
}

inline void TestValkeyModule_ScanCursorDestroy(ValkeyModuleScanCursor *cursor) {
  delete cursor;
}

inline void TestValkeyModule_CloseKeyDefaultImpl(ValkeyModuleKey *key) {
  delete key;
}

inline void TestValkeyModule_CloseKey(ValkeyModuleKey *key) {
  return kMockValkeyModule->CloseKey(key);
}

inline int TestValkeyModule_CreateCommand(ValkeyModuleCtx *ctx,
                                          const char *name,
                                          ValkeyModuleCmdFunc cmdfunc,
                                          const char *strflags, int firstkey,
                                          int lastkey, int keystep) {
  return kMockValkeyModule->CreateCommand(ctx, name, cmdfunc, strflags,
                                          firstkey, lastkey, keystep);
}

inline int TestValkeyModule_KeyTypeDefaultImpl(ValkeyModuleKey *key) {
  if (key->ctx && key->ctx->registered_keys.contains(key->key)) {
    return VALKEYMODULE_KEYTYPE_MODULE;
  }
  return VALKEYMODULE_KEYTYPE_EMPTY;
}

inline int TestValkeyModule_KeyType(ValkeyModuleKey *key) {
  return kMockValkeyModule->KeyType(key);
}

inline int TestValkeyModule_ModuleTypeSetValueDefaultImpl(ValkeyModuleKey *key,
                                                          ValkeyModuleType *mt,
                                                          void *value) {
  key->ctx->registered_keys[key->key] = RegisteredKey{
      .key = key->key,
      .data = value,
      .module_type = mt,
  };
  return VALKEYMODULE_OK;
}

inline int TestValkeyModule_ModuleTypeSetValue(ValkeyModuleKey *key,
                                               ValkeyModuleType *mt,
                                               void *value) {
  return kMockValkeyModule->ModuleTypeSetValue(key, mt, value);
}

inline int TestValkeyModule_DeleteKeyDefaultImpl(ValkeyModuleKey *key) {
  if (key->ctx != nullptr) {
    if (key->ctx->registered_keys.contains(key->key)) {
      key->ctx->registered_keys.erase(key->key);
      return VALKEYMODULE_OK;
    }
  }
  return VALKEYMODULE_ERR;
}

inline int TestValkeyModule_DeleteKey(ValkeyModuleKey *key) {
  return kMockValkeyModule->DeleteKey(key);
}

inline int TestValkeyModule_ReplyWithArray(ValkeyModuleCtx *ctx,
                                           long len) {  // NOLINT
  if (ctx) {
    ctx->reply_capture.ReplyWithArray(len);
  }
  return kMockValkeyModule->ReplyWithArray(ctx, len);
}

inline void TestValkeyModule_ReplySetArrayLength(ValkeyModuleCtx *ctx,
                                                 long len) {  // NOLINT
  if (ctx) {
    ctx->reply_capture.ReplySetArrayLength(len);
  }
  kMockValkeyModule->ReplySetArrayLength(ctx, len);
}

inline int TestValkeyModule_ReplyWithLongLong(ValkeyModuleCtx *ctx,
                                              long long ll) {  // NOLINT
  if (ctx) {
    ctx->reply_capture.ReplyWithLongLong(ll);
  }
  return kMockValkeyModule->ReplyWithLongLong(ctx, ll);
}

inline int TestValkeyModule_ReplyWithSimpleString(ValkeyModuleCtx *ctx,
                                                  const char *msg) {
  if (ctx) {
    ctx->reply_capture.ReplyWithSimpleString(msg);
  }
  return kMockValkeyModule->ReplyWithSimpleString(ctx, msg);
}

inline int TestValkeyModule_ReplyWithString(ValkeyModuleCtx *ctx,
                                            ValkeyModuleString *msg) {
  if (ctx) {
    ctx->reply_capture.ReplyWithString(msg);
  }
  return kMockValkeyModule->ReplyWithString(ctx, msg);
}

inline int TestValkeyModule_ReplyWithDouble(ValkeyModuleCtx *ctx, double val) {
  if (ctx) {
    ctx->reply_capture.ReplyWithDouble(val);
  }
  return kMockValkeyModule->ReplyWithDouble(ctx, val);
}

inline int TestValkeyModule_ReplyWithCString(ValkeyModuleCtx *ctx,
                                             const char *str) {
  if (ctx) {
    ctx->reply_capture.ReplyWithCString(str);
  }
  return kMockValkeyModule->ReplyWithCString(ctx, str);
}

inline int TestValkeyModule_ReplyWithStringBuffer(ValkeyModuleCtx *ctx,
                                                  const char *buf, size_t len) {
  if (ctx) {
    ctx->reply_capture.ReplyWithStringBuffer(buf, len);
  }
  return kMockValkeyModule->ReplyWithStringBuffer(ctx, buf, len);
}

inline int TestValkeyModule_RegisterInfoFunc(ValkeyModuleCtx *ctx,
                                             ValkeyModuleInfoFunc cb) {
  return kMockValkeyModule->RegisterInfoFunc(ctx, cb);
}

inline int TestValkeyModule_Init(ValkeyModuleCtx *ctx, const char *name,
                                 int ver, int apiver) {
  return kMockValkeyModule->Init(ctx, name, ver, apiver);
}

inline ValkeyModuleType *TestValkeyModule_CreateDataType(
    ValkeyModuleCtx *ctx, const char *name, int encver,
    ValkeyModuleTypeMethods *typemethods) {
  return kMockValkeyModule->CreateDataType(ctx, name, encver, typemethods);
}

inline int TestValkeyModule_ReplyWithError(ValkeyModuleCtx *ctx,
                                           const char *err) {
  if (ctx) {
    ctx->reply_capture.ReplyWithError(err);
  }
  return kMockValkeyModule->ReplyWithError(ctx, err);
}

inline int TestValkeyModule_SubscribeToServerEvent(
    ValkeyModuleCtx *ctx, ValkeyModuleEvent event,
    ValkeyModuleEventCallback cb) {
  return kMockValkeyModule->SubscribeToServerEvent(ctx, event, cb);
}

inline int TestValkeyModule_Scan(ValkeyModuleCtx *ctx,
                                 ValkeyModuleScanCursor *cursor,
                                 ValkeyModuleScanCB fn, void *privdata) {
  return kMockValkeyModule->Scan(ctx, cursor, fn, privdata);
}

inline int TestValkeyModule_ReplicateVerbatim(ValkeyModuleCtx *ctx) {
  return kMockValkeyModule->ReplicateVerbatim(ctx);
}

inline int TestValkeyModule_Replicate(ValkeyModuleCtx *ctx, const char *cmdname,
                                      const char *fmt, ...) {
  return kMockValkeyModule->Replicate(ctx, cmdname, fmt);
}

inline ValkeyModuleType *TestValkeyModule_ModuleTypeGetTypeDefaultImpl(
    ValkeyModuleKey *key) {
  if (key->ctx->registered_keys.contains(key->key)) {
    return key->ctx->registered_keys[key->key].module_type;
  } else {
    return nullptr;
  }
  return key->ctx->registered_keys[key->key].module_type;
}

inline ValkeyModuleType *TestValkeyModule_ModuleTypeGetType(
    ValkeyModuleKey *key) {
  return kMockValkeyModule->ModuleTypeGetType(key);
}

inline ValkeyModuleCtx *TestValkeyModule_GetDetachedThreadSafeContext(
    ValkeyModuleCtx *ctx) {
  return kMockValkeyModule->GetDetachedThreadSafeContext(ctx);
}

inline ValkeyModuleCtx *TestValkeyModule_GetThreadSafeContext(
    ValkeyModuleBlockedClient *bc) {
  return kMockValkeyModule->GetThreadSafeContext(bc);
}

inline void TestValkeyModule_FreeThreadSafeContext(ValkeyModuleCtx *ctx) {
  return kMockValkeyModule->FreeThreadSafeContext(ctx);
}

inline int TestValkeyModule_SelectDb(ValkeyModuleCtx *ctx, int newid) {
  return kMockValkeyModule->SelectDb(ctx, newid);
}

inline int TestValkeyModule_GetSelectedDb(ValkeyModuleCtx *ctx) {
  return kMockValkeyModule->GetSelectedDb(ctx);
}

inline void *TestValkeyModule_ModuleTypeGetValueDefaultImpl(
    ValkeyModuleKey *key) {
  if (key->ctx->registered_keys.contains(key->key)) {
    return key->ctx->registered_keys[key->key].data;
  } else {
    return nullptr;
  }
}

inline void *TestValkeyModule_ModuleTypeGetValue(ValkeyModuleKey *key) {
  return kMockValkeyModule->ModuleTypeGetValue(key);
}

inline unsigned long long TestValkeyModule_DbSize(  // NOLINT
    ValkeyModuleCtx *ctx) {
  return kMockValkeyModule->DbSize(ctx);
}

inline int TestValkeyModule_InfoAddSection(ValkeyModuleInfoCtx *ctx,
                                           const char *str) {
  if (ctx) {
    ctx->info_capture.InfoAddSection(str);
  }
  return kMockValkeyModule->InfoAddSection(ctx, str);
}

inline int TestValkeyModule_InfoAddFieldLongLong(ValkeyModuleInfoCtx *ctx,
                                                 const char *str,
                                                 long long field) {  // NOLINT
  if (ctx) {
    ctx->info_capture.InfoAddFieldLongLong(str, field, ctx->in_dict_field);
  }
  return kMockValkeyModule->InfoAddFieldLongLong(ctx, str, field);
}

inline int TestValkeyModule_InfoAddFieldCString(ValkeyModuleInfoCtx *ctx,
                                                const char *str,
                                                const char *field) {
  if (ctx) {
    ctx->info_capture.InfoAddFieldCString(str, field, ctx->in_dict_field);
  }
  return kMockValkeyModule->InfoAddFieldCString(ctx, str, field);
}

inline int TestValkeyModule_InfoAddFieldDouble(ValkeyModuleInfoCtx *ctx,
                                               const char *str, double field) {
  if (ctx) {
    ctx->info_capture.InfoAddFieldDouble(str, field, ctx->in_dict_field);
  }
  return kMockValkeyModule->InfoAddFieldDouble(ctx, str, field);
}

inline int TestValkeyModule_InfoBeginDictField(ValkeyModuleInfoCtx *ctx,
                                               const char *str) {
  if (ctx) {
    ctx->info_capture.InfoBeginDictField(str, ctx->in_dict_field);
    ctx->in_dict_field = 1;
  }
  return kMockValkeyModule->InfoBeginDictField(ctx, str);
}

inline int TestValkeyModule_InfoEndDictField(ValkeyModuleInfoCtx *ctx) {
  if (ctx) {
    ctx->info_capture.InfoEndDictField();
  }
  ctx->in_dict_field = 0;
  return kMockValkeyModule->InfoEndDictField(ctx);
}

inline int TestValkeyModule_RegisterStringConfig(
    ValkeyModuleCtx *ctx, const char *name, const char *default_val,
    unsigned int flags, ValkeyModuleConfigGetStringFunc getfn,
    ValkeyModuleConfigSetStringFunc setfn, ValkeyModuleConfigApplyFunc applyfn,
    void *privdata) {
  return kMockValkeyModule->RegisterStringConfig(
      ctx, name, default_val, flags, getfn, setfn, applyfn, privdata);
}

inline int TestValkeyModule_RegisterEnumConfig(
    ValkeyModuleCtx *ctx, const char *name, int default_val, unsigned int flags,
    const char **enum_values, const int *int_values, int num_enum_vals,
    ValkeyModuleConfigGetEnumFunc getfn, ValkeyModuleConfigSetEnumFunc setfn,
    ValkeyModuleConfigApplyFunc applyfn, void *privdata) {
  return kMockValkeyModule->RegisterEnumConfig(
      ctx, name, default_val, flags, enum_values, int_values, num_enum_vals,
      getfn, setfn, applyfn, privdata);
}

inline int TestValkeyModule_RegisterNumericConfig(
    ValkeyModuleCtx *ctx, const char *name, long long default_val,
    unsigned int flags, long long min, long long max,
    ValkeyModuleConfigGetNumericFunc getfn,
    ValkeyModuleConfigSetNumericFunc setfn, ValkeyModuleConfigApplyFunc applyfn,
    void *privdata) {
  return kMockValkeyModule->RegisterNumericConfig(
      ctx, name, default_val, flags, min, max, getfn, setfn, applyfn, privdata);
}

inline int TestValkeyModule_RegisterBoolConfig(
    ValkeyModuleCtx *ctx, const char *name, int default_val, unsigned int flags,
    ValkeyModuleConfigGetBoolFunc getfn, ValkeyModuleConfigSetBoolFunc setfn,
    ValkeyModuleConfigApplyFunc applyfn, void *privdata) {
  return kMockValkeyModule->RegisterBoolConfig(ctx, name, default_val, flags,
                                               getfn, setfn, applyfn, privdata);
}

inline int TestValkeyModule_LoadConfigs(ValkeyModuleCtx *ctx) {
  return kMockValkeyModule->LoadConfigs(ctx);
}

inline int TestValkeyModule_StringCompare(ValkeyModuleString *a,
                                          ValkeyModuleString *b) {
  if (a == nullptr && b == nullptr) {
    return 0;
  }
  if (a == nullptr && b != nullptr) {
    return -1;
  }
  if (a != nullptr && b == nullptr) {
    return 1;
  }
  return strcmp(a->data.c_str(), b->data.c_str());
}

inline int TestValkeyModule_SetConnectionProperties(
    const ValkeyModuleConnectionProperty *properties, int length) {
  return kMockValkeyModule->SetConnectionProperties(properties, length);
}

inline int TestValkeyModule_SetShardId(const char *shardId, int len) {
  return kMockValkeyModule->SetShardId(shardId, len);
}

inline int TestValkeyModule_GetClusterInfo(void *cli) {
  return kMockValkeyModule->GetClusterInfo(cli);
}

inline const char *TestValkeyModule_GetMyShardID() {
  return kMockValkeyModule->GetMyShardID();
}

inline int TestValkeyModule_GetContextFlags(ValkeyModuleCtx *ctx) {
  return kMockValkeyModule->GetContextFlags(ctx);
}

inline uint64_t TestValkeyModule_LoadUnsigned(ValkeyModuleIO *io) {
  return kMockValkeyModule->LoadUnsigned(io);
}

inline int64_t TestValkeyModule_LoadSigned(ValkeyModuleIO *io) {
  return kMockValkeyModule->LoadSigned(io);
}

inline double TestValkeyModule_LoadDouble(ValkeyModuleIO *io) {
  return kMockValkeyModule->LoadDouble(io);
}

inline char *TestValkeyModule_LoadStringBuffer(ValkeyModuleIO *io,
                                               size_t *lenptr) {
  return kMockValkeyModule->LoadStringBuffer(io, lenptr);
}

inline ValkeyModuleString *TestValkeyModule_LoadString(ValkeyModuleIO *io) {
  return kMockValkeyModule->LoadString(io);
}

inline void TestValkeyModule_SaveUnsigned(ValkeyModuleIO *io, uint64_t val) {
  return kMockValkeyModule->SaveUnsigned(io, val);
}

inline void TestValkeyModule_SaveSigned(ValkeyModuleIO *io, int64_t val) {
  return kMockValkeyModule->SaveSigned(io, val);
}

inline void TestValkeyModule_SaveDouble(ValkeyModuleIO *io, double val) {
  return kMockValkeyModule->SaveDouble(io, val);
}

inline void TestValkeyModule_SaveStringBuffer(ValkeyModuleIO *io,
                                              const char *str, size_t len) {
  return kMockValkeyModule->SaveStringBuffer(io, str, len);
}

inline int TestValkeyModule_IsIOError(ValkeyModuleIO *io) {
  return kMockValkeyModule->IsIOError(io);
}

inline int TestValkeyModule_ReplicationSetMasterCrossCluster(
    ValkeyModuleCtx *ctx, const char *ip, int port) {
  return kMockValkeyModule->ReplicationSetMasterCrossCluster(ctx, ip, port);
}

inline int TestValkeyModule_ReplicationUnsetMasterCrossCluster(
    ValkeyModuleCtx *ctx) {
  return kMockValkeyModule->ReplicationUnsetMasterCrossCluster(ctx);
}

inline const char *TestValkeyModule_GetMyClusterID() {
  return kMockValkeyModule->GetMyClusterID();
}

inline int TestValkeyModule_GetClusterNodeInfo(ValkeyModuleCtx *ctx,
                                               const char *id, char *ip,
                                               char *master_id, int *port,
                                               int *flags) {
  return kMockValkeyModule->GetClusterNodeInfo(ctx, id, ip, master_id, port,
                                               flags);
}

inline int TestValkeyModule_ReplicationSetSecondaryCluster(
    ValkeyModuleCtx *ctx, bool is_secondary_cluster) {
  return kMockValkeyModule->ReplicationSetSecondaryCluster(
      ctx, is_secondary_cluster);
}

inline ValkeyModuleCrossClusterReplicasList *
TestValkeyModule_GetCrossClusterReplicasList() {
  return kMockValkeyModule->GetCrossClusterReplicasList();
}

inline void TestValkeyModule_FreeCrossClusterReplicasList(
    ValkeyModuleCrossClusterReplicasList *list) {
  return kMockValkeyModule->FreeCrossClusterReplicasList(list);
}

inline void *TestValkeyModule_Alloc(size_t size) {
  return kMockValkeyModule->Alloc(size);
}

inline void TestValkeyModule_Free(void *ptr) {
  return kMockValkeyModule->Free(ptr);
}

inline void *TestValkeyModule_Realloc(void *ptr, size_t size) {
  return kMockValkeyModule->Realloc(ptr, size);
}

inline void *TestValkeyModule_Calloc(size_t nmemb, size_t size) {
  return kMockValkeyModule->Calloc(nmemb, size);
}

inline size_t TestValkeyModule_MallocUsableSize(void *ptr) {
  return kMockValkeyModule->MallocUsableSize(ptr);
}

inline size_t TestValkeyModule_GetClusterSize() {
  return kMockValkeyModule->GetClusterSize();
}
inline void *TestValkeyModule_GetSharedAPI(ValkeyModuleCtx *ctx,
                                           const char *arg1) {
  return kMockValkeyModule->GetSharedAPI(ctx, arg1);
}
inline int TestValkeyModule_WrongArity(ValkeyModuleCtx *ctx) {
  return kMockValkeyModule->WrongArity(ctx);
}

inline int TestValkeyModule_Fork(ValkeyModuleForkDoneHandler cb,
                                 void *user_data) {
  return kMockValkeyModule->Fork(cb, user_data);
}

inline int TestValkeyModule_ExitFromChild(int retcode) {
  return kMockValkeyModule->ExitFromChild(retcode);
}

inline ValkeyModuleRdbStream *TestValkeyModule_RdbStreamCreateFromRioHandler(
    const ValkeyModuleRIOHandler *handler) {
  return kMockValkeyModule->RdbStreamCreateFromRioHandler(handler);
}

inline void TestValkeyModule_RdbStreamFree(ValkeyModuleRdbStream *stream) {
  return kMockValkeyModule->RdbStreamFree(stream);
}

inline int TestValkeyModule_RdbSave(ValkeyModuleCtx *ctx,
                                    ValkeyModuleRdbStream *stream, int flags) {
  return kMockValkeyModule->RdbSave(ctx, stream, flags);
}

inline int TestValkeyModule_RdbLoad(ValkeyModuleCtx *ctx,
                                    ValkeyModuleRdbStream *stream, int flags) {
  return kMockValkeyModule->RdbLoad(ctx, stream, flags);
}

/* The same order as the reply types in valkey_module.h */
using CallReplyString = std::string;
using CallReplyInteger = long long;
using CallReplyArray = std::vector<std::unique_ptr<ValkeyModuleCallReply>>;
using CallReplyNull = void *;
using CallReplyMap =
    std::vector<std::pair<std::unique_ptr<ValkeyModuleCallReply>,
                          std::unique_ptr<ValkeyModuleCallReply>>>;
using CallReplyDouble = double;

using CallReplyVariant =
    std::variant<CallReplyString, CallReplyInteger, CallReplyArray,
                 CallReplyNull, CallReplyMap, CallReplyDouble>;

struct ValkeyModuleCallReply {
  int type = VALKEYMODULE_REPLY_UNKNOWN;
  CallReplyVariant val;
  std::string msg;
};
inline std::unique_ptr<ValkeyModuleCallReply> default_reply VALKEYMODULE_ATTR;

inline std::unique_ptr<ValkeyModuleCallReply> CreateValkeyModuleCallReply(
    CallReplyVariant value) {
  std::unique_ptr<ValkeyModuleCallReply> reply{new ValkeyModuleCallReply{
      .type = std::visit(
          [&](auto &value) {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, CallReplyString>) {
              return VALKEYMODULE_REPLY_STRING;
            } else if constexpr (std::is_same_v<T, CallReplyInteger>) {
              return VALKEYMODULE_REPLY_INTEGER;
            } else if constexpr (std::is_same_v<T, CallReplyArray>) {
              return VALKEYMODULE_REPLY_ARRAY;
            } else if constexpr (std::is_same_v<T, CallReplyNull>) {
              return VALKEYMODULE_REPLY_NULL;
            } else if constexpr (std::is_same_v<T, CallReplyMap>) {
              return VALKEYMODULE_REPLY_MAP;
            } else if constexpr (std::is_same_v<T, CallReplyDouble>) {
              return VALKEYMODULE_REPLY_DOUBLE;
            }
            return VALKEYMODULE_REPLY_UNKNOWN;
          },
          value),
      .val = std::move(value)}};
  return reply;
}

inline void AddElementToCallReplyMap(CallReplyMap &map, CallReplyVariant key,
                                     CallReplyVariant val) {
  std::unique_ptr<ValkeyModuleCallReply> k =
      CreateValkeyModuleCallReply(std::move(key));
  std::unique_ptr<ValkeyModuleCallReply> v =
      CreateValkeyModuleCallReply(std::move(val));
  map.emplace_back(std::pair<std::unique_ptr<ValkeyModuleCallReply>,
                             std::unique_ptr<ValkeyModuleCallReply>>(
      std::move(k), std::move(v)));
}

inline ValkeyModuleCallReply *TestValkeyModule_Call(ValkeyModuleCtx *ctx,
                                                    const char *cmdname,
                                                    const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  std::string format(fmt);
  if (format == "c") {
    const char *arg1 = va_arg(args, const char *);
    auto ret = kMockValkeyModule->Call(ctx, cmdname, fmt, arg1);
    return ret;
  }
  if (format == "cc") {
    const char *arg1 = va_arg(args, const char *);
    const char *arg2 = va_arg(args, const char *);
    auto ret = kMockValkeyModule->Call(ctx, cmdname, fmt, arg1, arg2);
    return ret;
  }
  if (format == "cs3") {
    const char *arg1 = va_arg(args, const char *);
    std::string sub_command(arg1);
    const ValkeyModuleString *arg2 = va_arg(args, ValkeyModuleString *);
    std::string maybe_username(arg2->data);
    if (sub_command == "GETUSER" && maybe_username == "default") {
      CallReplyMap reply_map;
      AddElementToCallReplyMap(reply_map, "commands", "+@all");
      AddElementToCallReplyMap(reply_map, "keys", "~*");
      default_reply = CreateValkeyModuleCallReply(std::move(reply_map));
      return default_reply.get();
    }
    auto ret =
        kMockValkeyModule->Call(ctx, cmdname, fmt, arg1, arg2->data.c_str());
    return ret;
  }
  if (format == "!Kcbb") {
    const char *arg1 = va_arg(args, const char *);
    const char *arg2 = va_arg(args, const char *);
    size_t arg2_len = va_arg(args, size_t);
    const char *arg3 = va_arg(args, const char *);
    size_t arg3_len = va_arg(args, size_t);
    auto ret = kMockValkeyModule->Call(ctx, cmdname, fmt, arg1, arg2, arg2_len,
                                       arg3, arg3_len);
    return ret;
  }
  CHECK(false && "Unsupported format specifier");
  return nullptr;
}

inline ValkeyModuleCallReply *TestValkeyModule_CallReplyArrayElement(
    ValkeyModuleCallReply *reply, size_t idx) {
  return kMockValkeyModule->CallReplyArrayElement(reply, idx);
}
inline ValkeyModuleCallReply *TestValkeyModule_CallReplyArrayElementImpl(
    ValkeyModuleCallReply *reply, size_t idx) {
  if (reply == nullptr || reply->type != VALKEYMODULE_REPLY_ARRAY) {
    return nullptr;
  }
  CHECK(std::holds_alternative<CallReplyArray>(reply->val));
  auto &list = std::get<CallReplyArray>(reply->val);
  if (list.size() <= idx) {
    return nullptr;
  }
  return list[idx].get();
}

inline int TestValkeyModule_CallReplyMapElement(ValkeyModuleCallReply *reply,
                                                size_t idx,
                                                ValkeyModuleCallReply **key,
                                                ValkeyModuleCallReply **val) {
  return kMockValkeyModule->CallReplyMapElement(reply, idx, key, val);
}
inline int TestValkeyModule_CallReplyMapElementImpl(
    ValkeyModuleCallReply *reply, size_t idx, ValkeyModuleCallReply **key,
    ValkeyModuleCallReply **val) {
  if (reply == nullptr || reply->type != VALKEYMODULE_REPLY_MAP) {
    return VALKEYMODULE_ERR;
  }
  CHECK(std::holds_alternative<CallReplyMap>(reply->val));
  auto &map = std::get<CallReplyMap>(reply->val);
  if (map.size() <= idx) {
    return VALKEYMODULE_ERR;
  }

  if (key != nullptr) {
    *key = map[idx].first.get();
  }
  if (val != nullptr) {
    *val = map[idx].second.get();
  }
  return VALKEYMODULE_OK;
}

inline const char *TestValkeyModule_CallReplyStringPtr(
    ValkeyModuleCallReply *reply, size_t *len) {
  return kMockValkeyModule->CallReplyStringPtr(reply, len);
}
inline const char *TestValkeyModule_CallReplyStringPtrImpl(
    ValkeyModuleCallReply *reply, size_t *len) {
  if (reply == nullptr || reply->type != VALKEYMODULE_REPLY_STRING) {
    return nullptr;
  }
  CHECK(std::holds_alternative<std::string>(reply->val));
  auto &s = std::get<std::string>(reply->val);
  *len = s.size();
  return s.c_str();
}

inline void TestValkeyModule_FreeCallReply(ValkeyModuleCallReply *reply) {
  return kMockValkeyModule->FreeCallReply(reply);
}

inline void TestValkeyModule_RegisterClusterMessageReceiver(
    ValkeyModuleCtx *ctx, uint8_t type,
    ValkeyModuleClusterMessageReceiver callback) {
  return kMockValkeyModule->RegisterClusterMessageReceiver(ctx, type, callback);
}

inline int TestValkeyModule_SendClusterMessage(ValkeyModuleCtx *ctx,
                                               const char *target_id,
                                               uint8_t type, const char *msg,
                                               uint32_t len) {
  return kMockValkeyModule->SendClusterMessage(ctx, target_id, type, msg, len);
}

inline char **TestValkeyModule_GetClusterNodesList(ValkeyModuleCtx *ctx,
                                                   size_t *numnodes) {
  return kMockValkeyModule->GetClusterNodesList(ctx, numnodes);
}

inline ValkeyModuleCtx *TestValkeyModule_GetContextFromIO(ValkeyModuleIO *rdb) {
  return kMockValkeyModule->GetContextFromIO(rdb);
}

inline int TestValkeyModule_GetDbIdFromIO(ValkeyModuleIO *rdb) {
  return kMockValkeyModule->GetDbIdFromIO(rdb);
}

inline void TestValkeyModule_FreeClusterNodesList(char **ids) {
  return kMockValkeyModule->FreeClusterNodesList(ids);
}

inline int TestValkeyModule_CallReplyType(ValkeyModuleCallReply *reply) {
  return kMockValkeyModule->CallReplyType(reply);
}

inline size_t TestValkeyModule_CallReplyLength(ValkeyModuleCallReply *reply) {
  return kMockValkeyModule->CallReplyLength(reply);
}

inline int TestValkeyModule_CallReplyTypeImpl(ValkeyModuleCallReply *reply) {
  return reply->type;
}

inline long long TestValkeyModule_CallReplyInteger(
    ValkeyModuleCallReply *reply) {
  return kMockValkeyModule->CallReplyInteger(reply);
}

inline long long TestValkeyModule_CallReplyIntegerImpl(
    ValkeyModuleCallReply *reply) {
  if (reply == nullptr || reply->type != VALKEYMODULE_REPLY_INTEGER) {
    return 0;
  }
  CHECK(std::holds_alternative<CallReplyInteger>(reply->val));
  return std::get<CallReplyInteger>(reply->val);
}

inline ValkeyModuleString *TestValkeyModule_CreateStringFromCallReply(
    ValkeyModuleCallReply *reply) {
  return kMockValkeyModule->CreateStringFromCallReply(reply);
}

inline ValkeyModuleString *TestValkeyModule_GetCurrentUserName(
    ValkeyModuleCtx *ctx) {
  return kMockValkeyModule->GetCurrentUserName(ctx);
}

inline ValkeyModuleString *TestValkeyModule_GetCurrentUserNameImpl(
    ValkeyModuleCtx *ctx) {
  return new ValkeyModuleString{std::string("default")};
}

inline long long TestValkeyModule_Milliseconds() {
  return kMockValkeyModule->Milliseconds();
}

// TestValkeyModule_Init initializes the module API function table with mock
// implementations of functions to prevent segmentation faults when
// executing tests and to allow validation of Valkey module API calls.
inline void TestValkeyModule_Init() {
  ValkeyModule_Log = &TestValkeyModule_Log;
  ValkeyModule_LogIOError = &TestValkeyModule_LogIOError;
  ValkeyModule_BlockClientOnAuth = &TestValkeyModule_BlockClientOnAuth;
  ValkeyModule_BlockedClientMeasureTimeEnd =
      &TestValkeyModule_BlockedClientMeasureTimeEnd;
  ValkeyModule_BlockedClientMeasureTimeStart =
      &TestValkeyModule_BlockedClientMeasureTimeStart;
  ValkeyModule_BlockClient = &TestValkeyModule_BlockClient;
  ValkeyModule_UnblockClient = &TestValkeyModule_UnblockClient;
  ValkeyModule_StringPtrLen = &TestValkeyModule_StringPtrLen;
  ValkeyModule_CreateStringPrintf = &TestValkeyModule_CreateStringPrintf;
  ValkeyModule_GetBlockedClientPrivateData =
      &TestValkeyModule_GetBlockedClientPrivateData;
  ValkeyModule_AuthenticateClientWithACLUser =
      &TestValkeyModule_AuthenticateClientWithACLUser;
  ValkeyModule_ACLAddLogEntryByUserName =
      &TestValkeyModule_ACLAddLogEntryByUserName;
  ValkeyModule_CreateString = &TestValkeyModule_CreateString;
  ValkeyModule_FreeString = &TestValkeyModule_FreeString;
  ValkeyModule_RetainString = &TestValkeyModule_RetainString;
  ValkeyModule_EventLoopAdd = &TestValkeyModule_EventLoopAdd;
  ValkeyModule_EventLoopAddOneShot = &TestValkeyModule_EventLoopAddOneShot;
  ValkeyModule_EventLoopDel = &TestValkeyModule_EventLoopDel;
  ValkeyModule_CreateTimer = &TestValkeyModule_CreateTimer;
  ValkeyModule_StopTimer = &TestValkeyModule_StopTimer;
  ValkeyModule_SetModuleOptions = &TestValkeyModule_SetModuleOptions;
  ValkeyModule_GetClientId = &TestValkeyModule_GetClientId;
  ValkeyModule_GetClientInfoById = &TestValkeyModule_GetClientInfoById;
  ValkeyModule_SubscribeToKeyspaceEvents =
      &TestValkeyModule_SubscribeToKeyspaceEvents;
  ValkeyModule_KeyExists = &TestValkeyModule_KeyExists;
  ValkeyModule_OpenKey = &TestValkeyModule_OpenKey;
  ValkeyModule_HashExternalize = &TestValkeyModule_HashExternalize;
  ValkeyModule_GetApi = &TestValkeyModule_GetApi;
  ValkeyModule_GetExpire = &TestValkeyModule_GetExpire;
  ValkeyModule_HashGet = &TestValkeyModule_HashGet;
  ValkeyModule_HashSet = &TestValkeyModule_HashSet;
  ValkeyModule_ScanKey = &TestValkeyModule_ScanKey;
  ValkeyModule_ScanCursorCreate = &TestValkeyModule_ScanCursorCreate;
  ValkeyModule_ScanCursorDestroy = &TestValkeyModule_ScanCursorDestroy;
  ValkeyModule_CloseKey = &TestValkeyModule_CloseKey;
  ValkeyModule_CreateCommand = &TestValkeyModule_CreateCommand;
  ValkeyModule_KeyType = &TestValkeyModule_KeyType;
  ValkeyModule_ModuleTypeSetValue = &TestValkeyModule_ModuleTypeSetValue;
  ValkeyModule_DeleteKey = &TestValkeyModule_DeleteKey;
  ValkeyModule_RegisterInfoFunc = &TestValkeyModule_RegisterInfoFunc;
  ValkeyModule_ReplyWithArray = &TestValkeyModule_ReplyWithArray;
  ValkeyModule_ReplySetArrayLength = &TestValkeyModule_ReplySetArrayLength;
  ValkeyModule_ReplyWithLongLong = &TestValkeyModule_ReplyWithLongLong;
  ValkeyModule_ReplyWithSimpleString = &TestValkeyModule_ReplyWithSimpleString;
  ValkeyModule_ReplyWithString = &TestValkeyModule_ReplyWithString;
  ValkeyModule_ReplyWithDouble = &TestValkeyModule_ReplyWithDouble;
  ValkeyModule_ReplyWithCString = &TestValkeyModule_ReplyWithCString;
  ValkeyModule_ReplyWithStringBuffer = &TestValkeyModule_ReplyWithStringBuffer;
  ValkeyModule_CreateDataType = &TestValkeyModule_CreateDataType;
  ValkeyModule_ReplyWithError = &TestValkeyModule_ReplyWithError;
  ValkeyModule_SubscribeToServerEvent =
      &TestValkeyModule_SubscribeToServerEvent;
  ValkeyModule_Scan = &TestValkeyModule_Scan;
  ValkeyModule_ReplicateVerbatim = &TestValkeyModule_ReplicateVerbatim;
  ValkeyModule_Replicate = &TestValkeyModule_Replicate;
  ValkeyModule_ModuleTypeGetType = &TestValkeyModule_ModuleTypeGetType;
  ValkeyModule_GetDetachedThreadSafeContext =
      &TestValkeyModule_GetDetachedThreadSafeContext;
  ValkeyModule_GetThreadSafeContext = &TestValkeyModule_GetThreadSafeContext;
  ValkeyModule_FreeThreadSafeContext = &TestValkeyModule_FreeThreadSafeContext;
  ValkeyModule_SelectDb = &TestValkeyModule_SelectDb;
  ValkeyModule_GetSelectedDb = &TestValkeyModule_GetSelectedDb;
  ValkeyModule_ModuleTypeGetValue = &TestValkeyModule_ModuleTypeGetValue;
  ValkeyModule_DbSize = &TestValkeyModule_DbSize;
  ValkeyModule_InfoAddSection = &TestValkeyModule_InfoAddSection;
  ValkeyModule_InfoAddFieldLongLong = &TestValkeyModule_InfoAddFieldLongLong;
  ValkeyModule_InfoAddFieldCString = &TestValkeyModule_InfoAddFieldCString;
  ValkeyModule_InfoAddFieldDouble = &TestValkeyModule_InfoAddFieldDouble;
  ValkeyModule_InfoBeginDictField = &TestValkeyModule_InfoBeginDictField;
  ValkeyModule_InfoEndDictField = &TestValkeyModule_InfoEndDictField;
  ValkeyModule_RegisterStringConfig = &TestValkeyModule_RegisterStringConfig;
  ValkeyModule_RegisterEnumConfig = &TestValkeyModule_RegisterEnumConfig;
  ValkeyModule_LoadConfigs = &TestValkeyModule_LoadConfigs;
  ValkeyModule_SetConnectionProperties =
      &TestValkeyModule_SetConnectionProperties;
  ValkeyModule_SetShardId = &TestValkeyModule_SetShardId;
  ValkeyModule_GetClusterInfo = &TestValkeyModule_GetClusterInfo;
  ValkeyModule_GetMyShardID = &TestValkeyModule_GetMyShardID;
  ValkeyModule_GetContextFlags = &TestValkeyModule_GetContextFlags;
  ValkeyModule_LoadUnsigned = &TestValkeyModule_LoadUnsigned;
  ValkeyModule_LoadSigned = &TestValkeyModule_LoadSigned;
  ValkeyModule_LoadDouble = &TestValkeyModule_LoadDouble;
  ValkeyModule_LoadStringBuffer = &TestValkeyModule_LoadStringBuffer;
  ValkeyModule_LoadString = &TestValkeyModule_LoadString;
  ValkeyModule_SaveUnsigned = &TestValkeyModule_SaveUnsigned;
  ValkeyModule_SaveSigned = &TestValkeyModule_SaveSigned;
  ValkeyModule_SaveDouble = &TestValkeyModule_SaveDouble;
  ValkeyModule_SaveStringBuffer = &TestValkeyModule_SaveStringBuffer;
  ValkeyModule_IsIOError = &TestValkeyModule_IsIOError;
  ValkeyModule_ReplicationSetMasterCrossCluster =
      &TestValkeyModule_ReplicationSetMasterCrossCluster;
  ValkeyModule_ReplicationUnsetMasterCrossCluster =
      &TestValkeyModule_ReplicationUnsetMasterCrossCluster;
  ValkeyModule_GetMyClusterID = &TestValkeyModule_GetMyClusterID;
  ValkeyModule_GetClusterNodeInfo = &TestValkeyModule_GetClusterNodeInfo;
  ValkeyModule_ReplicationSetSecondaryCluster =
      &TestValkeyModule_ReplicationSetSecondaryCluster;
  ValkeyModule_GetCrossClusterReplicasList =
      &TestValkeyModule_GetCrossClusterReplicasList;
  ValkeyModule_FreeCrossClusterReplicasList =
      &TestValkeyModule_FreeCrossClusterReplicasList;
  ValkeyModule_Alloc = &TestValkeyModule_Alloc;
  ValkeyModule_Free = &TestValkeyModule_Free;
  ValkeyModule_Realloc = &TestValkeyModule_Realloc;
  ValkeyModule_Calloc = &TestValkeyModule_Calloc;
  ValkeyModule_MallocUsableSize = &TestValkeyModule_MallocUsableSize;
  ValkeyModule_GetClusterSize = &TestValkeyModule_GetClusterSize;
  ValkeyModule_GetSharedAPI = &TestValkeyModule_GetSharedAPI;
  ValkeyModule_Call = &TestValkeyModule_Call;
  ValkeyModule_CallReplyArrayElement = &TestValkeyModule_CallReplyArrayElement;
  ValkeyModule_CallReplyMapElement = &TestValkeyModule_CallReplyMapElement;
  ValkeyModule_CallReplyStringPtr = &TestValkeyModule_CallReplyStringPtr;
  ValkeyModule_FreeCallReply = &TestValkeyModule_FreeCallReply;
  ValkeyModule_RegisterClusterMessageReceiver =
      &TestValkeyModule_RegisterClusterMessageReceiver;
  ValkeyModule_SendClusterMessage = &TestValkeyModule_SendClusterMessage;
  ValkeyModule_GetClusterNodesList = &TestValkeyModule_GetClusterNodesList;
  ValkeyModule_GetContextFromIO = &TestValkeyModule_GetContextFromIO;
  ValkeyModule_GetDbIdFromIO = &TestValkeyModule_GetDbIdFromIO;
  ValkeyModule_FreeClusterNodesList = &TestValkeyModule_FreeClusterNodesList;
  ValkeyModule_CallReplyType = &TestValkeyModule_CallReplyType;
  ValkeyModule_CallReplyLength = &TestValkeyModule_CallReplyLength;
  ValkeyModule_CallReplyInteger = &TestValkeyModule_CallReplyInteger;
  ValkeyModule_CreateStringFromCallReply =
      &TestValkeyModule_CreateStringFromCallReply;
  ValkeyModule_WrongArity = &TestValkeyModule_WrongArity;
  ValkeyModule_Fork = &TestValkeyModule_Fork;
  ValkeyModule_ExitFromChild = &TestValkeyModule_ExitFromChild;
  ValkeyModule_RdbStreamCreateFromRioHandler =
      &TestValkeyModule_RdbStreamCreateFromRioHandler;
  ValkeyModule_RdbStreamFree = &TestValkeyModule_RdbStreamFree;
  ValkeyModule_RdbSave = &TestValkeyModule_RdbSave;
  ValkeyModule_RdbLoad = &TestValkeyModule_RdbLoad;
  ValkeyModule_GetCurrentUserName = &TestValkeyModule_GetCurrentUserName;
  ValkeyModule_RegisterNumericConfig = &TestValkeyModule_RegisterNumericConfig;
  ValkeyModule_RegisterBoolConfig = &TestValkeyModule_RegisterBoolConfig;
  ValkeyModule_Milliseconds = &TestValkeyModule_Milliseconds;

  kMockValkeyModule = new testing::NiceMock<MockValkeyModule>();

  // Implement basic key registration functions with simple implementations by
  // default.
  ON_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
      .WillByDefault(TestValkeyModule_OpenKeyDefaultImpl);
  ON_CALL(*kMockValkeyModule, CloseKey(testing::_))
      .WillByDefault(TestValkeyModule_CloseKeyDefaultImpl);
  ON_CALL(*kMockValkeyModule,
          ModuleTypeSetValue(testing::_, testing::_, testing::_))
      .WillByDefault(TestValkeyModule_ModuleTypeSetValueDefaultImpl);
  ON_CALL(*kMockValkeyModule, ModuleTypeGetValue(testing::_))
      .WillByDefault(TestValkeyModule_ModuleTypeGetValueDefaultImpl);
  ON_CALL(*kMockValkeyModule, ModuleTypeGetType(testing::_))
      .WillByDefault(TestValkeyModule_ModuleTypeGetTypeDefaultImpl);
  ON_CALL(*kMockValkeyModule, KeyType(testing::_))
      .WillByDefault(TestValkeyModule_KeyTypeDefaultImpl);
  ON_CALL(*kMockValkeyModule, DeleteKey(testing::_))
      .WillByDefault(TestValkeyModule_DeleteKeyDefaultImpl);
  ON_CALL(*kMockValkeyModule, KeyExists(testing::_, testing::_))
      .WillByDefault(TestValkeyModule_KeyExistsDefaultImpl);
  ON_CALL(*kMockValkeyModule,
          HashExternalize(testing::_, testing::_, testing::_, testing::_))
      .WillByDefault(TestValkeyModule_HashExternalizeDefaultImpl);
  ON_CALL(*kMockValkeyModule, GetApi(testing::_, testing::_))
      .WillByDefault(TestValkeyModule_GetApiDefaultImpl);

  ON_CALL(*kMockValkeyModule, GetCurrentUserName(testing::_))
      .WillByDefault(TestValkeyModule_GetCurrentUserNameImpl);

  ON_CALL(*kMockValkeyModule, CallReplyType(testing::_))
      .WillByDefault(TestValkeyModule_CallReplyTypeImpl);
  ON_CALL(*kMockValkeyModule, CallReplyStringPtr(testing::_, testing::_))
      .WillByDefault(TestValkeyModule_CallReplyStringPtrImpl);
  ON_CALL(*kMockValkeyModule, CallReplyArrayElement(testing::_, testing::_))
      .WillByDefault(TestValkeyModule_CallReplyArrayElementImpl);
  ON_CALL(*kMockValkeyModule,
          CallReplyMapElement(testing::_, testing::_, testing::_, testing::_))
      .WillByDefault(TestValkeyModule_CallReplyMapElementImpl);
  ON_CALL(*kMockValkeyModule, CallReplyInteger(testing::_))
      .WillByDefault(TestValkeyModule_CallReplyIntegerImpl);
  ON_CALL(*kMockValkeyModule,
          Call(testing::_, testing::_, testing::_, testing::_, testing::_,
               testing::_, testing::_, testing::_))
      .WillByDefault([](ValkeyModuleCtx *ctx, const char *cmd, const char *fmt,
                        const char *arg1, const char *arg2, size_t arg2_len,
                        const char *arg3,
                        size_t arg3_len) -> ValkeyModuleCallReply * {
        static auto ok_reply = CreateValkeyModuleCallReply(std::string("OK"));
        return ok_reply.get();
      });
  ON_CALL(*kMockValkeyModule, Milliseconds()).WillByDefault([]() -> long long {
    static long long fake_time = 0;
    return ++fake_time;
  });
  static absl::once_flag flag;
  absl::call_once(flag, []() { vmsdk::TrackCurrentAsMainThread(); });
  CHECK(vmsdk::InitLogging(nullptr, "debug").ok());
}

inline void TestValkeyModule_Teardown() {
  delete kMockValkeyModule;
  // Explicitly delete any remaining reference pointer from `default_reply`
  default_reply.reset();
}

// NOLINTEND(readability-identifier-naming)
#endif  // VMSDK_SRC_TESTING_INFRA_MODULE
