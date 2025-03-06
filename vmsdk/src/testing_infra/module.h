/*
 * Copyright (c) 2025, ValkeySearch contributors
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
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

class MockRedisModule {
 public:
  MockRedisModule() {
    ON_CALL(*this, EventLoopAddOneShot)
        .WillByDefault(
            [](RedisModuleEventLoopOneShotFunc callback, void *data) -> int {
              if (callback) {
                callback(data);
              }
              return 0;
            });
  };
  MOCK_METHOD(RedisModuleBlockedClient *, BlockClientOnAuth,
              (RedisModuleCtx * ctx, RedisModuleAuthCallback reply_callback,
               void (*free_privdata)(RedisModuleCtx *, void *)));
  MOCK_METHOD(RedisModuleBlockedClient *, BlockClient,
              (RedisModuleCtx * ctx, RedisModuleCmdFunc reply_callback,
               RedisModuleCmdFunc timeout_callback,
               void (*free_privdata)(RedisModuleCtx *, void *),
               long long timeout_ms));  // NOLINT
  MOCK_METHOD(void, Log,
              (RedisModuleCtx * ctx, const char *levelstr, const char *msg));
  MOCK_METHOD(void, LogIOError,
              (RedisModuleIO * io, const char *levelstr, const char *msg));
  MOCK_METHOD(int, UnblockClient,
              (RedisModuleBlockedClient * bc, void *privdata));
  MOCK_METHOD(void *, GetBlockedClientPrivateData, (RedisModuleCtx * ctx));
  MOCK_METHOD(int, AuthenticateClientWithACLUser,
              (RedisModuleCtx * ctx, const char *name, size_t len,
               RedisModuleUserChangedFunc callback, void *privdata,
               uint64_t *client_id));
  MOCK_METHOD(void, ACLAddLogEntryByUserName,
              (RedisModuleCtx * ctx, RedisModuleString *user,
               RedisModuleString *object, RedisModuleACLLogEntryReason reason));
  MOCK_METHOD(int, EventLoopAdd,
              (int fd, int mask, RedisModuleEventLoopFunc func,
               void *user_data));
  MOCK_METHOD(int, EventLoopAddOneShot,
              (RedisModuleEventLoopOneShotFunc func, void *user_data));
  MOCK_METHOD(int, EventLoopDel, (int fd, int mask));
  MOCK_METHOD(RedisModuleTimerID, CreateTimer,
              (RedisModuleCtx * ctx, mstime_t period,
               RedisModuleTimerProc callback, void *data));
  MOCK_METHOD(int, StopTimer,
              (RedisModuleCtx * ctx, RedisModuleTimerID id, void **data));
  MOCK_METHOD(void, SetModuleOptions, (RedisModuleCtx * ctx, int options));
  MOCK_METHOD(unsigned long long, GetClientId,  // NOLINT
              (RedisModuleCtx *ctx));
  MOCK_METHOD(int, GetClientInfoById, (void *ci, uint64_t id));
  MOCK_METHOD(int, SubscribeToKeyspaceEvents,
              (RedisModuleCtx * ctx, int types,
               RedisModuleNotificationFunc cb));
  MOCK_METHOD(int, KeyExists, (RedisModuleCtx * ctx, RedisModuleString *key));
  MOCK_METHOD(RedisModuleKey *, OpenKey,
              (RedisModuleCtx * ctx, RedisModuleString *key, int flags));
  MOCK_METHOD(int, HashExternalize,
              (RedisModuleKey * key, RedisModuleString *field,
               RedisModuleHashExternCB fn, void *privdata));
  MOCK_METHOD(int, GetApi, (const char *name, void *func));
  MOCK_METHOD(int, HashGet,
              (RedisModuleKey * key, int flags, const char *field,
               int *exists_out, void *terminating_null));
  MOCK_METHOD(int, HashGet,
              (RedisModuleKey * key, int flags, const char *field,
               RedisModuleString **value_out, void *terminating_null));
  MOCK_METHOD(int, HashSet,
              (RedisModuleKey * key, int flags, RedisModuleString *field,
               RedisModuleString *value_out, void *terminating_null));
  MOCK_METHOD(void, CloseKey, (RedisModuleKey * key));
  MOCK_METHOD(int, CreateCommand,
              (RedisModuleCtx * ctx, const char *name,
               RedisModuleCmdFunc cmdfunc, const char *strflags, int firstkey,
               int lastkey, int keystep));
  MOCK_METHOD(int, KeyType, (RedisModuleKey * key));
  MOCK_METHOD(int, ModuleTypeSetValue,
              (RedisModuleKey * key, RedisModuleType *mt, void *value));
  MOCK_METHOD(int, DeleteKey, (RedisModuleKey * key));
  MOCK_METHOD(int, RegisterInfoFunc,
              (RedisModuleCtx * ctx, RedisModuleInfoFunc cb));
  MOCK_METHOD(int, Init,
              (RedisModuleCtx * ctx, const char *name, int ver, int apiver));
  MOCK_METHOD(int, ReplyWithArray, (RedisModuleCtx * ctx, long len));  // NOLINT
  MOCK_METHOD(void, ReplySetArrayLength,
              (RedisModuleCtx * ctx, long len));  // NOLINT
  MOCK_METHOD(int, ReplyWithLongLong,
              (RedisModuleCtx * ctx, long long ll));  // NOLINT
  MOCK_METHOD(int, ReplyWithSimpleString,
              (RedisModuleCtx * ctx, const char *str));
  MOCK_METHOD(int, ReplyWithString,
              (RedisModuleCtx * ctx, RedisModuleString *str));
  MOCK_METHOD(int, ReplyWithDouble, (RedisModuleCtx * ctx, double val));
  MOCK_METHOD(int, ReplyWithCString, (RedisModuleCtx * ctx, const char *str));
  MOCK_METHOD(int, ReplyWithStringBuffer,
              (RedisModuleCtx * ctx, const char *buf, size_t len));
  MOCK_METHOD(int, FreeString, (RedisModuleCtx * ctx, RedisModuleString *str));
  MOCK_METHOD(RedisModuleType *, CreateDataType,
              (RedisModuleCtx * ctx, const char *name, int encver,
               RedisModuleTypeMethods *typemethods));
  MOCK_METHOD(int, ReplyWithError, (RedisModuleCtx * ctx, const char *err));
  MOCK_METHOD(int, ScanKey,
              (RedisModuleKey * key, RedisModuleScanCursor *cursor,
               RedisModuleScanKeyCB fn, void *privdata));
  MOCK_METHOD(RedisModuleScanCursor *, ScanCursorCreate, ());
  MOCK_METHOD(void, ScanCursorDestroy, (RedisModuleScanCursor * cursor));
  MOCK_METHOD(int, SubscribeToServerEvent,
              (RedisModuleCtx * ctx, RedisModuleEvent event,
               RedisModuleEventCallback cb));
  MOCK_METHOD(int, Scan,
              (RedisModuleCtx * ctx, RedisModuleScanCursor *cursor,
               RedisModuleScanCB fn, void *privdata));
  MOCK_METHOD(int, ReplicateVerbatim, (RedisModuleCtx * ctx));
  MOCK_METHOD(RedisModuleType *, ModuleTypeGetType, (RedisModuleKey * key));
  MOCK_METHOD(RedisModuleCtx *, GetDetachedThreadSafeContext,
              (RedisModuleCtx * ctx));
  MOCK_METHOD(void, FreeThreadSafeContext, (RedisModuleCtx * ctx));
  MOCK_METHOD(int, SelectDb, (RedisModuleCtx * ctx, int newid));
  MOCK_METHOD(int, GetSelectedDb, (RedisModuleCtx * ctx));
  MOCK_METHOD(void *, ModuleTypeGetValue, (RedisModuleKey * key));
  MOCK_METHOD(unsigned long long, DbSize, (RedisModuleCtx * ctx));  // NOLINT
  MOCK_METHOD(int, InfoAddSection, (RedisModuleInfoCtx * ctx, const char *str));
  MOCK_METHOD(int, InfoBeginDictField,
              (RedisModuleInfoCtx * ctx, const char *str));
  MOCK_METHOD(int, InfoEndDictField, (RedisModuleInfoCtx * ctx));
  MOCK_METHOD(int, InfoAddFieldLongLong,
              (RedisModuleInfoCtx * ctx, const char *str,
               long long field));  // NOLINT
  MOCK_METHOD(int, InfoAddFieldCString,
              (RedisModuleInfoCtx * ctx, const char *str, const char *field));
  MOCK_METHOD(int, RegisterStringConfig,
              (RedisModuleCtx * ctx, const char *name, const char *default_val,
               unsigned int flags, RedisModuleConfigGetStringFunc getfn,
               RedisModuleConfigSetStringFunc setfn,
               RedisModuleConfigApplyFunc applyfn, void *privdata));
  MOCK_METHOD(int, RegisterEnumConfig,
              (RedisModuleCtx * ctx, const char *name, int default_val,
               unsigned int flags, const char **enum_values,
               const int *int_values, int num_enum_vals,
               RedisModuleConfigGetEnumFunc getfn,
               RedisModuleConfigSetEnumFunc setfn,
               RedisModuleConfigApplyFunc applyfn, void *privdata));
  MOCK_METHOD(int, LoadConfigs, (RedisModuleCtx * ctx));
  MOCK_METHOD(int, SetConnectionProperties,
              (const RedisModuleConnectionProperty *properties, int length));
  MOCK_METHOD(int, SetShardId, (const char *shard_id, int len));
  MOCK_METHOD(int, GetClusterInfo, (void *cli));
  MOCK_METHOD(const char *, GetMyShardID, ());
  MOCK_METHOD(int, GetContextFlags, (RedisModuleCtx * ctx));
  MOCK_METHOD(uint64_t, LoadUnsigned, (RedisModuleIO * io));
  MOCK_METHOD(int64_t, LoadSigned, (RedisModuleIO * io));
  MOCK_METHOD(double, LoadDouble, (RedisModuleIO * io));
  MOCK_METHOD(char *, LoadStringBuffer, (RedisModuleIO * io, size_t *lenptr));
  MOCK_METHOD(RedisModuleString *, LoadString, (RedisModuleIO * io));
  MOCK_METHOD(void, SaveUnsigned, (RedisModuleIO * io, uint64_t val));
  MOCK_METHOD(void, SaveSigned, (RedisModuleIO * io, int64_t val));
  MOCK_METHOD(void, SaveDouble, (RedisModuleIO * io, double val));
  MOCK_METHOD(void, SaveStringBuffer,
              (RedisModuleIO * io, const char *str, size_t len));
  MOCK_METHOD(int, IsIOError, (RedisModuleIO * io));
  MOCK_METHOD(int, ReplicationSetMasterCrossCluster,
              (RedisModuleCtx * ctx, const char *ip, const int port));
  MOCK_METHOD(int, ReplicationUnsetMasterCrossCluster, (RedisModuleCtx * ctx));
  MOCK_METHOD(const char *, GetMyClusterID, ());
  MOCK_METHOD(int, GetClusterNodeInfo,
              (RedisModuleCtx * ctx, const char *id, char *ip, char *master_id,
               int *port, int *flags));
  MOCK_METHOD(int, ReplicationSetSecondaryCluster,
              (RedisModuleCtx * ctx, bool is_secondary_cluster));
  MOCK_METHOD(RedisModuleCrossClusterReplicasList *,
              GetCrossClusterReplicasList, ());
  MOCK_METHOD(void, FreeCrossClusterReplicasList,
              (RedisModuleCrossClusterReplicasList * list));
  MOCK_METHOD(void *, Alloc, (size_t size));
  MOCK_METHOD(void, Free, (void *ptr));
  MOCK_METHOD(void *, Realloc, (void *ptr, size_t size));
  MOCK_METHOD(void *, Calloc, (size_t nmemb, size_t size));
  MOCK_METHOD(size_t, MallocUsableSize, (void *ptr));
  MOCK_METHOD(size_t, GetClusterSize, ());
  MOCK_METHOD(RedisModuleCallReply *, Call,
              (RedisModuleCtx * ctx, const char *cmd, const char *fmt,
               const char *arg1, const char *arg2));
  MOCK_METHOD(RedisModuleCallReply *, CallReplyArrayElement,
              (RedisModuleCallReply * reply, int index));
  MOCK_METHOD(const char *, CallReplyStringPtr,
              (RedisModuleCallReply * reply, size_t *len));
  MOCK_METHOD(void, FreeCallReply, (RedisModuleCallReply * reply));
  MOCK_METHOD(void, RegisterClusterMessageReceiver,
              (RedisModuleCtx * ctx, uint8_t type,
               RedisModuleClusterMessageReceiver callback));
  MOCK_METHOD(int, SendClusterMessage,
              (RedisModuleCtx * ctx, const char *target_id, uint8_t type,
               const char *msg, uint32_t len));
  MOCK_METHOD(char **, GetClusterNodesList,
              (RedisModuleCtx * ctx, size_t *numnodes));
  MOCK_METHOD(RedisModuleCtx *, GetContextFromIO, (RedisModuleIO * rdb));
  MOCK_METHOD(void, FreeClusterNodesList, (char **ids));
  MOCK_METHOD(int, CallReplyType, (RedisModuleCallReply * reply));
  MOCK_METHOD(RedisModuleString *, CreateStringFromCallReply,
              (RedisModuleCallReply * reply));
  MOCK_METHOD(int, WrongArity, (RedisModuleCtx * ctx));
  MOCK_METHOD(int, Fork, (RedisModuleForkDoneHandler cb, void *user_data));
  MOCK_METHOD(int, ExitFromChild, (int retcode));
  MOCK_METHOD(RedisModuleRdbStream *, RdbStreamCreateFromRioHandler,
              (const RedisModuleRIOHandler *handler));
  MOCK_METHOD(int, RdbLoad,
              (RedisModuleCtx * ctx, RedisModuleRdbStream *stream, int flags));
  MOCK_METHOD(int, RdbSave,
              (RedisModuleCtx * ctx, RedisModuleRdbStream *stream, int flags));
  MOCK_METHOD(void, RdbStreamFree, (RedisModuleRdbStream * stream));
  MOCK_METHOD(RedisModuleString *, GetCurrentUserName, (RedisModuleCtx * ctx));
};

// Global kMockRedisModule is a fake Redis module used for static wrappers
// around MockRedisModule methods.
MockRedisModule *kMockRedisModule;  // NOLINT

inline void TestRedisModule_Log(RedisModuleCtx *ctx [[maybe_unused]],
                                const char *levelstr [[maybe_unused]],
                                const char *fmt [[maybe_unused]], ...) {
  char out[2048];
  va_list args;
  va_start(args, fmt);
  vsnprintf(out, sizeof(out), fmt, args);
  va_end(args);
  printf("TestRedis[%s]: %s\n", levelstr, out);
  kMockRedisModule->Log(ctx, levelstr, out);
}

inline void TestRedisModule_LogIOError(RedisModuleIO *io [[maybe_unused]],
                                       const char *levelstr [[maybe_unused]],
                                       const char *fmt [[maybe_unused]], ...) {
  char out[2048];
  va_list args;
  va_start(args, fmt);
  vsnprintf(out, sizeof(out), fmt, args);
  va_end(args);
  printf("TestRedis[%s]: %s\n", levelstr, out);
  kMockRedisModule->LogIOError(io, levelstr, out);
}

inline int TestRedisModule_BlockedClientMeasureTimeStart(
    RedisModuleBlockedClient *bc [[maybe_unused]]) {
  return 0;
}

inline int TestRedisModule_BlockedClientMeasureTimeEnd(
    RedisModuleBlockedClient *bc [[maybe_unused]]) {
  return 0;
}

inline int TestRedisModule_UnblockClient(RedisModuleBlockedClient *bc,
                                         void *privdata) {
  return kMockRedisModule->UnblockClient(bc, privdata);
}

struct RedisModuleBlockedClient {};

// Simple test implementation of RedisModuleString
struct RedisModuleString {
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
    std::optional<ReplyArray> array_value;
    std::optional<double> double_value;
  };
  void ReplyWithArray(long len) {  // NOLINT
    auto result = AllocateElement();
    result->array_value = ReplyArray{.target_length = len};
    if (len == REDISMODULE_POSTPONED_ARRAY_LEN) {
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
  void ReplyWithString(RedisModuleString *str) {
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
    result->string_value = std::string(str);
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
    if (array.target_length == REDISMODULE_POSTPONED_ARRAY_LEN ||
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
  RedisModuleType *module_type;
  bool operator==(const RegisteredKey &other) const {
    return key == other.key && data == other.data &&
           module_type == other.module_type;
  }
};

struct RedisModuleCtx {
  ReplyCapture reply_capture;
  absl::flat_hash_map<std::string, RegisteredKey> registered_keys;
};

struct RedisModuleIO {};

class InfoCapture {
 public:
  void InfoAddSection(const char *str) { info_ << str << std::endl; }
  void InfoAddFieldLongLong(const char *str, long long field,  // NOLINT
                            int in_dict_field) {
    if (in_dict_field)
      info_ << str << "=" << field << ",";
    else
      info_ << str << ": " << field << std::endl;
  }
  void InfoAddFieldCString(const char *str, const char *field,
                           int in_dict_field) {
    if (in_dict_field)
      info_ << str << "=" << field << ",";
    else
      info_ << str << ": '" << field << "'" << std::endl;
  }
  void InfoEndDictField() {
    if (!info_.str().empty() && info_.str().back() == ',') {
      info_.seekp(-1, std::ios_base::end);
    }
    info_ << std::endl;
  }
  void InfoBeginDictField(const char *str, int in_dict_field) {
    if (in_dict_field) InfoEndDictField();
    info_ << str << ":";
  }
  std::string GetInfo() const { return info_.str(); }

 private:
  std::stringstream info_;
};

struct RedisModuleInfoCtx {
  InfoCapture info_capture;
  int in_dict_field = 0;
};

struct RedisModuleKey {
  RedisModuleCtx *ctx;
  std::string key;
};

inline const char *TestRedisModule_StringPtrLen(const RedisModuleString *str,
                                                size_t *len) {
  if (len != nullptr) *len = str->data.size();
  return str->data.c_str();
}

inline RedisModuleString *TestRedisModule_CreateStringPrintf(RedisModuleCtx *ctx
                                                             [[maybe_unused]],
                                                             const char *fmt,
                                                             ...) {
  char out[1024];
  va_list args;
  va_start(args, fmt);
  vsnprintf(out, sizeof(out), fmt, args);
  va_end(args);
  return new RedisModuleString{std::string(out)};
}

inline void *TestRedisModule_GetBlockedClientPrivateData(RedisModuleCtx *ctx) {
  return kMockRedisModule->GetBlockedClientPrivateData(ctx);
}

inline RedisModuleBlockedClient *TestRedisModule_BlockClientOnAuth(
    RedisModuleCtx *ctx, RedisModuleAuthCallback reply_callback,
    void (*free_privdata)(RedisModuleCtx *, void *)) {
  return kMockRedisModule->BlockClientOnAuth(ctx, reply_callback,
                                             free_privdata);
}

inline RedisModuleBlockedClient *TestRedisModule_BlockClient(
    RedisModuleCtx *ctx, RedisModuleCmdFunc reply_callback,
    RedisModuleCmdFunc timeout_callback,
    void (*free_privdata)(RedisModuleCtx *, void *),
    long long timeout_ms) {  // NOLINT
  return kMockRedisModule->BlockClient(ctx, reply_callback, timeout_callback,
                                       free_privdata, timeout_ms);
}

inline int TestRedisModule_AuthenticateClientWithACLUser(
    RedisModuleCtx *ctx, const char *name, size_t len,
    RedisModuleUserChangedFunc callback, void *privdata, uint64_t *client_id) {
  return kMockRedisModule->AuthenticateClientWithACLUser(
      ctx, name, len, callback, privdata, client_id);
}

inline void TestRedisModule_ACLAddLogEntryByUserName(
    RedisModuleCtx *ctx, RedisModuleString *user, RedisModuleString *object,
    RedisModuleACLLogEntryReason reason) {
  return kMockRedisModule->ACLAddLogEntryByUserName(ctx, user, object, reason);
}

inline RedisModuleString *TestRedisModule_CreateString(RedisModuleCtx *ctx
                                                       [[maybe_unused]],
                                                       const char *ptr,
                                                       size_t len) {
  return new RedisModuleString{std::string(ptr, len)};
}

inline void TestRedisModule_FreeString(RedisModuleCtx *ctx [[maybe_unused]],
                                       RedisModuleString *str) {
  str->cnt--;
  if (str->cnt == 0) {
    delete str;
  }
}

inline void TestRedisModule_RetainString(RedisModuleCtx *ctx [[maybe_unused]],
                                         RedisModuleString *str) {
  str->cnt++;
}

inline int TestRedisModule_EventLoopAdd(int fd, int mask,
                                        RedisModuleEventLoopFunc func,
                                        void *user_data) {
  return kMockRedisModule->EventLoopAdd(fd, mask, func, user_data);
}

inline int TestRedisModule_EventLoopAddOneShot(
    RedisModuleEventLoopOneShotFunc func, void *user_data) {
  return kMockRedisModule->EventLoopAddOneShot(func, user_data);
}

inline int TestRedisModule_EventLoopDel(int fd, int mask) {
  return kMockRedisModule->EventLoopDel(fd, mask);
}

inline RedisModuleTimerID TestRedisModule_CreateTimer(
    RedisModuleCtx *ctx, mstime_t period, RedisModuleTimerProc callback,
    void *data) {
  return kMockRedisModule->CreateTimer(ctx, period, callback, data);
}

inline int TestRedisModule_StopTimer(RedisModuleCtx *ctx, RedisModuleTimerID id,
                                     void **data) {
  return kMockRedisModule->StopTimer(ctx, id, data);
}

inline void TestRedisModule_SetModuleOptions(RedisModuleCtx *ctx, int options) {
  return kMockRedisModule->SetModuleOptions(ctx, options);
}

inline unsigned long long TestRedisModule_GetClientId(  // NOLINT
    RedisModuleCtx *ctx) {
  return kMockRedisModule->GetClientId(ctx);
}

inline int TestRedisModule_GetClientInfoById(void *ci, uint64_t id) {
  return kMockRedisModule->GetClientInfoById(ci, id);
}

inline int TestRedisModule_SubscribeToKeyspaceEvents(
    RedisModuleCtx *ctx, int types, RedisModuleNotificationFunc cb) {
  return kMockRedisModule->SubscribeToKeyspaceEvents(ctx, types, cb);
}

inline int TestRedisModule_KeyExistsDefaultImpl(RedisModuleCtx *ctx,
                                                RedisModuleString *key) {
  if (ctx != nullptr) {
    return ctx->registered_keys.contains(key->data) ? 1 : 0;
  }
  return 0;
}

inline int TestRedisModule_HashExternalizeDefaultImpl(
    RedisModuleKey *key, RedisModuleString *field, RedisModuleHashExternCB fn,
    void *privdata) {
  return REDISMODULE_OK;
}

inline int TestRedisModule_GetApiDefaultImpl(const char *name, void *func) {
  return REDISMODULE_OK;
}

inline int TestRedisModule_KeyExists(RedisModuleCtx *ctx,
                                     RedisModuleString *key) {
  return kMockRedisModule->KeyExists(ctx, key);
}

inline RedisModuleKey *TestRedisModule_OpenKeyDefaultImpl(
    RedisModuleCtx *ctx, RedisModuleString *key, int flags) {
  return new RedisModuleKey{ctx, key->data};
}

inline RedisModuleKey *TestRedisModule_OpenKey(RedisModuleCtx *ctx,
                                               RedisModuleString *key,
                                               int flags) {
  return kMockRedisModule->OpenKey(ctx, key, flags);
}

inline int TestRedisModule_HashExternalize(RedisModuleKey *key,
                                           RedisModuleString *field,
                                           RedisModuleHashExternCB fn,
                                           void *privdata) {
  return kMockRedisModule->HashExternalize(key, field, fn, privdata);
}

inline int TestRedisModule_GetApi(const char *name, void *func) {
  return kMockRedisModule->GetApi(name, func);
}

inline int TestRedisModule_HashGet(RedisModuleKey *key, int flags, ...) {
  va_list args;
  va_start(args, flags);

  const char *field = va_arg(args, const char *);
  int result;
  if (flags & REDISMODULE_HASH_EXISTS) {
    int *exists_out = va_arg(args, int *);
    void *terminating_null = va_arg(args, void *);
    result = kMockRedisModule->HashGet(key, flags, field, exists_out,
                                       terminating_null);
  } else {
    RedisModuleString **value_out = va_arg(args, RedisModuleString **);
    void *terminating_null = va_arg(args, void *);
    result = kMockRedisModule->HashGet(key, flags, field, value_out,
                                       terminating_null);
  }

  va_end(args);
  return result;
}

inline int TestRedisModule_HashSet(RedisModuleKey *key, int flags, ...) {
  va_list args;
  va_start(args, flags);

  RedisModuleString *field = va_arg(args, RedisModuleString *);
  RedisModuleString *value = va_arg(args, RedisModuleString *);
  void *terminating_null = va_arg(args, void *);
  int result =
      kMockRedisModule->HashSet(key, flags, field, value, terminating_null);
  va_end(args);
  return result;
}

struct RedisModuleScanCursor {
  int cursor{0};
};

inline int TestRedisModule_ScanKey(RedisModuleKey *key,
                                   RedisModuleScanCursor *cursor,
                                   RedisModuleScanKeyCB fn, void *privdata) {
  return kMockRedisModule->ScanKey(key, cursor, fn, privdata);
}

inline RedisModuleScanCursor *TestRedisModule_ScanCursorCreate() {
  return new RedisModuleScanCursor();
}

inline void TestRedisModule_ScanCursorDestroy(RedisModuleScanCursor *cursor) {
  delete cursor;
}

inline void TestRedisModule_CloseKeyDefaultImpl(RedisModuleKey *key) {
  delete key;
}

inline void TestRedisModule_CloseKey(RedisModuleKey *key) {
  return kMockRedisModule->CloseKey(key);
}

inline int TestRedisModule_CreateCommand(RedisModuleCtx *ctx, const char *name,
                                         RedisModuleCmdFunc cmdfunc,
                                         const char *strflags, int firstkey,
                                         int lastkey, int keystep) {
  return kMockRedisModule->CreateCommand(ctx, name, cmdfunc, strflags, firstkey,
                                         lastkey, keystep);
}

inline int TestRedisModule_KeyTypeDefaultImpl(RedisModuleKey *key) {
  if (key->ctx && key->ctx->registered_keys.contains(key->key)) {
    return REDISMODULE_KEYTYPE_MODULE;
  }
  return REDISMODULE_KEYTYPE_EMPTY;
}

inline int TestRedisModule_KeyType(RedisModuleKey *key) {
  return kMockRedisModule->KeyType(key);
}

inline int TestRedisModule_ModuleTypeSetValueDefaultImpl(RedisModuleKey *key,
                                                         RedisModuleType *mt,
                                                         void *value) {
  key->ctx->registered_keys[key->key] = RegisteredKey{
      .key = key->key,
      .data = value,
      .module_type = mt,
  };
  return REDISMODULE_OK;
}

inline int TestRedisModule_ModuleTypeSetValue(RedisModuleKey *key,
                                              RedisModuleType *mt,
                                              void *value) {
  return kMockRedisModule->ModuleTypeSetValue(key, mt, value);
}

inline int TestRedisModule_DeleteKeyDefaultImpl(RedisModuleKey *key) {
  if (key->ctx != nullptr) {
    if (key->ctx->registered_keys.contains(key->key)) {
      key->ctx->registered_keys.erase(key->key);
      return REDISMODULE_OK;
    }
  }
  return REDISMODULE_ERR;
}

inline int TestRedisModule_DeleteKey(RedisModuleKey *key) {
  return kMockRedisModule->DeleteKey(key);
}

inline int TestRedisModule_ReplyWithArray(RedisModuleCtx *ctx,
                                          long len) {  // NOLINT
  if (ctx) {
    ctx->reply_capture.ReplyWithArray(len);
  }
  return kMockRedisModule->ReplyWithArray(ctx, len);
}

inline void TestRedisModule_ReplySetArrayLength(RedisModuleCtx *ctx,
                                                long len) {  // NOLINT
  if (ctx) {
    ctx->reply_capture.ReplySetArrayLength(len);
  }
  kMockRedisModule->ReplySetArrayLength(ctx, len);
}

inline int TestRedisModule_ReplyWithLongLong(RedisModuleCtx *ctx,
                                             long long ll) {  // NOLINT
  if (ctx) {
    ctx->reply_capture.ReplyWithLongLong(ll);
  }
  return kMockRedisModule->ReplyWithLongLong(ctx, ll);
}

inline int TestRedisModule_ReplyWithSimpleString(RedisModuleCtx *ctx,
                                                 const char *msg) {
  if (ctx) {
    ctx->reply_capture.ReplyWithSimpleString(msg);
  }
  return kMockRedisModule->ReplyWithSimpleString(ctx, msg);
}

inline int TestRedisModule_ReplyWithString(RedisModuleCtx *ctx,
                                           RedisModuleString *msg) {
  if (ctx) {
    ctx->reply_capture.ReplyWithString(msg);
  }
  return kMockRedisModule->ReplyWithString(ctx, msg);
}

inline int TestRedisModule_ReplyWithDouble(RedisModuleCtx *ctx, double val) {
  if (ctx) {
    ctx->reply_capture.ReplyWithDouble(val);
  }
  return kMockRedisModule->ReplyWithDouble(ctx, val);
}

inline int TestRedisModule_ReplyWithCString(RedisModuleCtx *ctx,
                                            const char *str) {
  if (ctx) {
    ctx->reply_capture.ReplyWithCString(str);
  }
  return kMockRedisModule->ReplyWithCString(ctx, str);
}

inline int TestRedisModule_ReplyWithStringBuffer(RedisModuleCtx *ctx,
                                                 const char *buf, size_t len) {
  if (ctx) {
    ctx->reply_capture.ReplyWithStringBuffer(buf, len);
  }
  return kMockRedisModule->ReplyWithStringBuffer(ctx, buf, len);
}

inline int TestRedisModule_RegisterInfoFunc(RedisModuleCtx *ctx,
                                            RedisModuleInfoFunc cb) {
  return kMockRedisModule->RegisterInfoFunc(ctx, cb);
}

inline int TestRedisModule_Init(RedisModuleCtx *ctx, const char *name, int ver,
                                int apiver) {
  return kMockRedisModule->Init(ctx, name, ver, apiver);
}

inline RedisModuleType *TestRedisModule_CreateDataType(
    RedisModuleCtx *ctx, const char *name, int encver,
    RedisModuleTypeMethods *typemethods) {
  return kMockRedisModule->CreateDataType(ctx, name, encver, typemethods);
}

inline int TestRedisModule_ReplyWithError(RedisModuleCtx *ctx,
                                          const char *err) {
  if (ctx) {
    ctx->reply_capture.ReplyWithError(err);
  }
  return kMockRedisModule->ReplyWithError(ctx, err);
}

inline int TestRedisModule_SubscribeToServerEvent(RedisModuleCtx *ctx,
                                                  RedisModuleEvent event,
                                                  RedisModuleEventCallback cb) {
  return kMockRedisModule->SubscribeToServerEvent(ctx, event, cb);
}

inline int TestRedisModule_Scan(RedisModuleCtx *ctx,
                                RedisModuleScanCursor *cursor,
                                RedisModuleScanCB fn, void *privdata) {
  return kMockRedisModule->Scan(ctx, cursor, fn, privdata);
}

inline int TestRedisModule_ReplicateVerbatim(RedisModuleCtx *ctx) {
  return kMockRedisModule->ReplicateVerbatim(ctx);
}

inline RedisModuleType *TestRedisModule_ModuleTypeGetTypeDefaultImpl(
    RedisModuleKey *key) {
  if (key->ctx->registered_keys.contains(key->key)) {
    return key->ctx->registered_keys[key->key].module_type;
  } else {
    return nullptr;
  }
  return key->ctx->registered_keys[key->key].module_type;
}

inline RedisModuleType *TestRedisModule_ModuleTypeGetType(RedisModuleKey *key) {
  return kMockRedisModule->ModuleTypeGetType(key);
}

inline RedisModuleCtx *TestRedisModule_GetDetachedThreadSafeContext(
    RedisModuleCtx *ctx) {
  return kMockRedisModule->GetDetachedThreadSafeContext(ctx);
}

inline void TestRedisModule_FreeThreadSafeContext(RedisModuleCtx *ctx) {
  return kMockRedisModule->FreeThreadSafeContext(ctx);
}

inline int TestRedisModule_SelectDb(RedisModuleCtx *ctx, int newid) {
  return kMockRedisModule->SelectDb(ctx, newid);
}

inline int TestRedisModule_GetSelectedDb(RedisModuleCtx *ctx) {
  return kMockRedisModule->GetSelectedDb(ctx);
}

inline void *TestRedisModule_ModuleTypeGetValueDefaultImpl(
    RedisModuleKey *key) {
  if (key->ctx->registered_keys.contains(key->key)) {
    return key->ctx->registered_keys[key->key].data;
  } else {
    return nullptr;
  }
}

inline void *TestRedisModule_ModuleTypeGetValue(RedisModuleKey *key) {
  return kMockRedisModule->ModuleTypeGetValue(key);
}

inline unsigned long long TestRedisModule_DbSize(  // NOLINT
    RedisModuleCtx *ctx) {
  return kMockRedisModule->DbSize(ctx);
}

inline int TestRedisModule_InfoAddSection(RedisModuleInfoCtx *ctx,
                                          const char *str) {
  if (ctx) {
    ctx->info_capture.InfoAddSection(str);
  }
  return kMockRedisModule->InfoAddSection(ctx, str);
}

inline int TestRedisModule_InfoAddFieldLongLong(RedisModuleInfoCtx *ctx,
                                                const char *str,
                                                long long field) {  // NOLINT
  if (ctx) {
    ctx->info_capture.InfoAddFieldLongLong(str, field, ctx->in_dict_field);
  }
  return kMockRedisModule->InfoAddFieldLongLong(ctx, str, field);
}

inline int TestRedisModule_InfoAddFieldCString(RedisModuleInfoCtx *ctx,
                                               const char *str,
                                               const char *field) {
  if (ctx) {
    ctx->info_capture.InfoAddFieldCString(str, field, ctx->in_dict_field);
  }
  return kMockRedisModule->InfoAddFieldCString(ctx, str, field);
}

inline int TestRedisModule_InfoBeginDictField(RedisModuleInfoCtx *ctx,
                                              const char *str) {
  if (ctx) {
    ctx->info_capture.InfoBeginDictField(str, ctx->in_dict_field);
    ctx->in_dict_field = 1;
  }
  return kMockRedisModule->InfoBeginDictField(ctx, str);
}

inline int TestRedisModule_InfoEndDictField(RedisModuleInfoCtx *ctx) {
  if (ctx) {
    ctx->info_capture.InfoEndDictField();
  }
  ctx->in_dict_field = 0;
  return kMockRedisModule->InfoEndDictField(ctx);
}

inline int TestRedisModule_RegisterStringConfig(
    RedisModuleCtx *ctx, const char *name, const char *default_val,
    unsigned int flags, RedisModuleConfigGetStringFunc getfn,
    RedisModuleConfigSetStringFunc setfn, RedisModuleConfigApplyFunc applyfn,
    void *privdata) {
  return kMockRedisModule->RegisterStringConfig(
      ctx, name, default_val, flags, getfn, setfn, applyfn, privdata);
}

inline int TestRedisModule_RegisterEnumConfig(
    RedisModuleCtx *ctx, const char *name, int default_val, unsigned int flags,
    const char **enum_values, const int *int_values, int num_enum_vals,
    RedisModuleConfigGetEnumFunc getfn, RedisModuleConfigSetEnumFunc setfn,
    RedisModuleConfigApplyFunc applyfn, void *privdata) {
  return kMockRedisModule->RegisterEnumConfig(
      ctx, name, default_val, flags, enum_values, int_values, num_enum_vals,
      getfn, setfn, applyfn, privdata);
}

inline int TestRedisModule_LoadConfigs(RedisModuleCtx *ctx) {
  return kMockRedisModule->LoadConfigs(ctx);
}

inline int TestRedisModule_StringCompare(RedisModuleString *a,
                                         RedisModuleString *b) {
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

inline int TestRedisModule_SetConnectionProperties(
    const RedisModuleConnectionProperty *properties, int length) {
  return kMockRedisModule->SetConnectionProperties(properties, length);
}

inline int TestRedisModule_SetShardId(const char *shardId, int len) {
  return kMockRedisModule->SetShardId(shardId, len);
}

inline int TestRedisModule_GetClusterInfo(void *cli) {
  return kMockRedisModule->GetClusterInfo(cli);
}

inline const char *TestRedisModule_GetMyShardID() {
  return kMockRedisModule->GetMyShardID();
}

inline int TestRedisModule_GetContextFlags(RedisModuleCtx *ctx) {
  return kMockRedisModule->GetContextFlags(ctx);
}

inline uint64_t TestRedisModule_LoadUnsigned(RedisModuleIO *io) {
  return kMockRedisModule->LoadUnsigned(io);
}

inline int64_t TestRedisModule_LoadSigned(RedisModuleIO *io) {
  return kMockRedisModule->LoadSigned(io);
}

inline double TestRedisModule_LoadDouble(RedisModuleIO *io) {
  return kMockRedisModule->LoadDouble(io);
}

inline char *TestRedisModule_LoadStringBuffer(RedisModuleIO *io,
                                              size_t *lenptr) {
  return kMockRedisModule->LoadStringBuffer(io, lenptr);
}

inline RedisModuleString *TestRedisModule_LoadString(RedisModuleIO *io) {
  return kMockRedisModule->LoadString(io);
}

inline void TestRedisModule_SaveUnsigned(RedisModuleIO *io, uint64_t val) {
  return kMockRedisModule->SaveUnsigned(io, val);
}

inline void TestRedisModule_SaveSigned(RedisModuleIO *io, int64_t val) {
  return kMockRedisModule->SaveSigned(io, val);
}

inline void TestRedisModule_SaveDouble(RedisModuleIO *io, double val) {
  return kMockRedisModule->SaveDouble(io, val);
}

inline void TestRedisModule_SaveStringBuffer(RedisModuleIO *io, const char *str,
                                             size_t len) {
  return kMockRedisModule->SaveStringBuffer(io, str, len);
}

inline int TestRedisModule_IsIOError(RedisModuleIO *io) {
  return kMockRedisModule->IsIOError(io);
}

inline int TestRedisModule_ReplicationSetMasterCrossCluster(RedisModuleCtx *ctx,
                                                            const char *ip,
                                                            int port) {
  return kMockRedisModule->ReplicationSetMasterCrossCluster(ctx, ip, port);
}

inline int TestRedisModule_ReplicationUnsetMasterCrossCluster(
    RedisModuleCtx *ctx) {
  return kMockRedisModule->ReplicationUnsetMasterCrossCluster(ctx);
}

inline const char *TestRedisModule_GetMyClusterID() {
  return kMockRedisModule->GetMyClusterID();
}

inline int TestRedisModule_GetClusterNodeInfo(RedisModuleCtx *ctx,
                                              const char *id, char *ip,
                                              char *master_id, int *port,
                                              int *flags) {
  return kMockRedisModule->GetClusterNodeInfo(ctx, id, ip, master_id, port,
                                              flags);
}

inline int TestRedisModule_ReplicationSetSecondaryCluster(
    RedisModuleCtx *ctx, bool is_secondary_cluster) {
  return kMockRedisModule->ReplicationSetSecondaryCluster(ctx,
                                                          is_secondary_cluster);
}

inline RedisModuleCrossClusterReplicasList *
TestRedisModule_GetCrossClusterReplicasList() {
  return kMockRedisModule->GetCrossClusterReplicasList();
}

inline void TestRedisModule_FreeCrossClusterReplicasList(
    RedisModuleCrossClusterReplicasList *list) {
  return kMockRedisModule->FreeCrossClusterReplicasList(list);
}

inline void *TestRedisModule_Alloc(size_t size) {
  return kMockRedisModule->Alloc(size);
}

inline void TestRedisModule_Free(void *ptr) {
  return kMockRedisModule->Free(ptr);
}

inline void *TestRedisModule_Realloc(void *ptr, size_t size) {
  return kMockRedisModule->Realloc(ptr, size);
}

inline void *TestRedisModule_Calloc(size_t nmemb, size_t size) {
  return kMockRedisModule->Calloc(nmemb, size);
}

inline size_t TestRedisModule_MallocUsableSize(void *ptr) {
  return kMockRedisModule->MallocUsableSize(ptr);
}

inline size_t TestRedisModule_GetClusterSize() {
  return kMockRedisModule->GetClusterSize();
}

inline int TestRedisModule_WrongArity(RedisModuleCtx *ctx) {
  return kMockRedisModule->WrongArity(ctx);
}

inline int TestRedisModule_Fork(RedisModuleForkDoneHandler cb,
                                void *user_data) {
  return kMockRedisModule->Fork(cb, user_data);
}

inline int TestRedisModule_ExitFromChild(int retcode) {
  return kMockRedisModule->ExitFromChild(retcode);
}

inline RedisModuleRdbStream *TestRedisModule_RdbStreamCreateFromRioHandler(
    const RedisModuleRIOHandler *handler) {
  return kMockRedisModule->RdbStreamCreateFromRioHandler(handler);
}

inline void TestRedisModule_RdbStreamFree(RedisModuleRdbStream *stream) {
  return kMockRedisModule->RdbStreamFree(stream);
}

inline int TestRedisModule_RdbSave(RedisModuleCtx *ctx,
                                   RedisModuleRdbStream *stream, int flags) {
  return kMockRedisModule->RdbSave(ctx, stream, flags);
}

inline int TestRedisModule_RdbLoad(RedisModuleCtx *ctx,
                                   RedisModuleRdbStream *stream, int flags) {
  return kMockRedisModule->RdbLoad(ctx, stream, flags);
}

struct RedisModuleCallReply {
  std::string msg;
};

inline RedisModuleCallReply *TestRedisModule_Call(RedisModuleCtx *ctx,
                                                  const char *cmdname,
                                                  const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  std::string format(fmt);
  if (format == "cc") {
    const char *arg1 = va_arg(args, const char *);
    const char *arg2 = va_arg(args, const char *);
    auto ret = kMockRedisModule->Call(ctx, cmdname, fmt, arg1, arg2);
    return ret;
  }
  if (format == "cs3") {
    return nullptr;
  }
  CHECK(false && "Unsupported format specifier");
  return nullptr;
}

inline RedisModuleCallReply *TestRedisModule_CallReplyArrayElement(
    RedisModuleCallReply *reply, size_t idx) {
  return kMockRedisModule->CallReplyArrayElement(reply, idx);
}

inline const char *TestRedisModule_CallReplyStringPtr(
    RedisModuleCallReply *reply, size_t *len) {
  return kMockRedisModule->CallReplyStringPtr(reply, len);
}

inline void TestRedisModule_FreeCallReply(RedisModuleCallReply *reply) {
  return kMockRedisModule->FreeCallReply(reply);
}

inline void TestRedisModule_RegisterClusterMessageReceiver(
    RedisModuleCtx *ctx, uint8_t type,
    RedisModuleClusterMessageReceiver callback) {
  return kMockRedisModule->RegisterClusterMessageReceiver(ctx, type, callback);
}

inline int TestRedisModule_SendClusterMessage(RedisModuleCtx *ctx,
                                              const char *target_id,
                                              uint8_t type, const char *msg,
                                              uint32_t len) {
  return kMockRedisModule->SendClusterMessage(ctx, target_id, type, msg, len);
}

inline char **TestRedisModule_GetClusterNodesList(RedisModuleCtx *ctx,
                                                  size_t *numnodes) {
  return kMockRedisModule->GetClusterNodesList(ctx, numnodes);
}

inline RedisModuleCtx *TestRedisModule_GetContextFromIO(RedisModuleIO *rdb) {
  return kMockRedisModule->GetContextFromIO(rdb);
}

inline void TestRedisModule_FreeClusterNodesList(char **ids) {
  return kMockRedisModule->FreeClusterNodesList(ids);
}

inline int TestRedisModule_CallReplyType(RedisModuleCallReply *reply) {
  return kMockRedisModule->CallReplyType(reply);
}

inline RedisModuleString *TestRedisModule_CreateStringFromCallReply(
    RedisModuleCallReply *reply) {
  return kMockRedisModule->CreateStringFromCallReply(reply);
}

inline RedisModuleString *TestRedisModule_GetCurrentUserName(
    RedisModuleCtx *ctx) {
  return new RedisModuleString{std::string("default")};
}

// TestRedisModule_Init initializes the module API function table with mock
// implementations of functions to prevent segmentation faults when
// executing tests and to allow validation of Redis module API calls.
inline void TestRedisModule_Init() {
  RedisModule_Log = &TestRedisModule_Log;
  RedisModule_LogIOError = &TestRedisModule_LogIOError;
  RedisModule_BlockClientOnAuth = &TestRedisModule_BlockClientOnAuth;
  RedisModule_BlockedClientMeasureTimeEnd =
      &TestRedisModule_BlockedClientMeasureTimeEnd;
  RedisModule_BlockedClientMeasureTimeStart =
      &TestRedisModule_BlockedClientMeasureTimeStart;
  RedisModule_BlockClient = &TestRedisModule_BlockClient;
  RedisModule_UnblockClient = &TestRedisModule_UnblockClient;
  RedisModule_StringPtrLen = &TestRedisModule_StringPtrLen;
  RedisModule_CreateStringPrintf = &TestRedisModule_CreateStringPrintf;
  RedisModule_GetBlockedClientPrivateData =
      &TestRedisModule_GetBlockedClientPrivateData;
  RedisModule_AuthenticateClientWithACLUser =
      &TestRedisModule_AuthenticateClientWithACLUser;
  RedisModule_ACLAddLogEntryByUserName =
      &TestRedisModule_ACLAddLogEntryByUserName;
  RedisModule_CreateString = &TestRedisModule_CreateString;
  RedisModule_FreeString = &TestRedisModule_FreeString;
  RedisModule_RetainString = &TestRedisModule_RetainString;
  RedisModule_EventLoopAdd = &TestRedisModule_EventLoopAdd;
  RedisModule_EventLoopAddOneShot = &TestRedisModule_EventLoopAddOneShot;
  RedisModule_EventLoopDel = &TestRedisModule_EventLoopDel;
  RedisModule_CreateTimer = &TestRedisModule_CreateTimer;
  RedisModule_StopTimer = &TestRedisModule_StopTimer;
  RedisModule_SetModuleOptions = &TestRedisModule_SetModuleOptions;
  RedisModule_GetClientId = &TestRedisModule_GetClientId;
  RedisModule_GetClientInfoById = &TestRedisModule_GetClientInfoById;
  RedisModule_SubscribeToKeyspaceEvents =
      &TestRedisModule_SubscribeToKeyspaceEvents;
  RedisModule_KeyExists = &TestRedisModule_KeyExists;
  RedisModule_OpenKey = &TestRedisModule_OpenKey;
  RedisModule_HashExternalize = &TestRedisModule_HashExternalize;
  RedisModule_GetApi = &TestRedisModule_GetApi;
  RedisModule_HashGet = &TestRedisModule_HashGet;
  RedisModule_HashSet = &TestRedisModule_HashSet;
  RedisModule_ScanKey = &TestRedisModule_ScanKey;
  RedisModule_ScanCursorCreate = &TestRedisModule_ScanCursorCreate;
  RedisModule_ScanCursorDestroy = &TestRedisModule_ScanCursorDestroy;
  RedisModule_CloseKey = &TestRedisModule_CloseKey;
  RedisModule_CreateCommand = &TestRedisModule_CreateCommand;
  RedisModule_KeyType = &TestRedisModule_KeyType;
  RedisModule_ModuleTypeSetValue = &TestRedisModule_ModuleTypeSetValue;
  RedisModule_DeleteKey = &TestRedisModule_DeleteKey;
  RedisModule_RegisterInfoFunc = &TestRedisModule_RegisterInfoFunc;
  RedisModule_ReplyWithArray = &TestRedisModule_ReplyWithArray;
  RedisModule_ReplySetArrayLength = &TestRedisModule_ReplySetArrayLength;
  RedisModule_ReplyWithLongLong = &TestRedisModule_ReplyWithLongLong;
  RedisModule_ReplyWithSimpleString = &TestRedisModule_ReplyWithSimpleString;
  RedisModule_ReplyWithString = &TestRedisModule_ReplyWithString;
  RedisModule_ReplyWithDouble = &TestRedisModule_ReplyWithDouble;
  RedisModule_ReplyWithCString = &TestRedisModule_ReplyWithCString;
  RedisModule_ReplyWithStringBuffer = &TestRedisModule_ReplyWithStringBuffer;
  RedisModule_CreateDataType = &TestRedisModule_CreateDataType;
  RedisModule_ReplyWithError = &TestRedisModule_ReplyWithError;
  RedisModule_SubscribeToServerEvent = &TestRedisModule_SubscribeToServerEvent;
  RedisModule_Scan = &TestRedisModule_Scan;
  RedisModule_ReplicateVerbatim = &TestRedisModule_ReplicateVerbatim;
  RedisModule_ModuleTypeGetType = &TestRedisModule_ModuleTypeGetType;
  RedisModule_GetDetachedThreadSafeContext =
      &TestRedisModule_GetDetachedThreadSafeContext;
  RedisModule_FreeThreadSafeContext = &TestRedisModule_FreeThreadSafeContext;
  RedisModule_SelectDb = &TestRedisModule_SelectDb;
  RedisModule_GetSelectedDb = &TestRedisModule_GetSelectedDb;
  RedisModule_ModuleTypeGetValue = &TestRedisModule_ModuleTypeGetValue;
  RedisModule_DbSize = &TestRedisModule_DbSize;
  RedisModule_InfoAddSection = &TestRedisModule_InfoAddSection;
  RedisModule_InfoAddFieldLongLong = &TestRedisModule_InfoAddFieldLongLong;
  RedisModule_InfoAddFieldCString = &TestRedisModule_InfoAddFieldCString;
  RedisModule_InfoBeginDictField = &TestRedisModule_InfoBeginDictField;
  RedisModule_InfoEndDictField = &TestRedisModule_InfoEndDictField;
  RedisModule_RegisterStringConfig = &TestRedisModule_RegisterStringConfig;
  RedisModule_RegisterEnumConfig = &TestRedisModule_RegisterEnumConfig;
  RedisModule_LoadConfigs = &TestRedisModule_LoadConfigs;
  RedisModule_SetConnectionProperties =
      &TestRedisModule_SetConnectionProperties;
  RedisModule_SetShardId = &TestRedisModule_SetShardId;
  RedisModule_GetClusterInfo = &TestRedisModule_GetClusterInfo;
  RedisModule_GetMyShardID = &TestRedisModule_GetMyShardID;
  RedisModule_GetContextFlags = &TestRedisModule_GetContextFlags;
  RedisModule_LoadUnsigned = &TestRedisModule_LoadUnsigned;
  RedisModule_LoadSigned = &TestRedisModule_LoadSigned;
  RedisModule_LoadDouble = &TestRedisModule_LoadDouble;
  RedisModule_LoadStringBuffer = &TestRedisModule_LoadStringBuffer;
  RedisModule_LoadString = &TestRedisModule_LoadString;
  RedisModule_SaveUnsigned = &TestRedisModule_SaveUnsigned;
  RedisModule_SaveSigned = &TestRedisModule_SaveSigned;
  RedisModule_SaveDouble = &TestRedisModule_SaveDouble;
  RedisModule_SaveStringBuffer = &TestRedisModule_SaveStringBuffer;
  RedisModule_IsIOError = &TestRedisModule_IsIOError;
  RedisModule_ReplicationSetMasterCrossCluster =
      &TestRedisModule_ReplicationSetMasterCrossCluster;
  RedisModule_ReplicationUnsetMasterCrossCluster =
      &TestRedisModule_ReplicationUnsetMasterCrossCluster;
  RedisModule_GetMyClusterID = &TestRedisModule_GetMyClusterID;
  RedisModule_GetClusterNodeInfo = &TestRedisModule_GetClusterNodeInfo;
  RedisModule_ReplicationSetSecondaryCluster =
      &TestRedisModule_ReplicationSetSecondaryCluster;
  RedisModule_GetCrossClusterReplicasList =
      &TestRedisModule_GetCrossClusterReplicasList;
  RedisModule_FreeCrossClusterReplicasList =
      &TestRedisModule_FreeCrossClusterReplicasList;
  RedisModule_Alloc = &TestRedisModule_Alloc;
  RedisModule_Free = &TestRedisModule_Free;
  RedisModule_Realloc = &TestRedisModule_Realloc;
  RedisModule_Calloc = &TestRedisModule_Calloc;
  RedisModule_MallocUsableSize = &TestRedisModule_MallocUsableSize;
  RedisModule_GetClusterSize = &TestRedisModule_GetClusterSize;
  RedisModule_Call = &TestRedisModule_Call;
  RedisModule_CallReplyArrayElement = &TestRedisModule_CallReplyArrayElement;
  RedisModule_CallReplyStringPtr = &TestRedisModule_CallReplyStringPtr;
  RedisModule_FreeCallReply = &TestRedisModule_FreeCallReply;
  RedisModule_RegisterClusterMessageReceiver =
      &TestRedisModule_RegisterClusterMessageReceiver;
  RedisModule_SendClusterMessage = &TestRedisModule_SendClusterMessage;
  RedisModule_GetClusterNodesList = &TestRedisModule_GetClusterNodesList;
  RedisModule_GetContextFromIO = &TestRedisModule_GetContextFromIO;
  RedisModule_FreeClusterNodesList = &TestRedisModule_FreeClusterNodesList;
  RedisModule_CallReplyType = &TestRedisModule_CallReplyType;
  RedisModule_CreateStringFromCallReply =
      &TestRedisModule_CreateStringFromCallReply;
  RedisModule_WrongArity = &TestRedisModule_WrongArity;
  RedisModule_Fork = &TestRedisModule_Fork;
  RedisModule_ExitFromChild = &TestRedisModule_ExitFromChild;
  RedisModule_RdbStreamCreateFromRioHandler =
      &TestRedisModule_RdbStreamCreateFromRioHandler;
  RedisModule_RdbStreamFree = &TestRedisModule_RdbStreamFree;
  RedisModule_RdbSave = &TestRedisModule_RdbSave;
  RedisModule_RdbLoad = &TestRedisModule_RdbLoad;
  RedisModule_GetCurrentUserName = &TestRedisModule_GetCurrentUserName;
  kMockRedisModule = new testing::NiceMock<MockRedisModule>();

  // Implement basic key registration functions with simple implementations by
  // default.
  ON_CALL(*kMockRedisModule, OpenKey(testing::_, testing::_, testing::_))
      .WillByDefault(TestRedisModule_OpenKeyDefaultImpl);
  ON_CALL(*kMockRedisModule, CloseKey(testing::_))
      .WillByDefault(TestRedisModule_CloseKeyDefaultImpl);
  ON_CALL(*kMockRedisModule,
          ModuleTypeSetValue(testing::_, testing::_, testing::_))
      .WillByDefault(TestRedisModule_ModuleTypeSetValueDefaultImpl);
  ON_CALL(*kMockRedisModule, ModuleTypeGetValue(testing::_))
      .WillByDefault(TestRedisModule_ModuleTypeGetValueDefaultImpl);
  ON_CALL(*kMockRedisModule, ModuleTypeGetType(testing::_))
      .WillByDefault(TestRedisModule_ModuleTypeGetTypeDefaultImpl);
  ON_CALL(*kMockRedisModule, KeyType(testing::_))
      .WillByDefault(TestRedisModule_KeyTypeDefaultImpl);
  ON_CALL(*kMockRedisModule, DeleteKey(testing::_))
      .WillByDefault(TestRedisModule_DeleteKeyDefaultImpl);
  ON_CALL(*kMockRedisModule, KeyExists(testing::_, testing::_))
      .WillByDefault(TestRedisModule_KeyExistsDefaultImpl);
  ON_CALL(*kMockRedisModule,
          HashExternalize(testing::_, testing::_, testing::_, testing::_))
      .WillByDefault(TestRedisModule_HashExternalizeDefaultImpl);
  ON_CALL(*kMockRedisModule, GetApi(testing::_, testing::_))
      .WillByDefault(TestRedisModule_GetApiDefaultImpl);
  static absl::once_flag flag;
  absl::call_once(flag, []() { vmsdk::TrackCurrentAsMainThread(); });
  CHECK(vmsdk::InitLogging(nullptr, "debug").ok());
}

inline void TestRedisModule_Teardown() { delete kMockRedisModule; }

#endif  // VMSDK_SRC_TESTING_INFRA_MODULE
