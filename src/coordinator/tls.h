/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#pragma once

#include <memory>
#include <string>

#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "grpcpp/security/credentials.h"
#include "grpcpp/security/server_credentials.h"

namespace valkey_search::coordinator {

// TLS settings for the coordinator channel. They mirror the server's own TLS
// configuration, so cross-shard traffic is protected the same way as the
// client port, replication and the cluster bus.
struct TlsConfig {
  enum class ClientAuth { kNo, kOptional, kYes };

  std::string cert_file;  // tls-cert-file
  std::string key_file;   // tls-key-file
  std::string ca_file;    // tls-ca-cert-file
  // Identity used when connecting to a peer. Falls back to cert_file and
  // key_file when tls-client-cert-file / tls-client-key-file are not set.
  std::string client_cert_file;
  std::string client_key_file;
  ClientAuth client_auth = ClientAuth::kYes;  // tls-auth-clients
  // How often the certificate files are checked for changes. Taken from
  // tls-auto-reload-interval, or kDefaultRefreshIntervalSecs when that is 0
  // (the server does not reload), since pooled channels outlive a rotation.
  static constexpr unsigned int kDefaultRefreshIntervalSecs = 60;
  unsigned int refresh_interval_secs = kDefaultRefreshIntervalSecs;
};

// Reads the TLS settings for the coordinator channel. Called only when the
// coordinator-tls option is on, so an unusable configuration is an error and
// the module never silently falls back to plaintext.
//
// `get_config` returns the value of a server config, e.g. via CONFIG GET.
absl::StatusOr<TlsConfig> LoadTlsConfig(
    absl::FunctionRef<absl::StatusOr<std::string>(const char *)> get_config);

// Checks that the configured files hold a usable identity and trust root, so a
// bad configuration fails at startup and not on the first connection.
absl::Status ValidateTlsConfig(const TlsConfig &config);

// The credentials reload the certificate files when they change, so channels
// that outlive a certificate rotation keep working.
std::shared_ptr<grpc::ServerCredentials> MakeTlsServerCredentials(
    const TlsConfig &config);
std::shared_ptr<grpc::ChannelCredentials> MakeTlsChannelCredentials(
    const TlsConfig &config);

}  // namespace valkey_search::coordinator
