/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "tls.h"

#include <filesystem>
#include <memory>
#include <string>
#include <system_error>
#include <utility>

#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "grpc/grpc_security_constants.h"
#include "grpcpp/security/credentials.h"
#include "grpcpp/security/server_credentials.h"
#include "grpcpp/security/tls_certificate_provider.h"
#include "grpcpp/security/tls_certificate_verifier.h"
#include "grpcpp/security/tls_credentials_options.h"

namespace valkey_search::coordinator {

namespace {

absl::StatusOr<std::string> RequireFile(
    absl::FunctionRef<absl::StatusOr<std::string>(const char *)> get_config,
    const char *name) {
  auto value = get_config(name);
  if (!value.ok()) {
    return value.status();
  }
  if (value->empty()) {
    return absl::FailedPreconditionError(
        absl::StrCat("Coordinator TLS requires ", name, " to be set"));
  }
  return value;
}

// Reads a config the server may not have (older servers lack newer ones). A
// missing config reads as empty, as if it were unset.
absl::StatusOr<std::string> GetOptional(
    absl::FunctionRef<absl::StatusOr<std::string>(const char *)> get_config,
    const char *name) {
  auto value = get_config(name);
  if (absl::IsNotFound(value.status())) {
    return std::string();
  }
  return value;
}

absl::Status RejectIfSet(
    absl::FunctionRef<absl::StatusOr<std::string>(const char *)> get_config,
    const char *name, absl::string_view reason) {
  auto value = get_config(name);
  if (value.ok() && !value->empty()) {
    return absl::FailedPreconditionError(
        absl::StrCat("Coordinator TLS does not support ", name, ": ", reason));
  }
  // A lookup that failed for any reason other than the config not existing
  // must not read as "not set".
  if (!value.ok() && !absl::IsNotFound(value.status())) {
    return value.status();
  }
  return absl::OkStatus();
}

std::shared_ptr<grpc::experimental::FileWatcherCertificateProvider>
MakeProvider(const std::string &key_file, const std::string &cert_file,
             const std::string &ca_file, unsigned int refresh_interval_secs) {
  return std::make_shared<grpc::experimental::FileWatcherCertificateProvider>(
      key_file, cert_file, ca_file, refresh_interval_secs);
}

absl::Status ValidateFiles(const std::string &key_file,
                           const std::string &cert_file,
                           const std::string &ca_file) {
  // ValidateCredentials() only checks the PEM of what was loaded, and passes
  // when a file is missing, so make sure every file is there and not empty.
  for (const std::string *file : {&key_file, &cert_file, &ca_file}) {
    std::error_code ec;
    if (std::filesystem::file_size(*file, ec) == 0 || ec) {
      return absl::FailedPreconditionError(absl::StrCat(
          "Coordinator TLS cannot read ", *file, " (missing or empty)"));
    }
  }
  auto status = MakeProvider(key_file, cert_file, ca_file,
                             TlsConfig::kDefaultRefreshIntervalSecs)
                    ->ValidateCredentials();
  if (!status.ok()) {
    return absl::FailedPreconditionError(
        absl::StrCat("Coordinator TLS cannot use ", cert_file, " / ", key_file,
                     " / ", ca_file, ": ", status.message()));
  }
  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<TlsConfig> LoadTlsConfig(
    absl::FunctionRef<absl::StatusOr<std::string>(const char *)> get_config) {
  // gRPC loads PEM files itself, so encrypted keys and CA directories, which
  // the server accepts, cannot be honored here. Fail instead of downgrading or
  // silently ignoring them.
  for (const auto &[name, reason] :
       {std::pair<const char *, const char *>{"tls-key-file-pass",
                                              "encrypted keys"},
        {"tls-client-key-file-pass", "encrypted keys"},
        {"tls-ca-cert-dir", "a CA directory, use tls-ca-cert-file"}}) {
    auto status = RejectIfSet(get_config, name, reason);
    if (!status.ok()) {
      return status;
    }
  }

  TlsConfig config;
  auto cert = RequireFile(get_config, "tls-cert-file");
  if (!cert.ok()) return cert.status();
  config.cert_file = *std::move(cert);
  auto key = RequireFile(get_config, "tls-key-file");
  if (!key.ok()) return key.status();
  config.key_file = *std::move(key);

  auto ca = get_config("tls-ca-cert-file");
  if (!ca.ok()) return ca.status();
  if (ca->empty()) {
    return absl::FailedPreconditionError(
        "Coordinator TLS requires tls-ca-cert-file to be set");
  }
  config.ca_file = *std::move(ca);

  // A dedicated client identity is a pair; one half alone is a mistake, not a
  // request to use the server identity.
  auto client_cert = GetOptional(get_config, "tls-client-cert-file");
  if (!client_cert.ok()) return client_cert.status();
  auto client_key = GetOptional(get_config, "tls-client-key-file");
  if (!client_key.ok()) return client_key.status();
  if (client_cert->empty() != client_key->empty()) {
    return absl::FailedPreconditionError(
        "Coordinator TLS requires tls-client-cert-file and "
        "tls-client-key-file to be set together");
  }
  if (client_cert->empty()) {
    config.client_cert_file = config.cert_file;
    config.client_key_file = config.key_file;
  } else {
    config.client_cert_file = *std::move(client_cert);
    config.client_key_file = *std::move(client_key);
  }

  auto auth_clients = get_config("tls-auth-clients");
  if (!auth_clients.ok()) return auth_clients.status();
  if (*auth_clients == "no") {
    config.client_auth = TlsConfig::ClientAuth::kNo;
  } else if (*auth_clients == "optional") {
    config.client_auth = TlsConfig::ClientAuth::kOptional;
  } else if (*auth_clients == "yes" || auth_clients->empty()) {
    config.client_auth = TlsConfig::ClientAuth::kYes;
  } else {
    return absl::InternalError(
        absl::StrCat("Unexpected tls-auth-clients value: ", *auth_clients));
  }

  // Servers before 9.1 have no tls-auto-reload-interval; use the default then.
  auto reload_interval = GetOptional(get_config, "tls-auto-reload-interval");
  if (!reload_interval.ok()) return reload_interval.status();
  int interval_secs = 0;
  if (!reload_interval->empty() &&
      (!absl::SimpleAtoi(*reload_interval, &interval_secs) ||
       interval_secs < 0)) {
    return absl::InternalError(absl::StrCat(
        "Unexpected tls-auto-reload-interval value: ", *reload_interval));
  }
  if (interval_secs > 0) {
    config.refresh_interval_secs = interval_secs;
  }
  return config;
}

absl::Status ValidateTlsConfig(const TlsConfig &config) {
  auto status =
      ValidateFiles(config.key_file, config.cert_file, config.ca_file);
  if (status.ok() && (config.client_cert_file != config.cert_file ||
                      config.client_key_file != config.key_file)) {
    status = ValidateFiles(config.client_key_file, config.client_cert_file,
                           config.ca_file);
  }
  return status;
}

std::shared_ptr<grpc::ServerCredentials> MakeTlsServerCredentials(
    const TlsConfig &config) {
  grpc::experimental::TlsServerCredentialsOptions options(
      MakeProvider(config.key_file, config.cert_file, config.ca_file,
                   config.refresh_interval_secs));
  options.watch_root_certs();
  options.watch_identity_key_cert_pairs();
  switch (config.client_auth) {
    case TlsConfig::ClientAuth::kNo:
      options.set_cert_request_type(GRPC_SSL_DONT_REQUEST_CLIENT_CERTIFICATE);
      break;
    case TlsConfig::ClientAuth::kOptional:
      options.set_cert_request_type(
          GRPC_SSL_REQUEST_CLIENT_CERTIFICATE_AND_VERIFY);
      break;
    case TlsConfig::ClientAuth::kYes:
      options.set_cert_request_type(
          GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY);
      break;
  }
  return grpc::experimental::TlsServerCredentials(options);
}

std::shared_ptr<grpc::ChannelCredentials> MakeTlsChannelCredentials(
    const TlsConfig &config) {
  grpc::experimental::TlsChannelCredentialsOptions options;
  options.set_certificate_provider(
      MakeProvider(config.client_key_file, config.client_cert_file,
                   config.ca_file, config.refresh_interval_secs));
  options.watch_root_certs();
  options.watch_identity_key_cert_pairs();
  // Peers are dialed by the IP the cluster reports, and certificates are
  // issued to node identities rather than addresses. Like the cluster bus and
  // replication links, trust is established by the CA chain, not the hostname.
  options.set_verify_server_certs(true);
  options.set_check_call_host(false);
  options.set_certificate_verifier(
      std::make_shared<grpc::experimental::NoOpCertificateVerifier>());
  return grpc::experimental::TlsCredentials(options);
}

}  // namespace valkey_search::coordinator
