/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/coordinator/tls.h"

#include <memory>
#include <string>

#include "absl/functional/function_ref.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
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

// How often the certificate files are checked for changes.
constexpr unsigned int kCertRefreshIntervalSecs = 60;

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

absl::Status RejectIfSet(
    absl::FunctionRef<absl::StatusOr<std::string>(const char *)> get_config,
    const char *name, absl::string_view reason) {
  auto value = get_config(name);
  if (value.ok() && !value->empty()) {
    return absl::FailedPreconditionError(
        absl::StrCat("Coordinator TLS does not support ", name, ": ", reason));
  }
  return absl::OkStatus();
}

std::shared_ptr<grpc::experimental::FileWatcherCertificateProvider>
MakeProvider(const std::string &key_file, const std::string &cert_file,
             const std::string &ca_file) {
  return std::make_shared<grpc::experimental::FileWatcherCertificateProvider>(
      key_file, cert_file, ca_file, kCertRefreshIntervalSecs);
}

}  // namespace

absl::StatusOr<TlsConfig> LoadTlsConfig(
    absl::FunctionRef<absl::StatusOr<std::string>(const char *)> get_config) {
  // gRPC loads PEM files itself, so encrypted keys and CA directories, which
  // the server accepts, cannot be honored here. Fail instead of downgrading.
  for (const char *name : {"tls-key-file-pass", "tls-client-key-file-pass"}) {
    auto status = RejectIfSet(get_config, name, "encrypted keys");
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
        "Coordinator TLS requires tls-ca-cert-file to be set; tls-ca-cert-dir "
        "is not supported");
  }
  config.ca_file = *std::move(ca);

  auto client_cert = get_config("tls-client-cert-file");
  auto client_key = get_config("tls-client-key-file");
  if (client_cert.ok() && client_key.ok() && !client_cert->empty() &&
      !client_key->empty()) {
    config.client_cert_file = *std::move(client_cert);
    config.client_key_file = *std::move(client_key);
  } else {
    config.client_cert_file = config.cert_file;
    config.client_key_file = config.key_file;
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
  return config;
}

std::shared_ptr<grpc::ServerCredentials> MakeTlsServerCredentials(
    const TlsConfig &config) {
  grpc::experimental::TlsServerCredentialsOptions options(
      MakeProvider(config.key_file, config.cert_file, config.ca_file));
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
  options.set_certificate_provider(MakeProvider(
      config.client_key_file, config.client_cert_file, config.ca_file));
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
