/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/coordinator/tls.h"

#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <thread>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "gmock/gmock.h"
#include "grpcpp/channel.h"
#include "grpcpp/create_channel.h"
#include "grpcpp/generic/callback_generic_service.h"
#include "grpcpp/security/credentials.h"
#include "grpcpp/security/server_credentials.h"
#include "grpcpp/server.h"
#include "grpcpp/server_builder.h"
#include "gtest/gtest.h"

namespace valkey_search::coordinator {
namespace {

using ::testing::HasSubstr;

using ConfigMap = std::map<std::string, std::string>;

absl::StatusOr<TlsConfig> Load(const ConfigMap &config) {
  return LoadTlsConfig(
      [&config](const char *name) -> absl::StatusOr<std::string> {
        auto it = config.find(name);
        return it == config.end() ? std::string() : it->second;
      });
}

ConfigMap EnabledConfig() {
  return {{"tls-cert-file", "/certs/server.crt"},
          {"tls-key-file", "/certs/server.key"},
          {"tls-ca-cert-file", "/certs/ca.pem"},
          {"tls-auth-clients", "yes"},
          {"tls-auto-reload-interval", "0"}};
}

TEST(CoordinatorTlsConfigTest, ReadsServerConfig) {
  auto config = Load(EnabledConfig());
  ASSERT_TRUE(config.ok());
  EXPECT_EQ(config->cert_file, "/certs/server.crt");
  EXPECT_EQ(config->key_file, "/certs/server.key");
  EXPECT_EQ(config->ca_file, "/certs/ca.pem");
  EXPECT_EQ(config->client_auth, TlsConfig::ClientAuth::kYes);
}

TEST(CoordinatorTlsConfigTest, ClientIdentityFallsBackToServerIdentity) {
  auto config = Load(EnabledConfig());
  ASSERT_TRUE(config.ok());
  EXPECT_EQ(config->client_cert_file, "/certs/server.crt");
  EXPECT_EQ(config->client_key_file, "/certs/server.key");
}

TEST(CoordinatorTlsConfigTest, UsesDedicatedClientIdentityWhenSet) {
  auto values = EnabledConfig();
  values["tls-client-cert-file"] = "/certs/client.crt";
  values["tls-client-key-file"] = "/certs/client.key";
  auto config = Load(values);
  ASSERT_TRUE(config.ok());
  EXPECT_EQ(config->client_cert_file, "/certs/client.crt");
  EXPECT_EQ(config->client_key_file, "/certs/client.key");
}

TEST(CoordinatorTlsConfigTest, ParsesAuthClients) {
  for (const auto &[value, expected] :
       {std::pair{"yes", TlsConfig::ClientAuth::kYes},
        std::pair{"optional", TlsConfig::ClientAuth::kOptional},
        std::pair{"no", TlsConfig::ClientAuth::kNo}}) {
    auto values = EnabledConfig();
    values["tls-auth-clients"] = value;
    auto config = Load(values);
    ASSERT_TRUE(config.ok()) << value;
    EXPECT_EQ(config->client_auth, expected) << value;
  }
}

TEST(CoordinatorTlsConfigTest, RejectsUnknownAuthClients) {
  auto values = EnabledConfig();
  values["tls-auth-clients"] = "maybe";
  EXPECT_FALSE(Load(values).ok());
}

TEST(CoordinatorTlsConfigTest, FailsInsteadOfFallingBackToPlaintext) {
  for (const char *missing :
       {"tls-cert-file", "tls-key-file", "tls-ca-cert-file"}) {
    auto values = EnabledConfig();
    values.erase(missing);
    auto config = Load(values);
    ASSERT_FALSE(config.ok()) << missing;
    EXPECT_EQ(config.status().code(), absl::StatusCode::kFailedPrecondition);
  }
}

TEST(CoordinatorTlsConfigTest, RejectsHalfOfAClientIdentity) {
  for (const char *name : {"tls-client-cert-file", "tls-client-key-file"}) {
    auto values = EnabledConfig();
    values[name] = "/certs/client.file";
    auto config = Load(values);
    ASSERT_FALSE(config.ok()) << name;
    EXPECT_EQ(config.status().code(), absl::StatusCode::kFailedPrecondition);
  }
}

TEST(CoordinatorTlsConfigTest, UsesServerReloadInterval) {
  auto values = EnabledConfig();
  values["tls-auto-reload-interval"] = "5";
  auto config = Load(values);
  ASSERT_TRUE(config.ok());
  EXPECT_EQ(config->refresh_interval_secs, 5u);
}

TEST(CoordinatorTlsConfigTest, DefaultsReloadIntervalWhenServerDoesNotReload) {
  auto config = Load(EnabledConfig());  // tls-auto-reload-interval is 0
  ASSERT_TRUE(config.ok());
  EXPECT_EQ(config->refresh_interval_secs,
            TlsConfig::kDefaultRefreshIntervalSecs);
}

// Servers before 9.1 do not know tls-auto-reload-interval, and CONFIG GET for
// an unknown config must not take the module down.
TEST(CoordinatorTlsConfigTest, ToleratesServerWithoutReloadInterval) {
  auto values = EnabledConfig();
  auto config = LoadTlsConfig(
      [&values](const char *name) -> absl::StatusOr<std::string> {
        if (std::string(name) == "tls-auto-reload-interval") {
          return absl::NotFoundError("unknown config");
        }
        auto it = values.find(name);
        return it == values.end() ? std::string() : it->second;
      });
  ASSERT_TRUE(config.ok());
  EXPECT_EQ(config->refresh_interval_secs,
            TlsConfig::kDefaultRefreshIntervalSecs);
}

TEST(CoordinatorTlsConfigTest, RejectsInvalidReloadInterval) {
  for (const char *value : {"-1", "soon"}) {
    auto values = EnabledConfig();
    values["tls-auto-reload-interval"] = value;
    EXPECT_FALSE(Load(values).ok()) << value;
  }
}

TEST(CoordinatorTlsConfigTest, RejectsEncryptedKeys) {
  for (const char *name : {"tls-key-file-pass", "tls-client-key-file-pass"}) {
    auto values = EnabledConfig();
    values[name] = "secret";
    auto config = Load(values);
    ASSERT_FALSE(config.ok()) << name;
    EXPECT_THAT(config.status().message(), HasSubstr(name));
  }
}

// Handshake tests use real certificates generated with the openssl CLI.
class CoordinatorTlsHandshakeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (std::system("openssl version > /dev/null 2>&1") != 0) {
      GTEST_SKIP() << "openssl CLI not available";
    }
    dir_ = std::filesystem::temp_directory_path() /
           absl::StrCat("coordinator_tls_test_", getpid());
    std::filesystem::create_directories(dir_);
    MakeCa("ca");
    MakeCa("other_ca");
    MakeCert("server", "ca");
    MakeCert("client", "ca");
    MakeCert("rogue", "other_ca");
  }

  void TearDown() override {
    if (!dir_.empty()) {
      std::filesystem::remove_all(dir_);
    }
  }

  std::string Path(const std::string &name) const {
    return (dir_ / name).string();
  }

  void Run(const std::string &cmd) {
    ASSERT_EQ(std::system((cmd + " > /dev/null 2>&1").c_str()), 0) << cmd;
  }

  void MakeCa(const std::string &name) {
    Run(absl::StrCat(
        "openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=", name,
        " -keyout ", Path(name + ".key"), " -out ", Path(name + ".pem")));
  }

  void MakeCert(const std::string &name, const std::string &ca) {
    Run(absl::StrCat("openssl req -newkey rsa:2048 -nodes -subj /CN=", name,
                     " -keyout ", Path(name + ".key"), " -out ",
                     Path(name + ".csr")));
    Run(absl::StrCat("openssl x509 -req -days 1 -in ", Path(name + ".csr"),
                     " -CA ", Path(ca + ".pem"), " -CAkey ", Path(ca + ".key"),
                     " -CAcreateserial -out ", Path(name + ".crt")));
  }

  TlsConfig ConfigFor(
      const std::string &identity, const std::string &ca,
      TlsConfig::ClientAuth auth = TlsConfig::ClientAuth::kYes) {
    TlsConfig config;
    config.cert_file = config.client_cert_file = Path(identity + ".crt");
    config.key_file = config.client_key_file = Path(identity + ".key");
    config.ca_file = Path(ca + ".pem");
    config.client_auth = auth;
    return config;
  }

  std::unique_ptr<grpc::Server> StartServer(const TlsConfig &config,
                                            int *port) {
    grpc::ServerBuilder builder;
    // A server needs at least one service to start; handshakes are all these
    // tests exercise, so a generic service that is never called will do.
    builder.RegisterCallbackGenericService(&generic_service_);
    builder.AddListeningPort("127.0.0.1:0", MakeTlsServerCredentials(config),
                             port);
    return builder.BuildAndStart();
  }

  bool CanConnect(std::shared_ptr<grpc::ChannelCredentials> creds, int port) {
    auto channel = grpc::CreateChannel(absl::StrCat("127.0.0.1:", port), creds);
    return channel->WaitForConnected(std::chrono::system_clock::now() +
                                     std::chrono::seconds(3));
  }

  grpc::CallbackGenericService generic_service_;
  std::filesystem::path dir_;
};

TEST_F(CoordinatorTlsHandshakeTest, PeersWithSameCaConnect) {
  int port = 0;
  auto server = StartServer(ConfigFor("server", "ca"), &port);
  ASSERT_NE(server, nullptr);
  EXPECT_TRUE(
      CanConnect(MakeTlsChannelCredentials(ConfigFor("client", "ca")), port));
}

TEST_F(CoordinatorTlsHandshakeTest, PlaintextClientIsRejected) {
  int port = 0;
  auto server = StartServer(ConfigFor("server", "ca"), &port);
  ASSERT_NE(server, nullptr);
  EXPECT_FALSE(CanConnect(grpc::InsecureChannelCredentials(), port));
}

TEST_F(CoordinatorTlsHandshakeTest, ClientFromUntrustedCaIsRejected) {
  int port = 0;
  auto server = StartServer(ConfigFor("server", "ca"), &port);
  ASSERT_NE(server, nullptr);
  // Trusts the server's CA, but presents a certificate from another CA.
  auto rogue = ConfigFor("rogue", "ca");
  EXPECT_FALSE(CanConnect(MakeTlsChannelCredentials(rogue), port));
}

TEST_F(CoordinatorTlsHandshakeTest, ServerFromUntrustedCaIsRejected) {
  int port = 0;
  // The server's certificate is from another CA, while the client's own
  // certificate is accepted, so only the client's check of the server can
  // reject this connection.
  auto server = StartServer(ConfigFor("rogue", "ca"), &port);
  ASSERT_NE(server, nullptr);
  EXPECT_FALSE(
      CanConnect(MakeTlsChannelCredentials(ConfigFor("client", "ca")), port));
}

TEST_F(CoordinatorTlsHandshakeTest, ValidationAcceptsUsableFiles) {
  EXPECT_TRUE(ValidateTlsConfig(ConfigFor("server", "ca")).ok());
}

TEST_F(CoordinatorTlsHandshakeTest, ValidationRejectsUnusableFiles) {
  auto missing = ConfigFor("server", "ca");
  missing.cert_file = Path("missing.crt");
  EXPECT_FALSE(ValidateTlsConfig(missing).ok());

  auto not_pem = ConfigFor("server", "ca");
  std::ofstream(Path("garbage.pem")) << "not a certificate";
  not_pem.ca_file = Path("garbage.pem");
  EXPECT_FALSE(ValidateTlsConfig(not_pem).ok());

  // An unusable dedicated client identity is caught even when the server
  // identity is fine.
  auto bad_client = ConfigFor("server", "ca");
  bad_client.client_cert_file = Path("missing.crt");
  EXPECT_FALSE(ValidateTlsConfig(bad_client).ok());
}

// A certificate that is replaced on disk must take effect on the same
// credentials without a restart, which is what keeps pooled channels working
// across rotation.
TEST_F(CoordinatorTlsHandshakeTest, ReplacedCertificateIsPickedUp) {
  auto config = ConfigFor("server", "ca");
  config.cert_file = Path("live.crt");
  config.key_file = Path("live.key");
  config.refresh_interval_secs = 1;
  auto copy = [this](const std::string &from, const std::string &to) {
    std::filesystem::copy_file(
        Path(from), Path(to),
        std::filesystem::copy_options::overwrite_existing);
  };
  // Start with an identity the client does not trust.
  copy("rogue.key", "live.key");
  copy("rogue.crt", "live.crt");

  int port = 0;
  auto server = StartServer(config, &port);
  ASSERT_NE(server, nullptr);
  auto client_creds = MakeTlsChannelCredentials(ConfigFor("client", "ca"));
  EXPECT_FALSE(CanConnect(client_creds, port));

  copy("server.key", "live.key");
  copy("server.crt", "live.crt");
  std::this_thread::sleep_for(std::chrono::seconds(3));
  EXPECT_TRUE(CanConnect(client_creds, port));
}

}  // namespace
}  // namespace valkey_search::coordinator
