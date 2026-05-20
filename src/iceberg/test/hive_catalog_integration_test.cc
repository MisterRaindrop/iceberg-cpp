/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/// \file hive_catalog_integration_test.cc
/// \brief End-to-end tests that exercise HiveCatalog against a live
///        Hive Metastore + MinIO stack brought up via the docker-compose
///        fixture in `resources/iceberg-hive-fixture/`.
///
/// Compiled only when ICEBERG_BUILD_HIVE_INTEGRATION_TESTS=ON. The tests
/// share the same DockerCompose helper used by the REST integration
/// suite; if the docker stack is unavailable the suite is skipped
/// rather than failing.

#include <unistd.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>

#include <arpa/inet.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "iceberg/catalog/hive/hive_catalog.h"
#include "iceberg/catalog/hive/hive_catalog_properties.h"
#include "iceberg/test/test_resource.h"
#include "iceberg/test/util/docker_compose_util.h"

namespace iceberg::hive {

namespace {

constexpr std::string_view kDockerProjectName = "iceberg-hive-catalog-service";
constexpr std::string_view kHmsHost = "localhost";
constexpr uint16_t kHmsPort = 9083;
constexpr int kReadyMaxRetries = 60;
constexpr std::chrono::milliseconds kReadyRetryDelay{1000};

/// \brief Wait until `port` accepts TCP connections, or fail after a few
///        seconds. Copied from the REST integration test.
bool WaitForPort(uint16_t port) {
  for (int attempt = 0; attempt < kReadyMaxRetries; ++attempt) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return false;
    timeval timeout{.tv_sec = 1, .tv_usec = 0};
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    const bool connected =
        ::connect(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0;
    ::close(sock);
    if (connected) return true;
    std::this_thread::sleep_for(kReadyRetryDelay);
  }
  return false;
}

}  // namespace

class HiveCatalogIntegrationTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    docker_ = std::make_unique<DockerCompose>(
        std::string(kDockerProjectName),
        std::filesystem::path(GetResourcePath("iceberg-hive-fixture")));
    docker_->Up();
    ASSERT_TRUE(WaitForPort(kHmsPort)) << "HMS did not come up on port " << kHmsPort;
  }

  static void TearDownTestSuite() {
    if (docker_) {
      docker_->Down();
      docker_.reset();
    }
  }

  static HiveCatalogProperties MakeProperties(std::string_view io_impl) {
    return HiveCatalogProperties::FromMap(
        {{std::string(HiveCatalogProperties::kUri.key()),
          std::format("thrift://{}:{}", kHmsHost, kHmsPort)},
         {std::string(HiveCatalogProperties::kName.key()), "hive_integration"},
         {std::string(HiveCatalogProperties::kIOImpl.key()), std::string(io_impl)},
         {std::string(HiveCatalogProperties::kWarehouse.key()), "file:///tmp/iceberg"}});
  }

 private:
  static std::unique_ptr<DockerCompose> docker_;
};

std::unique_ptr<DockerCompose> HiveCatalogIntegrationTest::docker_;  // definition

TEST_F(HiveCatalogIntegrationTest, CatalogCanConnect) {
  auto catalog = HiveCatalog::Make(MakeProperties("arrow-fs-local"));
  ASSERT_TRUE(catalog.has_value()) << catalog.error().message;
  EXPECT_FALSE((*catalog)->name().empty());
}

TEST_F(HiveCatalogIntegrationTest, NamespaceCrudRoundTrip) {
  auto catalog_result = HiveCatalog::Make(MakeProperties("arrow-fs-local"));
  ASSERT_TRUE(catalog_result.has_value()) << catalog_result.error().message;
  auto& catalog = *catalog_result;

  Namespace ns{.levels = {"iceberg_cpp_it_ns"}};

  // Cleanup from any prior aborted run.
  (void)catalog->DropNamespace(ns);

  ASSERT_TRUE(catalog->CreateNamespace(ns, {{"owner", "iceberg-cpp"}}).has_value());

  auto exists = catalog->NamespaceExists(ns);
  ASSERT_TRUE(exists.has_value()) << exists.error().message;
  EXPECT_TRUE(*exists);

  auto props = catalog->GetNamespaceProperties(ns);
  ASSERT_TRUE(props.has_value()) << props.error().message;
  EXPECT_EQ(props->at("owner"), "iceberg-cpp");

  auto namespaces = catalog->ListNamespaces(Namespace{});
  ASSERT_TRUE(namespaces.has_value()) << namespaces.error().message;
  EXPECT_THAT(*namespaces, ::testing::Contains(ns));

  ASSERT_TRUE(catalog
                  ->UpdateNamespaceProperties(ns, /*updates=*/{{"team", "data"}},
                                              /*removals=*/{"owner"})
                  .has_value());
  auto updated_props = catalog->GetNamespaceProperties(ns);
  ASSERT_TRUE(updated_props.has_value()) << updated_props.error().message;
  EXPECT_EQ(updated_props->at("team"), "data");
  EXPECT_FALSE(updated_props->contains("owner"));

  ASSERT_TRUE(catalog->DropNamespace(ns).has_value());
}

TEST_F(HiveCatalogIntegrationTest, TableExistsReturnsFalseForMissing) {
  auto catalog_result = HiveCatalog::Make(MakeProperties("arrow-fs-local"));
  ASSERT_TRUE(catalog_result.has_value()) << catalog_result.error().message;
  auto& catalog = *catalog_result;

  Namespace ns{.levels = {"iceberg_cpp_it_ns"}};
  (void)catalog->DropNamespace(ns);
  ASSERT_TRUE(catalog->CreateNamespace(ns, {}).has_value());

  auto exists = catalog->TableExists(TableIdentifier{.ns = ns, .name = "does_not_exist"});
  ASSERT_TRUE(exists.has_value()) << exists.error().message;
  EXPECT_FALSE(*exists);

  auto tables = catalog->ListTables(ns);
  ASSERT_TRUE(tables.has_value()) << tables.error().message;
  EXPECT_TRUE(tables->empty());

  ASSERT_TRUE(catalog->DropNamespace(ns).has_value());
}

}  // namespace iceberg::hive
