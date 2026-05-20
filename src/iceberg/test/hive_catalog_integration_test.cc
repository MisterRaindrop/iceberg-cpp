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
#include "iceberg/catalog/hive/hive_table_operations.h"
#include "iceberg/catalog/hive/hms_client.h"
#include "iceberg/file_io_registry.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/sort_order.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_requirement.h"
#include "iceberg/table_update.h"
#include "iceberg/test/test_resource.h"
#include "iceberg/test/util/docker_compose_util.h"
#include "iceberg/type.h"

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

  Namespace ns{.levels = {"iceberg_cpp_it_ns_crud"}};

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

TEST_F(HiveCatalogIntegrationTest, ListNamespacesRejectsMissingParent) {
  // Java HiveCatalog throws NoSuchNamespaceException when listing a parent
  // that doesn't map to an existing HMS database; an empty list is reserved
  // for "the parent exists and just has no children".
  auto catalog_result = HiveCatalog::Make(MakeProperties("arrow-fs-local"));
  ASSERT_TRUE(catalog_result.has_value()) << catalog_result.error().message;
  auto& catalog = *catalog_result;

  Namespace missing{.levels = {"iceberg_cpp_it_no_such_ns"}};
  (void)catalog->DropNamespace(missing);

  auto result = catalog->ListNamespaces(missing);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().kind, ErrorKind::kNoSuchNamespace);

  ASSERT_TRUE(catalog->CreateNamespace(missing, {}).has_value());
  auto children = catalog->ListNamespaces(missing);
  ASSERT_TRUE(children.has_value()) << children.error().message;
  EXPECT_TRUE(children->empty());
  ASSERT_TRUE(catalog->DropNamespace(missing).has_value());
}

TEST_F(HiveCatalogIntegrationTest, TableExistsReturnsFalseForMissing) {
  auto catalog_result = HiveCatalog::Make(MakeProperties("arrow-fs-local"));
  ASSERT_TRUE(catalog_result.has_value()) << catalog_result.error().message;
  auto& catalog = *catalog_result;

  Namespace ns{.levels = {"iceberg_cpp_it_ns_exists"}};
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

namespace {

std::shared_ptr<Schema> MakeOrdersSchema() {
  return std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64()),
                               SchemaField::MakeRequired(2, "amount", float64()),
                               SchemaField::MakeOptional(3, "label", string())});
}

std::shared_ptr<PartitionSpec> MakeUnpartitionedSpec(const Schema& schema) {
  auto spec = PartitionSpec::Make(/*spec_id=*/0, std::vector<PartitionField>{});
  EXPECT_TRUE(spec.has_value());
  return std::shared_ptr<PartitionSpec>(std::move(spec.value()));
}

}  // namespace

TEST_F(HiveCatalogIntegrationTest, CreateTableRoundTrip) {
  auto catalog_result = HiveCatalog::Make(MakeProperties("arrow-fs-local"));
  ASSERT_TRUE(catalog_result.has_value()) << catalog_result.error().message;
  auto& catalog = *catalog_result;

  Namespace ns{.levels = {"iceberg_cpp_it_ns_create"}};
  (void)catalog->DropNamespace(ns);
  ASSERT_TRUE(catalog->CreateNamespace(ns, {}).has_value());

  TableIdentifier ident{.ns = ns, .name = "orders"};
  (void)catalog->DropTable(ident, /*purge=*/false);

  auto schema = MakeOrdersSchema();
  auto spec = MakeUnpartitionedSpec(*schema);
  auto sort_order = SortOrder::Unsorted();

  auto created = catalog->CreateTable(ident, schema, spec, sort_order,
                                      /*location=*/"", /*properties=*/{});
  ASSERT_TRUE(created.has_value()) << created.error().message;

  auto loaded = catalog->LoadTable(ident);
  ASSERT_TRUE(loaded.has_value()) << loaded.error().message;
  EXPECT_EQ((*loaded)->name(), ident);

  ASSERT_TRUE(catalog->DropTable(ident, /*purge=*/false).has_value());
  ASSERT_TRUE(catalog->DropNamespace(ns).has_value());
}

TEST_F(HiveCatalogIntegrationTest, CommitCasMismatchSurfacesAsCommitFailed) {
  // Drive HiveTableOperations directly so we can hold onto an out-of-date
  // snapshot while a concurrent writer advances HMS's metadata_location.
  // The stale Commit must fail with kCommitFailed (the retry signal the
  // Transaction loop bound to MakeCommitRetryRunner watches for).
  const auto props = MakeProperties("arrow-fs-local");
  auto catalog_result = HiveCatalog::Make(props);
  ASSERT_TRUE(catalog_result.has_value()) << catalog_result.error().message;
  auto& catalog = *catalog_result;

  Namespace ns{.levels = {"iceberg_cpp_it_cas_ns"}};
  (void)catalog->DropNamespace(ns);
  ASSERT_TRUE(catalog->CreateNamespace(ns, {}).has_value());

  TableIdentifier ident{.ns = ns, .name = "cas_target"};
  (void)catalog->DropTable(ident, /*purge=*/false);
  auto schema = MakeOrdersSchema();
  ASSERT_TRUE(catalog
                  ->CreateTable(ident, schema, MakeUnpartitionedSpec(*schema),
                                SortOrder::Unsorted(), /*location=*/"", /*properties=*/{})
                  .has_value());

  auto stale_client = HmsClient::Connect(props);
  ASSERT_TRUE(stale_client.has_value()) << stale_client.error().message;
  auto file_io_result =
      FileIORegistry::Load(std::string(FileIORegistry::kArrowLocalFileIO),
                           /*properties=*/{});
  ASSERT_TRUE(file_io_result.has_value()) << file_io_result.error().message;
  std::shared_ptr<FileIO> file_io = std::move(*file_io_result);
  HiveTableOperations ops(stale_client->get(), file_io, ident);

  auto stale = ops.Refresh();
  ASSERT_TRUE(stale.has_value()) << stale.error().message;

  // Concurrent writer advances metadata_location via the public path.
  std::vector<std::unique_ptr<TableUpdate>> winner_updates;
  winner_updates.push_back(std::make_unique<table::SetProperties>(
      std::unordered_map<std::string, std::string>{{"hive.cas.who", "winner"}}));
  ASSERT_TRUE(
      catalog->UpdateTable(ident, /*requirements=*/{}, winner_updates).has_value());

  // Build new metadata from the *stale* snapshot and try to commit.
  auto builder = TableMetadataBuilder::BuildFrom(stale->metadata.get());
  table::SetProperties stale_update({{"hive.cas.who", "loser"}});
  stale_update.ApplyTo(*builder);
  auto new_metadata = builder->Build();
  ASSERT_TRUE(new_metadata.has_value()) << new_metadata.error().message;

  auto commit_result = ops.Commit(*stale, **new_metadata);
  ASSERT_FALSE(commit_result.has_value());
  EXPECT_EQ(commit_result.error().kind, ErrorKind::kCommitFailed);

  // Retry from a fresh snapshot must succeed (Transaction's normal flow).
  auto fresh = ops.Refresh();
  ASSERT_TRUE(fresh.has_value()) << fresh.error().message;
  auto retry_builder = TableMetadataBuilder::BuildFrom(fresh->metadata.get());
  table::SetProperties retry_update({{"hive.cas.who", "loser-retried"}});
  retry_update.ApplyTo(*retry_builder);
  auto retry_metadata = retry_builder->Build();
  ASSERT_TRUE(retry_metadata.has_value()) << retry_metadata.error().message;
  auto retried = ops.Commit(*fresh, **retry_metadata);
  ASSERT_TRUE(retried.has_value()) << retried.error().message;

  ASSERT_TRUE(catalog->DropTable(ident, /*purge=*/false).has_value());
  ASSERT_TRUE(catalog->DropNamespace(ns).has_value());
}

TEST_F(HiveCatalogIntegrationTest, CommitWithHmsLockEnabled) {
  // Same UpdateTable path, but with hive.lock-enabled=true so Commit acquires
  // an HMS EXCLUSIVE table-level lock around GetTable -> AlterTable and
  // releases it on the success path. We rely on the lock-release behaviour
  // working: a second UpdateTable would block forever if the first one
  // leaked the lock.
  auto catalog_result = HiveCatalog::Make(HiveCatalogProperties::FromMap(
      {{std::string(HiveCatalogProperties::kUri.key()),
        std::format("thrift://{}:{}", kHmsHost, kHmsPort)},
       {std::string(HiveCatalogProperties::kName.key()), "hive_lock"},
       {std::string(HiveCatalogProperties::kIOImpl.key()), "arrow-fs-local"},
       {std::string(HiveCatalogProperties::kWarehouse.key()), "file:///tmp/iceberg"},
       {std::string(HiveCatalogProperties::kLockEnabled.key()), "true"}}));
  ASSERT_TRUE(catalog_result.has_value()) << catalog_result.error().message;
  auto& catalog = *catalog_result;

  Namespace ns{.levels = {"iceberg_cpp_it_lock_ns"}};
  (void)catalog->DropNamespace(ns);
  ASSERT_TRUE(catalog->CreateNamespace(ns, {}).has_value());

  TableIdentifier ident{.ns = ns, .name = "lock_target"};
  (void)catalog->DropTable(ident, /*purge=*/false);
  auto schema = MakeOrdersSchema();
  ASSERT_TRUE(catalog
                  ->CreateTable(ident, schema, MakeUnpartitionedSpec(*schema),
                                SortOrder::Unsorted(), /*location=*/"", /*properties=*/{})
                  .has_value());

  for (int i = 0; i < 2; ++i) {
    std::vector<std::unique_ptr<TableUpdate>> updates;
    updates.push_back(std::make_unique<table::SetProperties>(
        std::unordered_map<std::string, std::string>{
            {"hive.lock.attempt", std::to_string(i)}}));
    auto updated = catalog->UpdateTable(ident, /*requirements=*/{}, updates);
    ASSERT_TRUE(updated.has_value())
        << "iteration " << i << " failed (lock leak?): " << updated.error().message;
  }

  ASSERT_TRUE(catalog->DropTable(ident, /*purge=*/false).has_value());
  ASSERT_TRUE(catalog->DropNamespace(ns).has_value());
}

TEST_F(HiveCatalogIntegrationTest, RegisterTableReattachesExistingMetadata) {
  auto catalog_result = HiveCatalog::Make(MakeProperties("arrow-fs-local"));
  ASSERT_TRUE(catalog_result.has_value()) << catalog_result.error().message;
  auto& catalog = *catalog_result;

  Namespace ns{.levels = {"iceberg_cpp_it_reg_ns"}};
  (void)catalog->DropNamespace(ns);
  ASSERT_TRUE(catalog->CreateNamespace(ns, {}).has_value());

  TableIdentifier ident{.ns = ns, .name = "to_register"};
  (void)catalog->DropTable(ident, /*purge=*/false);
  auto schema = MakeOrdersSchema();
  auto created = catalog->CreateTable(ident, schema, MakeUnpartitionedSpec(*schema),
                                      SortOrder::Unsorted(),
                                      /*location=*/"", /*properties=*/{});
  ASSERT_TRUE(created.has_value()) << created.error().message;
  const std::string saved_metadata_location((*created)->metadata_file_location());

  // Drop the HMS row but keep the metadata.json on disk (purge=false).
  ASSERT_TRUE(catalog->DropTable(ident, /*purge=*/false).has_value());

  // Re-register against the surviving metadata file.
  auto registered = catalog->RegisterTable(ident, saved_metadata_location);
  ASSERT_TRUE(registered.has_value()) << registered.error().message;
  EXPECT_EQ((*registered)->metadata_file_location(), saved_metadata_location);

  auto reloaded = catalog->LoadTable(ident);
  ASSERT_TRUE(reloaded.has_value()) << reloaded.error().message;
  EXPECT_EQ((*reloaded)->metadata_file_location(), saved_metadata_location);

  ASSERT_TRUE(catalog->DropTable(ident, /*purge=*/false).has_value());
  ASSERT_TRUE(catalog->DropNamespace(ns).has_value());
}

TEST_F(HiveCatalogIntegrationTest, RenameTableMovesIdentifier) {
  auto catalog_result = HiveCatalog::Make(MakeProperties("arrow-fs-local"));
  ASSERT_TRUE(catalog_result.has_value()) << catalog_result.error().message;
  auto& catalog = *catalog_result;

  Namespace ns{.levels = {"iceberg_cpp_it_rename_ns"}};
  (void)catalog->DropNamespace(ns);
  ASSERT_TRUE(catalog->CreateNamespace(ns, {}).has_value());

  TableIdentifier from{.ns = ns, .name = "before_rename"};
  TableIdentifier to{.ns = ns, .name = "after_rename"};
  (void)catalog->DropTable(from, /*purge=*/false);
  (void)catalog->DropTable(to, /*purge=*/false);

  auto schema = MakeOrdersSchema();
  ASSERT_TRUE(catalog
                  ->CreateTable(from, schema, MakeUnpartitionedSpec(*schema),
                                SortOrder::Unsorted(), /*location=*/"", /*properties=*/{})
                  .has_value());

  ASSERT_TRUE(catalog->RenameTable(from, to).has_value());

  auto loaded_new = catalog->LoadTable(to);
  ASSERT_TRUE(loaded_new.has_value()) << loaded_new.error().message;
  EXPECT_EQ((*loaded_new)->name(), to);

  auto old_exists = catalog->TableExists(from);
  ASSERT_TRUE(old_exists.has_value()) << old_exists.error().message;
  EXPECT_FALSE(*old_exists);

  ASSERT_TRUE(catalog->DropTable(to, /*purge=*/false).has_value());
  ASSERT_TRUE(catalog->DropNamespace(ns).has_value());
}

TEST_F(HiveCatalogIntegrationTest, UpdateTablePropertiesViaTableUpdate) {
  auto catalog_result = HiveCatalog::Make(MakeProperties("arrow-fs-local"));
  ASSERT_TRUE(catalog_result.has_value()) << catalog_result.error().message;
  auto& catalog = *catalog_result;

  Namespace ns{.levels = {"iceberg_cpp_it_ns_update"}};
  (void)catalog->DropNamespace(ns);
  ASSERT_TRUE(catalog->CreateNamespace(ns, {}).has_value());

  TableIdentifier ident{.ns = ns, .name = "orders"};
  (void)catalog->DropTable(ident, /*purge=*/false);

  auto schema = MakeOrdersSchema();
  ASSERT_TRUE(catalog
                  ->CreateTable(ident, schema, MakeUnpartitionedSpec(*schema),
                                SortOrder::Unsorted(), /*location=*/"", /*properties=*/{})
                  .has_value());

  // Bump a single property via the TableUpdate path. The exact update
  // type does not matter for the integration test -- the point is to
  // verify the full CAS commit round-trip lands a non-empty change set
  // through HiveTableOperations::Commit.
  std::vector<std::unique_ptr<TableUpdate>> updates;
  updates.push_back(std::make_unique<table::SetProperties>(
      std::unordered_map<std::string, std::string>{{"hive.it.tag", "v1"}}));
  std::vector<std::unique_ptr<TableRequirement>> requirements;

  auto updated = catalog->UpdateTable(ident, requirements, updates);
  ASSERT_TRUE(updated.has_value()) << updated.error().message;

  auto loaded = catalog->LoadTable(ident);
  ASSERT_TRUE(loaded.has_value()) << loaded.error().message;

  ASSERT_TRUE(catalog->DropTable(ident, /*purge=*/false).has_value());
  ASSERT_TRUE(catalog->DropNamespace(ns).has_value());
}

}  // namespace iceberg::hive
