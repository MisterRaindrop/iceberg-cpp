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

/// \file hive_catalog_replay_test.cc
/// \brief Replay-recovery unit tests for HiveCatalog ops.
///
/// HmsClientPool::Run retries a transport failure exactly once. Each
/// mutating HiveCatalog op carries per-op recovery logic (captured
/// `*_attempted` flags, metadata_location identity checks, etc.) so the
/// caller does not see phantom AlreadyExists / NoSuchTable / etc. for
/// a first attempt whose reply was lost. These tests drive the actual
/// HiveCatalog code path against a scripted `FakeHmsClient` that lets
/// each RPC return a predetermined Result (success, NoSuchTable,
/// AlreadyExists, ServiceUnavailable). The pool factory is wired up so
/// the second attempt receives a different client -- mirroring the
/// production reconnect behavior without touching Thrift.

#include <deque>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/catalog/hive/hive_catalog.h"
#include "iceberg/catalog/hive/hive_catalog_properties.h"
#include "iceberg/catalog/hive/hive_utils.h"
#include "iceberg/catalog/hive/hms_client.h"
#include "iceberg/catalog/hive/hms_client_pool.h"
#include "iceberg/result.h"

namespace iceberg::hive {

namespace {

// A HmsClient subclass whose every RPC pops the next scripted Result
// from a per-method queue. Tests construct the response sequence the
// HiveCatalog op should observe across the two attempts the pool may
// make; the test then asks the catalog to perform the op and asserts
// on the surfaced error kind.
//
// The fake intentionally avoids the production HmsClient::Impl Thrift
// state -- it inherits via the protected default constructor added for
// exactly this case.
class FakeHmsClient : public HmsClient {
 public:
  FakeHmsClient() = default;

  // Scripting API ---------------------------------------------------
  void ScriptGetTable(Result<HiveTable> r) { get_table_q_.push_back(std::move(r)); }
  void ScriptCreateTable(Status s) { create_table_q_.push_back(std::move(s)); }
  void ScriptDropTable(Status s) { drop_table_q_.push_back(std::move(s)); }
  void ScriptGetDatabase(Result<HiveDatabase> r) {
    get_database_q_.push_back(std::move(r));
  }
  void ScriptCreateDatabase(Status s) { create_database_q_.push_back(std::move(s)); }
  void ScriptDropDatabase(Status s) { drop_database_q_.push_back(std::move(s)); }

  // Observe how many times each scripted method was called. Useful to
  // confirm the per-op `attempted` flags advance correctly.
  int get_table_calls() const { return get_table_calls_; }
  int drop_table_calls() const { return drop_table_calls_; }
  int create_database_calls() const { return create_database_calls_; }
  int drop_database_calls() const { return drop_database_calls_; }
  int create_table_calls() const { return create_table_calls_; }

  // HmsClient overrides --------------------------------------------
  Result<HiveTable> GetTable(std::string_view, std::string_view) override {
    ++get_table_calls_;
    return PopOrDefault(get_table_q_, NoSuchTableDefault());
  }
  Status CreateTable(const HiveTable&) override {
    ++create_table_calls_;
    return PopOrDefault(create_table_q_, Status{});
  }
  Status DropTable(std::string_view, std::string_view, bool) override {
    ++drop_table_calls_;
    return PopOrDefault(drop_table_q_, Status{});
  }
  Result<HiveDatabase> GetDatabase(std::string_view) override {
    return PopOrDefault(get_database_q_, NoSuchNamespaceDefault());
  }
  Status CreateDatabase(const HiveDatabase&) override {
    ++create_database_calls_;
    return PopOrDefault(create_database_q_, Status{});
  }
  Status DropDatabase(std::string_view, bool) override {
    ++drop_database_calls_;
    return PopOrDefault(drop_database_q_, Status{});
  }

 private:
  template <typename T>
  static T PopOrDefault(std::deque<T>& q, T fallback) {
    if (q.empty()) return fallback;
    T r = std::move(q.front());
    q.pop_front();
    return r;
  }
  static Result<HiveTable> NoSuchTableDefault() {
    return NoSuchTable("FakeHmsClient: unscripted GetTable call");
  }
  static Result<HiveDatabase> NoSuchNamespaceDefault() {
    return NoSuchNamespace("FakeHmsClient: unscripted GetDatabase call");
  }

  std::deque<Result<HiveTable>> get_table_q_;
  std::deque<Status> create_table_q_;
  std::deque<Status> drop_table_q_;
  std::deque<Result<HiveDatabase>> get_database_q_;
  std::deque<Status> create_database_q_;
  std::deque<Status> drop_database_q_;
  int get_table_calls_ = 0;
  int create_table_calls_ = 0;
  int drop_table_calls_ = 0;
  int create_database_calls_ = 0;
  int drop_database_calls_ = 0;
};

// Helper: produce a HiveTable that looks like a registered Iceberg
// table with the supplied metadata_location.
HiveTable MakeIcebergRow(std::string db, std::string name, std::string loc) {
  HiveTable t;
  t.db_name = std::move(db);
  t.table_name = std::move(name);
  t.table_type = "EXTERNAL_TABLE";
  t.parameters[std::string(kMetadataLocationKey)] = std::move(loc);
  t.parameters[std::string(kTableTypeKey)] = std::string(kTableTypeIceberg);
  t.parameters[std::string(kExternalKey)] = std::string(kExternalTrue);
  return t;
}

// Build a HiveCatalog whose pool's first checkout returns `first` and
// every subsequent factory call (lazy fill OR Reconnect) pops from
// `factory_queue`. When the queue runs out the factory returns an
// IOError so unexpected pool behaviour surfaces loudly.
std::shared_ptr<HiveCatalog> MakeTestCatalog(
    std::unique_ptr<HmsClient> first,
    std::deque<Result<std::unique_ptr<HmsClient>>> factory_queue = {}) {
  auto queue = std::make_shared<std::deque<Result<std::unique_ptr<HmsClient>>>>(
      std::move(factory_queue));
  HmsClientPool::ClientFactory factory = [queue]() -> Result<std::unique_ptr<HmsClient>> {
    if (queue->empty()) {
      return IOError("test factory exhausted; unexpected reconnect attempt");
    }
    auto next = std::move(queue->front());
    queue->pop_front();
    return next;
  };
  auto pool = HmsClientPool::MakeForTesting(/*pool_size=*/1, std::move(factory),
                                            std::move(first));
  return HiveCatalog::MakeForTesting(HiveCatalogProperties::default_properties(),
                                     std::move(pool), /*file_io=*/nullptr);
}

constexpr std::string_view kDb = "warehouse";
constexpr std::string_view kTable = "orders";

}  // namespace

// ----- DropTable -----------------------------------------------------

TEST(HiveCatalogReplay, DropTableSecondAttemptSeesNoSuchTableAfterServerSucceeded) {
  // First lambda invocation: GetTable ok, then the DropTable RPC's
  // transport drops (kServiceUnavailable). Pool retries with a fresh
  // client whose GetTable returns NoSuchTable -- our previous drop
  // landed. The captured `drop_attempted` flag persists across the
  // two invocations, so the recovery branch fires and the catalog
  // returns success rather than the misleading NoSuchTable.
  auto first = std::make_unique<FakeHmsClient>();
  first->ScriptGetTable(
      MakeIcebergRow(std::string(kDb), std::string(kTable), "loc/v1.metadata.json"));
  first->ScriptDropTable(ServiceUnavailable("transport closed mid-drop"));

  auto second = std::make_unique<FakeHmsClient>();
  second->ScriptGetTable(NoSuchTable("table was dropped on previous attempt"));

  std::deque<Result<std::unique_ptr<HmsClient>>> factory;
  factory.push_back(std::move(second));
  auto catalog = MakeTestCatalog(std::move(first), std::move(factory));

  auto status = catalog->DropTable(
      TableIdentifier{.ns = Namespace{{std::string(kDb)}}, .name = std::string(kTable)},
      /*purge=*/false);
  EXPECT_TRUE(status.has_value()) << "expected the replay branch to declare success";
}

TEST(HiveCatalogReplay, DropTablePropagatesGenuineNoSuchTableOnFirstAttempt) {
  // The captured `drop_attempted` flag is still false when GetTable
  // returns NoSuchTable on the very first call, so the recovery
  // branch must NOT fire -- the table genuinely never existed.
  auto first = std::make_unique<FakeHmsClient>();
  first->ScriptGetTable(NoSuchTable("table genuinely missing"));
  auto catalog = MakeTestCatalog(std::move(first));

  auto status = catalog->DropTable(
      TableIdentifier{.ns = Namespace{{std::string(kDb)}}, .name = std::string(kTable)},
      /*purge=*/false);
  ASSERT_FALSE(status.has_value());
  EXPECT_EQ(status.error().kind, ErrorKind::kNoSuchTable);
}

TEST(HiveCatalogReplay, DropTablePreReadTransportFailureStaysRetriable) {
  // Both lambda invocations transport-fail during the pre-read
  // GetTable. The mutation never happened, so the helper must
  // preserve `kServiceUnavailable` -- conversion to
  // `kCommitStateUnknown` would block legitimate retries.
  auto first = std::make_unique<FakeHmsClient>();
  first->ScriptGetTable(ServiceUnavailable("transport blip 1"));

  auto second = std::make_unique<FakeHmsClient>();
  second->ScriptGetTable(ServiceUnavailable("transport blip 2"));

  std::deque<Result<std::unique_ptr<HmsClient>>> factory;
  factory.push_back(std::move(second));
  auto catalog = MakeTestCatalog(std::move(first), std::move(factory));

  auto status = catalog->DropTable(
      TableIdentifier{.ns = Namespace{{std::string(kDb)}}, .name = std::string(kTable)},
      /*purge=*/false);
  ASSERT_FALSE(status.has_value());
  EXPECT_EQ(status.error().kind, ErrorKind::kServiceUnavailable)
      << "pre-mutation transport failure must remain retriable";
}

// ----- CreateNamespace ----------------------------------------------

TEST(HiveCatalogReplay, CreateNamespaceRetryHittingAlreadyExistsIsCommitStateUnknown) {
  // First lambda invocation: CreateDatabase transport-fails. Pool
  // reconnects. Second invocation: CreateDatabase returns
  // AlreadyExists. We cannot prove the row is OURS (namespace
  // creation has no unique identity token), so the recovery returns
  // kCommitStateUnknown rather than masquerading as a fresh
  // AlreadyExists collision.
  auto first = std::make_unique<FakeHmsClient>();
  first->ScriptCreateDatabase(ServiceUnavailable("transport closed mid-create"));

  auto second = std::make_unique<FakeHmsClient>();
  second->ScriptCreateDatabase(AlreadyExists("warehouse already exists"));

  std::deque<Result<std::unique_ptr<HmsClient>>> factory;
  factory.push_back(std::move(second));
  auto catalog = MakeTestCatalog(std::move(first), std::move(factory));

  auto status =
      catalog->CreateNamespace(Namespace{{std::string(kDb)}}, {{"comment", "test"}});
  ASSERT_FALSE(status.has_value());
  EXPECT_EQ(status.error().kind, ErrorKind::kCommitStateUnknown);
}

TEST(HiveCatalogReplay, CreateNamespaceFirstAttemptAlreadyExistsIsForeignCollision) {
  // First lambda invocation: CreateDatabase returns AlreadyExists
  // immediately. `create_attempted` is still false, so the recovery
  // branch must NOT fire -- propagate AlreadyExists verbatim.
  auto first = std::make_unique<FakeHmsClient>();
  first->ScriptCreateDatabase(AlreadyExists("foreign db wins the race"));
  auto catalog = MakeTestCatalog(std::move(first));

  auto status = catalog->CreateNamespace(Namespace{{std::string(kDb)}}, {});
  ASSERT_FALSE(status.has_value());
  EXPECT_EQ(status.error().kind, ErrorKind::kAlreadyExists);
}

// ----- DropNamespace -------------------------------------------------

TEST(HiveCatalogReplay, DropNamespaceTrulyMissingPropagatesNoSuchNamespace) {
  // The GetDatabase-first guard means a never-existed namespace stays
  // NoSuchNamespace even after pool reconnects -- the recovery branch
  // is gated on having SEEN the database exist first.
  auto first = std::make_unique<FakeHmsClient>();
  first->ScriptGetDatabase(NoSuchNamespace("never existed"));
  auto catalog = MakeTestCatalog(std::move(first));

  auto status = catalog->DropNamespace(Namespace{{std::string(kDb)}});
  ASSERT_FALSE(status.has_value());
  EXPECT_EQ(status.error().kind, ErrorKind::kNoSuchNamespace);
}

TEST(HiveCatalogReplay, DropNamespaceSecondAttemptRecoversAfterMutationLanded) {
  // First lambda invocation: GetDatabase ok, DropDatabase
  // transport-fails. Pool reconnects. Second invocation: GetDatabase
  // returns NoSuchNamespace -- our drop landed. The captured
  // `drop_attempted` (set true after the successful GetDatabase)
  // persists across attempts, so the retry's NoSuchNamespace counts
  // as a confirmed-success.
  HiveDatabase db;
  db.name = std::string(kDb);
  auto first = std::make_unique<FakeHmsClient>();
  first->ScriptGetDatabase(db);
  first->ScriptDropDatabase(ServiceUnavailable("transport closed mid-drop"));

  auto second = std::make_unique<FakeHmsClient>();
  second->ScriptGetDatabase(NoSuchNamespace("ns dropped on previous attempt"));

  std::deque<Result<std::unique_ptr<HmsClient>>> factory;
  factory.push_back(std::move(second));
  auto catalog = MakeTestCatalog(std::move(first), std::move(factory));

  auto status = catalog->DropNamespace(Namespace{{std::string(kDb)}});
  EXPECT_TRUE(status.has_value());
}

}  // namespace iceberg::hive
