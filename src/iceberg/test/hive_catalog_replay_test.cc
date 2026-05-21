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
#include <nlohmann/json.hpp>

#include "iceberg/catalog/hive/hive_catalog.h"
#include "iceberg/catalog/hive/hive_catalog_properties.h"
#include "iceberg/catalog/hive/hive_errors.h"
#include "iceberg/catalog/hive/hive_utils.h"
#include "iceberg/catalog/hive/hms_client.h"
#include "iceberg/catalog/hive/hms_client_pool.h"
#include "iceberg/file_io.h"
#include "iceberg/json_serde_internal.h"
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_metadata.h"
#include "iceberg/type.h"

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
  void ScriptAlterTable(Status s) { alter_table_q_.push_back(std::move(s)); }
  void ScriptGetDatabase(Result<HiveDatabase> r) {
    get_database_q_.push_back(std::move(r));
  }
  void ScriptCreateDatabase(Status s) { create_database_q_.push_back(std::move(s)); }
  void ScriptDropDatabase(Status s) { drop_database_q_.push_back(std::move(s)); }
  void ScriptAlterDatabase(Status s) { alter_database_q_.push_back(std::move(s)); }

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
  Status AlterTable(std::string_view, std::string_view, const HiveTable&) override {
    ++alter_table_calls_;
    return PopOrDefault(alter_table_q_, Status{});
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
  Status AlterDatabase(std::string_view, const HiveDatabase&) override {
    ++alter_database_calls_;
    return PopOrDefault(alter_database_q_, Status{});
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
  std::deque<Status> alter_table_q_;
  std::deque<Result<HiveDatabase>> get_database_q_;
  std::deque<Status> create_database_q_;
  std::deque<Status> drop_database_q_;
  std::deque<Status> alter_database_q_;
  int get_table_calls_ = 0;
  int create_table_calls_ = 0;
  int drop_table_calls_ = 0;
  int alter_table_calls_ = 0;
  int create_database_calls_ = 0;
  int drop_database_calls_ = 0;
  int alter_database_calls_ = 0;
};

// Minimal FileIO that returns a single canned blob for ReadFile. Lets
// the RegisterTable test exercise the HMS replay path without dragging
// arrow's filesystem layer (or actual disk) into hive_catalog_test.
class FakeFileIO : public FileIO {
 public:
  explicit FakeFileIO(std::string content) : content_(std::move(content)) {}

  Result<std::string> ReadFile(const std::string&, std::optional<size_t>) override {
    return content_;
  }

 private:
  std::string content_;
};

// Serialize a small but schema-valid TableMetadata to JSON so the
// RegisterTable test can hand it to FakeFileIO and the production
// `TableMetadataUtil::Read` accepts it.
std::string MinimalMetadataJson(std::string metadata_location) {
  std::vector<SchemaField> fields;
  fields.emplace_back(/*field_id=*/1, "id", iceberg::int64(),
                      /*optional=*/false);
  auto schema = std::make_shared<Schema>(std::move(fields), /*schema_id=*/0);
  TableMetadata metadata{
      .format_version = 2,
      .table_uuid = "f0000000-0000-0000-0000-000000000001",
      .location = "file:///tmp/iceberg",
      .last_sequence_number = 0,
      .schemas = {schema},
      .current_schema_id = 0,
      .partition_specs = {PartitionSpec::Unpartitioned()},
      .default_spec_id = 0,
      .last_partition_id = 0,
      .sort_orders = {SortOrder::Unsorted()},
      .default_sort_order_id = 0,
      .next_row_id = 0,
  };
  (void)metadata_location;  // location is encoded by the FakeFileIO mapping
  auto json = ToJson(metadata);
  auto str = ToJsonString(json);
  return str.has_value() ? std::move(*str) : std::string{"{}"};
}

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
// IOError so unexpected pool behaviour surfaces loudly. `file_io` is
// only required for ops that touch storage (CreateTable / UpdateTable
// / RegisterTable); the namespace ops and DropTable / RenameTable
// stay HMS-only so a null FileIO is fine for those tests.
std::shared_ptr<HiveCatalog> MakeTestCatalog(
    std::unique_ptr<HmsClient> first,
    std::deque<Result<std::unique_ptr<HmsClient>>> factory_queue = {},
    std::shared_ptr<FileIO> file_io = nullptr) {
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
                                     std::move(pool), std::move(file_io));
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

// ----- UpdateNamespaceProperties ------------------------------------

TEST(HiveCatalogReplay, UpdateNamespacePropertiesPreReadTransportStaysRetriable) {
  // Pool sees `kServiceUnavailable` from the GetDatabase pre-read on
  // BOTH attempts (initial + reconnect retry). Because the mutating
  // AlterDatabase was never reached, the captured `mutation_attempted`
  // flag stays false, the helper passes the result through unchanged,
  // and the caller still sees `kServiceUnavailable` -- safely retriable.
  auto first = std::make_unique<FakeHmsClient>();
  first->ScriptGetDatabase(ServiceUnavailable("pre-read transport blip 1"));

  auto second = std::make_unique<FakeHmsClient>();
  second->ScriptGetDatabase(ServiceUnavailable("pre-read transport blip 2"));

  std::deque<Result<std::unique_ptr<HmsClient>>> factory;
  factory.push_back(std::move(second));
  auto catalog = MakeTestCatalog(std::move(first), std::move(factory));

  auto status =
      catalog->UpdateNamespaceProperties(Namespace{{std::string(kDb)}}, {{"x", "1"}}, {});
  ASSERT_FALSE(status.has_value());
  EXPECT_EQ(status.error().kind, ErrorKind::kServiceUnavailable)
      << "AlterDatabase was never issued; the helper must NOT convert to "
         "kCommitStateUnknown";
}

// ----- RenameTable ---------------------------------------------------

TEST(HiveCatalogReplay, RenameTableTargetUnverifiedYieldsCommitStateUnknown) {
  // First lambda: GetTable(from) ok, AlterTable transport-fails. Pool
  // retries. Second lambda: GetTable(from) returns NoSuchTable (the
  // first rename may have landed), so the recovery path queries
  // GetTable(to). That GetTable transport-fails -- we cannot confirm
  // the destination carries our metadata_location. Without the
  // tighter recovery added in Fix #39 this would surface the source's
  // NoSuchTable; now it must be `kCommitStateUnknown`.
  auto first = std::make_unique<FakeHmsClient>();
  first->ScriptGetTable(MakeIcebergRow(std::string(kDb), "src", "loc/v1.metadata.json"));
  first->ScriptAlterTable(ServiceUnavailable("transport closed mid-rename"));

  auto second = std::make_unique<FakeHmsClient>();
  second->ScriptGetTable(NoSuchTable("source renamed away or dropped"));
  second->ScriptGetTable(ServiceUnavailable("could not verify target"));

  std::deque<Result<std::unique_ptr<HmsClient>>> factory;
  factory.push_back(std::move(second));
  auto catalog = MakeTestCatalog(std::move(first), std::move(factory));

  auto status = catalog->RenameTable(
      TableIdentifier{.ns = Namespace{{std::string(kDb)}}, .name = "src"},
      TableIdentifier{.ns = Namespace{{std::string(kDb)}}, .name = "dst"});
  ASSERT_FALSE(status.has_value());
  EXPECT_EQ(status.error().kind, ErrorKind::kCommitStateUnknown);
}

// ----- RegisterTable -------------------------------------------------

TEST(HiveCatalogReplay, RegisterTableVerifyFailureYieldsCommitStateUnknown) {
  // First lambda: CreateTable transport-fails. Pool retries. Second
  // lambda: CreateTable returns AlreadyExists (our first attempt may
  // have landed), so the recovery path queries GetTable to compare
  // metadata_location. That GetTable fails with a non-NoSuchTable
  // error -- verification is impossible. Per Fix #38 this must
  // surface as `kCommitStateUnknown` rather than the raw AlreadyExists
  // a foreign-row collision would produce.
  constexpr std::string_view kMetadataLoc = "file:///tmp/iceberg/00000-xxx.metadata.json";
  auto file_io =
      std::make_shared<FakeFileIO>(MinimalMetadataJson(std::string(kMetadataLoc)));

  auto first = std::make_unique<FakeHmsClient>();
  first->ScriptCreateTable(ServiceUnavailable("transport closed mid-register"));

  auto second = std::make_unique<FakeHmsClient>();
  second->ScriptCreateTable(AlreadyExists("table already exists"));
  second->ScriptGetTable(
      MetaError("get_table", "HMS internal error during verification"));

  std::deque<Result<std::unique_ptr<HmsClient>>> factory;
  factory.push_back(std::move(second));
  auto catalog = MakeTestCatalog(std::move(first), std::move(factory), file_io);

  auto result = catalog->RegisterTable(
      TableIdentifier{.ns = Namespace{{std::string(kDb)}}, .name = std::string(kTable)},
      std::string(kMetadataLoc));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().kind, ErrorKind::kCommitStateUnknown);
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
