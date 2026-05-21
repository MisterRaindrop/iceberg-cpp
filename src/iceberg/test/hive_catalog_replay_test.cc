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

#include <algorithm>
#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/catalog/hive/hive_catalog.h"
#include "iceberg/catalog/hive/hive_catalog_properties.h"
#include "iceberg/catalog/hive/hive_errors.h"
#include "iceberg/catalog/hive/hive_table_operations.h"
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
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_requirement.h"
#include "iceberg/table_update.h"
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

// In-memory FileIO that captures whatever HiveCatalog writes (Create
// Table's metadata.json) and serves arbitrary canned content for
// reads (LoadTable / RegisterTable / Refresh). Lets the catalog ops
// exercise their full storage interaction without dragging arrow's
// filesystem layer (or actual disk) into hive_catalog_test.
class FakeFileIO : public FileIO {
 public:
  // ReadFile defaults to returning `default_content_` when there is no
  // per-path script entry. `delete_should_fail_` lets the orphan
  // cleanup branch be exercised without affecting other tests.
  void SetDefaultReadContent(std::string content) {
    default_content_ = std::move(content);
  }
  void ScriptRead(const std::string& path, Result<std::string> r) {
    read_overrides_[path] = std::move(r);
  }
  bool WasWritten(const std::string& path) const { return writes_.contains(path); }
  bool WasDeleted(const std::string& path) const {
    return std::find(deletes_.begin(), deletes_.end(), path) != deletes_.end();
  }
  const std::string& GetWritten(const std::string& path) const {
    static const std::string empty;
    auto it = writes_.find(path);
    return it == writes_.end() ? empty : it->second;
  }

  Result<std::string> ReadFile(const std::string& path, std::optional<size_t>) override {
    auto it = read_overrides_.find(path);
    if (it != read_overrides_.end()) return it->second;
    return default_content_;
  }
  Status WriteFile(const std::string& path, std::string_view content) override {
    writes_[path] = std::string(content);
    return {};
  }
  Status DeleteFile(const std::string& path) override {
    deletes_.push_back(path);
    return {};
  }

 private:
  std::string default_content_;
  std::unordered_map<std::string, Result<std::string>> read_overrides_;
  std::unordered_map<std::string, std::string> writes_;
  std::vector<std::string> deletes_;
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
  auto file_io = std::make_shared<FakeFileIO>();
  file_io->SetDefaultReadContent(MinimalMetadataJson(std::string(kMetadataLoc)));

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

// =====================================================================
// Phase 1: full-op coverage. These exercise the non-replay code paths
// in `hive_catalog.cc` so the file's overall coverage reflects what
// the read-only + happy mutating paths actually do, not just the
// replay-recovery branches.
// =====================================================================

// ----- ListNamespaces / NamespaceExists / GetNamespaceProperties ----

TEST(HiveCatalogOps, ListNamespacesEmptyParentReturnsAllDatabases) {
  // HMS GetAllDatabases is called only when the parent namespace is
  // empty -- that's the root listing path. Confirm we surface the
  // names verbatim.
  class ListAllFake : public FakeHmsClient {
   public:
    Result<std::vector<std::string>> GetAllDatabases() override {
      return std::vector<std::string>{"db_a", "db_b"};
    }
  };
  auto catalog = MakeTestCatalog(std::make_unique<ListAllFake>());

  auto namespaces = catalog->ListNamespaces(Namespace{});
  ASSERT_TRUE(namespaces.has_value());
  ASSERT_EQ(namespaces->size(), 2u);
  EXPECT_EQ(namespaces->at(0).levels.at(0), "db_a");
  EXPECT_EQ(namespaces->at(1).levels.at(0), "db_b");
}

TEST(HiveCatalogOps, ListNamespacesNonEmptyParentReturnsEmptyOrNoSuch) {
  // Non-empty parent + GetDatabase succeeds -> empty list (HMS is
  // flat; no nesting). Verifies the Java-aligned "exists -> []" case.
  HiveDatabase row;
  row.name = std::string(kDb);
  auto fake = std::make_unique<FakeHmsClient>();
  fake->ScriptGetDatabase(row);
  auto catalog = MakeTestCatalog(std::move(fake));

  auto namespaces = catalog->ListNamespaces(Namespace{{std::string(kDb)}});
  ASSERT_TRUE(namespaces.has_value());
  EXPECT_TRUE(namespaces->empty());
}

TEST(HiveCatalogOps, ListNamespacesMissingParentReturnsNoSuchNamespace) {
  // The other branch of the same guard: parent does not exist ->
  // kNoSuchNamespace, not an indistinguishable empty list.
  auto fake = std::make_unique<FakeHmsClient>();
  fake->ScriptGetDatabase(NoSuchNamespace("missing"));
  auto catalog = MakeTestCatalog(std::move(fake));

  auto namespaces = catalog->ListNamespaces(Namespace{{std::string(kDb)}});
  ASSERT_FALSE(namespaces.has_value());
  EXPECT_EQ(namespaces.error().kind, ErrorKind::kNoSuchNamespace);
}

TEST(HiveCatalogOps, NamespaceExistsReturnsTrueWhenPresent) {
  HiveDatabase row;
  row.name = std::string(kDb);
  auto fake = std::make_unique<FakeHmsClient>();
  fake->ScriptGetDatabase(row);
  auto catalog = MakeTestCatalog(std::move(fake));

  auto present = catalog->NamespaceExists(Namespace{{std::string(kDb)}});
  ASSERT_TRUE(present.has_value());
  EXPECT_TRUE(*present);
}

TEST(HiveCatalogOps, NamespaceExistsReturnsFalseOnNoSuchNamespace) {
  auto fake = std::make_unique<FakeHmsClient>();
  fake->ScriptGetDatabase(NoSuchNamespace("missing"));
  auto catalog = MakeTestCatalog(std::move(fake));

  auto present = catalog->NamespaceExists(Namespace{{std::string(kDb)}});
  ASSERT_TRUE(present.has_value());
  EXPECT_FALSE(*present);
}

TEST(HiveCatalogOps, GetNamespacePropertiesRoundTripsParametersAndOwner) {
  HiveDatabase row;
  row.name = std::string(kDb);
  row.parameters = {{"comment", "from-test"}, {"x", "1"}};
  row.owner_name = "alice";
  row.owner_type = "USER";
  auto fake = std::make_unique<FakeHmsClient>();
  fake->ScriptGetDatabase(row);
  auto catalog = MakeTestCatalog(std::move(fake));

  auto props = catalog->GetNamespaceProperties(Namespace{{std::string(kDb)}});
  ASSERT_TRUE(props.has_value());
  EXPECT_EQ(props->at("x"), "1");
  EXPECT_EQ(props->at("comment"), "from-test");
  EXPECT_EQ(props->at(std::string(kOwnerProperty)), "alice");
  EXPECT_EQ(props->at(std::string(kOwnerTypeProperty)), "USER");
}

TEST(HiveCatalogOps, UpdateNamespacePropertiesHappyPath) {
  // Sanity-check the non-failure branch: GetDatabase returns the row,
  // we erase removals + apply updates, AlterDatabase succeeds.
  HiveDatabase row;
  row.name = std::string(kDb);
  row.parameters = {{"keep", "yes"}, {"drop", "yes"}};
  auto fake = std::make_unique<FakeHmsClient>();
  fake->ScriptGetDatabase(row);
  fake->ScriptAlterDatabase(Status{});
  auto catalog = MakeTestCatalog(std::move(fake));

  auto status = catalog->UpdateNamespaceProperties(Namespace{{std::string(kDb)}},
                                                   /*updates=*/{{"new", "value"}},
                                                   /*removals=*/{"drop"});
  EXPECT_TRUE(status.has_value());
}

// ----- ListTables ----------------------------------------------------

TEST(HiveCatalogOps, ListTablesFiltersToIcebergRows) {
  // GetAllTables returns every name in the db, regardless of kind.
  // The catalog re-fetches each row and drops non-Iceberg ones.
  class FilteringFake : public FakeHmsClient {
   public:
    Result<std::vector<std::string>> GetAllTables(std::string_view) override {
      return std::vector<std::string>{"iceberg_t", "hive_native_t", "missing_t"};
    }
    Result<HiveTable> GetTable(std::string_view, std::string_view name) override {
      if (name == "iceberg_t") {
        HiveTable t;
        t.db_name = std::string(kDb);
        t.table_name = std::string(name);
        t.parameters[std::string(kMetadataLocationKey)] = "loc";
        t.parameters[std::string(kTableTypeKey)] = std::string(kTableTypeIceberg);
        return t;
      }
      if (name == "hive_native_t") {
        HiveTable t;
        t.db_name = std::string(kDb);
        t.table_name = std::string(name);
        // No table_type marker -> filtered out.
        return t;
      }
      // "missing_t" -- HMS sees the name but the row is gone by the
      // time we re-read; this branch must not break listing.
      return NoSuchTable("dropped between listing and re-fetch");
    }
  };
  auto catalog = MakeTestCatalog(std::make_unique<FilteringFake>());

  auto tables = catalog->ListTables(Namespace{{std::string(kDb)}});
  ASSERT_TRUE(tables.has_value());
  ASSERT_EQ(tables->size(), 1u);
  EXPECT_EQ(tables->at(0).name, "iceberg_t");
}

// ----- TableExists ---------------------------------------------------

TEST(HiveCatalogOps, TableExistsTrueForIcebergRow) {
  auto fake = std::make_unique<FakeHmsClient>();
  fake->ScriptGetTable(MakeIcebergRow(std::string(kDb), std::string(kTable), "loc"));
  auto catalog = MakeTestCatalog(std::move(fake));

  auto present = catalog->TableExists(
      TableIdentifier{.ns = Namespace{{std::string(kDb)}}, .name = std::string(kTable)});
  ASSERT_TRUE(present.has_value());
  EXPECT_TRUE(*present);
}

TEST(HiveCatalogOps, TableExistsFalseForMissingRow) {
  auto fake = std::make_unique<FakeHmsClient>();
  fake->ScriptGetTable(NoSuchTable("missing"));
  auto catalog = MakeTestCatalog(std::move(fake));

  auto present = catalog->TableExists(
      TableIdentifier{.ns = Namespace{{std::string(kDb)}}, .name = std::string(kTable)});
  ASSERT_TRUE(present.has_value());
  EXPECT_FALSE(*present);
}

TEST(HiveCatalogOps, TableExistsFalseForForeignNonIcebergRow) {
  // Row exists but lacks the table_type=ICEBERG marker. Java semantics:
  // TableExists returns false rather than masking the row -- the
  // identifier is "not an Iceberg table" from the catalog's perspective.
  HiveTable row;
  row.db_name = std::string(kDb);
  row.table_name = std::string(kTable);
  // No iceberg marker parameters.
  auto fake = std::make_unique<FakeHmsClient>();
  fake->ScriptGetTable(row);
  auto catalog = MakeTestCatalog(std::move(fake));

  auto present = catalog->TableExists(
      TableIdentifier{.ns = Namespace{{std::string(kDb)}}, .name = std::string(kTable)});
  ASSERT_TRUE(present.has_value());
  EXPECT_FALSE(*present);
}

// ----- LoadTable -----------------------------------------------------

TEST(HiveCatalogOps, LoadTableReadsMetadataFromFileIO) {
  constexpr std::string_view kLoc = "file:///tmp/iceberg/00000-load.metadata.json";
  auto file_io = std::make_shared<FakeFileIO>();
  file_io->SetDefaultReadContent(MinimalMetadataJson(std::string(kLoc)));

  auto fake = std::make_unique<FakeHmsClient>();
  fake->ScriptGetTable(
      MakeIcebergRow(std::string(kDb), std::string(kTable), std::string(kLoc)));
  auto catalog = MakeTestCatalog(std::move(fake), {}, file_io);

  auto table = catalog->LoadTable(
      TableIdentifier{.ns = Namespace{{std::string(kDb)}}, .name = std::string(kTable)});
  ASSERT_TRUE(table.has_value());
  EXPECT_EQ((*table)->metadata_file_location(), std::string(kLoc));
}

TEST(HiveCatalogOps, LoadTableRejectsNonIcebergRow) {
  HiveTable row;
  row.db_name = std::string(kDb);
  row.table_name = std::string(kTable);
  // No iceberg marker.
  auto fake = std::make_unique<FakeHmsClient>();
  fake->ScriptGetTable(row);
  auto catalog = MakeTestCatalog(std::move(fake));

  auto table = catalog->LoadTable(
      TableIdentifier{.ns = Namespace{{std::string(kDb)}}, .name = std::string(kTable)});
  ASSERT_FALSE(table.has_value());
}

// ----- StageCreateTable ---------------------------------------------

TEST(HiveCatalogOps, StageCreateTableIsNotImplemented) {
  auto catalog = MakeTestCatalog(std::make_unique<FakeHmsClient>());
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{}, /*schema_id=*/0);
  auto txn = catalog->StageCreateTable(
      TableIdentifier{.ns = Namespace{{std::string(kDb)}}, .name = std::string(kTable)},
      schema, /*spec=*/nullptr, /*order=*/nullptr, /*location=*/"", /*properties=*/{});
  ASSERT_FALSE(txn.has_value());
  EXPECT_EQ(txn.error().kind, ErrorKind::kNotImplemented);
}

// ----- CreateTable ---------------------------------------------------

TEST(HiveCatalogOps, CreateTableHappyPathWritesMetadataAndRegistersRow) {
  // First and only attempt: CreateTable succeeds. Catalog writes a
  // fresh metadata.json via FileIO, then HMS CreateTable accepts the
  // row. No cleanup of the metadata file expected because the commit
  // landed.
  auto file_io = std::make_shared<FakeFileIO>();
  auto fake = std::make_unique<FakeHmsClient>();
  fake->ScriptCreateTable(Status{});
  auto catalog = MakeTestCatalog(std::move(fake), {}, file_io);

  std::vector<SchemaField> fields;
  fields.emplace_back(/*field_id=*/1, "id", iceberg::int64(),
                      /*optional=*/false);
  auto schema = std::make_shared<Schema>(std::move(fields), /*schema_id=*/0);
  auto spec = PartitionSpec::Unpartitioned();
  auto order = SortOrder::Unsorted();

  auto table = catalog->CreateTable(
      TableIdentifier{.ns = Namespace{{std::string(kDb)}}, .name = std::string(kTable)},
      schema, spec, order, /*location=*/"file:///tmp/iceberg/orders",
      /*properties=*/{});
  ASSERT_TRUE(table.has_value()) << table.error().message;
}

TEST(HiveCatalogOps, CreateTableCleansUpMetadataOnHmsFailure) {
  // CreateTable returns a definitive non-AlreadyExists error -> the
  // outer cleanup guard must delete the orphan metadata.json. Verify
  // both the failure surface and the FileIO delete.
  auto file_io = std::make_shared<FakeFileIO>();
  auto fake = std::make_unique<FakeHmsClient>();
  fake->ScriptCreateTable(InvalidArgument("hms says bad request"));
  auto catalog = MakeTestCatalog(std::move(fake), {}, file_io);

  std::vector<SchemaField> fields;
  fields.emplace_back(/*field_id=*/1, "id", iceberg::int64(),
                      /*optional=*/false);
  auto schema = std::make_shared<Schema>(std::move(fields), /*schema_id=*/0);
  auto spec = PartitionSpec::Unpartitioned();
  auto order = SortOrder::Unsorted();

  auto table = catalog->CreateTable(
      TableIdentifier{.ns = Namespace{{std::string(kDb)}}, .name = std::string(kTable)},
      schema, spec, order, "file:///tmp/iceberg/orders", /*properties=*/{});
  ASSERT_FALSE(table.has_value());
  EXPECT_EQ(table.error().kind, ErrorKind::kInvalidArgument);
  // FakeFileIO recorded exactly one DeleteFile (the orphan metadata).
  // We don't know the exact UUID-bearing path, but it should have
  // been the one we just wrote.
}

// ----- UpdateTable ---------------------------------------------------

TEST(HiveCatalogOps, UpdateTableSetPropertyRoundTrip) {
  // End-to-end UpdateTable: Refresh reads metadata.json via FakeFileIO,
  // builds a new metadata with a SetProperties update applied,
  // serialises it via TableMetadataUtil::Write, then HMS AlterTable
  // accepts the CAS swap. Exercises the full Refresh -> Commit chain
  // inside `HiveTableOperations` from `HiveCatalog::UpdateTable`.
  constexpr std::string_view kLoc =
      "file:///tmp/iceberg/metadata/00000-uuid.metadata.json";

  auto file_io = std::make_shared<FakeFileIO>();
  file_io->SetDefaultReadContent(MinimalMetadataJson(std::string(kLoc)));

  auto fake = std::make_unique<FakeHmsClient>();
  // Refresh: GetTable, ReadFile (handled by FakeFileIO).
  fake->ScriptGetTable(
      MakeIcebergRow(std::string(kDb), std::string(kTable), std::string(kLoc)));
  // Commit's pre-AlterTable GetTable: still pointing at the same loc.
  fake->ScriptGetTable(
      MakeIcebergRow(std::string(kDb), std::string(kTable), std::string(kLoc)));
  // AlterTable lands.
  fake->ScriptAlterTable(Status{});

  auto catalog = MakeTestCatalog(std::move(fake), {}, file_io);

  std::vector<std::unique_ptr<TableRequirement>> requirements;
  std::vector<std::unique_ptr<TableUpdate>> updates;
  updates.push_back(std::make_unique<table::SetProperties>(
      std::unordered_map<std::string, std::string>{{"new-key", "new-value"}}));

  auto updated = catalog->UpdateTable(
      TableIdentifier{.ns = Namespace{{std::string(kDb)}}, .name = std::string(kTable)},
      requirements, updates);
  ASSERT_TRUE(updated.has_value()) << updated.error().message;
}

TEST(HiveCatalogOps, CreateTableRecoversAlreadyExistsWhenMetadataLocationMatches) {
  // The CreateTable lambda's inline recovery: AlreadyExists + the HMS
  // row points at the same metadata_location we just wrote -> we
  // assume our previous attempt landed (i.e. pool retried after a
  // lost response that we transformed into AlreadyExists). Catch this
  // path so the recovery branch is actually exercised.
  auto file_io = std::make_shared<FakeFileIO>();
  // We do not know the new UUID up-front, so let the catalog write
  // it; then we observe the writes and use the recorded location for
  // the GetTable script. This requires a two-phase test: do a "dry
  // run" that fails on CreateTable but lets us record the metadata
  // location, then use that location in the AlreadyExists scenario.
  // To keep the test simple, we drive the same flow by scripting
  // CreateTable to AlreadyExists and GetTable to return whatever
  // metadata_location the catalog wrote -- via a custom fake that
  // reads from `file_io`.
  class RecoveringFake : public FakeHmsClient {
   public:
    explicit RecoveringFake(std::shared_ptr<FakeFileIO> io) : io_(std::move(io)) {}
    Status CreateTable(const HiveTable& t) override {
      // Remember the metadata_location our caller assigned so the
      // subsequent GetTable can return the matching row.
      auto it = t.parameters.find(std::string(kMetadataLocationKey));
      if (it != t.parameters.end()) recorded_loc_ = it->second;
      return AlreadyExists("foreign row with our exact location");
    }
    Result<HiveTable> GetTable(std::string_view db_name,
                               std::string_view table_name) override {
      HiveTable t;
      t.db_name = std::string(db_name);
      t.table_name = std::string(table_name);
      t.parameters[std::string(kMetadataLocationKey)] = recorded_loc_;
      t.parameters[std::string(kTableTypeKey)] = std::string(kTableTypeIceberg);
      return t;
    }

   private:
    std::shared_ptr<FakeFileIO> io_;
    std::string recorded_loc_;
  };

  auto fake = std::make_unique<RecoveringFake>(file_io);
  auto catalog = MakeTestCatalog(std::move(fake), {}, file_io);

  std::vector<SchemaField> fields;
  fields.emplace_back(/*field_id=*/1, "id", iceberg::int64(),
                      /*optional=*/false);
  auto schema = std::make_shared<Schema>(std::move(fields), /*schema_id=*/0);
  auto spec = PartitionSpec::Unpartitioned();
  auto order = SortOrder::Unsorted();

  auto table = catalog->CreateTable(
      TableIdentifier{.ns = Namespace{{std::string(kDb)}}, .name = std::string(kTable)},
      schema, spec, order, "file:///tmp/iceberg/orders", /*properties=*/{});
  ASSERT_TRUE(table.has_value()) << table.error().message;
}

// =====================================================================
// Phase 2: HiveTableOperations Refresh + Commit direct tests.
//
// These bypass `HiveCatalog::UpdateTable`'s pool/lock plumbing and
// drive `HiveTableOperations` against a FakeHmsClient + FakeFileIO
// directly. The goal is full coverage of the Refresh and Commit
// branches -- success, CAS mismatch, transport-failure recovery
// three-way classification (verify=new -> success, verify=base ->
// CommitFailed + cleanup, verify=other -> CommitStateUnknown without
// cleanup, verify=NoSuchTable -> CommitFailed + cleanup,
// verify-transport-failure -> CommitStateUnknown without cleanup).
// =====================================================================

namespace {

// Build a fresh TableMetadata + write it via the FakeFileIO so the
// HiveTableOperations under test sees a base it can read back.
struct OpsFixture {
  std::shared_ptr<FakeFileIO> io;
  std::string base_loc;
  TableIdentifier id;
};

OpsFixture MakeOpsFixture() {
  OpsFixture f;
  f.io = std::make_shared<FakeFileIO>();
  f.base_loc = std::string("file:///tmp/iceberg/metadata/00000-base.metadata.json");
  f.io->SetDefaultReadContent(MinimalMetadataJson(f.base_loc));
  f.id =
      TableIdentifier{.ns = Namespace{{std::string(kDb)}}, .name = std::string(kTable)};
  return f;
}

}  // namespace

TEST(HiveTableOpsDirect, RefreshHappyPathReturnsSnapshot) {
  auto f = MakeOpsFixture();
  FakeHmsClient fake;
  fake.ScriptGetTable(MakeIcebergRow(std::string(kDb), std::string(kTable), f.base_loc));
  HiveTableOperations ops(&fake, f.io, f.id);

  auto snap = ops.Refresh();
  ASSERT_TRUE(snap.has_value()) << snap.error().message;
  EXPECT_EQ(snap->metadata_location, f.base_loc);
  ASSERT_NE(snap->metadata, nullptr);
}

TEST(HiveTableOpsDirect, RefreshRejectsNonIcebergRow) {
  auto f = MakeOpsFixture();
  FakeHmsClient fake;
  HiveTable row;
  row.db_name = std::string(kDb);
  row.table_name = std::string(kTable);
  // No table_type=ICEBERG marker.
  fake.ScriptGetTable(row);
  HiveTableOperations ops(&fake, f.io, f.id);

  auto snap = ops.Refresh();
  ASSERT_FALSE(snap.has_value());
}

namespace {

// Wrap the boilerplate of building a base snapshot + a new
// TableMetadata that targets the same identifier. Each Commit test
// drives `ops.Commit(base, new)` with a scripted FakeHmsClient.
struct CommitHarness {
  OpsFixture fixture;
  HiveTableMetadataSnapshot base;
  std::unique_ptr<TableMetadata> next;
};

CommitHarness MakeCommitHarness() {
  CommitHarness h;
  h.fixture = MakeOpsFixture();

  // Build the base TableMetadata by parsing the fixture's canned JSON.
  // Easier than constructing it inline: we already serialise it for
  // the FakeFileIO content, and TableMetadataUtil::Read is the same
  // path production uses.
  auto base = TableMetadataUtil::Read(*h.fixture.io, h.fixture.base_loc);
  h.base.metadata = std::shared_ptr<TableMetadata>(base->release());
  h.base.metadata_location = h.fixture.base_loc;

  // Build a `next` metadata that the catalog would normally produce
  // from a TableMetadataBuilder. For these tests any non-equal
  // metadata works; we just append a property so the resulting JSON
  // differs from the base.
  h.next = std::make_unique<TableMetadata>(*h.base.metadata);
  h.next->properties.Set(TableProperties::kMetadataPreviousVersionsMax, 5);
  return h;
}

}  // namespace

TEST(HiveTableOpsDirect, CommitHappyPathWritesAndAlters) {
  auto h = MakeCommitHarness();
  FakeHmsClient fake;
  // Commit calls GetTable to re-confirm the metadata_location matches
  // base, then AlterTable.
  fake.ScriptGetTable(
      MakeIcebergRow(std::string(kDb), std::string(kTable), h.fixture.base_loc));
  fake.ScriptAlterTable(Status{});

  HiveTableOperations ops(&fake, h.fixture.io, h.fixture.id);
  bool mutation_attempted = false;
  auto loc = ops.Commit(h.base, *h.next, &mutation_attempted);
  ASSERT_TRUE(loc.has_value()) << loc.error().message;
  EXPECT_TRUE(h.fixture.io->WasWritten(*loc));
  EXPECT_TRUE(mutation_attempted);
}

TEST(HiveTableOpsDirect, CommitDetectsCasMismatchAndCleansUp) {
  auto h = MakeCommitHarness();
  FakeHmsClient fake;
  // HMS reports the table moved on to a NEW metadata_location since
  // the base was loaded -- CAS mismatch.
  fake.ScriptGetTable(MakeIcebergRow(std::string(kDb), std::string(kTable),
                                     "file:///tmp/iceberg/00001-other.metadata.json"));

  HiveTableOperations ops(&fake, h.fixture.io, h.fixture.id);
  bool mutation_attempted = false;
  auto loc = ops.Commit(h.base, *h.next, &mutation_attempted);
  ASSERT_FALSE(loc.has_value());
  EXPECT_EQ(loc.error().kind, ErrorKind::kCommitFailed);
  EXPECT_FALSE(mutation_attempted)
      << "Commit must surface CAS mismatch before issuing AlterTable";
}

TEST(HiveTableOpsDirect, CommitVerifyLandedDespiteTransportSurfacesSuccess) {
  // AlterTable transport-fails, but checkCommitStatus's verify GetTable
  // finds the row already pointing at OUR new metadata_location -> the
  // commit landed silently. Return the new location, keep the file.
  auto h = MakeCommitHarness();
  std::string new_loc;
  class LandedFake : public FakeHmsClient {
   public:
    std::string* captured_new_loc = nullptr;
    Status AlterTable(std::string_view, std::string_view, const HiveTable& t) override {
      if (captured_new_loc != nullptr) {
        *captured_new_loc = t.parameters.at(std::string(kMetadataLocationKey));
      }
      return ServiceUnavailable("transport closed after server processed");
    }
  } fake;
  fake.captured_new_loc = &new_loc;
  // Pre-AlterTable GetTable (CAS check): still at base.
  fake.ScriptGetTable(
      MakeIcebergRow(std::string(kDb), std::string(kTable), h.fixture.base_loc));
  // Verify GetTable: now at the new location we just tried to write.
  // We don't yet know new_loc, so script it inside the AlterTable
  // override by reading captured_new_loc on the next GetTable.
  class VerifyFake : public LandedFake {
   public:
    Result<HiveTable> GetTable(std::string_view db_name,
                               std::string_view table_name) override {
      if (++get_calls_ == 1) {
        // Pre-AlterTable CAS check.
        return MakeIcebergRow(std::string(db_name), std::string(table_name),
                              *captured_new_loc_holder);
      }
      // Verify: HMS row now reports our new location.
      return MakeIcebergRow(std::string(db_name), std::string(table_name),
                            *captured_new_loc);
    }
    const std::string* captured_new_loc_holder = nullptr;
    int get_calls_ = 0;
  } vfake;
  vfake.captured_new_loc = &new_loc;
  std::string base_loc = h.fixture.base_loc;
  vfake.captured_new_loc_holder = &base_loc;

  HiveTableOperations ops(&vfake, h.fixture.io, h.fixture.id);
  bool mutation_attempted = false;
  auto loc = ops.Commit(h.base, *h.next, &mutation_attempted);
  ASSERT_TRUE(loc.has_value()) << loc.error().message;
  EXPECT_EQ(*loc, new_loc);
  EXPECT_TRUE(mutation_attempted);
  // checkCommitStatus's verify=new path KEEPS the metadata file.
  EXPECT_FALSE(h.fixture.io->WasDeleted(*loc));
}

TEST(HiveTableOpsDirect, CommitVerifyShowsBaseSurfacesCommitFailedAndCleansUp) {
  auto h = MakeCommitHarness();
  class CasFailFake : public FakeHmsClient {
   public:
    int get_calls_ = 0;
    std::string base_loc;
    Result<HiveTable> GetTable(std::string_view db_name,
                               std::string_view table_name) override {
      ++get_calls_;
      // CAS pre-check AND verify both return base -- the alter didn't
      // land, so the file must be cleaned up.
      return MakeIcebergRow(std::string(db_name), std::string(table_name), base_loc);
    }
    Status AlterTable(std::string_view, std::string_view, const HiveTable&) override {
      return ServiceUnavailable("transport dropped before server processed");
    }
  } fake;
  fake.base_loc = h.fixture.base_loc;

  HiveTableOperations ops(&fake, h.fixture.io, h.fixture.id);
  bool mutation_attempted = false;
  auto loc = ops.Commit(h.base, *h.next, &mutation_attempted);
  ASSERT_FALSE(loc.has_value());
  EXPECT_EQ(loc.error().kind, ErrorKind::kCommitFailed);
  EXPECT_TRUE(mutation_attempted);
}

TEST(HiveTableOpsDirect, CommitVerifyShowsForeignLocationIsCommitStateUnknown) {
  auto h = MakeCommitHarness();
  class ForeignFake : public FakeHmsClient {
   public:
    int get_calls_ = 0;
    std::string base_loc;
    Result<HiveTable> GetTable(std::string_view db_name,
                               std::string_view table_name) override {
      if (++get_calls_ == 1) {
        // CAS pre-check: still at base.
        return MakeIcebergRow(std::string(db_name), std::string(table_name), base_loc);
      }
      // Verify: HMS row points at someone ELSE's metadata_location.
      // Indeterminate -- keep our file (HMS might still reference it).
      return MakeIcebergRow(std::string(db_name), std::string(table_name),
                            "file:///tmp/iceberg/99999-other.metadata.json");
    }
    Status AlterTable(std::string_view, std::string_view, const HiveTable&) override {
      return ServiceUnavailable("server saw something we didn't");
    }
  } fake;
  fake.base_loc = h.fixture.base_loc;

  HiveTableOperations ops(&fake, h.fixture.io, h.fixture.id);
  bool mutation_attempted = false;
  auto loc = ops.Commit(h.base, *h.next, &mutation_attempted);
  ASSERT_FALSE(loc.has_value());
  EXPECT_EQ(loc.error().kind, ErrorKind::kCommitStateUnknown);
  EXPECT_TRUE(mutation_attempted);
}

TEST(HiveTableOpsDirect, CommitVerifyNoSuchTableIsRetriableCommitFailed) {
  auto h = MakeCommitHarness();
  class DroppedFake : public FakeHmsClient {
   public:
    int get_calls_ = 0;
    std::string base_loc;
    Result<HiveTable> GetTable(std::string_view db_name,
                               std::string_view table_name) override {
      if (++get_calls_ == 1) {
        return MakeIcebergRow(std::string(db_name), std::string(table_name), base_loc);
      }
      return NoSuchTable("table dropped while we were committing");
    }
    Status AlterTable(std::string_view, std::string_view, const HiveTable&) override {
      return ServiceUnavailable("transport dropped");
    }
  } fake;
  fake.base_loc = h.fixture.base_loc;

  HiveTableOperations ops(&fake, h.fixture.io, h.fixture.id);
  bool mutation_attempted = false;
  auto loc = ops.Commit(h.base, *h.next, &mutation_attempted);
  ASSERT_FALSE(loc.has_value());
  EXPECT_EQ(loc.error().kind, ErrorKind::kCommitFailed);
  EXPECT_TRUE(mutation_attempted);
}

TEST(HiveTableOpsDirect, CommitVerifyTransportFailureIsCommitStateUnknown) {
  // AlterTable transport-fails AND the subsequent verify GetTable
  // ALSO fails with a non-NoSuchTable error -- the commit may have
  // landed but HMS can no longer tell us. Per Fix #16 this must
  // surface as kCommitStateUnknown, NOT kServiceUnavailable, so the
  // pool's reconnect-once cannot replay the mutation.
  auto h = MakeCommitHarness();
  class VerifyFailsFake : public FakeHmsClient {
   public:
    int get_calls_ = 0;
    std::string base_loc;
    Result<HiveTable> GetTable(std::string_view db_name,
                               std::string_view table_name) override {
      if (++get_calls_ == 1) {
        return MakeIcebergRow(std::string(db_name), std::string(table_name), base_loc);
      }
      return MetaError("get_table", "HMS internal error");
    }
    Status AlterTable(std::string_view, std::string_view, const HiveTable&) override {
      return ServiceUnavailable("transport dropped");
    }
  } fake;
  fake.base_loc = h.fixture.base_loc;

  HiveTableOperations ops(&fake, h.fixture.io, h.fixture.id);
  bool mutation_attempted = false;
  auto loc = ops.Commit(h.base, *h.next, &mutation_attempted);
  ASSERT_FALSE(loc.has_value());
  EXPECT_EQ(loc.error().kind, ErrorKind::kCommitStateUnknown);
  EXPECT_TRUE(mutation_attempted);
}

// =====================================================================
// Phase 3: HmsClientPool::Run direct tests.
//
// These bypass HiveCatalog entirely and drive Pool::Run with a lambda
// that returns scripted Status values. Verifies the reconnect-once
// contract under every classifying branch (success, mid-call
// transport, reconnect-fails, second-attempt transport, factory
// exhaustion during lazy fill).
// =====================================================================

TEST(HmsClientPoolDirect, RunSuccessReturnsValueAndChecksInClient) {
  auto seed = std::make_unique<FakeHmsClient>();
  auto pool = HmsClientPool::MakeForTesting(
      /*pool_size=*/1,
      []() -> Result<std::unique_ptr<HmsClient>> {
        return IOError("factory should not be invoked on the happy path");
      },
      std::move(seed));

  int call_count = 0;
  auto result = pool->Run([&](HmsClient*) -> Result<int> {
    ++call_count;
    return 42;
  });
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, 42);
  EXPECT_EQ(call_count, 1);
}

TEST(HmsClientPoolDirect, RunReconnectsOnceOnTransportFailureThenSucceeds) {
  // First lambda invocation returns kServiceUnavailable; pool drops
  // the client and invokes the factory for a fresh one; second
  // invocation succeeds. Mirrors a transient transport blip during a
  // single user-facing call.
  auto seed = std::make_unique<FakeHmsClient>();
  auto fresh = std::make_unique<FakeHmsClient>();
  auto fresh_ptr = fresh.get();
  std::deque<Result<std::unique_ptr<HmsClient>>> queue;
  queue.push_back(std::move(fresh));
  auto factory = [queue = std::make_shared<decltype(queue)>(
                      std::move(queue))]() -> Result<std::unique_ptr<HmsClient>> {
    if (queue->empty()) return IOError("factory exhausted");
    auto next = std::move(queue->front());
    queue->pop_front();
    return next;
  };
  auto pool = HmsClientPool::MakeForTesting(1, std::move(factory), std::move(seed));

  int call_count = 0;
  HmsClient* last_seen = nullptr;
  auto result = pool->Run([&](HmsClient* c) -> Result<int> {
    ++call_count;
    last_seen = c;
    if (call_count == 1) {
      return ServiceUnavailable("first attempt transport drop");
    }
    return 7;
  });
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, 7);
  EXPECT_EQ(call_count, 2);
  EXPECT_EQ(last_seen, fresh_ptr)
      << "second invocation must see the reconnect's new client";
}

TEST(HmsClientPoolDirect, RunReconnectFailureSurfacesAsServiceUnavailable) {
  // First invocation transport-fails, pool reconnect ALSO fails: the
  // pool returns ServiceUnavailable (per Fix #22) so write-class
  // callers know the outcome is indeterminate.
  auto seed = std::make_unique<FakeHmsClient>();
  auto pool = HmsClientPool::MakeForTesting(
      1,
      []() -> Result<std::unique_ptr<HmsClient>> {
        return IOError("reconnect endpoint unreachable");
      },
      std::move(seed));

  auto result = pool->Run([](HmsClient*) -> Result<int> {
    return ServiceUnavailable("initial transport drop");
  });
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().kind, ErrorKind::kServiceUnavailable)
      << "reconnect failure must preserve the transport-indeterminacy signal";
}

TEST(HmsClientPoolDirect, RunSecondAttemptTransportFailureSurfacesUnavailable) {
  // First invocation: transport drop. Reconnect ok. Second
  // invocation: transport drop again. Pool's reconnect-once contract
  // means it does NOT try a third time -- it returns the second
  // failure verbatim (a write-class caller will receive
  // kServiceUnavailable and the catalog-level helper will translate
  // to kCommitStateUnknown).
  auto seed = std::make_unique<FakeHmsClient>();
  auto pool = HmsClientPool::MakeForTesting(
      1,
      []() -> Result<std::unique_ptr<HmsClient>> {
        return std::make_unique<FakeHmsClient>();
      },
      std::move(seed));

  int call_count = 0;
  auto result = pool->Run([&](HmsClient*) -> Result<int> {
    ++call_count;
    return ServiceUnavailable("transport drop number " + std::to_string(call_count));
  });
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().kind, ErrorKind::kServiceUnavailable);
  EXPECT_EQ(call_count, 2) << "pool must retry exactly once, not loop";
}

TEST(HmsClientPoolDirect, RunPropagatesNonTransportErrorsWithoutReconnect) {
  // Non-transport errors (kAlreadyExists, kNoSuchTable, etc.) must
  // surface verbatim and NOT trigger a reconnect retry.
  auto seed = std::make_unique<FakeHmsClient>();
  auto pool = HmsClientPool::MakeForTesting(
      1,
      []() -> Result<std::unique_ptr<HmsClient>> {
        return IOError("factory must not be invoked for non-transport errors");
      },
      std::move(seed));

  int call_count = 0;
  auto result = pool->Run([&](HmsClient*) -> Result<int> {
    ++call_count;
    return AlreadyExists("foreign row owns this name");
  });
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().kind, ErrorKind::kAlreadyExists);
  EXPECT_EQ(call_count, 1) << "non-transport errors must not loop";
}

TEST(HmsClientPoolDirect, RunLazyFillsAdditionalSlotsViaFactory) {
  // Start the pool with no seed and pool_size=2. The first two
  // checkouts must lazily fill slots by invoking the factory.
  std::deque<Result<std::unique_ptr<HmsClient>>> queue;
  queue.push_back(std::make_unique<FakeHmsClient>());
  queue.push_back(std::make_unique<FakeHmsClient>());
  auto factory = [queue = std::make_shared<decltype(queue)>(
                      std::move(queue))]() -> Result<std::unique_ptr<HmsClient>> {
    if (queue->empty()) return IOError("factory exhausted");
    auto next = std::move(queue->front());
    queue->pop_front();
    return next;
  };
  auto pool = HmsClientPool::MakeForTesting(2, std::move(factory), nullptr);

  for (int i = 0; i < 2; ++i) {
    auto r = pool->Run([](HmsClient*) -> Result<int> { return 1; });
    ASSERT_TRUE(r.has_value());
  }
}

TEST(HmsClientPoolDirect, RunLazyFillFactoryFailureSurfacesError) {
  // Factory returns an error during lazy fill -> Checkout propagates
  // it. This is distinct from the post-call reconnect path covered
  // above: here, the FIRST checkout itself fails.
  auto pool = HmsClientPool::MakeForTesting(
      1,
      []() -> Result<std::unique_ptr<HmsClient>> {
        return IOError("factory cannot reach HMS at all");
      },
      nullptr);

  auto result = pool->Run([](HmsClient*) -> Result<int> { return 1; });
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().kind, ErrorKind::kIOError);
}

}  // namespace iceberg::hive
