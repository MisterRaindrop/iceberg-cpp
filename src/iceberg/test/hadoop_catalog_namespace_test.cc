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

#include <algorithm>
#include <memory>
#include <string>

#include <arrow/filesystem/localfs.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/arrow/arrow_io_register.h"
#include "iceberg/catalog/hadoop/hadoop_catalog.h"
#include "iceberg/catalog/hadoop/hadoop_catalog_properties.h"
#include "iceberg/catalog/hadoop/hadoop_file_layout.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/sort_order.h"
#include "iceberg/table.h"
#include "iceberg/table_requirements.h"
#include "iceberg/table_update.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/temp_file_test_base.h"
#include "iceberg/type.h"

namespace iceberg::hadoop {

class HadoopCatalogNamespaceTest : public ::iceberg::TempFileTestBase {
 protected:
  void SetUp() override {
    ::iceberg::TempFileTestBase::SetUp();
    warehouse_ = "file://" + CreateTempDirectory();
    file_io_ = std::make_shared<::iceberg::arrow::ArrowFileSystemFileIO>(
        std::make_shared<::arrow::fs::LocalFileSystem>());
    auto props = HadoopCatalogProperties::FromMap({
        {"warehouse", warehouse_},
        {"name", "hadoop_test"},
    });
    auto cat = HadoopCatalog::Make("hadoop_test", file_io_, std::move(props));
    ASSERT_TRUE(cat.has_value()) << cat.error().message;
    catalog_ = *cat;
  }

  std::string warehouse_;
  std::shared_ptr<FileIO> file_io_;
  std::shared_ptr<HadoopCatalog> catalog_;
};

TEST_F(HadoopCatalogNamespaceTest, CreateThenExistsThenDrop) {
  Namespace ns{.levels = {"db"}};
  EXPECT_TRUE(catalog_->CreateNamespace(ns, {}).has_value());

  auto exists = catalog_->NamespaceExists(ns);
  ASSERT_TRUE(exists.has_value());
  EXPECT_TRUE(*exists);

  EXPECT_TRUE(catalog_->DropNamespace(ns).has_value());
  auto after_drop = catalog_->NamespaceExists(ns);
  ASSERT_TRUE(after_drop.has_value());
  EXPECT_FALSE(*after_drop);
}

TEST_F(HadoopCatalogNamespaceTest, CreateRejectsExistingNamespace) {
  Namespace ns{.levels = {"db"}};
  ASSERT_TRUE(catalog_->CreateNamespace(ns, {}).has_value());
  auto again = catalog_->CreateNamespace(ns, {});
  ASSERT_FALSE(again.has_value());
  EXPECT_EQ(ErrorKind::kAlreadyExists, again.error().kind);
}

TEST_F(HadoopCatalogNamespaceTest, CreateRejectsNonEmptyProperties) {
  Namespace ns{.levels = {"db"}};
  auto res = catalog_->CreateNamespace(ns, {{"owner", "alice"}});
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(ErrorKind::kNotSupported, res.error().kind);
}

TEST_F(HadoopCatalogNamespaceTest, CreateRejectsEmptyNamespace) {
  Namespace empty;
  auto res = catalog_->CreateNamespace(empty, {});
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(ErrorKind::kInvalidArgument, res.error().kind);
}

TEST_F(HadoopCatalogNamespaceTest, ListNamespacesAtRootAndNested) {
  ASSERT_TRUE(catalog_->CreateNamespace(Namespace{.levels = {"db_a"}}, {}).has_value());
  ASSERT_TRUE(catalog_->CreateNamespace(Namespace{.levels = {"db_b"}}, {}).has_value());
  ASSERT_TRUE(
      catalog_->CreateNamespace(Namespace{.levels = {"db_a", "team1"}}, {}).has_value());
  ASSERT_TRUE(
      catalog_->CreateNamespace(Namespace{.levels = {"db_a", "team2"}}, {}).has_value());

  auto roots = catalog_->ListNamespaces(Namespace{});
  ASSERT_TRUE(roots.has_value());
  ASSERT_EQ(roots->size(), 2);
  std::vector<std::string> leaves;
  for (const auto& n : *roots) {
    ASSERT_EQ(n.levels.size(), 1);
    leaves.push_back(n.levels.front());
  }
  std::ranges::sort(leaves);
  EXPECT_EQ(leaves[0], "db_a");
  EXPECT_EQ(leaves[1], "db_b");

  auto children = catalog_->ListNamespaces(Namespace{.levels = {"db_a"}});
  ASSERT_TRUE(children.has_value());
  ASSERT_EQ(children->size(), 2);
}

TEST_F(HadoopCatalogNamespaceTest, ListNamespacesFiltersOutTableDirectories) {
  // Build a table-shaped directory at <warehouse>/db/looks_like_table and
  // verify ListNamespaces does NOT include it.
  ASSERT_TRUE(catalog_->CreateNamespace(Namespace{.levels = {"db"}}, {}).has_value());
  ICEBERG_UNWRAP_OR_FAIL(auto warehouse, catalog_->config().Warehouse());
  ICEBERG_UNWRAP_OR_FAIL(auto ns_dir,
                         hadoop::NamespaceDir(warehouse, Namespace{.levels = {"db"}}));
  const std::string table_dir = ns_dir + "/looks_like_table";
  ASSERT_TRUE(file_io_->CreateDir(table_dir + "/metadata").has_value());
  ASSERT_TRUE(
      file_io_->WriteFile(table_dir + "/metadata/v1.metadata.json", "{}").has_value());

  auto ls = catalog_->ListNamespaces(Namespace{.levels = {"db"}});
  ASSERT_TRUE(ls.has_value());
  for (const auto& child : *ls) {
    EXPECT_NE(child.levels.back(), "looks_like_table")
        << "table-shaped directory must not be reported as a namespace";
  }

  // NamespaceExists on a table directory must also report false.
  auto exists =
      catalog_->NamespaceExists(Namespace{.levels = {"db", "looks_like_table"}});
  ASSERT_TRUE(exists.has_value());
  EXPECT_FALSE(*exists);
}

TEST_F(HadoopCatalogNamespaceTest, DropRejectsNonExistent) {
  auto res = catalog_->DropNamespace(Namespace{.levels = {"missing"}});
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(ErrorKind::kNoSuchNamespace, res.error().kind);
}

TEST_F(HadoopCatalogNamespaceTest, DropRejectsNonEmpty) {
  ASSERT_TRUE(catalog_->CreateNamespace(Namespace{.levels = {"db"}}, {}).has_value());
  ASSERT_TRUE(
      catalog_->CreateNamespace(Namespace{.levels = {"db", "child"}}, {}).has_value());
  auto res = catalog_->DropNamespace(Namespace{.levels = {"db"}});
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(ErrorKind::kNamespaceNotEmpty, res.error().kind);
}

TEST_F(HadoopCatalogNamespaceTest, GetNamespacePropertiesReturnsLocationOnly) {
  Namespace ns{.levels = {"db"}};
  ASSERT_TRUE(catalog_->CreateNamespace(ns, {}).has_value());
  auto props = catalog_->GetNamespaceProperties(ns);
  ASSERT_TRUE(props.has_value());
  ASSERT_EQ(props->size(), 1);
  EXPECT_TRUE(props->contains("location"));
  EXPECT_TRUE(props->at("location").ends_with("/db"));
}

TEST_F(HadoopCatalogNamespaceTest, GetNamespacePropertiesRejectsMissing) {
  auto res = catalog_->GetNamespaceProperties(Namespace{.levels = {"missing"}});
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(ErrorKind::kNoSuchNamespace, res.error().kind);
}

TEST_F(HadoopCatalogNamespaceTest, MakeWithoutExplicitFileIORoutesByScheme) {
  // The bundle's static initialiser registers arrow-fs-local / arrow-fs-s3
  // (and arrow-fs-hdfs when enabled). Call the entry point explicitly so we
  // don't depend on linker dead-stripping behaviour for the kArrowFileIOs
  // static init in this test translation unit.
  iceberg::arrow::EnsureArrowFileIOsRegistered();

  // file:// warehouse must succeed without passing a FileIO.
  auto props = HadoopCatalogProperties::FromMap({
      {"warehouse", warehouse_},
      {"name", "auto"},
  });
  auto cat = HadoopCatalog::Make("auto", std::move(props));
  ASSERT_TRUE(cat.has_value()) << cat.error().message;

  // hdfs:// warehouse should fail cleanly when ICEBERG_HDFS=OFF: the
  // FileIORegistry has no `arrow-fs-hdfs` entry to load.
  auto hdfs_props = HadoopCatalogProperties::FromMap({
      {"warehouse", "hdfs://nn:8020/wh"},
      {"name", "hdfs-test"},
  });
  auto hdfs_cat = HadoopCatalog::Make("hdfs-test", std::move(hdfs_props));
#if ICEBERG_HDFS_ENABLED
  // When HDFS is built in, construction succeeds; we don't actually talk to
  // a namenode here.
  EXPECT_TRUE(hdfs_cat.has_value() || hdfs_cat.error().kind == ErrorKind::kIOError);
#else
  EXPECT_FALSE(hdfs_cat.has_value());
#endif
}

TEST_F(HadoopCatalogNamespaceTest, UpdateNamespacePropertiesIsNotSupported) {
  ASSERT_TRUE(catalog_->CreateNamespace(Namespace{.levels = {"db"}}, {}).has_value());
  auto res =
      catalog_->UpdateNamespaceProperties(Namespace{.levels = {"db"}}, {{"k", "v"}}, {});
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(ErrorKind::kNotSupported, res.error().kind);
}

TEST_F(HadoopCatalogNamespaceTest, ListTablesFiltersToTableShapedDirs) {
  ASSERT_TRUE(catalog_->CreateNamespace(Namespace{.levels = {"db"}}, {}).has_value());
  TableIdentifier table{.ns = Namespace{.levels = {"db"}}, .name = "events"};
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});
  ASSERT_TRUE(catalog_
                  ->CreateTable(table, schema, PartitionSpec::Unpartitioned(),
                                SortOrder::Unsorted(), "", {})
                  .has_value());
  // Add a plain subdirectory under db/ that is NOT a table.
  ICEBERG_UNWRAP_OR_FAIL(auto db_dir,
                         hadoop::NamespaceDir(warehouse_, Namespace{.levels = {"db"}}));
  ASSERT_TRUE(file_io_->CreateDir(db_dir + "/team_a").has_value());

  auto tables = catalog_->ListTables(Namespace{.levels = {"db"}});
  ASSERT_TRUE(tables.has_value());
  ASSERT_EQ(tables->size(), 1);
  EXPECT_EQ(tables->front(), table);

  auto exists = catalog_->TableExists(table);
  ASSERT_TRUE(exists.has_value());
  EXPECT_TRUE(*exists);

  auto missing = catalog_->TableExists(
      TableIdentifier{.ns = Namespace{.levels = {"db"}}, .name = "absent"});
  ASSERT_TRUE(missing.has_value());
  EXPECT_FALSE(*missing);
}

TEST_F(HadoopCatalogNamespaceTest, DropTablePurgeTrueRefusesSnapshottedTable) {
  // Iceberg's write API lets a snapshot reference data files by absolute
  // path. iceberg_hadoop has no manifest reader, so it cannot enumerate
  // those files to honour Catalog::DropTable(purge=true)'s "delete all
  // data" contract. The safe stance: refuse purge=true once a table has
  // any committed snapshots and tell the operator to run external
  // manifest-walk cleanup first.
  ASSERT_TRUE(catalog_->CreateNamespace(Namespace{.levels = {"db"}}, {}).has_value());
  TableIdentifier table{.ns = Namespace{.levels = {"db"}}, .name = "events"};
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});
  ICEBERG_UNWRAP_OR_FAIL(
      auto created, catalog_->CreateTable(table, schema, PartitionSpec::Unpartitioned(),
                                          SortOrder::Unsorted(), "", {}));

  // Inject a synthetic snapshot entry into the v1.metadata.json so the
  // table looks "committed". Iceberg's CreateTable doesn't produce a
  // snapshot, so we mutate the on-disk metadata directly via FileIO to
  // simulate a writer that has committed a FastAppend.
  const std::string metadata_path(created->metadata_file_location());
  ICEBERG_UNWRAP_OR_FAIL(auto body, file_io_->ReadFile(metadata_path, std::nullopt));
  const std::string empty_snap_key = "\"snapshots\":[]";
  auto pos = body.find(empty_snap_key);
  std::string replaced;
  if (pos == std::string::npos) {
    // Some serialisers emit `"snapshots": []` with a space; handle both.
    const std::string spaced = "\"snapshots\": []";
    pos = body.find(spaced);
    ASSERT_NE(pos, std::string::npos)
        << "v1 metadata JSON must contain an empty \"snapshots\" array; got: " << body;
    replaced = body.substr(0, pos) +
               "\"snapshots\":[{\"snapshot-id\":1,\"sequence-number\":1,"
               "\"timestamp-ms\":0,\"manifest-list\":\"file:///fake/m.avro\","
               "\"summary\":{\"operation\":\"append\"},\"schema-id\":0}]" +
               body.substr(pos + spaced.size());
  } else {
    replaced = body.substr(0, pos) +
               "\"snapshots\":[{\"snapshot-id\":1,\"sequence-number\":1,"
               "\"timestamp-ms\":0,\"manifest-list\":\"file:///fake/m.avro\","
               "\"summary\":{\"operation\":\"append\"},\"schema-id\":0}]" +
               body.substr(pos + empty_snap_key.size());
  }
  ASSERT_TRUE(file_io_->DeleteFile(metadata_path).has_value());
  ASSERT_TRUE(file_io_->WriteFile(metadata_path, replaced).has_value());

  auto drop = catalog_->DropTable(table, /*purge=*/true);
  ASSERT_FALSE(drop.has_value());
  EXPECT_EQ(ErrorKind::kNotSupported, drop.error().kind);

  // Table must remain on disk after the refusal.
  auto still_table = catalog_->TableExists(table);
  ASSERT_TRUE(still_table.has_value());
  EXPECT_TRUE(*still_table);
}

TEST_F(HadoopCatalogNamespaceTest, RejectsNestingInsideExistingTable) {
  // Once `db.team` is a table, creating a namespace or table beneath it
  // would let `DropTable(db.team, purge=true)` recursively wipe the
  // descendants. Refuse both creates up front.
  ASSERT_TRUE(catalog_->CreateNamespace(Namespace{.levels = {"db"}}, {}).has_value());
  TableIdentifier parent_id{.ns = Namespace{.levels = {"db"}}, .name = "team"};
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});
  ASSERT_TRUE(catalog_
                  ->CreateTable(parent_id, schema, PartitionSpec::Unpartitioned(),
                                SortOrder::Unsorted(), "", {})
                  .has_value());

  // CreateNamespace beneath the table must be rejected.
  auto ns_under_table =
      catalog_->CreateNamespace(Namespace{.levels = {"db", "team", "proj"}}, {});
  ASSERT_FALSE(ns_under_table.has_value());
  EXPECT_EQ(ErrorKind::kInvalidArgument, ns_under_table.error().kind);

  // CreateTable beneath the table (in a notional sub-namespace whose
  // first level happens to be the existing table dir) must also be
  // rejected.
  TableIdentifier nested_id{.ns = Namespace{.levels = {"db", "team", "sub"}},
                            .name = "child"};
  auto tbl_under_table = catalog_->CreateTable(
      nested_id, schema, PartitionSpec::Unpartitioned(), SortOrder::Unsorted(), "", {});
  ASSERT_FALSE(tbl_under_table.has_value());
  EXPECT_EQ(ErrorKind::kInvalidArgument, tbl_under_table.error().kind);
}

TEST_F(HadoopCatalogNamespaceTest, FileLockDropAcrossInstancesRoundTrip) {
  // Two HadoopCatalog instances sharing the same warehouse and
  // lock-impl=file. Instance A drops a table; instance B creates a
  // table at the same path right after. The drop must NOT corrupt
  // instance B's create even though _lock briefly co-existed with
  // instance B's first Acquire attempt. Regression for the case where
  // DropTable used to recursive-delete the table dir (and _lock) while
  // holding the lock -- with the new ordering, _lock is removed only
  // by Release after everything else has been cleared, so instance B's
  // Acquire after that point starts from a clean slate.
  auto make_catalog = [&](std::string_view name) {
    auto props = HadoopCatalogProperties::FromMap({
        {"warehouse", warehouse_},
        {"name", std::string(name)},
        {"lock-impl", "file"},
        {"lock.acquire-timeout-ms", "2000"},
    });
    return HadoopCatalog::Make(name, file_io_, std::move(props));
  };
  ICEBERG_UNWRAP_OR_FAIL(auto cat_a, make_catalog("inst_a"));
  ICEBERG_UNWRAP_OR_FAIL(auto cat_b, make_catalog("inst_b"));
  ASSERT_TRUE(cat_a->CreateNamespace(Namespace{.levels = {"db"}}, {}).has_value());

  TableIdentifier id{.ns = Namespace{.levels = {"db"}}, .name = "shared"};
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});
  ASSERT_TRUE(cat_a
                  ->CreateTable(id, schema, PartitionSpec::Unpartitioned(),
                                SortOrder::Unsorted(), "", {})
                  .has_value());

  // Instance A drops, instance B re-creates -- must both succeed and
  // produce a usable table.
  ASSERT_TRUE(cat_a->DropTable(id, /*purge=*/true).has_value());
  ASSERT_TRUE(cat_b
                  ->CreateTable(id, schema, PartitionSpec::Unpartitioned(),
                                SortOrder::Unsorted(), "", {})
                  .has_value());
  ICEBERG_UNWRAP_OR_FAIL(auto loaded, cat_b->LoadTable(id));
  EXPECT_EQ(loaded->name(), id);
}

TEST_F(HadoopCatalogNamespaceTest, FileLockCreateDropCreateRoundTrip) {
  // Regression for the case where DropTable recursively deletes the table
  // dir while still holding the lock (which lives at
  // <table>/metadata/_lock). The RAII guard's Release() used to fail
  // because the lock file is gone, leaving stale local state that
  // collided with the next Acquire at the same path -- the next
  // CreateTable then failed with InvalidArgument. Release() now handles
  // the missing-file case gracefully so create -> drop -> create
  // succeeds end-to-end.
  auto props = HadoopCatalogProperties::FromMap({
      {"warehouse", warehouse_},
      {"name", "file_lock_drop"},
      {"lock-impl", "file"},
      {"lock.acquire-timeout-ms", "1000"},
  });
  ICEBERG_UNWRAP_OR_FAIL(
      auto file_lock_catalog,
      HadoopCatalog::Make("file_lock_drop", file_io_, std::move(props)));
  ASSERT_TRUE(
      file_lock_catalog->CreateNamespace(Namespace{.levels = {"db"}}, {}).has_value());
  TableIdentifier id{.ns = Namespace{.levels = {"db"}}, .name = "evts"};
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});
  ASSERT_TRUE(file_lock_catalog
                  ->CreateTable(id, schema, PartitionSpec::Unpartitioned(),
                                SortOrder::Unsorted(), "", {})
                  .has_value());
  ASSERT_TRUE(file_lock_catalog->DropTable(id, /*purge=*/true).has_value());

  // The second create must NOT fail with "unreleased local heartbeat state".
  ASSERT_TRUE(file_lock_catalog
                  ->CreateTable(id, schema, PartitionSpec::Unpartitioned(),
                                SortOrder::Unsorted(), "", {})
                  .has_value())
      << "post-drop CreateTable must succeed under lock-impl=file";
}

TEST_F(HadoopCatalogNamespaceTest, UpdateTableAccumulatesMetadataLog) {
  // The non-create UpdateTable path must call SetPreviousMetadataLocation
  // on the builder so the produced metadata's `metadata_log` records the
  // previous version. Without it, the on-disk byte parity with Java that
  // mkdocs/docs/catalogs/hadoop.md advertises is broken.
  ASSERT_TRUE(catalog_->CreateNamespace(Namespace{.levels = {"db"}}, {}).has_value());
  TableIdentifier id{.ns = Namespace{.levels = {"db"}}, .name = "log_evts"};
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});
  ICEBERG_UNWRAP_OR_FAIL(auto created,
                         catalog_->CreateTable(id, schema, PartitionSpec::Unpartitioned(),
                                               SortOrder::Unsorted(), "", {}));
  const std::string v1_location(created->metadata_file_location());

  // Force a non-create commit by adding a property.
  std::vector<std::unique_ptr<TableRequirement>> reqs;
  reqs.push_back(std::make_unique<table::AssertUUID>(created->metadata()->table_uuid));
  std::vector<std::unique_ptr<TableUpdate>> updates;
  updates.push_back(std::make_unique<table::SetProperties>(
      std::unordered_map<std::string, std::string>{{"key", "value"}}));
  ICEBERG_UNWRAP_OR_FAIL(auto updated, catalog_->UpdateTable(id, reqs, updates));

  // The newly published metadata must contain a metadata_log entry
  // pointing at v1.
  ASSERT_FALSE(updated->metadata()->metadata_log.empty())
      << "post-commit metadata_log must record the previous version";
  EXPECT_EQ(updated->metadata()->metadata_log.back().metadata_file, v1_location);
}

TEST_F(HadoopCatalogNamespaceTest, DropTablePurgeRefusesTraversalInStatistic) {
  // External statistics paths that contain literal `..` or
  // percent-encoded `%2e%2e` segments resolve to OUTSIDE the table
  // directory once Arrow normalises the URI. The naive `IsPathInside`
  // string-prefix check used to accept them as "inside" because the
  // literal prefix matched. The normalized check must refuse.
  ASSERT_TRUE(catalog_->CreateNamespace(Namespace{.levels = {"db"}}, {}).has_value());
  TableIdentifier table{.ns = Namespace{.levels = {"db"}}, .name = "trav"};
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});
  ICEBERG_UNWRAP_OR_FAIL(
      auto created, catalog_->CreateTable(table, schema, PartitionSpec::Unpartitioned(),
                                          SortOrder::Unsorted(), "", {}));
  ICEBERG_UNWRAP_OR_FAIL(auto table_dir, hadoop::TableDir(warehouse_, table));

  auto inject_stat_path = [&](const std::string& stat_path) {
    const std::string metadata_path(created->metadata_file_location());
    ICEBERG_UNWRAP_OR_FAIL(auto body, file_io_->ReadFile(metadata_path, std::nullopt));
    const std::string empty_compact = "\"statistics\":[]";
    const std::string empty_spaced = "\"statistics\": []";
    auto pos = body.find(empty_compact);
    size_t replaced_len = empty_compact.size();
    if (pos == std::string::npos) {
      pos = body.find(empty_spaced);
      replaced_len = empty_spaced.size();
    }
    ASSERT_NE(pos, std::string::npos);
    const std::string seeded = std::format(
        "\"statistics\":[{{\"snapshot-id\":1,\"statistics-path\":\"{}\","
        "\"file-size-in-bytes\":1,\"file-footer-size-in-bytes\":1,"
        "\"blob-metadata\":[]}}]",
        stat_path);
    const std::string mutated =
        body.substr(0, pos) + seeded + body.substr(pos + replaced_len);
    ASSERT_TRUE(file_io_->DeleteFile(metadata_path).has_value());
    ASSERT_TRUE(file_io_->WriteFile(metadata_path, mutated).has_value());
  };

  // Literal `..` segment that resolves outside the table dir. The
  // percent-encoded `%2e%2e` variant is covered by the unit test
  // `IsPathInsideNormalizedRejectsTraversal` in hadoop_file_layout_test;
  // the catalog-side wiring is identical for both.
  inject_stat_path(table_dir + "/../outside/x.puffin");
  auto drop_raw = catalog_->DropTable(table, /*purge=*/true);
  ASSERT_FALSE(drop_raw.has_value());
  EXPECT_EQ(ErrorKind::kNotSupported, drop_raw.error().kind);
}

TEST_F(HadoopCatalogNamespaceTest, DropTablePurgeIgnoresSiblingPrefix) {
  // Component-aware descendant check: a statistics path at a sibling
  // directory (e.g. `<table>_backup/...`) must NOT be classified as
  // inside the table dir. Otherwise DropTable(purge=true) would happily
  // recursive-delete the table and leave the sibling's data behind.
  ASSERT_TRUE(catalog_->CreateNamespace(Namespace{.levels = {"db"}}, {}).has_value());
  TableIdentifier table{.ns = Namespace{.levels = {"db"}}, .name = "stats"};
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});
  ICEBERG_UNWRAP_OR_FAIL(
      auto created, catalog_->CreateTable(table, schema, PartitionSpec::Unpartitioned(),
                                          SortOrder::Unsorted(), "", {}));

  // Inject a statistics entry whose path shares a prefix with the table
  // dir but lives in a sibling (e.g. `<warehouse>/db/stats_backup/...`).
  ICEBERG_UNWRAP_OR_FAIL(auto table_dir, hadoop::TableDir(warehouse_, table));
  const std::string sibling_path = table_dir + "_backup/x.puffin";
  const std::string metadata_path(created->metadata_file_location());
  ICEBERG_UNWRAP_OR_FAIL(auto body, file_io_->ReadFile(metadata_path, std::nullopt));
  const std::string empty_stats_compact = "\"statistics\":[]";
  const std::string empty_stats_spaced = "\"statistics\": []";
  auto pos = body.find(empty_stats_compact);
  size_t replaced_len = empty_stats_compact.size();
  if (pos == std::string::npos) {
    pos = body.find(empty_stats_spaced);
    replaced_len = empty_stats_spaced.size();
  }
  ASSERT_NE(pos, std::string::npos);
  const std::string seeded = std::format(
      "\"statistics\":[{{\"snapshot-id\":1,\"statistics-path\":\"{}\","
      "\"file-size-in-bytes\":1,\"file-footer-size-in-bytes\":1,"
      "\"blob-metadata\":[]}}]",
      sibling_path);
  const std::string mutated =
      body.substr(0, pos) + seeded + body.substr(pos + replaced_len);
  ASSERT_TRUE(file_io_->DeleteFile(metadata_path).has_value());
  ASSERT_TRUE(file_io_->WriteFile(metadata_path, mutated).has_value());

  auto drop = catalog_->DropTable(table, /*purge=*/true);
  ASSERT_FALSE(drop.has_value());
  EXPECT_EQ(ErrorKind::kNotSupported, drop.error().kind);
}

TEST_F(HadoopCatalogNamespaceTest, DropTablePurgeTrueRefusesExternalStatistics) {
  // Snapshot-less tables can still reference external `.puffin` paths via
  // UpdateStatistics, so DropTable(purge=true) must inspect statistics too,
  // not just snapshots, before recursively deleting -- otherwise the
  // external file is silently orphaned despite the operation reporting
  // success.
  ASSERT_TRUE(catalog_->CreateNamespace(Namespace{.levels = {"db"}}, {}).has_value());
  TableIdentifier table{.ns = Namespace{.levels = {"db"}}, .name = "stats"};
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});
  ICEBERG_UNWRAP_OR_FAIL(
      auto created, catalog_->CreateTable(table, schema, PartitionSpec::Unpartitioned(),
                                          SortOrder::Unsorted(), "", {}));

  // Inject a single statistics entry whose path is outside the table dir.
  const std::string metadata_path(created->metadata_file_location());
  ICEBERG_UNWRAP_OR_FAIL(auto body, file_io_->ReadFile(metadata_path, std::nullopt));
  const std::string empty_stats_compact = "\"statistics\":[]";
  const std::string empty_stats_spaced = "\"statistics\": []";
  auto pos = body.find(empty_stats_compact);
  size_t replaced_len = empty_stats_compact.size();
  if (pos == std::string::npos) {
    pos = body.find(empty_stats_spaced);
    replaced_len = empty_stats_spaced.size();
  }
  ASSERT_NE(pos, std::string::npos)
      << "v1 metadata JSON must contain an empty \"statistics\" array; got: " << body;
  const std::string seeded =
      "\"statistics\":[{\"snapshot-id\":1,\"statistics-path\":\"file:///external/"
      "x.puffin\",\"file-size-in-bytes\":1,\"file-footer-size-in-bytes\":1,"
      "\"blob-metadata\":[]}]";
  std::string mutated = body.substr(0, pos) + seeded + body.substr(pos + replaced_len);
  ASSERT_TRUE(file_io_->DeleteFile(metadata_path).has_value());
  ASSERT_TRUE(file_io_->WriteFile(metadata_path, mutated).has_value());

  auto drop = catalog_->DropTable(table, /*purge=*/true);
  ASSERT_FALSE(drop.has_value());
  EXPECT_EQ(ErrorKind::kNotSupported, drop.error().kind);

  // The table directory is still intact -- we did not partially delete it.
  auto still_there = catalog_->TableExists(table);
  ASSERT_TRUE(still_there.has_value());
  EXPECT_TRUE(*still_there);
}

TEST_F(HadoopCatalogNamespaceTest, DropTablePurgeFalseIsNotSupported) {
  // HadoopCatalog cannot honour Catalog::DropTable(purge=false) without
  // turning the leftover data/ subtree into a namespace-shaped orphan that
  // a subsequent CreateTable would silently adopt. Surfacing
  // kNotSupported is the only safe choice for the lightweight layout.
  ASSERT_TRUE(catalog_->CreateNamespace(Namespace{.levels = {"db"}}, {}).has_value());
  TableIdentifier table{.ns = Namespace{.levels = {"db"}}, .name = "events"};
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});
  ASSERT_TRUE(catalog_
                  ->CreateTable(table, schema, PartitionSpec::Unpartitioned(),
                                SortOrder::Unsorted(), "", {})
                  .has_value());

  auto res = catalog_->DropTable(table, /*purge=*/false);
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(ErrorKind::kNotSupported, res.error().kind);

  // Table is still intact after the refusal.
  auto still_there = catalog_->TableExists(table);
  ASSERT_TRUE(still_there.has_value());
  EXPECT_TRUE(*still_there);
}

TEST_F(HadoopCatalogNamespaceTest, DropTablePurgeTrueRemovesData) {
  // The companion of DropTablePurgeFalsePreservesData: purge=true must
  // actually remove the data files under the table directory.
  ASSERT_TRUE(catalog_->CreateNamespace(Namespace{.levels = {"db"}}, {}).has_value());
  TableIdentifier table{.ns = Namespace{.levels = {"db"}}, .name = "events_purge"};
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});
  ASSERT_TRUE(catalog_
                  ->CreateTable(table, schema, PartitionSpec::Unpartitioned(),
                                SortOrder::Unsorted(), "", {})
                  .has_value());

  ICEBERG_UNWRAP_OR_FAIL(auto table_dir, hadoop::TableDir(warehouse_, table));
  const std::string data_path = hadoop::DataDir(table_dir) + "/sample.parquet";
  ASSERT_TRUE(file_io_->CreateDir(hadoop::DataDir(table_dir)).has_value());
  ASSERT_TRUE(file_io_->WriteFile(data_path, "dummy").has_value());

  EXPECT_TRUE(catalog_->DropTable(table, /*purge=*/true).has_value());
  ICEBERG_UNWRAP_OR_FAIL(auto data_exists, file_io_->Exists(data_path));
  EXPECT_FALSE(data_exists) << "purge=true must delete data files";
}

TEST_F(HadoopCatalogNamespaceTest, CreateNamespaceRejectsTablePath) {
  // Symmetric guard with NamespaceExists/ListNamespaces: once a path is a
  // table, CreateNamespace at that exact path must refuse instead of
  // returning the generic "already exists" used for plain directories --
  // the conflict is with a table, not a sibling namespace.
  ASSERT_TRUE(catalog_->CreateNamespace(Namespace{.levels = {"db"}}, {}).has_value());
  TableIdentifier table{.ns = Namespace{.levels = {"db"}}, .name = "events"};
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});
  ASSERT_TRUE(catalog_
                  ->CreateTable(table, schema, PartitionSpec::Unpartitioned(),
                                SortOrder::Unsorted(), "", {})
                  .has_value());

  auto res = catalog_->CreateNamespace(Namespace{.levels = {"db", "events"}}, {});
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(ErrorKind::kAlreadyExists, res.error().kind);
}

TEST_F(HadoopCatalogNamespaceTest, GetNamespacePropertiesRejectsTablePath) {
  // GetNamespaceProperties must not silently surface a table directory as
  // a namespace; doing so would let users mutate it via UpdateNamespace*
  // (NotSupported) and break the listing/load symmetry users rely on.
  ASSERT_TRUE(catalog_->CreateNamespace(Namespace{.levels = {"db"}}, {}).has_value());
  TableIdentifier table{.ns = Namespace{.levels = {"db"}}, .name = "events"};
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});
  ASSERT_TRUE(catalog_
                  ->CreateTable(table, schema, PartitionSpec::Unpartitioned(),
                                SortOrder::Unsorted(), "", {})
                  .has_value());

  auto res = catalog_->GetNamespaceProperties(Namespace{.levels = {"db", "events"}});
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(ErrorKind::kNoSuchNamespace, res.error().kind);
}

TEST_F(HadoopCatalogNamespaceTest, DropNamespaceRejectsTablePath) {
  // Crucial: DropNamespace on a table directory must NOT silently
  // recursively delete the table's metadata and data. Surface NoSuchNamespace
  // so the operator routes through DropTable (which honours purge).
  ASSERT_TRUE(catalog_->CreateNamespace(Namespace{.levels = {"db"}}, {}).has_value());
  TableIdentifier table{.ns = Namespace{.levels = {"db"}}, .name = "events"};
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});
  ASSERT_TRUE(catalog_
                  ->CreateTable(table, schema, PartitionSpec::Unpartitioned(),
                                SortOrder::Unsorted(), "", {})
                  .has_value());

  auto res = catalog_->DropNamespace(Namespace{.levels = {"db", "events"}});
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(ErrorKind::kNoSuchNamespace, res.error().kind);

  // Table must still be intact afterwards.
  auto still_table = catalog_->TableExists(table);
  ASSERT_TRUE(still_table.has_value());
  EXPECT_TRUE(*still_table);
}

TEST_F(HadoopCatalogNamespaceTest, CreateTableRejectsPopulatedNamespace) {
  // Refuse to bury an existing namespace (and its child tables/namespaces)
  // under a new table. The check sees a child entry whose name is not
  // metadata/data and refuses the create.
  ASSERT_TRUE(catalog_->CreateNamespace(Namespace{.levels = {"db"}}, {}).has_value());
  ASSERT_TRUE(
      catalog_->CreateNamespace(Namespace{.levels = {"db", "team"}}, {}).has_value());
  ASSERT_TRUE(catalog_->CreateNamespace(Namespace{.levels = {"db", "team", "proj"}}, {})
                  .has_value());

  TableIdentifier conflict{.ns = Namespace{.levels = {"db"}}, .name = "team"};
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});
  auto res = catalog_->CreateTable(conflict, schema, PartitionSpec::Unpartitioned(),
                                   SortOrder::Unsorted(), "", {});
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(ErrorKind::kInvalidArgument, res.error().kind);

  // The pre-existing namespace must still be intact and listable.
  auto exists = catalog_->NamespaceExists(Namespace{.levels = {"db", "team"}});
  ASSERT_TRUE(exists.has_value());
  EXPECT_TRUE(*exists);
}

TEST_F(HadoopCatalogNamespaceTest, RegisterRejectsMismatchedLocation) {
  // RegisterTable that imports metadata from another table's path would
  // leave the new table's recorded `location` pointing at the OTHER
  // table's data directory. DropTable(purge=true) on the new identifier
  // then deletes only its own subtree, orphaning the actually-referenced
  // data files. Refuse the registration to keep Catalog::DropTable's
  // contract intact.
  ASSERT_TRUE(catalog_->CreateNamespace(Namespace{.levels = {"db"}}, {}).has_value());
  TableIdentifier source_id{.ns = Namespace{.levels = {"db"}}, .name = "source"};
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});
  ICEBERG_UNWRAP_OR_FAIL(
      auto source,
      catalog_->CreateTable(source_id, schema, PartitionSpec::Unpartitioned(),
                            SortOrder::Unsorted(), "", {}));

  TableIdentifier registered_id{.ns = Namespace{.levels = {"db"}}, .name = "registered"};
  auto registered = catalog_->RegisterTable(
      registered_id, std::string(source->metadata_file_location()));
  ASSERT_FALSE(registered.has_value());
  EXPECT_EQ(ErrorKind::kInvalidArgument, registered.error().kind);
}

TEST_F(HadoopCatalogNamespaceTest, CreateThenLoadTableRoundTrip) {
  ASSERT_TRUE(catalog_->CreateNamespace(Namespace{.levels = {"db"}}, {}).has_value());
  TableIdentifier id{.ns = Namespace{.levels = {"db"}}, .name = "events"};

  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});
  auto spec = PartitionSpec::Unpartitioned();
  auto order = SortOrder::Unsorted();
  auto created = catalog_->CreateTable(id, schema, spec, order, /*location=*/"", {});
  ASSERT_TRUE(created.has_value()) << created.error().message;
  EXPECT_EQ((*created)->name(), id);

  auto loaded = catalog_->LoadTable(id);
  ASSERT_TRUE(loaded.has_value()) << loaded.error().message;
  EXPECT_EQ((*loaded)->name(), id);

  // Creating again over the existing table must fail with kAlreadyExists.
  auto again = catalog_->CreateTable(id, schema, spec, order, /*location=*/"", {});
  ASSERT_FALSE(again.has_value());
  EXPECT_EQ(ErrorKind::kAlreadyExists, again.error().kind);
}

TEST_F(HadoopCatalogNamespaceTest, CreateTableRejectsExternalLocation) {
  // Catalog::DropTable(purge=true) deletes the warehouse-derived table
  // directory only; allowing a custom location would leave the actual data
  // unreachable to purge. The catalog refuses up front so the contract
  // can't be silently violated.
  ASSERT_TRUE(catalog_->CreateNamespace(Namespace{.levels = {"db"}}, {}).has_value());
  TableIdentifier id{.ns = Namespace{.levels = {"db"}}, .name = "elsewhere"};
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});

  auto res = catalog_->CreateTable(id, schema, PartitionSpec::Unpartitioned(),
                                   SortOrder::Unsorted(),
                                   /*location=*/"file:///some/other/place", {});
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(ErrorKind::kInvalidArgument, res.error().kind);
}

TEST_F(HadoopCatalogNamespaceTest, CreateTableRejectsWriteDataPath) {
  // `write.data.path` would direct data files outside the table dir, which
  // makes DropTable(purge=true) lose them silently. Refuse the property.
  ASSERT_TRUE(catalog_->CreateNamespace(Namespace{.levels = {"db"}}, {}).has_value());
  TableIdentifier id{.ns = Namespace{.levels = {"db"}}, .name = "redirected"};
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});

  auto res = catalog_->CreateTable(id, schema, PartitionSpec::Unpartitioned(),
                                   SortOrder::Unsorted(), /*location=*/"",
                                   {{"write.data.path", "file:///elsewhere/data"}});
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(ErrorKind::kInvalidArgument, res.error().kind);
}

TEST_F(HadoopCatalogNamespaceTest, RejectsTraversalAliasesInIdentifiers) {
  // CreateNamespace / CreateTable / NamespaceExists / DropTable must all
  // refuse "." and ".." as a namespace level or table name -- otherwise a
  // caller can map operations to the parent of the warehouse and read,
  // write or recursively delete outside the catalog's scope.
  ASSERT_TRUE(catalog_->CreateNamespace(Namespace{.levels = {"db"}}, {}).has_value());

  // Namespace level == "."
  auto ns_dot = catalog_->CreateNamespace(Namespace{.levels = {"db", "."}}, {});
  ASSERT_FALSE(ns_dot.has_value());
  EXPECT_EQ(ErrorKind::kInvalidArgument, ns_dot.error().kind);

  // Namespace level == ".."
  auto ns_dd = catalog_->CreateNamespace(Namespace{.levels = {".."}}, {});
  ASSERT_FALSE(ns_dd.has_value());
  EXPECT_EQ(ErrorKind::kInvalidArgument, ns_dd.error().kind);

  // Table name == "." would point at the namespace directory.
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});
  auto tbl_dot = catalog_->CreateTable(
      TableIdentifier{.ns = Namespace{.levels = {"db"}}, .name = "."}, schema,
      PartitionSpec::Unpartitioned(), SortOrder::Unsorted(), "", {});
  ASSERT_FALSE(tbl_dot.has_value());
  EXPECT_EQ(ErrorKind::kInvalidArgument, tbl_dot.error().kind);

  // Table name == ".."
  auto tbl_dd = catalog_->CreateTable(
      TableIdentifier{.ns = Namespace{.levels = {"db"}}, .name = ".."}, schema,
      PartitionSpec::Unpartitioned(), SortOrder::Unsorted(), "", {});
  ASSERT_FALSE(tbl_dd.has_value());
  EXPECT_EQ(ErrorKind::kInvalidArgument, tbl_dd.error().kind);

  // ".." used as namespace level on DropTable / TableExists -- the
  // identifier validator must run before any path is built, so we never
  // touch <warehouse>/.. on disk.
  auto drop_dd = catalog_->DropTable(
      TableIdentifier{.ns = Namespace{.levels = {".."}}, .name = "victim"}, true);
  ASSERT_FALSE(drop_dd.has_value());
  EXPECT_EQ(ErrorKind::kInvalidArgument, drop_dd.error().kind);

  auto exists_dot = catalog_->TableExists(
      TableIdentifier{.ns = Namespace{.levels = {"db"}}, .name = "."});
  ASSERT_FALSE(exists_dot.has_value());
  EXPECT_EQ(ErrorKind::kInvalidArgument, exists_dot.error().kind);
}

TEST_F(HadoopCatalogNamespaceTest, CreateTableHonoursGzipCodec) {
  // Round-2 fix: the initial v1 publish honours write.metadata.compression-codec,
  // not just commits. A gzipped CreateTable should land at the canonical
  // v1.gz.metadata.json (core form).
  ASSERT_TRUE(catalog_->CreateNamespace(Namespace{.levels = {"db"}}, {}).has_value());
  TableIdentifier id{.ns = Namespace{.levels = {"db"}}, .name = "compressed"};
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});

  ICEBERG_UNWRAP_OR_FAIL(
      auto created, catalog_->CreateTable(
                        id, schema, PartitionSpec::Unpartitioned(), SortOrder::Unsorted(),
                        /*location=*/"", {{"write.metadata.compression-codec", "gzip"}}));
  EXPECT_TRUE(created->metadata_file_location().ends_with("v1.gz.metadata.json"));

  // Round-trip: LoadTable must decode the gzipped v1 transparently.
  ICEBERG_UNWRAP_OR_FAIL(auto loaded, catalog_->LoadTable(id));
  EXPECT_EQ(loaded->name(), id);
}

TEST_F(HadoopCatalogNamespaceTest, LoadMissingTableReturnsNoSuchTable) {
  // Round-5 fix: a LoadTable on a namespace that has no metadata/ subdir must
  // surface kNoSuchTable rather than the underlying arrow kIOError.
  ASSERT_TRUE(catalog_->CreateNamespace(Namespace{.levels = {"db"}}, {}).has_value());
  TableIdentifier missing{.ns = Namespace{.levels = {"db"}}, .name = "nope"};
  auto res = catalog_->LoadTable(missing);
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(ErrorKind::kNoSuchTable, res.error().kind);
}

TEST_F(HadoopCatalogNamespaceTest, UpdateTableCreateRejectsExistingTable) {
  // Exercise the is_create branch of UpdateTable's fast pre-check: when the
  // table already exists, the call must surface kAlreadyExists before paying
  // for a lock acquire. Round-2 routed the is_create path through the same
  // safe sequence as CreateTable; the pre-check is the visible contract.
  ASSERT_TRUE(catalog_->CreateNamespace(Namespace{.levels = {"db"}}, {}).has_value());
  TableIdentifier id{.ns = Namespace{.levels = {"db"}}, .name = "exists"};
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});

  // Seed the table via the regular CreateTable path first.
  ASSERT_TRUE(catalog_
                  ->CreateTable(id, schema, PartitionSpec::Unpartitioned(),
                                SortOrder::Unsorted(), /*location=*/"", {})
                  .has_value());

  // Now try a create-shape UpdateTable: AssertDoesNotExist + no updates. The
  // fast pre-check in UpdateTable must surface kAlreadyExists without
  // touching the lock manager.
  std::vector<std::unique_ptr<TableUpdate>> updates;
  std::vector<std::unique_ptr<TableRequirement>> requirements;
  requirements.push_back(std::make_unique<table::AssertDoesNotExist>());
  auto duplicate = catalog_->UpdateTable(id, requirements, updates);
  ASSERT_FALSE(duplicate.has_value());
  EXPECT_EQ(ErrorKind::kAlreadyExists, duplicate.error().kind);
}

}  // namespace iceberg::hadoop
