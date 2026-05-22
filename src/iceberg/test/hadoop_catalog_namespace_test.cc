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

TEST_F(HadoopCatalogNamespaceTest,
       DropTableRemovesEverythingAndReturnsNoSuchOnSecondTry) {
  ASSERT_TRUE(catalog_->CreateNamespace(Namespace{.levels = {"db"}}, {}).has_value());
  TableIdentifier table{.ns = Namespace{.levels = {"db"}}, .name = "events"};
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});
  ASSERT_TRUE(catalog_
                  ->CreateTable(table, schema, PartitionSpec::Unpartitioned(),
                                SortOrder::Unsorted(), "", {})
                  .has_value());

  EXPECT_TRUE(catalog_->DropTable(table, /*purge=*/false).has_value());
  auto exists_after = catalog_->TableExists(table);
  ASSERT_TRUE(exists_after.has_value());
  EXPECT_FALSE(*exists_after);

  auto again = catalog_->DropTable(table, false);
  ASSERT_FALSE(again.has_value());
  EXPECT_EQ(ErrorKind::kNoSuchTable, again.error().kind);
}

TEST_F(HadoopCatalogNamespaceTest, RegisterExistingMetadataBecomesTable) {
  ASSERT_TRUE(catalog_->CreateNamespace(Namespace{.levels = {"db"}}, {}).has_value());
  TableIdentifier source_id{.ns = Namespace{.levels = {"db"}}, .name = "source"};
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});
  ICEBERG_UNWRAP_OR_FAIL(
      auto source,
      catalog_->CreateTable(source_id, schema, PartitionSpec::Unpartitioned(),
                            SortOrder::Unsorted(), "", {}));

  // Register a fresh table pointing at the same metadata file.
  TableIdentifier registered_id{.ns = Namespace{.levels = {"db"}}, .name = "registered"};
  auto registered = catalog_->RegisterTable(
      registered_id, std::string(source->metadata_file_location()));
  ASSERT_TRUE(registered.has_value()) << registered.error().message;
  EXPECT_EQ((*registered)->name(), registered_id);

  // Re-registering must fail with kAlreadyExists.
  auto again = catalog_->RegisterTable(registered_id,
                                       std::string(source->metadata_file_location()));
  ASSERT_FALSE(again.has_value());
  EXPECT_EQ(ErrorKind::kAlreadyExists, again.error().kind);
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

TEST_F(HadoopCatalogNamespaceTest, CreateTableHonoursGzipCodec) {
  // Round-2 fix: the initial v1 publish honours write.metadata.compression-codec,
  // not just commits. A gzipped CreateTable should land at v1.metadata.json.gz.
  ASSERT_TRUE(catalog_->CreateNamespace(Namespace{.levels = {"db"}}, {}).has_value());
  TableIdentifier id{.ns = Namespace{.levels = {"db"}}, .name = "compressed"};
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});

  ICEBERG_UNWRAP_OR_FAIL(
      auto created, catalog_->CreateTable(
                        id, schema, PartitionSpec::Unpartitioned(), SortOrder::Unsorted(),
                        /*location=*/"", {{"write.metadata.compression-codec", "gzip"}}));
  EXPECT_TRUE(created->metadata_file_location().ends_with("v1.metadata.json.gz"));

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
