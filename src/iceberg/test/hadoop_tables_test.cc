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

#include "iceberg/catalog/hadoop/hadoop_tables.h"

#include <memory>
#include <string>

#include <arrow/filesystem/localfs.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/arrow/arrow_io_register.h"
#include "iceberg/catalog/hadoop/hadoop_file_layout.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/sort_order.h"
#include "iceberg/table.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/temp_file_test_base.h"
#include "iceberg/type.h"

namespace iceberg::hadoop {

class HadoopTablesTest : public ::iceberg::TempFileTestBase {
 protected:
  void SetUp() override {
    ::iceberg::TempFileTestBase::SetUp();
    root_ = "file://" + CreateTempDirectory();
    file_io_ = std::make_shared<::iceberg::arrow::ArrowFileSystemFileIO>(
        std::make_shared<::arrow::fs::LocalFileSystem>());
    tables_ = std::make_shared<HadoopTables>(file_io_);
  }

  std::string root_;
  std::shared_ptr<FileIO> file_io_;
  std::shared_ptr<HadoopTables> tables_;
};

TEST_F(HadoopTablesTest, CreateLoadDropRoundTrip) {
  const std::string path = root_ + "/standalone";
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});

  auto created = tables_->Create(schema, PartitionSpec::Unpartitioned(),
                                 SortOrder::Unsorted(), path, /*properties=*/{});
  ASSERT_TRUE(created.has_value()) << created.error().message;
  EXPECT_TRUE((*created)->metadata_file_location().ends_with("v1.metadata.json"));

  // Exists should now return true.
  auto exists = tables_->Exists(path);
  ASSERT_TRUE(exists.has_value());
  EXPECT_TRUE(*exists);

  // Load returns a table; identifier name comes from the path leaf.
  auto loaded = tables_->Load(path);
  ASSERT_TRUE(loaded.has_value());
  EXPECT_EQ((*loaded)->name().name, "standalone");

  // Create over an existing table fails with kAlreadyExists.
  auto again = tables_->Create(schema, PartitionSpec::Unpartitioned(),
                               SortOrder::Unsorted(), path, /*properties=*/{});
  ASSERT_FALSE(again.has_value());
  EXPECT_EQ(ErrorKind::kAlreadyExists, again.error().kind);

  ASSERT_TRUE(tables_->DropTable(path, /*purge=*/true).has_value());
  auto exists_after = tables_->Exists(path);
  ASSERT_TRUE(exists_after.has_value());
  EXPECT_FALSE(*exists_after);
}

TEST_F(HadoopTablesTest, RegisterRejectsMismatchedLocation) {
  // RegisterTable that imports metadata from a different on-disk location
  // would silently mismatch the table's recorded `location` against the
  // registration path. DropTable(purge=true) on the registration path
  // would only wipe its own subtree, leaving the data referenced by the
  // source metadata orphaned -- a Catalog::DropTable contract violation.
  // Refuse the registration up front.
  const std::string source_path = root_ + "/source";
  const std::string registered_path = root_ + "/registered";
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});

  ICEBERG_UNWRAP_OR_FAIL(
      auto source,
      tables_->Create(schema, PartitionSpec::Unpartitioned(), SortOrder::Unsorted(),
                      source_path, /*properties=*/{}));

  auto registered = tables_->RegisterTable(registered_path,
                                           std::string(source->metadata_file_location()));
  ASSERT_FALSE(registered.has_value());
  EXPECT_EQ(ErrorKind::kInvalidArgument, registered.error().kind);
}

TEST_F(HadoopTablesTest, LoadMissingTableReturnsNoSuchTable) {
  auto res = tables_->Load(root_ + "/nope");
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(ErrorKind::kNoSuchTable, res.error().kind);
}

TEST_F(HadoopTablesTest, DropMissingTableReturnsNoSuchTable) {
  // is_table check fires before the purge gate, so even with purge=false a
  // missing-path call surfaces kNoSuchTable.
  auto res = tables_->DropTable(root_ + "/nope", /*purge=*/false);
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(ErrorKind::kNoSuchTable, res.error().kind);
}

TEST_F(HadoopTablesTest, DropTablePurgeFalseIsNotSupported) {
  // Same rationale as HadoopCatalog::DropTablePurgeFalseIsNotSupported:
  // leaving the data/ tree behind exposes a namespace-shaped orphan.
  const std::string path = root_ + "/preserve";
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});
  ASSERT_TRUE(tables_
                  ->Create(schema, PartitionSpec::Unpartitioned(), SortOrder::Unsorted(),
                           path, /*properties=*/{})
                  .has_value());

  auto res = tables_->DropTable(path, /*purge=*/false);
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(ErrorKind::kNotSupported, res.error().kind);

  // Table is still intact after the refusal.
  auto exists = tables_->Exists(path);
  ASSERT_TRUE(exists.has_value());
  EXPECT_TRUE(*exists);
}

TEST_F(HadoopTablesTest, DropTablePurgeTrueRemovesData) {
  const std::string path = root_ + "/purge";
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});
  ASSERT_TRUE(tables_
                  ->Create(schema, PartitionSpec::Unpartitioned(), SortOrder::Unsorted(),
                           path, /*properties=*/{})
                  .has_value());

  const std::string data_path = hadoop::DataDir(path) + "/sample.parquet";
  ASSERT_TRUE(file_io_->CreateDir(hadoop::DataDir(path)).has_value());
  ASSERT_TRUE(file_io_->WriteFile(data_path, "dummy").has_value());

  ASSERT_TRUE(tables_->DropTable(path, /*purge=*/true).has_value());
  ICEBERG_UNWRAP_OR_FAIL(auto data_exists, file_io_->Exists(data_path));
  EXPECT_FALSE(data_exists) << "purge=true must delete data files";
}

TEST_F(HadoopTablesTest, CreateRejectsPopulatedPath) {
  // If the target path already contains unrelated content (e.g. a namespace
  // populated by HadoopCatalog), Create must refuse rather than burying the
  // existing tree under a new metadata/.
  const std::string path = root_ + "/occupied";
  ASSERT_TRUE(file_io_->CreateDir(path + "/child").has_value());

  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});
  auto res = tables_->Create(schema, PartitionSpec::Unpartitioned(),
                             SortOrder::Unsorted(), path, /*properties=*/{});
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(ErrorKind::kInvalidArgument, res.error().kind);
}

TEST_F(HadoopTablesTest, RegisterRejectsPopulatedPath) {
  // RegisterTable must run the same occupied-path guard as Create:
  // registering a table at a path that already holds a populated
  // namespace would bury the descendants and make them purgeable.
  const std::string source_path = root_ + "/src";
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});
  ICEBERG_UNWRAP_OR_FAIL(
      auto source,
      tables_->Create(schema, PartitionSpec::Unpartitioned(), SortOrder::Unsorted(),
                      source_path, /*properties=*/{}));

  // Build an occupied target: a directory with a non-table child.
  const std::string target = root_ + "/occupied_reg";
  ASSERT_TRUE(file_io_->CreateDir(target + "/child_ns").has_value());

  auto res =
      tables_->RegisterTable(target, std::string(source->metadata_file_location()));
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(ErrorKind::kInvalidArgument, res.error().kind);
}

TEST_F(HadoopTablesTest, AutoDetectRejectsMixedSchemes) {
  // The auto-detect overload caches the first scheme it sees. A subsequent
  // call against a different scheme should be rejected up front rather than
  // silently routed through the cached (wrong) FileIO.
  ::iceberg::arrow::EnsureArrowFileIOsRegistered();
  HadoopTables auto_tables;
  // Force first call to register the local FileIO via the file:// scheme.
  auto first = auto_tables.Exists(root_ + "/probe");
  ASSERT_TRUE(first.has_value()) << first.error().message;
  EXPECT_FALSE(*first);
  // A different-scheme call must surface kInvalidArgument before reaching
  // FileIORegistry (which would otherwise return kNotImplemented when HDFS
  // isn't built into this binary).
  auto mixed = auto_tables.Exists("hdfs://example/path");
  ASSERT_FALSE(mixed.has_value());
  EXPECT_EQ(ErrorKind::kInvalidArgument, mixed.error().kind);
}

TEST_F(HadoopTablesTest, MutatingApisRejectLockRootPath) {
  // The bare-path API cannot know a path IS some warehouse's lock root, but
  // any path whose leaf is the reserved lock-root name could be one: a
  // lock-impl=file catalog stores its lock files there. Creating a table
  // over it, or purging it, would corrupt active file locks. Create /
  // RegisterTable / DropTable must all refuse the reserved leaf name.
  const std::string lock_path = root_ + "/" + std::string(hadoop::kLockRootDirName);
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});

  auto created = tables_->Create(schema, PartitionSpec::Unpartitioned(),
                                 SortOrder::Unsorted(), lock_path, /*properties=*/{});
  ASSERT_FALSE(created.has_value());
  EXPECT_EQ(ErrorKind::kInvalidArgument, created.error().kind);

  auto dropped = tables_->DropTable(lock_path, /*purge=*/true);
  ASSERT_FALSE(dropped.has_value());
  EXPECT_EQ(ErrorKind::kInvalidArgument, dropped.error().kind);

  auto registered = tables_->RegisterTable(lock_path, root_ + "/whatever.metadata.json");
  ASSERT_FALSE(registered.has_value());
  EXPECT_EQ(ErrorKind::kInvalidArgument, registered.error().kind);

  // A trailing slash must not bypass the guard: the layout strips it before
  // joining, so `.../_iceberg_catalog_locks/` hits the same physical root.
  auto created_slash =
      tables_->Create(schema, PartitionSpec::Unpartitioned(), SortOrder::Unsorted(),
                      lock_path + "/", /*properties=*/{});
  ASSERT_FALSE(created_slash.has_value());
  EXPECT_EQ(ErrorKind::kInvalidArgument, created_slash.error().kind);
}

TEST_F(HadoopTablesTest, TrailingSlashKeepsLeafTableName) {
  // The layout joiner strips trailing slashes, so `.../events/` and
  // `.../events` refer to the same physical table. PathToIdentifier must
  // mirror that normalisation -- without it, Table::name().name comes back
  // empty for the trailing-slash form even though Load/Create/Register
  // succeeded against the right directory.
  const std::string path = root_ + "/events/";
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});

  ICEBERG_UNWRAP_OR_FAIL(auto created,
                         tables_->Create(schema, PartitionSpec::Unpartitioned(),
                                         SortOrder::Unsorted(), path, /*properties=*/{}));
  EXPECT_EQ(created->name().name, "events")
      << "trailing-slash path must still yield the leaf as the table name";

  ICEBERG_UNWRAP_OR_FAIL(auto loaded, tables_->Load(path));
  EXPECT_EQ(loaded->name().name, "events");
}

TEST_F(HadoopTablesTest, RootOnlyPathRejectedByAllMutatingAndReadApis) {
  // After trailing-slash stripping, a `file:///` / `/` path has no leaf
  // component. Refuse it across the whole API surface: producing a Table
  // with an empty name is nonsense, and operating on the filesystem root is
  // never what the caller meant.
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});
  for (const std::string& bad : {std::string("file:///"), std::string("file:////"),
                                 std::string("file:///") /* keep */}) {
    auto created = tables_->Create(schema, PartitionSpec::Unpartitioned(),
                                   SortOrder::Unsorted(), bad, /*properties=*/{});
    ASSERT_FALSE(created.has_value()) << bad;
    EXPECT_EQ(ErrorKind::kInvalidArgument, created.error().kind);
    auto load = tables_->Load(bad);
    ASSERT_FALSE(load.has_value()) << bad;
    EXPECT_EQ(ErrorKind::kInvalidArgument, load.error().kind);
    auto exists = tables_->Exists(bad);
    ASSERT_FALSE(exists.has_value()) << bad;
    EXPECT_EQ(ErrorKind::kInvalidArgument, exists.error().kind);
    auto drop = tables_->DropTable(bad, /*purge=*/true);
    ASSERT_FALSE(drop.has_value()) << bad;
    EXPECT_EQ(ErrorKind::kInvalidArgument, drop.error().kind);
    auto reg = tables_->RegisterTable(bad, root_ + "/whatever.metadata.json");
    ASSERT_FALSE(reg.has_value()) << bad;
    EXPECT_EQ(ErrorKind::kInvalidArgument, reg.error().kind);
  }
}

TEST_F(HadoopTablesTest, MutatingApisRejectRawUriQueryOrFragment) {
  // The bare-path API receives the location directly; a URI path with a raw
  // `?`/`#` would be truncated at the marker by arrow's URI parser, so the
  // metadata/data would not land at the literal path. Refuse it.
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});
  for (const std::string& bad : {root_ + "/t?x=1", root_ + "/t#frag"}) {
    auto created = tables_->Create(schema, PartitionSpec::Unpartitioned(),
                                   SortOrder::Unsorted(), bad, /*properties=*/{});
    ASSERT_FALSE(created.has_value()) << "expected rejection for " << bad;
    EXPECT_EQ(ErrorKind::kInvalidArgument, created.error().kind);
  }
}

TEST_F(HadoopTablesTest, RegisterRejectsUuidlessMetadata) {
  // Mirror HadoopCatalog::RegisterTable: refuse importing metadata with no
  // table-uuid, which would defeat the commit-time ABA guard. `location` is
  // set to the target so the uuid check (which runs first) is what trips.
  const std::string src_path = root_ + "/src";
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64())});
  ICEBERG_UNWRAP_OR_FAIL(
      auto src, tables_->Create(schema, PartitionSpec::Unpartitioned(),
                                SortOrder::Unsorted(), src_path, /*properties=*/{}));

  const std::string tgt_path = root_ + "/imported";
  ICEBERG_UNWRAP_OR_FAIL(
      auto raw,
      file_io_->ReadFile(std::string(src->metadata_file_location()), std::nullopt));
  auto json = nlohmann::json::parse(raw);
  json["location"] = tgt_path;
  json.erase("table-uuid");
  const std::string ext_path = root_ + "/external_v1.metadata.json";
  ASSERT_TRUE(file_io_->WriteFile(ext_path, json.dump()).has_value());

  auto registered = tables_->RegisterTable(tgt_path, ext_path);
  ASSERT_FALSE(registered.has_value());
  EXPECT_EQ(ErrorKind::kInvalidArgument, registered.error().kind);
  EXPECT_NE(registered.error().message.find("table-uuid"), std::string::npos);
}

}  // namespace iceberg::hadoop
