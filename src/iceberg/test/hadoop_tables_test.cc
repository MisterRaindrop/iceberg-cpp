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

#include "iceberg/arrow/arrow_io_internal.h"
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
    auto file_io = std::make_shared<::iceberg::arrow::ArrowFileSystemFileIO>(
        std::make_shared<::arrow::fs::LocalFileSystem>());
    tables_ = std::make_shared<HadoopTables>(std::move(file_io));
  }

  std::string root_;
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

  ASSERT_TRUE(tables_->DropTable(path, /*purge=*/false).has_value());
  auto exists_after = tables_->Exists(path);
  ASSERT_TRUE(exists_after.has_value());
  EXPECT_FALSE(*exists_after);
}

TEST_F(HadoopTablesTest, RegisterReusesExternalMetadata) {
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
  ASSERT_TRUE(registered.has_value()) << registered.error().message;

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, tables_->Load(registered_path));
  EXPECT_NE(reloaded, nullptr);
}

TEST_F(HadoopTablesTest, LoadMissingTableReturnsNoSuchTable) {
  auto res = tables_->Load(root_ + "/nope");
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(ErrorKind::kNoSuchTable, res.error().kind);
}

TEST_F(HadoopTablesTest, DropMissingTableReturnsNoSuchTable) {
  auto res = tables_->DropTable(root_ + "/nope", /*purge=*/false);
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(ErrorKind::kNoSuchTable, res.error().kind);
}

}  // namespace iceberg::hadoop
