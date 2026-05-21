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

#include "iceberg/catalog/hadoop/hadoop_table_operations.h"

#include <fstream>
#include <memory>
#include <sstream>
#include <string>

#include <arrow/filesystem/localfs.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/catalog/hadoop/hadoop_file_layout.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/temp_file_test_base.h"
#include "iceberg/test/test_resource.h"

namespace iceberg::hadoop {

namespace {

// Slurp a fixture file from src/iceberg/test/resources.
std::string SlurpResource(const std::string& name) {
  std::ifstream file(GetResourcePath(name));
  std::stringstream buffer;
  buffer << file.rdbuf();
  return buffer.str();
}

}  // namespace

class HadoopTableOperationsTest : public ::iceberg::TempFileTestBase {
 protected:
  void SetUp() override {
    ::iceberg::TempFileTestBase::SetUp();
    warehouse_ = "file://" + CreateTempDirectory();
    table_dir_ = warehouse_ + "/db/events";
    file_io_ = std::make_shared<::iceberg::arrow::ArrowFileSystemFileIO>(
        std::make_shared<::arrow::fs::LocalFileSystem>());
    // Pre-create the metadata directory.
    ASSERT_TRUE(file_io_->CreateDir(MetadataDir(table_dir_)).has_value());
  }

  // Write a metadata fixture into the table's metadata directory.
  void SeedMetadataFile(int64_t version, MetadataCompressionCodec codec,
                        const std::string& body) {
    const std::string path =
        MetadataDir(table_dir_) + "/" + MetadataFileName(version, codec);
    ASSERT_TRUE(file_io_->WriteFile(path, body).has_value());
  }

  void SeedVersionHint(const std::string& body) {
    ASSERT_TRUE(file_io_->WriteFile(VersionHintPath(table_dir_), body).has_value());
  }

  std::string warehouse_;
  std::string table_dir_;
  std::shared_ptr<FileIO> file_io_;
};

TEST_F(HadoopTableOperationsTest, ReadVersionHintParsesTrimmedDecimal) {
  SeedVersionHint("42\n");
  ICEBERG_UNWRAP_OR_FAIL(auto version, ReadVersionHint(*file_io_, table_dir_));
  EXPECT_EQ(version, 42);

  // Whitespace and CRLF are tolerated.
  SeedVersionHint("  7\r\n");
  ICEBERG_UNWRAP_OR_FAIL(auto v7, ReadVersionHint(*file_io_, table_dir_));
  EXPECT_EQ(v7, 7);
}

TEST_F(HadoopTableOperationsTest, ReadVersionHintRejectsNonNumeric) {
  SeedVersionHint("abc");
  auto res = ReadVersionHint(*file_io_, table_dir_);
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(ErrorKind::kInvalid, res.error().kind);
}

TEST_F(HadoopTableOperationsTest, FindLatestMetadataVersionPicksMax) {
  const std::string body = SlurpResource("TableMetadataV1Valid.json");
  SeedMetadataFile(1, MetadataCompressionCodec::kNone, body);
  SeedMetadataFile(3, MetadataCompressionCodec::kNone, body);
  SeedMetadataFile(2, MetadataCompressionCodec::kGzip,
                   "ignored-not-valid-gzip-but-name-counts");
  // Drop a UUID-prefixed temp file to make sure it is ignored.
  ASSERT_TRUE(
      file_io_->WriteFile(MetadataDir(table_dir_) + "/abc-uuid.metadata.json", "ignored")
          .has_value());

  ICEBERG_UNWRAP_OR_FAIL(auto latest, FindLatestMetadataVersion(*file_io_, table_dir_));
  EXPECT_EQ(latest, 3);
}

TEST_F(HadoopTableOperationsTest, FindLatestMetadataVersionFailsWhenEmpty) {
  auto res = FindLatestMetadataVersion(*file_io_, table_dir_);
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(ErrorKind::kNoSuchTable, res.error().kind);
}

TEST_F(HadoopTableOperationsTest, ResolveUsesVersionHintWhenPresent) {
  const std::string body = SlurpResource("TableMetadataV1Valid.json");
  SeedMetadataFile(1, MetadataCompressionCodec::kNone, body);
  SeedMetadataFile(5, MetadataCompressionCodec::kNone, body);
  SeedVersionHint("5\n");

  ICEBERG_UNWRAP_OR_FAIL(auto pointer, ResolveCurrentMetadata(*file_io_, table_dir_));
  EXPECT_EQ(pointer.version, 5);
  EXPECT_FALSE(pointer.from_listdir_fallback);
  EXPECT_TRUE(pointer.location.ends_with("v5.metadata.json"));
}

TEST_F(HadoopTableOperationsTest, ResolveFallsBackToListdirWhenHintMissing) {
  const std::string body = SlurpResource("TableMetadataV1Valid.json");
  SeedMetadataFile(2, MetadataCompressionCodec::kNone, body);
  SeedMetadataFile(4, MetadataCompressionCodec::kNone, body);

  ICEBERG_UNWRAP_OR_FAIL(auto pointer, ResolveCurrentMetadata(*file_io_, table_dir_));
  EXPECT_EQ(pointer.version, 4);
  EXPECT_TRUE(pointer.from_listdir_fallback);
  EXPECT_TRUE(pointer.location.ends_with("v4.metadata.json"));
}

TEST_F(HadoopTableOperationsTest, RefreshReturnsTableMetadata) {
  const std::string body = SlurpResource("TableMetadataV1Valid.json");
  SeedMetadataFile(3, MetadataCompressionCodec::kNone, body);
  SeedVersionHint("3");

  HadoopTableOperations ops(file_io_, table_dir_);
  ICEBERG_UNWRAP_OR_FAIL(auto metadata, ops.Refresh());
  ASSERT_NE(metadata, nullptr);
  EXPECT_EQ(metadata->table_uuid, "d20125c8-7284-442c-9aea-15fee620737c");
  EXPECT_EQ(ops.current_version(), 3);
  EXPECT_TRUE(ops.current_metadata_location().ends_with("v3.metadata.json"));
}

TEST_F(HadoopTableOperationsTest, RefreshFailsWhenNoMetadata) {
  HadoopTableOperations ops(file_io_, table_dir_);
  auto res = ops.Refresh();
  ASSERT_FALSE(res.has_value());
  EXPECT_EQ(ErrorKind::kNoSuchTable, res.error().kind);
}

}  // namespace iceberg::hadoop
