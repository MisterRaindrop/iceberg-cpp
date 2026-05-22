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

#include "iceberg/catalog/hadoop/hadoop_file_layout.h"

#include <gtest/gtest.h>

namespace iceberg::hadoop {

TEST(HadoopFileLayoutTest, ParseCodecRoundTrip) {
  EXPECT_EQ(MetadataCompressionCodec::kNone, *ParseMetadataCompressionCodec(""));
  EXPECT_EQ(MetadataCompressionCodec::kNone, *ParseMetadataCompressionCodec("none"));
  EXPECT_EQ(MetadataCompressionCodec::kNone, *ParseMetadataCompressionCodec("NONE"));
  EXPECT_EQ(MetadataCompressionCodec::kGzip, *ParseMetadataCompressionCodec("gzip"));
  EXPECT_EQ(MetadataCompressionCodec::kGzip, *ParseMetadataCompressionCodec("Gzip"));
  EXPECT_EQ(MetadataCompressionCodec::kZstd, *ParseMetadataCompressionCodec("zstd"));

  auto bad = ParseMetadataCompressionCodec("snappy");
  ASSERT_FALSE(bad.has_value());
  EXPECT_EQ(ErrorKind::kInvalidArgument, bad.error().kind);

  EXPECT_EQ("none", MetadataCompressionCodecName(MetadataCompressionCodec::kNone));
  EXPECT_EQ("gzip", MetadataCompressionCodecName(MetadataCompressionCodec::kGzip));
  EXPECT_EQ("zstd", MetadataCompressionCodecName(MetadataCompressionCodec::kZstd));
}

TEST(HadoopFileLayoutTest, NamespaceLevelValidation) {
  EXPECT_TRUE(ValidateNamespaceLevel("db").has_value());
  EXPECT_TRUE(ValidateNamespaceLevel("namespaces-with-dashes").has_value());
  EXPECT_FALSE(ValidateNamespaceLevel("").has_value());
  EXPECT_FALSE(ValidateNamespaceLevel("has/slash").has_value());

  Namespace ok{.levels = {"a", "b", "c"}};
  EXPECT_TRUE(ValidateNamespace(ok).has_value());
  Namespace bad{.levels = {"a", "b/c"}};
  EXPECT_FALSE(ValidateNamespace(bad).has_value());
}

TEST(HadoopFileLayoutTest, TableIdentifierValidation) {
  TableIdentifier good{.ns = Namespace{.levels = {"db"}}, .name = "events"};
  EXPECT_TRUE(ValidateTableIdentifier(good).has_value());

  TableIdentifier missing_name{.ns = Namespace{.levels = {"db"}}, .name = ""};
  EXPECT_FALSE(ValidateTableIdentifier(missing_name).has_value());

  TableIdentifier slash{.ns = Namespace{.levels = {"db"}}, .name = "a/b"};
  EXPECT_FALSE(ValidateTableIdentifier(slash).has_value());
}

TEST(HadoopFileLayoutTest, NamespaceDirJoinsLevels) {
  auto root = NamespaceDir("file:///tmp/wh/", Namespace{});
  ASSERT_TRUE(root.has_value());
  EXPECT_EQ("file:///tmp/wh", *root);

  auto db = NamespaceDir("file:///tmp/wh", Namespace{.levels = {"db"}});
  ASSERT_TRUE(db.has_value());
  EXPECT_EQ("file:///tmp/wh/db", *db);

  auto nested = NamespaceDir("hdfs://nn:8020/wh", Namespace{.levels = {"team", "proj"}});
  ASSERT_TRUE(nested.has_value());
  EXPECT_EQ("hdfs://nn:8020/wh/team/proj", *nested);

  // Trailing slash on the warehouse is normalised away.
  auto trailing = NamespaceDir("s3://bucket/wh///", Namespace{.levels = {"db"}});
  ASSERT_TRUE(trailing.has_value());
  EXPECT_EQ("s3://bucket/wh/db", *trailing);

  // Bad namespace level surfaces as an error from this layer too.
  auto bad = NamespaceDir("file:///tmp/wh", Namespace{.levels = {"a/b"}});
  EXPECT_FALSE(bad.has_value());

  // Missing warehouse is rejected.
  auto empty = NamespaceDir("", Namespace{.levels = {"db"}});
  EXPECT_FALSE(empty.has_value());
}

TEST(HadoopFileLayoutTest, TableDirRoutesThroughNamespace) {
  TableIdentifier id{.ns = Namespace{.levels = {"db", "team"}}, .name = "events"};
  auto path = TableDir("file:///tmp/wh", id);
  ASSERT_TRUE(path.has_value());
  EXPECT_EQ("file:///tmp/wh/db/team/events", *path);
}

TEST(HadoopFileLayoutTest, MetadataAndAuxiliaryPaths) {
  const std::string table = "file:///tmp/wh/db/events";
  EXPECT_EQ("file:///tmp/wh/db/events/metadata", MetadataDir(table));
  EXPECT_EQ("file:///tmp/wh/db/events/data", DataDir(table));
  EXPECT_EQ("file:///tmp/wh/db/events/metadata/v1.metadata.json",
            MetadataFilePath(table, 1, MetadataCompressionCodec::kNone));
  EXPECT_EQ("file:///tmp/wh/db/events/metadata/v17.metadata.json.gz",
            MetadataFilePath(table, 17, MetadataCompressionCodec::kGzip));
  EXPECT_EQ("file:///tmp/wh/db/events/metadata/v17.metadata.json.zstd",
            MetadataFilePath(table, 17, MetadataCompressionCodec::kZstd));
  EXPECT_EQ("file:///tmp/wh/db/events/metadata/version-hint.text",
            VersionHintPath(table));
  EXPECT_EQ("file:///tmp/wh/db/events/metadata/_lock", LockFilePath(table));
}

TEST(HadoopFileLayoutTest, ParseMetadataFileNameAcceptsAllCodecs) {
  auto plain = ParseMetadataFileName("v3.metadata.json");
  ASSERT_TRUE(plain.has_value());
  EXPECT_EQ(3, plain->version);
  EXPECT_EQ(MetadataCompressionCodec::kNone, plain->codec);

  auto gzipped = ParseMetadataFileName("v42.metadata.json.gz");
  ASSERT_TRUE(gzipped.has_value());
  EXPECT_EQ(42, gzipped->version);
  EXPECT_EQ(MetadataCompressionCodec::kGzip, gzipped->codec);

  auto zstd = ParseMetadataFileName("v0.metadata.json.zstd");
  ASSERT_TRUE(zstd.has_value());
  EXPECT_EQ(0, zstd->version);
  EXPECT_EQ(MetadataCompressionCodec::kZstd, zstd->codec);
}

TEST(HadoopFileLayoutTest, ParseMetadataFileNameRejectsNonHadoopNames) {
  // UUID-prefixed temp files (the kind HadoopTableOperations writes before
  // renaming to the final vN name) must not be matched as committed metadata.
  EXPECT_FALSE(ParseMetadataFileName("00001-deadbeef.metadata.json").has_value());
  EXPECT_FALSE(ParseMetadataFileName("v.metadata.json").has_value());
  EXPECT_FALSE(ParseMetadataFileName("v3a.metadata.json").has_value());
  EXPECT_FALSE(ParseMetadataFileName("v3.metadata").has_value());
  EXPECT_FALSE(ParseMetadataFileName("v3.metadata.json.snappy").has_value());
  EXPECT_FALSE(ParseMetadataFileName("").has_value());
}

}  // namespace iceberg::hadoop
