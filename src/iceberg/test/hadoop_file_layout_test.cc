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
  // Reserved by the on-disk layout: a namespace named `metadata` or `data`
  // would let later CreateTable calls bury an existing nested table by
  // having `metadata/` happen to be a real namespace.
  EXPECT_FALSE(ValidateNamespaceLevel("metadata").has_value());
  EXPECT_FALSE(ValidateNamespaceLevel("data").has_value());

  Namespace ok{.levels = {"a", "b", "c"}};
  EXPECT_TRUE(ValidateNamespace(ok).has_value());
  Namespace bad{.levels = {"a", "b/c"}};
  EXPECT_FALSE(ValidateNamespace(bad).has_value());
  Namespace reserved{.levels = {"a", "metadata"}};
  EXPECT_FALSE(ValidateNamespace(reserved).has_value());
}

TEST(HadoopFileLayoutTest, IdentifierRejectsUriSeparators) {
  // `?` and `#` are URI reserved separators -- arrow's URI parser would
  // strip them and everything after, so `db?use_mmap` would resolve to
  // the SAME directory as `db` and let one identifier hijack another's
  // data. Reject at the validator before the component is ever joined
  // into a URI.
  EXPECT_FALSE(ValidateNamespaceLevel("db?use_mmap").has_value());
  EXPECT_FALSE(ValidateNamespaceLevel("db#anchor").has_value());
  TableIdentifier qmark{.ns = Namespace{.levels = {"db"}}, .name = "events?x=1"};
  EXPECT_FALSE(ValidateTableIdentifier(qmark).has_value());
  TableIdentifier hash{.ns = Namespace{.levels = {"db"}}, .name = "evts#anchor"};
  EXPECT_FALSE(ValidateTableIdentifier(hash).has_value());
}

TEST(HadoopFileLayoutTest, CanonicalizeWarehouseKeepsReparseableUri) {
  // CanonicalizeWarehouse must NOT percent-decode -- the result is
  // re-used as an IO URI prefix and decoding would corrupt reserved
  // characters. It only strips a trailing slash.
  auto kept = CanonicalizeWarehouse("file:///tmp/a%23b");
  ASSERT_TRUE(kept.has_value());
  EXPECT_EQ(*kept, "file:///tmp/a%23b")
      << "warehouse must stay re-parseable; %23 (#) must not be decoded";

  auto with_slash = CanonicalizeWarehouse("file:///wh/");
  auto without_slash = CanonicalizeWarehouse("file:///wh");
  ASSERT_TRUE(with_slash.has_value());
  ASSERT_TRUE(without_slash.has_value());
  EXPECT_EQ(*with_slash, *without_slash);
}

TEST(HadoopFileLayoutTest, CanonicalLockKeyCollapsesAliases) {
  // CanonicalLockKey IS the alias-collapsing function (used only for
  // lock identity, never IO). `%20` vs space, `.`/`..` segments, and
  // trailing slashes all fold to one key.
  EXPECT_EQ(CanonicalLockKey("file:///tmp/my%20wh"),
            CanonicalLockKey("file:///tmp/my wh"));
  EXPECT_EQ(CanonicalLockKey("file:///tmp/wh"), CanonicalLockKey("file:///tmp/./wh"));
  EXPECT_EQ(CanonicalLockKey("file:///tmp/a/b/../wh"),
            CanonicalLockKey("file:///tmp/a/wh"));
  EXPECT_EQ(CanonicalLockKey("file:///wh/"), CanonicalLockKey("file:///wh"));
  // %2F decodes to '/', which then collapses identically to a literal
  // slash in the key.
  EXPECT_EQ(CanonicalLockKey("file:///tmp/wh%2Fdb"),
            CanonicalLockKey("file:///tmp/wh/db"));
  // Distinct physical locations stay distinct.
  EXPECT_NE(CanonicalLockKey("file:///tmp/wh/a"), CanonicalLockKey("file:///tmp/wh/b"));
  // `..` at the absolute-path root is clamped (POSIX `/..` == `/`), matching
  // how arrow/the OS resolve the path at IO time. Without the clamp,
  // `file:///../../tmp/wh` would keep a `/../../` prefix and produce a
  // different lock key than `file:///tmp/wh`, letting two catalogs touch the
  // same table under different locks.
  EXPECT_EQ(CanonicalLockKey("file:///../../tmp/wh"), CanonicalLockKey("file:///tmp/wh"));
  EXPECT_EQ(CanonicalLockKey("file:///tmp/a/../../tmp/wh"),
            CanonicalLockKey("file:///tmp/wh"));
}

TEST(HadoopFileLayoutTest, RejectUnsafeTablePathGuardsLockRootAndUriMarkers) {
  // Lock-root leaf is rejected, with or without a trailing slash (the
  // layout strips it before joining, so both forms hit the same physical
  // lock root).
  EXPECT_FALSE(
      RejectUnsafeTablePath("file:///wh/_iceberg_catalog_locks", "test").has_value());
  EXPECT_FALSE(
      RejectUnsafeTablePath("file:///wh/_iceberg_catalog_locks/", "test").has_value());
  EXPECT_FALSE(
      RejectUnsafeTablePath("file:///wh/_iceberg_catalog_locks//", "test").has_value());
  // Raw `?`/`#` in a URI path are rejected (arrow truncates the path there).
  EXPECT_FALSE(RejectUnsafeTablePath("file:///wh/t?x=1", "test").has_value());
  EXPECT_FALSE(RejectUnsafeTablePath("file:///wh/t#frag", "test").has_value());
  // A normal table path is accepted; a bare (non-URI) path with `?`/`#`
  // treats them as literal filename bytes and is allowed.
  EXPECT_TRUE(RejectUnsafeTablePath("file:///wh/db/events", "test").has_value());
  EXPECT_TRUE(RejectUnsafeTablePath("/wh/db/weird?name", "test").has_value());
}

TEST(HadoopFileLayoutTest, IdentifierAllowsUtf8) {
  // Java HadoopCatalog accepts Unicode identifiers. We must not reject
  // valid UTF-8 byte sequences (>= 0x80) wholesale; only control bytes,
  // 0x7f, '%' and '\\' are off-limits. Regression for the previous round
  // which rejected anything > 0x7e and broke `客户.订单`-style names.
  EXPECT_TRUE(ValidateNamespaceLevel("客户").has_value());
  EXPECT_TRUE(ValidateNamespaceLevel("\xe5\xae\xa2\xe6\x88\xb7").has_value());  // 客户
  TableIdentifier utf8{.ns = Namespace{.levels = {"db"}}, .name = "订单"};
  EXPECT_TRUE(ValidateTableIdentifier(utf8).has_value());
}

TEST(HadoopFileLayoutTest, IsPathInsideNormalizedDecodesParent) {
  // The PARENT dir may legitimately contain percent encoding (e.g. a
  // warehouse with a space in its name). When the candidate path is
  // already in decoded form, comparing it raw against the encoded
  // parent would falsely report "outside" -- the previous round
  // decoded only the candidate. Both sides must be decoded.
  EXPECT_TRUE(IsPathInsideNormalized("file:///tmp/my wh/db/t/metadata/v1.metadata.json",
                                     "file:///tmp/my%20wh/db/t"));
  EXPECT_TRUE(IsPathInsideNormalized("file:///tmp/my%20wh/db/t/metadata/v1.metadata.json",
                                     "file:///tmp/my%20wh/db/t"));
}

TEST(HadoopFileLayoutTest, IsPathInsideNormalizedDoesNotDecodeLiteralPaths) {
  // FileIO percent-decodes only the URI form. Literal local paths pass
  // through to the OS unchanged. The descendant check must match that
  // behaviour: decoding a literal `/tmp/t%2Foutside/x.puffin` to
  // `/tmp/t/outside/x.puffin` would falsely classify it as inside
  // `/tmp/t`, while the real file lives at the sibling
  // `/tmp/t%2Foutside/` directory.
  EXPECT_FALSE(IsPathInsideNormalized("/tmp/table%2Foutside/x.puffin", "/tmp/table"));
  EXPECT_FALSE(IsPathInsideNormalized("/tmp/table%2F%2E%2E/x.puffin", "/tmp/table"));
  // Literal `/tmp/table/x.puffin` IS inside `/tmp/table` -- no decode happens
  // and the path-component-aware check passes.
  EXPECT_TRUE(IsPathInsideNormalized("/tmp/table/x.puffin", "/tmp/table"));
  // Mixed shape (URI vs literal) -- conservatively refuse.
  EXPECT_FALSE(IsPathInsideNormalized("file:///tmp/table/x.puffin", "/tmp/table"));
  EXPECT_FALSE(IsPathInsideNormalized("/tmp/table/x.puffin", "file:///tmp/table"));
}

TEST(HadoopFileLayoutTest, IsPathInsideNormalizedRejectsBackslash) {
  // Backslash is a directory separator on Windows. `\\..\\` (or
  // %5c..%5c) between `<table>` and the rest of the path would resolve
  // to `<table>/../...` at IO time and escape, but a `/`-only segment
  // scan would miss it. Treat `\\` as a separator too.
  EXPECT_FALSE(IsPathInsideNormalized("file:///wh/db/t\\..\\outside\\x.puffin",
                                      "file:///wh/db/t"));
  EXPECT_FALSE(IsPathInsideNormalized("file:///wh/db/t%5c..%5coutside%5cx.puffin",
                                      "file:///wh/db/t"));
  EXPECT_FALSE(IsPathInsideNormalized("file:///wh/db/t%5C..%5Coutside%5Cx.puffin",
                                      "file:///wh/db/t"));
}

TEST(HadoopFileLayoutTest, IsPathInsideNormalizedRejectsTraversal) {
  // Raw `..` and percent-encoded `%2e%2e` segments must both fall out
  // of the descendant check, even when the literal string prefix looks
  // contained. arrow's URI parser percent-decodes these before issuing
  // the IO, so trusting a naive prefix test would let
  // `file:///wh/db/t/../outside/x.puffin` slip past as "inside `<table>`".
  EXPECT_FALSE(
      IsPathInsideNormalized("file:///wh/db/t/../outside/x.puffin", "file:///wh/db/t"));
  EXPECT_FALSE(IsPathInsideNormalized("file:///wh/db/t/%2e%2e/outside/x.puffin",
                                      "file:///wh/db/t"));
  EXPECT_FALSE(IsPathInsideNormalized("file:///wh/db/t/%2E%2E/outside/x.puffin",
                                      "file:///wh/db/t"));
  // `.` segments are also rejected (a path containing `.` is not
  // canonical; well-behaved writers should not emit them).
  EXPECT_FALSE(IsPathInsideNormalized("file:///wh/db/t/./x.puffin", "file:///wh/db/t"));
  // Legitimate descendants still pass.
  EXPECT_TRUE(IsPathInsideNormalized("file:///wh/db/t/x.puffin", "file:///wh/db/t"));
  EXPECT_TRUE(
      IsPathInsideNormalized("file:///wh/db/t/data/0/x.parquet", "file:///wh/db/t"));
  // Component-aware sibling check inherited from IsPathInside.
  EXPECT_FALSE(
      IsPathInsideNormalized("file:///wh/db/t_backup/x.puffin", "file:///wh/db/t"));
  // Malformed percent encoding is conservatively refused.
  EXPECT_FALSE(IsPathInsideNormalized("file:///wh/db/t/%2", "file:///wh/db/t"));
}

TEST(HadoopFileLayoutTest, NamespaceLevelRejectsPercentEncoding) {
  // arrow's URI parser percent-decodes path components, so `%2e%2e` would
  // resolve to `..` and escape the warehouse. Reject the '%' marker
  // (and any non-printable / non-ASCII byte) at the validator before the
  // component is joined into a URI.
  EXPECT_FALSE(ValidateNamespaceLevel("%2e%2e").has_value());
  EXPECT_FALSE(ValidateNamespaceLevel("%2E%2E").has_value());
  EXPECT_FALSE(ValidateNamespaceLevel("a%2fb").has_value());  // %2f -> '/'
  EXPECT_FALSE(ValidateNamespaceLevel("plain%name").has_value());
  // Non-printable / non-ASCII rejected too.
  std::string with_nul{"abc\0def", 7};
  EXPECT_FALSE(ValidateNamespaceLevel(with_nul).has_value());
  EXPECT_FALSE(ValidateNamespaceLevel("back\\slash").has_value());

  TableIdentifier pct{.ns = Namespace{.levels = {"db"}}, .name = "%2e"};
  EXPECT_FALSE(ValidateTableIdentifier(pct).has_value());
}

TEST(HadoopFileLayoutTest, IsPathInsideRejectsSiblings) {
  // Component-aware descendant check: `/wh/db/stats_backup` is a SIBLING
  // of `/wh/db/stats`, not a child. A naive starts_with() check used to
  // misclassify it; IsPathInside must require a `/` boundary.
  EXPECT_TRUE(IsPathInside("file:///wh/db/stats", "file:///wh/db/stats"));
  EXPECT_TRUE(IsPathInside("file:///wh/db/stats/", "file:///wh/db/stats"));
  EXPECT_TRUE(IsPathInside("file:///wh/db/stats/x.puffin", "file:///wh/db/stats"));
  EXPECT_TRUE(IsPathInside("file:///wh/db/stats/x.puffin", "file:///wh/db/stats/"));
  // Siblings sharing a prefix must NOT be classified as descendants.
  EXPECT_FALSE(IsPathInside("file:///wh/db/stats_backup", "file:///wh/db/stats"));
  EXPECT_FALSE(
      IsPathInside("file:///wh/db/stats_backup/x.puffin", "file:///wh/db/stats"));
  EXPECT_FALSE(IsPathInside("file:///other", "file:///wh/db/stats"));
}

TEST(HadoopFileLayoutTest, NamespaceLevelRejectsTraversalAliases) {
  // POSIX path components "." and ".." would let a caller resolve to the
  // parent namespace dir or escape the warehouse entirely. Reject them
  // before they reach the filesystem layer.
  EXPECT_FALSE(ValidateNamespaceLevel(".").has_value());
  EXPECT_FALSE(ValidateNamespaceLevel("..").has_value());

  Namespace dot_in_levels{.levels = {"db", "."}};
  EXPECT_FALSE(ValidateNamespace(dot_in_levels).has_value());
  Namespace dotdot_in_levels{.levels = {"..", "victim"}};
  EXPECT_FALSE(ValidateNamespace(dotdot_in_levels).has_value());
}

TEST(HadoopFileLayoutTest, TableIdentifierValidation) {
  TableIdentifier good{.ns = Namespace{.levels = {"db"}}, .name = "events"};
  EXPECT_TRUE(ValidateTableIdentifier(good).has_value());

  TableIdentifier missing_name{.ns = Namespace{.levels = {"db"}}, .name = ""};
  EXPECT_FALSE(ValidateTableIdentifier(missing_name).has_value());

  TableIdentifier slash{.ns = Namespace{.levels = {"db"}}, .name = "a/b"};
  EXPECT_FALSE(ValidateTableIdentifier(slash).has_value());

  // Reserved names: a table called `metadata` or `data` would collide with
  // the table-internal subdirectory layout.
  TableIdentifier reserved_name{.ns = Namespace{.levels = {"db"}}, .name = "metadata"};
  EXPECT_FALSE(ValidateTableIdentifier(reserved_name).has_value());
  TableIdentifier reserved_data{.ns = Namespace{.levels = {"db"}}, .name = "data"};
  EXPECT_FALSE(ValidateTableIdentifier(reserved_data).has_value());
  // Reserved namespace level also fails even when the leaf table name is ok.
  TableIdentifier reserved_ns{.ns = Namespace{.levels = {"db", "metadata"}},
                              .name = "events"};
  EXPECT_FALSE(ValidateTableIdentifier(reserved_ns).has_value());

  // Traversal aliases as table name (table "." would resolve to the
  // namespace dir, ".." would escape it).
  TableIdentifier dot{.ns = Namespace{.levels = {"db"}}, .name = "."};
  EXPECT_FALSE(ValidateTableIdentifier(dot).has_value());
  TableIdentifier dotdot{.ns = Namespace{.levels = {"db"}}, .name = ".."};
  EXPECT_FALSE(ValidateTableIdentifier(dotdot).has_value());
  // Traversal alias inside the namespace levels.
  TableIdentifier traverse_ns{.ns = Namespace{.levels = {".."}}, .name = "events"};
  EXPECT_FALSE(ValidateTableIdentifier(traverse_ns).has_value());
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
  // Core canonical gzip shape (matches
  // TableMetadataUtil::Codec::kGzipTableMetadataFileSuffix).
  EXPECT_EQ("file:///tmp/wh/db/events/metadata/v17.gz.metadata.json",
            MetadataFilePath(table, 17, MetadataCompressionCodec::kGzip));
  EXPECT_EQ("file:///tmp/wh/db/events/metadata/version-hint.text",
            VersionHintPath(table));
  EXPECT_EQ("file:///tmp/wh/db/events/metadata/_lock", LockFilePath(table));
}

TEST(HadoopFileLayoutTest, ParseMetadataFileNameAcceptsBothGzipShapes) {
  auto plain = ParseMetadataFileName("v3.metadata.json");
  ASSERT_TRUE(plain.has_value());
  EXPECT_EQ(3, plain->version);
  EXPECT_EQ(MetadataCompressionCodec::kNone, plain->codec);

  // Legacy Hadoop writers emit ".metadata.json.gz" -- still accepted.
  auto legacy = ParseMetadataFileName("v42.metadata.json.gz");
  ASSERT_TRUE(legacy.has_value());
  EXPECT_EQ(42, legacy->version);
  EXPECT_EQ(MetadataCompressionCodec::kGzip, legacy->codec);

  // Core canonical: ".gz.metadata.json".
  auto canonical = ParseMetadataFileName("v42.gz.metadata.json");
  ASSERT_TRUE(canonical.has_value());
  EXPECT_EQ(42, canonical->version);
  EXPECT_EQ(MetadataCompressionCodec::kGzip, canonical->codec);
}

TEST(HadoopFileLayoutTest, ParseMetadataFileNameRejectsZstd) {
  // zstd files MUST NOT be classified as readable metadata: the decoder
  // returns NotSupported, so accepting them on the listing side would
  // produce TableExists=true / LoadTable=JSON parse error.
  auto zstd_legacy = ParseMetadataFileName("v0.metadata.json.zstd");
  EXPECT_FALSE(zstd_legacy.has_value());
  auto zstd_canonical = ParseMetadataFileName("v0.zstd.metadata.json");
  EXPECT_FALSE(zstd_canonical.has_value());
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
