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

#include <format>
#include <string>
#include <utility>

#include <nlohmann/json.hpp>

#include "iceberg/catalog/hadoop/hadoop_file_layout.h"
#include "iceberg/catalog/hadoop/hadoop_table_operations.h"
#include "iceberg/file_io.h"
#include "iceberg/file_io_registry.h"
#include "iceberg/json_serde_internal.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/sort_order.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/gzip_internal.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/uuid.h"

namespace iceberg::hadoop {

namespace {

// Convert an absolute table path into a synthetic TableIdentifier so we can
// reuse Table::Make (which keys tables by identifier). The leaf of the path
// becomes the table name; the empty namespace signals "no catalog".
TableIdentifier PathToIdentifier(std::string_view path) {
  return TableIdentifier{.ns = Namespace{}, .name = std::string(Basename(path))};
}

// Publish a v1.metadata.json[.codec] + version-hint.text under `path` via
// UUID-named temp files + atomic rename. HadoopTables has no LockManager, so
// the rename(overwrite=false) is the only cross-writer guard -- on backends
// where that primitive is truly atomic (LocalFileSystem, libhdfs O_EXCL),
// concurrent register/create calls cannot overwrite each other. On
// non-atomic backends the documented caveat from HadoopCatalog applies.
Status PublishInitialMetadata(FileIO& io, const std::string& path,
                              MetadataCompressionCodec codec, std::string_view bytes,
                              std::string& out_metadata_path) {
  const std::string metadata_dir = hadoop::MetadataDir(path);
  ICEBERG_RETURN_UNEXPECTED(io.CreateDir(metadata_dir));

  const std::string metadata_path = hadoop::MetadataFilePath(path, /*version=*/1, codec);
  const std::string metadata_tmp = std::format("{}/{}-v1.metadata.json.tmp", metadata_dir,
                                               Uuid::GenerateV7().ToString());
  if (auto write = io.WriteFile(metadata_tmp, bytes); !write.has_value()) {
    std::ignore = io.DeleteFile(metadata_tmp);
    return write;
  }
  if (auto rename = io.Rename(metadata_tmp, metadata_path, /*overwrite=*/false);
      !rename.has_value()) {
    std::ignore = io.DeleteFile(metadata_tmp);
    // Distinguish "destination already exists" from real backend errors;
    // permission denied / NotSupported / IOError must propagate so the
    // operator can diagnose, not surface as a misleading "table exists".
    if (rename.error().kind == ErrorKind::kAlreadyExists) {
      return AlreadyExists(
          "HadoopTables: v1.metadata.json already exists at '{}' "
          "(possibly an orphan from a crashed creator).",
          metadata_path);
    }
    return rename;
  }

  const std::string hint_path = hadoop::VersionHintPath(path);
  const std::string hint_tmp =
      std::format("{}.tmp.{}", hint_path, Uuid::GenerateV7().ToString());
  if (auto hint_write = io.WriteFile(hint_tmp, std::string("1\n"));
      !hint_write.has_value()) {
    std::ignore = io.DeleteFile(metadata_path);
    return hint_write;
  }
  if (auto hint_rename = io.Rename(hint_tmp, hint_path, /*overwrite=*/true);
      !hint_rename.has_value()) {
    std::ignore = io.DeleteFile(hint_tmp);
    std::ignore = io.DeleteFile(metadata_path);
    return hint_rename;
  }
  out_metadata_path = metadata_path;
  return {};
}

}  // namespace

HadoopTables::HadoopTables(std::shared_ptr<FileIO> file_io,
                           HadoopCatalogProperties config)
    : file_io_(std::move(file_io)), config_(std::move(config)) {}

HadoopTables::HadoopTables() = default;

HadoopTables::~HadoopTables() = default;

namespace {
std::string DetectIoName(std::string_view path) {
  if (path.starts_with("hdfs://")) {
    return std::string(FileIORegistry::kArrowHdfsFileIO);
  }
  if (IsS3Scheme(path)) {
    return std::string(FileIORegistry::kArrowS3FileIO);
  }
  return std::string(FileIORegistry::kArrowLocalFileIO);
}

// Pull the authority (host[:port]) from `hdfs://nn[:port]/...`. Returns an
// empty view for non-hdfs paths or hdfs URIs without an authority.
std::string_view HdfsAuthority(std::string_view path) {
  constexpr std::string_view kHdfsPrefix = "hdfs://";
  if (!path.starts_with(kHdfsPrefix)) {
    return {};
  }
  std::string_view tail = path.substr(kHdfsPrefix.size());
  const auto slash = tail.find('/');
  return slash == std::string_view::npos ? tail : tail.substr(0, slash);
}
}  // namespace

Result<std::shared_ptr<FileIO>> HadoopTables::ResolveFileIO(std::string_view path) {
  std::lock_guard lk(resolve_mutex_);
  std::string io_name = DetectIoName(path);
  const std::string_view authority = HdfsAuthority(path);
  if (file_io_ != nullptr) {
    // The auto-detect overload caches the first scheme it sees. Subsequent
    // calls against a different scheme are rejected up front instead of
    // silently routing through the wrong FileIO -- callers must construct a
    // fresh HadoopTables (or use the explicit-FileIO ctor) to mix schemes.
    if (!cached_scheme_.empty() && cached_scheme_ != io_name) {
      return InvalidArgument(
          "HadoopTables: cannot mix FileIO schemes within one instance "
          "('{}' was already used; '{}' was requested for path '{}').",
          cached_scheme_, io_name, path);
    }
    // For HDFS, also reject pointing the same instance at two different
    // namenodes -- the cached FileIO is bound to the first authority and
    // would silently route the second request to the wrong cluster.
    if (!cached_authority_.empty() && !authority.empty() &&
        cached_authority_ != authority) {
      return InvalidArgument(
          "HadoopTables: cannot mix HDFS authorities within one instance "
          "('{}' was already used; '{}' was requested for path '{}').",
          cached_authority_, authority, path);
    }
    return file_io_;
  }

  // First-time resolve. If this is an HDFS path, inject fs.defaultFS from
  // the authority so the underlying HadoopFileSystem connects to the
  // namenode in the URI rather than the JVM core-site default. Mirrors the
  // same behaviour HadoopCatalog::Make(name, config) implements.
  auto configs = config_.configs();  // copy: don't mutate the user's config
  if (!authority.empty()) {
    auto fs_default =
        configs.find(std::string(HadoopCatalogProperties::kFsDefaultFS.key()));
    if (fs_default == configs.end() || fs_default->second.empty()) {
      configs[std::string(HadoopCatalogProperties::kFsDefaultFS.key())] =
          std::format("hdfs://{}", authority);
    }
  }
  ICEBERG_ASSIGN_OR_RAISE(auto io, FileIORegistry::Load(io_name, configs));
  file_io_ = std::shared_ptr<FileIO>(std::move(io));
  cached_scheme_ = std::move(io_name);
  cached_authority_ = std::string(authority);
  return file_io_;
}

Result<std::shared_ptr<Table>> HadoopTables::Load(const std::string& path) {
  ICEBERG_ASSIGN_OR_RAISE(auto io, ResolveFileIO(path));

  // Java's HadoopTables.load returns kNoSuchTable when the path is not a
  // valid Hadoop table directory; cpp follows the same contract so callers
  // can distinguish "table does not exist" from arbitrary FileIO failures.
  ICEBERG_ASSIGN_OR_RAISE(auto is_table, hadoop::IsHadoopTableDir(*io, path));
  if (!is_table) {
    return NoSuchTable("HadoopTables::Load: '{}' is not a Hadoop table.", path);
  }

  HadoopTableOperations ops(io, path);
  ICEBERG_ASSIGN_OR_RAISE(auto metadata, ops.Refresh());

  // HadoopTables does not own a Catalog, so we return a StaticTable (which
  // does not require a Catalog backreference). Mutability still goes through
  // the HadoopTableOperations primitives directly; callers wanting full
  // UpdateTable semantics should round-trip through HadoopCatalog.
  ICEBERG_ASSIGN_OR_RAISE(auto static_table,
                          StaticTable::Make(PathToIdentifier(path), std::move(metadata),
                                            ops.current_metadata_location(), io));
  return std::shared_ptr<Table>(static_table);
}

Result<bool> HadoopTables::Exists(const std::string& path) {
  ICEBERG_ASSIGN_OR_RAISE(auto io, ResolveFileIO(path));
  return hadoop::IsHadoopTableDir(*io, path);
}

Result<std::shared_ptr<Table>> HadoopTables::Create(
    const std::shared_ptr<Schema>& schema, const std::shared_ptr<PartitionSpec>& spec,
    const std::shared_ptr<SortOrder>& order, const std::string& path,
    const std::unordered_map<std::string, std::string>& properties) {
  if (schema == nullptr || spec == nullptr || order == nullptr) {
    return InvalidArgument(
        "HadoopTables::Create requires non-null schema, spec, and order.");
  }
  if (path.empty()) {
    return InvalidArgument("HadoopTables::Create requires a non-empty path.");
  }
  ICEBERG_ASSIGN_OR_RAISE(auto io, ResolveFileIO(path));

  ICEBERG_ASSIGN_OR_RAISE(auto already_table, hadoop::IsHadoopTableDir(*io, path));
  if (already_table) {
    return AlreadyExists("Table already exists at '{}'.", path);
  }

  ICEBERG_ASSIGN_OR_RAISE(auto metadata,
                          TableMetadata::Make(*schema, *spec, *order, path, properties));

  // Honor write.metadata.compression-codec if set in the table properties.
  ICEBERG_ASSIGN_OR_RAISE(auto codec, ResolveCommitCodec(*metadata));
  ICEBERG_ASSIGN_OR_RAISE(auto bytes, EncodeMetadataWithCodec(*metadata, codec));

  std::string metadata_path;
  ICEBERG_RETURN_UNEXPECTED(
      PublishInitialMetadata(*io, path, codec, bytes, metadata_path));

  std::shared_ptr<TableMetadata> metadata_ptr = std::move(metadata);
  ICEBERG_ASSIGN_OR_RAISE(
      auto static_table,
      StaticTable::Make(PathToIdentifier(path), metadata_ptr, metadata_path, io));
  return std::shared_ptr<Table>(static_table);
}

Status HadoopTables::DropTable(const std::string& path, bool /*purge*/) {
  ICEBERG_ASSIGN_OR_RAISE(auto io, ResolveFileIO(path));
  ICEBERG_ASSIGN_OR_RAISE(auto is_table, hadoop::IsHadoopTableDir(*io, path));
  if (!is_table) {
    return NoSuchTable("Table '{}' does not exist.", path);
  }
  // Same rationale as HadoopCatalog::DropTable -- recursive delete handles
  // the default LocationProvider; tables with custom data paths need
  // expire_snapshots first.
  return io->DeleteDir(path, /*recursive=*/true);
}

Result<std::shared_ptr<Table>> HadoopTables::RegisterTable(
    const std::string& path, const std::string& metadata_file_location) {
  if (metadata_file_location.empty()) {
    return InvalidArgument(
        "HadoopTables::RegisterTable requires a non-empty metadata_file_location.");
  }
  ICEBERG_ASSIGN_OR_RAISE(auto io, ResolveFileIO(path));

  ICEBERG_ASSIGN_OR_RAISE(auto already_table, hadoop::IsHadoopTableDir(*io, path));
  if (already_table) {
    return AlreadyExists("Table already exists at '{}'.", path);
  }

  const std::string_view file_name = Basename(metadata_file_location);
  MetadataCompressionCodec codec = MetadataCompressionCodec::kNone;
  if (file_name.ends_with(".metadata.json.gz")) {
    codec = MetadataCompressionCodec::kGzip;
  } else if (file_name.ends_with(".metadata.json.zstd")) {
    codec = MetadataCompressionCodec::kZstd;
  }

  // Read raw bytes once; parse + copy from the same buffer.
  ICEBERG_ASSIGN_OR_RAISE(auto raw, io->ReadFile(metadata_file_location, std::nullopt));
  std::string body = raw;
  if (codec == MetadataCompressionCodec::kGzip) {
    GZipDecompressor decompressor;
    ICEBERG_RETURN_UNEXPECTED(decompressor.Init());
    ICEBERG_ASSIGN_OR_RAISE(body, decompressor.Decompress(raw));
  } else if (codec == MetadataCompressionCodec::kZstd) {
    return NotSupported(
        "HadoopTables::RegisterTable: zstd-compressed metadata is not yet "
        "supported by iceberg-cpp.");
  }
  ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(body));
  ICEBERG_ASSIGN_OR_RAISE(auto metadata, TableMetadataFromJson(json));

  std::string target;
  ICEBERG_RETURN_UNEXPECTED(PublishInitialMetadata(*io, path, codec, raw, target));

  std::shared_ptr<TableMetadata> metadata_ptr = std::move(metadata);
  ICEBERG_ASSIGN_OR_RAISE(auto static_table, StaticTable::Make(PathToIdentifier(path),
                                                               metadata_ptr, target, io));
  return std::shared_ptr<Table>(static_table);
}

}  // namespace iceberg::hadoop
