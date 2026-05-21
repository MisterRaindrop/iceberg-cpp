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

#include <string>
#include <utility>

#include "iceberg/catalog/hadoop/hadoop_file_layout.h"
#include "iceberg/catalog/hadoop/hadoop_table_operations.h"
#include "iceberg/file_io.h"
#include "iceberg/file_io_registry.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/sort_order.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/macros.h"

namespace iceberg::hadoop {

namespace {

// Convert an absolute table path into a synthetic TableIdentifier so we can
// reuse Table::Make (which keys tables by identifier). The leaf of the path
// becomes the table name; the empty namespace signals "no catalog".
TableIdentifier PathToIdentifier(std::string_view path) {
  std::string_view name = path;
  auto slash = name.find_last_of('/');
  if (slash != std::string_view::npos) {
    name.remove_prefix(slash + 1);
  }
  return TableIdentifier{.ns = Namespace{}, .name = std::string(name)};
}

}  // namespace

HadoopTables::HadoopTables(std::shared_ptr<FileIO> file_io,
                           HadoopCatalogProperties config)
    : file_io_(std::move(file_io)), config_(std::move(config)) {}

HadoopTables::HadoopTables() = default;

HadoopTables::~HadoopTables() = default;

Result<std::shared_ptr<FileIO>> HadoopTables::ResolveFileIO(std::string_view path) {
  if (file_io_ != nullptr) {
    return file_io_;
  }
  std::string io_name(FileIORegistry::kArrowLocalFileIO);
  if (path.starts_with("hdfs://")) {
    io_name = std::string(FileIORegistry::kArrowHdfsFileIO);
  } else if (path.starts_with("s3://") || path.starts_with("s3a://") ||
             path.starts_with("s3n://")) {
    io_name = std::string(FileIORegistry::kArrowS3FileIO);
  }
  ICEBERG_ASSIGN_OR_RAISE(auto io, FileIORegistry::Load(io_name, config_.configs()));
  file_io_ = std::shared_ptr<FileIO>(std::move(io));
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

  ICEBERG_RETURN_UNEXPECTED(io->CreateDir(hadoop::MetadataDir(path)));
  const std::string metadata_path =
      hadoop::MetadataFilePath(path, /*version=*/1, MetadataCompressionCodec::kNone);
  ICEBERG_RETURN_UNEXPECTED(TableMetadataUtil::Write(*io, metadata_path, *metadata));
  ICEBERG_RETURN_UNEXPECTED(
      io->WriteFile(hadoop::VersionHintPath(path), std::string("1\n")));

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

  ICEBERG_ASSIGN_OR_RAISE(auto metadata,
                          TableMetadataUtil::Read(*io, metadata_file_location));

  std::string_view source = metadata_file_location;
  auto slash = source.find_last_of('/');
  std::string_view file_name =
      slash == std::string_view::npos ? source : source.substr(slash + 1);
  MetadataCompressionCodec codec = MetadataCompressionCodec::kNone;
  if (file_name.ends_with(".metadata.json.gz")) {
    codec = MetadataCompressionCodec::kGzip;
  } else if (file_name.ends_with(".metadata.json.zstd")) {
    codec = MetadataCompressionCodec::kZstd;
  }

  ICEBERG_RETURN_UNEXPECTED(io->CreateDir(hadoop::MetadataDir(path)));
  const std::string target = hadoop::MetadataFilePath(path, /*version=*/1, codec);
  ICEBERG_ASSIGN_OR_RAISE(auto raw, io->ReadFile(metadata_file_location, std::nullopt));
  ICEBERG_RETURN_UNEXPECTED(io->WriteFile(target, raw));
  ICEBERG_RETURN_UNEXPECTED(
      io->WriteFile(hadoop::VersionHintPath(path), std::string("1\n")));

  std::shared_ptr<TableMetadata> metadata_ptr = std::move(metadata);
  ICEBERG_ASSIGN_OR_RAISE(auto static_table, StaticTable::Make(PathToIdentifier(path),
                                                               metadata_ptr, target, io));
  return std::shared_ptr<Table>(static_table);
}

}  // namespace iceberg::hadoop
