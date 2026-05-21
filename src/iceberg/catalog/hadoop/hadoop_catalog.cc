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

#include "iceberg/catalog/hadoop/hadoop_catalog.h"

#include <iostream>
#include <string_view>
#include <utility>

#include "iceberg/catalog/hadoop/hadoop_file_layout.h"
#include "iceberg/catalog/hadoop/hadoop_table_operations.h"
#include "iceberg/file_io.h"
#include "iceberg/file_io_registry.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_requirements.h"
#include "iceberg/table_update.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"

namespace iceberg::hadoop {

namespace {

constexpr std::string_view kStubMessage =
    "HadoopCatalog method is a stub; implemented in subsequent commits";

// S3 / object stores do not provide atomic rename semantics, which the
// HadoopCatalog commit protocol depends on for CAS via `version-hint.text`.
// Emit a runtime warning to make the foot-gun visible without blocking the
// user; this matches the spirit of the Java HadoopCatalog docs.
void MaybeWarnS3Warehouse(std::string_view warehouse) {
  if (warehouse.starts_with("s3://") || warehouse.starts_with("s3a://") ||
      warehouse.starts_with("s3n://")) {
    std::cerr << "[iceberg::hadoop] WARNING: HadoopCatalog on object storage ('"
              << warehouse
              << "') has non-atomic rename; concurrent commits may corrupt "
                 "version-hint.text. Prefer Glue/REST/Hive for S3 workloads."
              << '\n';
  }
}

}  // namespace

HadoopCatalog::~HadoopCatalog() = default;

Result<std::shared_ptr<HadoopCatalog>> HadoopCatalog::Make(
    std::string_view name, std::shared_ptr<FileIO> file_io,
    HadoopCatalogProperties config) {
  if (file_io == nullptr) {
    return InvalidArgument("HadoopCatalog::Make requires a non-null FileIO.");
  }
  ICEBERG_ASSIGN_OR_RAISE(auto warehouse, config.Warehouse());
  MaybeWarnS3Warehouse(warehouse);
  ICEBERG_ASSIGN_OR_RAISE(auto lock_manager,
                          hadoop::MakeLockManagerWithIO(config, file_io));

  return std::shared_ptr<HadoopCatalog>(
      new HadoopCatalog(std::string(name), std::move(file_io), std::move(config),
                        std::shared_ptr<LockManager>(std::move(lock_manager))));
}

Result<std::shared_ptr<HadoopCatalog>> HadoopCatalog::Make(
    std::string_view name, HadoopCatalogProperties config) {
  ICEBERG_ASSIGN_OR_RAISE(auto warehouse, config.Warehouse());

  // Select the FileIO factory keyed by warehouse scheme. The
  // FileIORegistry has the right entries pre-registered (arrow-fs-local,
  // arrow-fs-s3, arrow-fs-hdfs); we only need to pick a name.
  std::string io_name(FileIORegistry::kArrowLocalFileIO);
  if (warehouse.starts_with("hdfs://")) {
    io_name = std::string(FileIORegistry::kArrowHdfsFileIO);
  } else if (warehouse.starts_with("s3://") || warehouse.starts_with("s3a://") ||
             warehouse.starts_with("s3n://")) {
    io_name = std::string(FileIORegistry::kArrowS3FileIO);
  }

  // Allow an explicit override.
  const auto override_impl = config.Get(HadoopCatalogProperties::kIOImpl);
  if (!override_impl.empty()) {
    io_name = override_impl;
  }

  ICEBERG_ASSIGN_OR_RAISE(auto file_io, FileIORegistry::Load(io_name, config.configs()));
  return Make(name, std::shared_ptr<FileIO>(std::move(file_io)), std::move(config));
}

HadoopCatalog::HadoopCatalog(std::string name, std::shared_ptr<FileIO> file_io,
                             HadoopCatalogProperties config,
                             std::shared_ptr<LockManager> lock_manager)
    : name_(std::move(name)),
      file_io_(std::move(file_io)),
      config_(std::move(config)),
      lock_manager_(std::move(lock_manager)) {}

std::string_view HadoopCatalog::name() const { return name_; }

Status HadoopCatalog::CreateNamespace(
    const Namespace& ns, const std::unordered_map<std::string, std::string>& properties) {
  // Java HadoopCatalog rejects any non-empty metadata payload. We mirror that
  // contract here so cross-language users do not silently lose data. The
  // optional namespace.properties persistence is a follow-up (Phase 5).
  if (!properties.empty()) {
    return NotSupported(
        "HadoopCatalog::CreateNamespace does not support namespace properties; "
        "Java HadoopCatalog rejects this configuration.");
  }
  if (ns.levels.empty()) {
    return InvalidArgument(
        "HadoopCatalog::CreateNamespace requires a non-empty namespace.");
  }
  ICEBERG_ASSIGN_OR_RAISE(auto warehouse, config_.Warehouse());
  ICEBERG_ASSIGN_OR_RAISE(auto ns_dir, hadoop::NamespaceDir(warehouse, ns));

  ICEBERG_ASSIGN_OR_RAISE(auto already, file_io_->Exists(ns_dir));
  if (already) {
    return AlreadyExists("Namespace '{}' already exists at {}.", ns.ToString(), ns_dir);
  }
  ICEBERG_RETURN_UNEXPECTED(file_io_->CreateDir(ns_dir));
  return {};
}

Result<std::vector<Namespace>> HadoopCatalog::ListNamespaces(const Namespace& ns) const {
  ICEBERG_ASSIGN_OR_RAISE(auto warehouse, config_.Warehouse());
  ICEBERG_ASSIGN_OR_RAISE(auto parent_dir, hadoop::NamespaceDir(warehouse, ns));

  // Listing the warehouse root when ns is empty is allowed; otherwise the
  // parent must exist.
  ICEBERG_ASSIGN_OR_RAISE(auto parent_exists, file_io_->Exists(parent_dir));
  if (!parent_exists) {
    if (ns.levels.empty()) {
      return std::vector<Namespace>{};
    }
    return NoSuchNamespace("Namespace '{}' does not exist.", ns.ToString());
  }
  ICEBERG_ASSIGN_OR_RAISE(auto is_dir, file_io_->IsDirectory(parent_dir));
  if (!is_dir) {
    return NoSuchNamespace("Namespace '{}' is not a directory.", ns.ToString());
  }

  ICEBERG_ASSIGN_OR_RAISE(auto entries, file_io_->ListDir(parent_dir));
  std::vector<Namespace> children;
  for (const auto& entry : entries) {
    if (!entry.is_directory) {
      continue;
    }
    std::string_view path = entry.location;
    auto slash = path.find_last_of('/');
    std::string_view leaf =
        slash == std::string_view::npos ? path : path.substr(slash + 1);
    if (leaf.empty()) {
      continue;
    }
    // Skip directories that look like tables.
    ICEBERG_ASSIGN_OR_RAISE(auto is_table, hadoop::IsHadoopTableDir(*file_io_, path));
    if (is_table) {
      continue;
    }
    Namespace child;
    child.levels = ns.levels;
    child.levels.emplace_back(leaf);
    children.push_back(std::move(child));
  }
  return children;
}

Result<std::unordered_map<std::string, std::string>>
HadoopCatalog::GetNamespaceProperties(const Namespace& ns) const {
  ICEBERG_ASSIGN_OR_RAISE(auto warehouse, config_.Warehouse());
  ICEBERG_ASSIGN_OR_RAISE(auto ns_dir, hadoop::NamespaceDir(warehouse, ns));
  ICEBERG_ASSIGN_OR_RAISE(auto exists, file_io_->Exists(ns_dir));
  if (!exists) {
    return NoSuchNamespace("Namespace '{}' does not exist.", ns.ToString());
  }
  // Java HadoopCatalog returns a single-entry map keyed by "location". We
  // mirror that behaviour exactly.
  return std::unordered_map<std::string, std::string>{{"location", ns_dir}};
}

Status HadoopCatalog::DropNamespace(const Namespace& ns) {
  if (ns.levels.empty()) {
    return InvalidArgument(
        "HadoopCatalog::DropNamespace requires a non-empty namespace.");
  }
  ICEBERG_ASSIGN_OR_RAISE(auto warehouse, config_.Warehouse());
  ICEBERG_ASSIGN_OR_RAISE(auto ns_dir, hadoop::NamespaceDir(warehouse, ns));
  ICEBERG_ASSIGN_OR_RAISE(auto exists, file_io_->Exists(ns_dir));
  if (!exists) {
    return NoSuchNamespace("Namespace '{}' does not exist.", ns.ToString());
  }
  ICEBERG_ASSIGN_OR_RAISE(auto entries, file_io_->ListDir(ns_dir));
  if (!entries.empty()) {
    return NamespaceNotEmpty("Namespace '{}' is not empty.", ns.ToString());
  }
  ICEBERG_RETURN_UNEXPECTED(file_io_->DeleteDir(ns_dir, /*recursive=*/false));
  return {};
}

Result<bool> HadoopCatalog::NamespaceExists(const Namespace& ns) const {
  ICEBERG_ASSIGN_OR_RAISE(auto warehouse, config_.Warehouse());
  ICEBERG_ASSIGN_OR_RAISE(auto ns_dir, hadoop::NamespaceDir(warehouse, ns));
  ICEBERG_ASSIGN_OR_RAISE(auto exists, file_io_->Exists(ns_dir));
  if (!exists) {
    return false;
  }
  ICEBERG_ASSIGN_OR_RAISE(auto is_dir, file_io_->IsDirectory(ns_dir));
  if (!is_dir) {
    return false;
  }
  // A namespace directory that holds a metadata/ folder with versioned
  // metadata is actually a table; report it as not a namespace.
  ICEBERG_ASSIGN_OR_RAISE(auto is_table, hadoop::IsHadoopTableDir(*file_io_, ns_dir));
  return !is_table;
}

Status HadoopCatalog::UpdateNamespaceProperties(
    const Namespace& /*ns*/,
    const std::unordered_map<std::string, std::string>& /*updates*/,
    const std::unordered_set<std::string>& /*removals*/) {
  // Java HadoopCatalog throws UnsupportedOperationException for both
  // setProperties and removeProperties. We expose the same contract.
  return NotSupported(
      "HadoopCatalog::UpdateNamespaceProperties is not supported; "
      "Java HadoopCatalog rejects namespace property mutation.");
}

Result<std::vector<TableIdentifier>> HadoopCatalog::ListTables(
    const Namespace& ns) const {
  ICEBERG_ASSIGN_OR_RAISE(auto warehouse, config_.Warehouse());
  ICEBERG_ASSIGN_OR_RAISE(auto ns_dir, hadoop::NamespaceDir(warehouse, ns));
  ICEBERG_ASSIGN_OR_RAISE(auto ns_exists, file_io_->Exists(ns_dir));
  if (!ns_exists) {
    if (ns.levels.empty()) {
      return std::vector<TableIdentifier>{};
    }
    return NoSuchNamespace("Namespace '{}' does not exist.", ns.ToString());
  }

  ICEBERG_ASSIGN_OR_RAISE(auto entries, file_io_->ListDir(ns_dir));
  std::vector<TableIdentifier> tables;
  for (const auto& entry : entries) {
    if (!entry.is_directory) {
      continue;
    }
    ICEBERG_ASSIGN_OR_RAISE(auto is_table,
                            hadoop::IsHadoopTableDir(*file_io_, entry.location));
    if (!is_table) {
      continue;
    }
    std::string_view path = entry.location;
    auto slash = path.find_last_of('/');
    std::string_view name =
        slash == std::string_view::npos ? path : path.substr(slash + 1);
    tables.push_back(TableIdentifier{.ns = ns, .name = std::string(name)});
  }
  return tables;
}

Result<std::shared_ptr<Table>> HadoopCatalog::CreateTable(
    const TableIdentifier& identifier, const std::shared_ptr<Schema>& schema,
    const std::shared_ptr<PartitionSpec>& spec, const std::shared_ptr<SortOrder>& order,
    const std::string& location,
    const std::unordered_map<std::string, std::string>& properties) {
  ICEBERG_RETURN_UNEXPECTED(hadoop::ValidateTableIdentifier(identifier));
  if (schema == nullptr || spec == nullptr || order == nullptr) {
    return InvalidArgument(
        "HadoopCatalog::CreateTable requires non-null schema, spec, and order.");
  }
  ICEBERG_ASSIGN_OR_RAISE(auto warehouse, config_.Warehouse());
  ICEBERG_ASSIGN_OR_RAISE(auto table_dir, hadoop::TableDir(warehouse, identifier));

  // Java's HadoopCatalog rejects creation when the table directory already
  // looks like an Iceberg table. We deliberately do not refuse plain
  // pre-existing directories so users can drop the metadata layout into a
  // pre-created folder; an existing table is the foot-gun we guard against.
  ICEBERG_ASSIGN_OR_RAISE(auto already_table,
                          hadoop::IsHadoopTableDir(*file_io_, table_dir));
  if (already_table) {
    return AlreadyExists("Table '{}' already exists at {}.", identifier.ToString(),
                         table_dir);
  }

  const std::string base_location = location.empty() ? table_dir : location;

  ICEBERG_ASSIGN_OR_RAISE(auto metadata, TableMetadata::Make(*schema, *spec, *order,
                                                             base_location, properties));

  // Ensure the metadata directory exists before writing the version-1 file.
  const std::string metadata_dir = hadoop::MetadataDir(table_dir);
  ICEBERG_RETURN_UNEXPECTED(file_io_->CreateDir(metadata_dir));

  const std::string metadata_path =
      hadoop::MetadataFilePath(table_dir, /*version=*/1, MetadataCompressionCodec::kNone);

  auto write_result = TableMetadataUtil::Write(*file_io_, metadata_path, *metadata);
  if (!write_result.has_value()) {
    // Best-effort cleanup of the freshly written file so a retry can succeed.
    std::ignore = file_io_->DeleteFile(metadata_path);
    return std::unexpected<Error>(write_result.error());
  }

  // Write version-hint.text so subsequent Refresh calls find the new file
  // even on filesystems whose listdir order is non-deterministic.
  auto hint_result =
      file_io_->WriteFile(hadoop::VersionHintPath(table_dir), std::string("1\n"));
  if (!hint_result.has_value()) {
    std::ignore = file_io_->DeleteFile(metadata_path);
    return std::unexpected<Error>(hint_result.error());
  }

  std::shared_ptr<TableMetadata> metadata_ptr = std::move(metadata);
  return Table::Make(identifier, metadata_ptr, metadata_path, file_io_,
                     shared_from_this());
}

Result<std::shared_ptr<Table>> HadoopCatalog::UpdateTable(
    const TableIdentifier& identifier,
    const std::vector<std::unique_ptr<TableRequirement>>& requirements,
    const std::vector<std::unique_ptr<TableUpdate>>& updates) {
  ICEBERG_RETURN_UNEXPECTED(hadoop::ValidateTableIdentifier(identifier));
  ICEBERG_ASSIGN_OR_RAISE(auto warehouse, config_.Warehouse());
  ICEBERG_ASSIGN_OR_RAISE(auto table_dir, hadoop::TableDir(warehouse, identifier));

  // Build the per-call owner id so concurrent calls from the same catalog
  // can be told apart in the lock manager logs.
  std::string owner_id = name_ + ":" + identifier.ToString();
  HadoopTableOperations ops(file_io_, table_dir, lock_manager_, std::move(owner_id));

  ICEBERG_ASSIGN_OR_RAISE(auto is_create, TableRequirements::IsCreate(requirements));
  std::shared_ptr<TableMetadata> base;
  std::unique_ptr<TableMetadataBuilder> builder;
  if (is_create) {
    // Reject if a table already exists at this location.
    ICEBERG_ASSIGN_OR_RAISE(auto already_table,
                            hadoop::IsHadoopTableDir(*file_io_, table_dir));
    if (already_table) {
      return AlreadyExists("Table '{}' already exists at {}.", identifier.ToString(),
                           table_dir);
    }
    int8_t format_version = TableMetadata::kDefaultTableFormatVersion;
    for (const auto& update : updates) {
      if (update->kind() == TableUpdate::Kind::kUpgradeFormatVersion) {
        format_version =
            internal::checked_cast<const table::UpgradeFormatVersion&>(*update)
                .format_version();
      }
    }
    builder = TableMetadataBuilder::BuildFromEmpty(format_version);
  } else {
    ICEBERG_ASSIGN_OR_RAISE(base, ops.Refresh());
    builder = TableMetadataBuilder::BuildFrom(base.get());
  }

  for (const auto& requirement : requirements) {
    ICEBERG_RETURN_UNEXPECTED(requirement->Validate(base.get()));
  }
  for (const auto& update : updates) {
    update->ApplyTo(*builder);
  }
  ICEBERG_ASSIGN_OR_RAISE(auto updated, builder->Build());

  if (is_create) {
    // For create transactions, write v1 + version-hint via the same path as
    // CreateTable so the on-disk layout is consistent.
    const std::string metadata_dir = hadoop::MetadataDir(table_dir);
    ICEBERG_RETURN_UNEXPECTED(file_io_->CreateDir(metadata_dir));
    const std::string metadata_path = hadoop::MetadataFilePath(
        table_dir, /*version=*/1, MetadataCompressionCodec::kNone);
    ICEBERG_RETURN_UNEXPECTED(
        TableMetadataUtil::Write(*file_io_, metadata_path, *updated));
    ICEBERG_RETURN_UNEXPECTED(
        file_io_->WriteFile(hadoop::VersionHintPath(table_dir), std::string("1\n")));
    std::shared_ptr<TableMetadata> updated_ptr = std::move(updated);
    return Table::Make(identifier, updated_ptr, metadata_path, file_io_,
                       shared_from_this());
  }

  // Otherwise delegate to HadoopTableOperations::Commit for the CAS path.
  ICEBERG_RETURN_UNEXPECTED(ops.Commit(*base, *updated));
  std::shared_ptr<TableMetadata> updated_ptr = std::move(updated);
  return Table::Make(identifier, updated_ptr, ops.current_metadata_location(), file_io_,
                     shared_from_this());
}

Result<std::shared_ptr<Transaction>> HadoopCatalog::StageCreateTable(
    const TableIdentifier& /*identifier*/, const std::shared_ptr<Schema>& /*schema*/,
    const std::shared_ptr<PartitionSpec>& /*spec*/,
    const std::shared_ptr<SortOrder>& /*order*/, const std::string& /*location*/,
    const std::unordered_map<std::string, std::string>& /*properties*/) {
  return NotSupported("HadoopCatalog::StageCreateTable: {}", kStubMessage);
}

Result<bool> HadoopCatalog::TableExists(const TableIdentifier& identifier) const {
  ICEBERG_RETURN_UNEXPECTED(hadoop::ValidateTableIdentifier(identifier));
  ICEBERG_ASSIGN_OR_RAISE(auto warehouse, config_.Warehouse());
  ICEBERG_ASSIGN_OR_RAISE(auto table_dir, hadoop::TableDir(warehouse, identifier));
  return hadoop::IsHadoopTableDir(*file_io_, table_dir);
}

Status HadoopCatalog::DropTable(const TableIdentifier& identifier, bool purge) {
  ICEBERG_RETURN_UNEXPECTED(hadoop::ValidateTableIdentifier(identifier));
  ICEBERG_ASSIGN_OR_RAISE(auto warehouse, config_.Warehouse());
  ICEBERG_ASSIGN_OR_RAISE(auto table_dir, hadoop::TableDir(warehouse, identifier));

  ICEBERG_ASSIGN_OR_RAISE(auto is_table, hadoop::IsHadoopTableDir(*file_io_, table_dir));
  if (!is_table) {
    return NoSuchTable("Table '{}' does not exist at {}.", identifier.ToString(),
                       table_dir);
  }
  if (purge) {
    // Java's DropTable(purge=true) goes through CatalogUtil.dropTableData first
    // to remove snapshot data files. The cpp helper for that is introduced in
    // H17 alongside the suppress-permission-error path; for now we recursively
    // delete the entire table directory which matches the purge=true outcome
    // for tables whose data lives under <table>/data/ (the default LocationProvider).
    return file_io_->DeleteDir(table_dir, /*recursive=*/true);
  }
  // Java treats both purge values as a recursive delete; cpp follows suit.
  return file_io_->DeleteDir(table_dir, /*recursive=*/true);
}

Status HadoopCatalog::RenameTable(const TableIdentifier& /*from*/,
                                  const TableIdentifier& /*to*/) {
  // Java HadoopCatalog explicitly throws UnsupportedOperationException for
  // RenameTable; the cpp implementation follows the same contract. H17 will
  // re-state this rule alongside the relevant docs once the surrounding APIs
  // exist.
  return NotSupported(
      "HadoopCatalog::RenameTable is not supported because filesystem rename is "
      "non-atomic across the catalog tree.");
}

Result<std::shared_ptr<Table>> HadoopCatalog::LoadTable(
    const TableIdentifier& identifier) {
  ICEBERG_RETURN_UNEXPECTED(hadoop::ValidateTableIdentifier(identifier));
  ICEBERG_ASSIGN_OR_RAISE(auto warehouse, config_.Warehouse());
  ICEBERG_ASSIGN_OR_RAISE(auto table_dir, hadoop::TableDir(warehouse, identifier));

  HadoopTableOperations ops(file_io_, table_dir);
  ICEBERG_ASSIGN_OR_RAISE(auto metadata, ops.Refresh());
  return Table::Make(identifier, std::move(metadata), ops.current_metadata_location(),
                     file_io_, shared_from_this());
}

Result<std::shared_ptr<Table>> HadoopCatalog::RegisterTable(
    const TableIdentifier& identifier, const std::string& metadata_file_location) {
  ICEBERG_RETURN_UNEXPECTED(hadoop::ValidateTableIdentifier(identifier));
  if (metadata_file_location.empty()) {
    return InvalidArgument(
        "HadoopCatalog::RegisterTable requires a non-empty metadata_file_location.");
  }
  ICEBERG_ASSIGN_OR_RAISE(auto warehouse, config_.Warehouse());
  ICEBERG_ASSIGN_OR_RAISE(auto table_dir, hadoop::TableDir(warehouse, identifier));

  ICEBERG_ASSIGN_OR_RAISE(auto already_table,
                          hadoop::IsHadoopTableDir(*file_io_, table_dir));
  if (already_table) {
    return AlreadyExists("Table '{}' already exists at {}.", identifier.ToString(),
                         table_dir);
  }

  // Read the external metadata and copy it to v1.metadata.json (with codec
  // detected from the source filename so registering a gzipped metadata file
  // produces a gzipped local copy too).
  ICEBERG_ASSIGN_OR_RAISE(auto metadata,
                          TableMetadataUtil::Read(*file_io_, metadata_file_location));

  std::string_view source = metadata_file_location;
  auto slash = source.find_last_of('/');
  std::string_view file_name =
      slash == std::string_view::npos ? source : source.substr(slash + 1);
  hadoop::MetadataCompressionCodec codec = hadoop::MetadataCompressionCodec::kNone;
  if (file_name.ends_with(".metadata.json.gz")) {
    codec = hadoop::MetadataCompressionCodec::kGzip;
  } else if (file_name.ends_with(".metadata.json.zstd")) {
    codec = hadoop::MetadataCompressionCodec::kZstd;
  }

  const std::string metadata_dir = hadoop::MetadataDir(table_dir);
  ICEBERG_RETURN_UNEXPECTED(file_io_->CreateDir(metadata_dir));

  const std::string target = hadoop::MetadataFilePath(table_dir, /*version=*/1, codec);
  // Read the raw bytes again (preserving codec encoding) and write them to
  // the target. TableMetadataUtil::Read would decode for us; using the raw
  // file contents keeps the file bytes byte-for-byte identical so external
  // readers see the same content they wrote.
  ICEBERG_ASSIGN_OR_RAISE(auto raw,
                          file_io_->ReadFile(metadata_file_location, std::nullopt));
  ICEBERG_RETURN_UNEXPECTED(file_io_->WriteFile(target, raw));
  ICEBERG_RETURN_UNEXPECTED(
      file_io_->WriteFile(hadoop::VersionHintPath(table_dir), std::string("1\n")));

  std::shared_ptr<TableMetadata> metadata_ptr = std::move(metadata);
  return Table::Make(identifier, metadata_ptr, target, file_io_, shared_from_this());
}

}  // namespace iceberg::hadoop
