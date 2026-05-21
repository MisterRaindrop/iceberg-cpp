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
#include "iceberg/file_io.h"
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

  return std::shared_ptr<HadoopCatalog>(
      new HadoopCatalog(std::string(name), std::move(file_io), std::move(config)));
}

HadoopCatalog::HadoopCatalog(std::string name, std::shared_ptr<FileIO> file_io,
                             HadoopCatalogProperties config)
    : name_(std::move(name)), file_io_(std::move(file_io)), config_(std::move(config)) {}

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
    const Namespace& /*ns*/) const {
  return NotSupported("HadoopCatalog::ListTables: {}", kStubMessage);
}

Result<std::shared_ptr<Table>> HadoopCatalog::CreateTable(
    const TableIdentifier& /*identifier*/, const std::shared_ptr<Schema>& /*schema*/,
    const std::shared_ptr<PartitionSpec>& /*spec*/,
    const std::shared_ptr<SortOrder>& /*order*/, const std::string& /*location*/,
    const std::unordered_map<std::string, std::string>& /*properties*/) {
  return NotSupported("HadoopCatalog::CreateTable: {}", kStubMessage);
}

Result<std::shared_ptr<Table>> HadoopCatalog::UpdateTable(
    const TableIdentifier& /*identifier*/,
    const std::vector<std::unique_ptr<TableRequirement>>& /*requirements*/,
    const std::vector<std::unique_ptr<TableUpdate>>& /*updates*/) {
  return NotSupported("HadoopCatalog::UpdateTable: {}", kStubMessage);
}

Result<std::shared_ptr<Transaction>> HadoopCatalog::StageCreateTable(
    const TableIdentifier& /*identifier*/, const std::shared_ptr<Schema>& /*schema*/,
    const std::shared_ptr<PartitionSpec>& /*spec*/,
    const std::shared_ptr<SortOrder>& /*order*/, const std::string& /*location*/,
    const std::unordered_map<std::string, std::string>& /*properties*/) {
  return NotSupported("HadoopCatalog::StageCreateTable: {}", kStubMessage);
}

Result<bool> HadoopCatalog::TableExists(const TableIdentifier& /*identifier*/) const {
  return NotSupported("HadoopCatalog::TableExists: {}", kStubMessage);
}

Status HadoopCatalog::DropTable(const TableIdentifier& /*identifier*/, bool /*purge*/) {
  return NotSupported("HadoopCatalog::DropTable: {}", kStubMessage);
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
    const TableIdentifier& /*identifier*/) {
  return NotSupported("HadoopCatalog::LoadTable: {}", kStubMessage);
}

Result<std::shared_ptr<Table>> HadoopCatalog::RegisterTable(
    const TableIdentifier& /*identifier*/,
    const std::string& /*metadata_file_location*/) {
  return NotSupported("HadoopCatalog::RegisterTable: {}", kStubMessage);
}

}  // namespace iceberg::hadoop
