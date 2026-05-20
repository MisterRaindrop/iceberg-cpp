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

#include "iceberg/catalog/hive/hive_catalog.h"

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include "iceberg/catalog/hive/hive_table_operations.h"
#include "iceberg/catalog/hive/hive_utils.h"
#include "iceberg/catalog/hive/hms_client.h"
#include "iceberg/file_io_registry.h"
#include "iceberg/json_serde_internal.h"
#include "iceberg/partition_spec.h"
#include "iceberg/sort_order.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_requirement.h"
#include "iceberg/table_update.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/uuid.h"

namespace iceberg::hive {

namespace {

constexpr std::string_view kNotImplementedMessage =
    "HiveCatalog method is not yet implemented; "
    "see the iceberg-cpp HiveCatalog roadmap (Phase 1 / Phase 2 commits).";

// Pick the FileIO implementation name for a given `config`. If the user
// supplied `io-impl` explicitly, honour it; otherwise infer from the
// warehouse URI's scheme (file:// or no scheme -> arrow-fs-local,
// s3:// -> arrow-fs-s3). HDFS / GCS / Azure schemes are not auto-
// detectable today.
Result<std::string> ResolveIoImpl(const HiveCatalogProperties& config) {
  std::string io_impl = config.Get(HiveCatalogProperties::kIOImpl);
  if (!io_impl.empty()) {
    return io_impl;
  }
  const std::string warehouse = config.Get(HiveCatalogProperties::kWarehouse);
  if (warehouse.empty()) {
    return std::string(FileIORegistry::kArrowLocalFileIO);
  }
  const auto pos = warehouse.find("://");
  if (pos == std::string::npos) {
    return std::string(FileIORegistry::kArrowLocalFileIO);
  }
  const auto scheme = std::string_view(warehouse).substr(0, pos);
  if (scheme == "file") {
    return std::string(FileIORegistry::kArrowLocalFileIO);
  }
  if (scheme == "s3") {
    return std::string(FileIORegistry::kArrowS3FileIO);
  }
  return NotSupported(
      "Cannot auto-detect FileIO for warehouse '{}'; set the '{}' property "
      "explicitly.",
      warehouse, HiveCatalogProperties::kIOImpl.key());
}

Result<std::unique_ptr<FileIO>> MakeHiveFileIO(const HiveCatalogProperties& config) {
  ICEBERG_ASSIGN_OR_RAISE(auto io_impl, ResolveIoImpl(config));
  return FileIORegistry::Load(io_impl, config.configs());
}

}  // namespace

HiveCatalog::HiveCatalog(HiveCatalogProperties config, std::unique_ptr<HmsClient> client,
                         std::shared_ptr<FileIO> file_io)
    : config_(std::move(config)),
      name_(config_.Get(HiveCatalogProperties::kName)),
      client_(std::move(client)),
      file_io_(std::move(file_io)) {}

HiveCatalog::~HiveCatalog() = default;

Result<std::shared_ptr<HiveCatalog>> HiveCatalog::Make(
    const HiveCatalogProperties& config) {
  ICEBERG_ASSIGN_OR_RAISE(auto client, HmsClient::Connect(config));
  ICEBERG_ASSIGN_OR_RAISE(auto file_io, MakeHiveFileIO(config));
  return std::shared_ptr<HiveCatalog>(
      new HiveCatalog(config, std::move(client), std::move(file_io)));
}

std::string_view HiveCatalog::name() const { return name_; }

Status HiveCatalog::CreateNamespace(
    const Namespace& ns, const std::unordered_map<std::string, std::string>& properties) {
  ICEBERG_ASSIGN_OR_RAISE(auto database, ConvertToHiveDatabase(ns, properties));
  return client_->CreateDatabase(database);
}

Result<std::vector<Namespace>> HiveCatalog::ListNamespaces(const Namespace& ns) const {
  if (!ns.levels.empty()) {
    return std::vector<Namespace>{};
  }
  ICEBERG_ASSIGN_OR_RAISE(auto names, client_->GetAllDatabases());
  std::vector<Namespace> namespaces;
  namespaces.reserve(names.size());
  for (auto& db_name : names) {
    namespaces.push_back(Namespace{.levels = {std::move(db_name)}});
  }
  return namespaces;
}

Result<std::unordered_map<std::string, std::string>> HiveCatalog::GetNamespaceProperties(
    const Namespace& ns) const {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(ns));
  ICEBERG_ASSIGN_OR_RAISE(auto database, client_->GetDatabase(ns.levels[0]));
  return ConvertFromHiveDatabase(database).properties;
}

Status HiveCatalog::DropNamespace(const Namespace& ns) {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(ns));
  return client_->DropDatabase(ns.levels[0], /*cascade=*/false);
}

Result<bool> HiveCatalog::NamespaceExists(const Namespace& ns) const {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(ns));
  auto database = client_->GetDatabase(ns.levels[0]);
  if (database.has_value()) {
    return true;
  }
  if (database.error().kind == ErrorKind::kNoSuchNamespace) {
    return false;
  }
  return std::unexpected(database.error());
}

Status HiveCatalog::UpdateNamespaceProperties(
    const Namespace& ns, const std::unordered_map<std::string, std::string>& updates,
    const std::unordered_set<std::string>& removals) {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(ns));
  ICEBERG_ASSIGN_OR_RAISE(auto database, client_->GetDatabase(ns.levels[0]));

  auto namespace_view = ConvertFromHiveDatabase(database);
  auto& properties = namespace_view.properties;
  for (const auto& key : removals) {
    properties.erase(key);
  }
  for (const auto& [key, value] : updates) {
    properties[key] = value;
  }
  ICEBERG_ASSIGN_OR_RAISE(auto altered, ConvertToHiveDatabase(ns, properties));
  return client_->AlterDatabase(ns.levels[0], altered);
}

Result<std::vector<TableIdentifier>> HiveCatalog::ListTables(const Namespace& ns) const {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(ns));
  ICEBERG_ASSIGN_OR_RAISE(auto names, client_->GetAllTables(ns.levels[0]));
  std::vector<TableIdentifier> identifiers;
  identifiers.reserve(names.size());
  for (auto& table_name : names) {
    identifiers.push_back(TableIdentifier{.ns = ns, .name = std::move(table_name)});
  }
  return identifiers;
}

Result<std::shared_ptr<Table>> HiveCatalog::CreateTable(
    const TableIdentifier& identifier, const std::shared_ptr<Schema>& schema,
    const std::shared_ptr<PartitionSpec>& spec, const std::shared_ptr<SortOrder>& order,
    const std::string& location_in,
    const std::unordered_map<std::string, std::string>& properties) {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(identifier.ns));
  ICEBERG_RETURN_UNEXPECTED(identifier.Validate());
  if (!schema || !spec || !order) {
    return InvalidArgument(
        "HiveCatalog::CreateTable requires non-null schema, partition spec, "
        "and sort order.");
  }

  std::string location = location_in;
  if (location.empty()) {
    location = GetDefaultTableLocation(config_.Get(HiveCatalogProperties::kWarehouse),
                                       identifier.ns, identifier.name);
  }

  // Build the initial TableMetadata + serialise to JSON. Do every step
  // that can fail without side-effects on the filesystem first (schema
  // conversion, JSON serialisation, HMS table construction) so a bad
  // schema never leaves an orphan metadata.json in the warehouse.
  ICEBERG_ASSIGN_OR_RAISE(
      auto metadata, TableMetadata::Make(*schema, *spec, *order, location, properties));
  const std::string metadata_location = std::format(
      "{}/metadata/00000-{}.metadata.json", location, Uuid::GenerateV4().ToString());
  ICEBERG_ASSIGN_OR_RAISE(const auto json_str, ToJsonString(*metadata));
  ICEBERG_ASSIGN_OR_RAISE(auto columns, SchemaToHiveColumns(*schema));
  ICEBERG_ASSIGN_OR_RAISE(
      auto hive_table,
      ConvertToHiveTable(identifier, columns, metadata_location, location, properties));

  // Only now write the file; if CreateTable in HMS fails we still need
  // to roll back the file we just wrote.
  ICEBERG_RETURN_UNEXPECTED(file_io_->WriteFile(metadata_location, json_str));
  auto create_status = client_->CreateTable(hive_table);
  if (!create_status.has_value()) {
    (void)file_io_->DeleteFile(metadata_location);
    return std::unexpected(create_status.error());
  }

  return Table::Make(identifier, std::shared_ptr<TableMetadata>(metadata.release()),
                     metadata_location, file_io_, shared_from_this());
}

Result<std::shared_ptr<Table>> HiveCatalog::UpdateTable(
    const TableIdentifier& identifier,
    const std::vector<std::unique_ptr<TableRequirement>>& requirements,
    const std::vector<std::unique_ptr<TableUpdate>>& updates) {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(identifier.ns));
  ICEBERG_RETURN_UNEXPECTED(identifier.Validate());

  HiveTableOperations ops(
      client_.get(), file_io_, identifier,
      /*lock_enabled=*/config_.Get(HiveCatalogProperties::kLockEnabled));
  ICEBERG_ASSIGN_OR_RAISE(auto base, ops.Refresh());

  // Validate requirements against the current metadata before mutating.
  for (const auto& requirement : requirements) {
    if (!requirement) continue;
    ICEBERG_RETURN_UNEXPECTED(requirement->Validate(base.metadata.get()));
  }

  // Apply updates via TableMetadataBuilder and build the new metadata.
  auto builder = TableMetadataBuilder::BuildFrom(base.metadata.get());
  for (const auto& update : updates) {
    if (!update) continue;
    update->ApplyTo(*builder);
  }
  ICEBERG_ASSIGN_OR_RAISE(auto new_metadata, builder->Build());

  ICEBERG_RETURN_UNEXPECTED(ops.Commit(base, *new_metadata));
  return LoadTable(identifier);
}

Result<std::shared_ptr<Transaction>> HiveCatalog::StageCreateTable(
    const TableIdentifier& /*identifier*/, const std::shared_ptr<Schema>& /*schema*/,
    const std::shared_ptr<PartitionSpec>& /*spec*/,
    const std::shared_ptr<SortOrder>& /*order*/, const std::string& /*location*/,
    const std::unordered_map<std::string, std::string>& /*properties*/) {
  return NotImplemented("{}", kNotImplementedMessage);
}

Result<bool> HiveCatalog::TableExists(const TableIdentifier& identifier) const {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(identifier.ns));
  ICEBERG_RETURN_UNEXPECTED(identifier.Validate());
  auto table = client_->GetTable(identifier.ns.levels[0], identifier.name);
  if (table.has_value()) {
    return true;
  }
  if (table.error().kind == ErrorKind::kNoSuchTable) {
    return false;
  }
  return std::unexpected(table.error());
}

Status HiveCatalog::DropTable(const TableIdentifier& identifier, bool purge) {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(identifier.ns));
  ICEBERG_RETURN_UNEXPECTED(identifier.Validate());
  // The full data-file purge requires walking the manifest tree; iceberg
  // currently only deletes the HMS row + leaves data lifecycle to the
  // expire-snapshots action (see C17/C19). Surface a clear error rather
  // than silently dropping the table while leaving data behind.
  if (purge) {
    return NotImplemented(
        "HiveCatalog::DropTable(purge=true) is not yet supported; call "
        "DropTable(purge=false) and use ExpireSnapshots to clean data.");
  }
  return client_->DropTable(identifier.ns.levels[0], identifier.name,
                            /*delete_data=*/false);
}

Status HiveCatalog::RenameTable(const TableIdentifier& from, const TableIdentifier& to) {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(from.ns));
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(to.ns));
  ICEBERG_RETURN_UNEXPECTED(from.Validate());
  ICEBERG_RETURN_UNEXPECTED(to.Validate());

  ICEBERG_ASSIGN_OR_RAISE(auto table, client_->GetTable(from.ns.levels[0], from.name));
  table.db_name = to.ns.levels[0];
  table.table_name = to.name;
  return client_->AlterTable(from.ns.levels[0], from.name, table);
}

Result<std::shared_ptr<Table>> HiveCatalog::LoadTable(const TableIdentifier& identifier) {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(identifier.ns));
  ICEBERG_RETURN_UNEXPECTED(identifier.Validate());

  ICEBERG_ASSIGN_OR_RAISE(auto hive_table,
                          client_->GetTable(identifier.ns.levels[0], identifier.name));
  ICEBERG_ASSIGN_OR_RAISE(auto metadata_location,
                          GetMetadataLocation(hive_table.parameters));

  ICEBERG_ASSIGN_OR_RAISE(auto metadata_json,
                          file_io_->ReadFile(metadata_location, /*length=*/std::nullopt));

  nlohmann::json metadata_obj;
  try {
    metadata_obj = nlohmann::json::parse(metadata_json);
  } catch (const nlohmann::json::parse_error& e) {
    return JsonParseError("Failed to parse metadata at '{}': {}", metadata_location,
                          e.what());
  }
  ICEBERG_ASSIGN_OR_RAISE(auto metadata, TableMetadataFromJson(metadata_obj));

  return Table::Make(identifier, std::shared_ptr<TableMetadata>(metadata.release()),
                     std::move(metadata_location), file_io_, shared_from_this());
}

Result<std::shared_ptr<Table>> HiveCatalog::RegisterTable(
    const TableIdentifier& identifier, const std::string& metadata_file_location) {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(identifier.ns));
  ICEBERG_RETURN_UNEXPECTED(identifier.Validate());

  // RegisterTable assumes a pre-existing metadata.json. Read the columns
  // from it so the HMS Table record exposes a faithful column list to
  // Hive/Spark/Trino clients.
  ICEBERG_ASSIGN_OR_RAISE(
      auto metadata_json,
      file_io_->ReadFile(metadata_file_location, /*length=*/std::nullopt));
  nlohmann::json metadata_obj;
  try {
    metadata_obj = nlohmann::json::parse(metadata_json);
  } catch (const nlohmann::json::parse_error& e) {
    return JsonParseError("Failed to parse metadata at '{}': {}", metadata_file_location,
                          e.what());
  }
  ICEBERG_ASSIGN_OR_RAISE(auto metadata, TableMetadataFromJson(metadata_obj));

  std::vector<HiveColumn> columns;
  auto schema = metadata->Schema();
  if (schema.has_value() && *schema != nullptr) {
    ICEBERG_ASSIGN_OR_RAISE(columns, SchemaToHiveColumns(**schema));
  }
  const std::string location =
      metadata->location.empty()
          ? GetDefaultTableLocation(config_.Get(HiveCatalogProperties::kWarehouse),
                                    identifier.ns, identifier.name)
          : metadata->location;
  ICEBERG_ASSIGN_OR_RAISE(
      auto hive_table,
      ConvertToHiveTable(identifier, columns, metadata_file_location, location,
                         /*table_properties=*/{}));
  ICEBERG_RETURN_UNEXPECTED(client_->CreateTable(hive_table));

  return Table::Make(identifier, std::shared_ptr<TableMetadata>(metadata.release()),
                     metadata_file_location, file_io_, shared_from_this());
}

}  // namespace iceberg::hive
