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
#include "iceberg/catalog/hive/hms_client_pool.h"
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
    "HiveCatalog method is not yet implemented; see mkdocs/docs/catalogs/hive.md "
    "for the current status matrix.";

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

HiveCatalog::HiveCatalog(HiveCatalogProperties config,
                         std::unique_ptr<HmsClientPool> client_pool,
                         std::shared_ptr<FileIO> file_io)
    : config_(std::move(config)),
      name_(config_.Get(HiveCatalogProperties::kName)),
      client_pool_(std::move(client_pool)),
      file_io_(std::move(file_io)) {}

HiveCatalog::~HiveCatalog() = default;

Result<std::shared_ptr<HiveCatalog>> HiveCatalog::Make(
    const HiveCatalogProperties& config) {
  ICEBERG_ASSIGN_OR_RAISE(auto client_pool, HmsClientPool::Make(config));
  ICEBERG_ASSIGN_OR_RAISE(auto file_io, MakeHiveFileIO(config));
  return std::shared_ptr<HiveCatalog>(
      new HiveCatalog(config, std::move(client_pool), std::move(file_io)));
}

std::string_view HiveCatalog::name() const { return name_; }

Status HiveCatalog::CreateNamespace(
    const Namespace& ns, const std::unordered_map<std::string, std::string>& properties) {
  ICEBERG_ASSIGN_OR_RAISE(auto database, ConvertToHiveDatabase(ns, properties));
  return client_pool_->Run(
      [&](HmsClient* client) { return client->CreateDatabase(database); });
}

Result<std::vector<Namespace>> HiveCatalog::ListNamespaces(const Namespace& ns) const {
  // HMS is flat: it has no notion of nested namespaces, so any non-empty
  // parent either matches an existing database (in which case it has no
  // children) or does not exist. Java's HiveCatalog throws
  // `NoSuchNamespaceException` in the latter case and an empty list in
  // the former; mirror that contract instead of swallowing both into
  // an indistinguishable empty list. Run the existence check + listing
  // on a single pooled client to keep the read consistent.
  return client_pool_->Run([&](HmsClient* client) -> Result<std::vector<Namespace>> {
    if (!ns.levels.empty()) {
      ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(ns));
      auto database = client->GetDatabase(ns.levels[0]);
      if (!database.has_value()) {
        if (database.error().kind == ErrorKind::kNoSuchNamespace) {
          return NoSuchNamespace("Hive namespace {} does not exist.", ns.levels[0]);
        }
        return std::unexpected(database.error());
      }
      return std::vector<Namespace>{};
    }
    ICEBERG_ASSIGN_OR_RAISE(auto names, client->GetAllDatabases());
    std::vector<Namespace> namespaces;
    namespaces.reserve(names.size());
    for (auto& db_name : names) {
      namespaces.push_back(Namespace{.levels = {std::move(db_name)}});
    }
    return namespaces;
  });
}

Result<std::unordered_map<std::string, std::string>> HiveCatalog::GetNamespaceProperties(
    const Namespace& ns) const {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(ns));
  return client_pool_->Run(
      [&](HmsClient* client) -> Result<std::unordered_map<std::string, std::string>> {
        ICEBERG_ASSIGN_OR_RAISE(auto database, client->GetDatabase(ns.levels[0]));
        return ConvertFromHiveDatabase(database).properties;
      });
}

Status HiveCatalog::DropNamespace(const Namespace& ns) {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(ns));
  return client_pool_->Run([&](HmsClient* client) {
    return client->DropDatabase(ns.levels[0], /*cascade=*/false);
  });
}

Result<bool> HiveCatalog::NamespaceExists(const Namespace& ns) const {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(ns));
  return client_pool_->Run([&](HmsClient* client) -> Result<bool> {
    auto database = client->GetDatabase(ns.levels[0]);
    if (database.has_value()) {
      return true;
    }
    if (database.error().kind == ErrorKind::kNoSuchNamespace) {
      return false;
    }
    return std::unexpected(database.error());
  });
}

Status HiveCatalog::UpdateNamespaceProperties(
    const Namespace& ns, const std::unordered_map<std::string, std::string>& updates,
    const std::unordered_set<std::string>& removals) {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(ns));
  // Read-modify-write on the same pooled client so a concurrent writer
  // cannot squeeze in between the GetDatabase and AlterDatabase calls
  // observed via different sockets.
  return client_pool_->Run([&](HmsClient* client) -> Status {
    ICEBERG_ASSIGN_OR_RAISE(auto database, client->GetDatabase(ns.levels[0]));
    auto namespace_view = ConvertFromHiveDatabase(database);
    auto& properties = namespace_view.properties;
    for (const auto& key : removals) {
      properties.erase(key);
    }
    for (const auto& [key, value] : updates) {
      properties[key] = value;
    }
    ICEBERG_ASSIGN_OR_RAISE(auto altered, ConvertToHiveDatabase(ns, properties));
    return client->AlterDatabase(ns.levels[0], altered);
  });
}

Result<std::vector<TableIdentifier>> HiveCatalog::ListTables(const Namespace& ns) const {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(ns));
  // Match Java HiveCatalog: HMS GetAllTables returns every table in the
  // database regardless of kind. Filter to only the rows that carry the
  // Iceberg marker, so the identifiers returned here are safely usable by
  // LoadTable / DropTable / RenameTable (all of which now reject non-Iceberg
  // rows). The probes run on the same pooled client to avoid serialising
  // the listing across multiple HMS connections.
  return client_pool_->Run(
      [&](HmsClient* client) -> Result<std::vector<TableIdentifier>> {
        ICEBERG_ASSIGN_OR_RAISE(auto names, client->GetAllTables(ns.levels[0]));
        std::vector<TableIdentifier> identifiers;
        identifiers.reserve(names.size());
        for (auto& table_name : names) {
          TableIdentifier ident{.ns = ns, .name = std::move(table_name)};
          auto table = client->GetTable(ident.ns.levels[0], ident.name);
          if (!table.has_value()) {
            if (table.error().kind == ErrorKind::kNoSuchTable) continue;
            return std::unexpected(table.error());
          }
          if (!ValidateIcebergTable(ident, table->parameters).has_value()) {
            continue;
          }
          identifiers.push_back(std::move(ident));
        }
        return identifiers;
      });
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

  // CheckCommitStatus-style recovery for CreateTable: when HMS reports a
  // failure we cannot blindly delete `metadata_location`, because the
  // pool may have retried after a lost Thrift response on a server-side
  // success. The retry would then observe `AlreadyExistsException`
  // (`kAlreadyExists`) -- which is indistinguishable at the wire level
  // from "someone else created a table with this name". Disambiguate by
  // re-reading HMS and comparing `metadata_location`:
  //   * `kAlreadyExists` AND HMS row points at our file -> our own row
  //     recovered after a lost reply; treat as success.
  //   * `kAlreadyExists` AND HMS row points elsewhere -> genuine name
  //     collision with a foreign table; cleanup our orphan file.
  //   * Any other failure with a definitive error kind (e.g. invalid
  //     args, namespace missing) -> cleanup.
  //   * `kCommitStateUnknown` / `kServiceUnavailable` -> outcome
  //     genuinely indeterminate; leave the file in place so we do not
  //     orphan a possibly-live HMS table.
  auto create_status =
      client_pool_->Run([&](HmsClient* client) -> Result<bool /*recovered*/> {
        auto create = client->CreateTable(hive_table);
        if (create.has_value()) {
          return false;
        }
        if (create.error().kind == ErrorKind::kAlreadyExists) {
          auto existing = client->GetTable(identifier.ns.levels[0], identifier.name);
          if (existing.has_value()) {
            auto loc = GetMetadataLocation(existing->parameters);
            if (loc.has_value() && *loc == metadata_location) {
              return true;  // our row, response was lost on first try
            }
          }
        }
        return std::unexpected(create.error());
      });

  if (!create_status.has_value()) {
    const auto kind = create_status.error().kind;
    if (kind != ErrorKind::kCommitStateUnknown &&
        kind != ErrorKind::kServiceUnavailable) {
      (void)file_io_->DeleteFile(metadata_location);
    }
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

  // Hold one pooled client for the entire Refresh -> Commit chain so the
  // base snapshot, the CAS re-read, the AlterTable and (when enabled) the
  // lock / unlock RPCs all observe the same TCP connection. Without this
  // they could race against each other via separate sockets.
  const bool lock_enabled = config_.Get(HiveCatalogProperties::kLockEnabled);
  const HmsLockOptions lock_options{
      .check_min_wait_ms = config_.Get(HiveCatalogProperties::kLockCheckMinWaitMs),
      .check_max_wait_ms = config_.Get(HiveCatalogProperties::kLockCheckMaxWaitMs),
      .acquire_timeout_ms = config_.Get(HiveCatalogProperties::kLockAcquireTimeoutMs),
      .heartbeat_interval_ms =
          config_.Get(HiveCatalogProperties::kLockHeartbeatIntervalMs),
  };
  return client_pool_->Run([&](HmsClient* client) -> Result<std::shared_ptr<Table>> {
    HiveTableOperations ops(
        client, file_io_, identifier, lock_enabled, lock_options,
        lock_enabled ? std::optional<HiveCatalogProperties>{config_} : std::nullopt);
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

    ICEBERG_ASSIGN_OR_RAISE(auto new_metadata_location, ops.Commit(base, *new_metadata));
    // Java BaseMetastoreCatalog returns the in-memory metadata directly
    // here instead of re-reading from HMS. Mirror that to avoid a second
    // GetTable + ReadFile per successful UpdateTable.
    return Table::Make(identifier, std::shared_ptr<TableMetadata>(new_metadata.release()),
                       std::move(new_metadata_location), file_io_, shared_from_this());
  });
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
  // Treat non-Iceberg HMS tables as if they do not exist from the catalog's
  // perspective; otherwise a TableExists=true would mislead callers into
  // attempting LoadTable / DropTable / RenameTable, all of which now reject
  // non-Iceberg rows with kNoSuchTable.
  return client_pool_->Run([&](HmsClient* client) -> Result<bool> {
    auto table = client->GetTable(identifier.ns.levels[0], identifier.name);
    if (!table.has_value()) {
      if (table.error().kind == ErrorKind::kNoSuchTable) {
        return false;
      }
      return std::unexpected(table.error());
    }
    auto iceberg = ValidateIcebergTable(identifier, table->parameters);
    if (!iceberg.has_value()) {
      if (iceberg.error().kind == ErrorKind::kNoSuchTable) {
        return false;
      }
      return std::unexpected(iceberg.error());
    }
    return true;
  });
}

Status HiveCatalog::DropTable(const TableIdentifier& identifier, bool purge) {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(identifier.ns));
  ICEBERG_RETURN_UNEXPECTED(identifier.Validate());
  // The full data-file purge requires walking the manifest tree; iceberg
  // currently only deletes the HMS row + leaves data lifecycle to the
  // expire-snapshots action. Surface a clear error rather than silently
  // dropping the table while leaving data behind.
  if (purge) {
    return NotImplemented(
        "HiveCatalog::DropTable(purge=true) is not yet supported; call "
        "DropTable(purge=false) and use ExpireSnapshots to clean data.");
  }
  // Refuse to drop a row that does not carry the Iceberg marker; the user's
  // identifier may collide with a native Hive table that the catalog should
  // not be allowed to delete. Read-then-delete on the same pooled client to
  // close the obvious TOCTOU window.
  return client_pool_->Run([&](HmsClient* client) -> Status {
    ICEBERG_ASSIGN_OR_RAISE(auto table,
                            client->GetTable(identifier.ns.levels[0], identifier.name));
    ICEBERG_RETURN_UNEXPECTED(ValidateIcebergTable(identifier, table.parameters));
    return client->DropTable(identifier.ns.levels[0], identifier.name,
                             /*delete_data=*/false);
  });
}

Status HiveCatalog::RenameTable(const TableIdentifier& from, const TableIdentifier& to) {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(from.ns));
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(to.ns));
  ICEBERG_RETURN_UNEXPECTED(from.Validate());
  ICEBERG_RETURN_UNEXPECTED(to.Validate());

  return client_pool_->Run([&](HmsClient* client) -> Status {
    ICEBERG_ASSIGN_OR_RAISE(auto table, client->GetTable(from.ns.levels[0], from.name));
    ICEBERG_RETURN_UNEXPECTED(ValidateIcebergTable(from, table.parameters));
    table.db_name = to.ns.levels[0];
    table.table_name = to.name;
    return client->AlterTable(from.ns.levels[0], from.name, table);
  });
}

Result<std::shared_ptr<Table>> HiveCatalog::LoadTable(const TableIdentifier& identifier) {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(identifier.ns));
  ICEBERG_RETURN_UNEXPECTED(identifier.Validate());

  ICEBERG_ASSIGN_OR_RAISE(auto hive_table, client_pool_->Run([&](HmsClient* client) {
    return client->GetTable(identifier.ns.levels[0], identifier.name);
  }));
  ICEBERG_RETURN_UNEXPECTED(ValidateIcebergTable(identifier, hive_table.parameters));
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
  ICEBERG_RETURN_UNEXPECTED(client_pool_->Run(
      [&](HmsClient* client) { return client->CreateTable(hive_table); }));

  return Table::Make(identifier, std::shared_ptr<TableMetadata>(metadata.release()),
                     metadata_file_location, file_io_, shared_from_this());
}

}  // namespace iceberg::hive
